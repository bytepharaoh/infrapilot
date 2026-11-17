// Package app wires the check-runner service and its background components.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	"github.com/bytepharaoh/infrapilot/pkg/app"
	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/grpcclient"
	"github.com/bytepharaoh/infrapilot/pkg/grpcserver"
	"github.com/bytepharaoh/infrapilot/pkg/health"
	"github.com/bytepharaoh/infrapilot/pkg/httpserver"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/pkg/ssrf"
	"github.com/bytepharaoh/infrapilot/pkg/telemetry"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/probe"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/projection"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store"
	checkgrpc "github.com/bytepharaoh/infrapilot/services/check-runner/internal/transport/grpc"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/worker"
)

// Options contains the resolved runtime dependencies and bounds.
type Options struct {
	Common            config.Common
	ControlPlane      config.GRPCClient
	Kafka             kafka.Config
	DatabaseURL       string
	BatchDatabaseURL  string
	DBMaxConns        int32
	Workers           int32
	TenantConcurrency int32
	EdgeIdentities    []string
	DenyCIDRs         []string
	Logger            *slog.Logger
}

// Serve supervises check scheduling, projection, outbox, and RPC intake in dependency order.
func Serve(ctx context.Context, o Options) error {
	if o.DatabaseURL == "" || o.BatchDatabaseURL == "" || len(o.EdgeIdentities) == 0 || o.Common.GRPC.Disabled {
		return errors.New("check runner requires database roles, edge identities and gRPC")
	}
	if (o.Common.Environment == config.EnvStaging || o.Common.Environment == config.EnvProd) && o.Kafka.CertFile == "" {
		return errors.New("check runner requires Kafka mTLS in staging and production")
	}
	policy, err := ssrf.New(o.DenyCIDRs, nil)
	if err != nil {
		return err
	}
	c, logger := o.Common, o.Logger
	tel, err := telemetry.Setup(ctx, telemetry.Config{
		OTLPEndpoint: c.Telemetry.ExporterOTLPEndpoint, OTLPInsecure: c.Telemetry.ExporterOTLPInsecure,
		TraceSampleRatio: c.Telemetry.TracesSamplerArg, MetricsPush: c.Telemetry.MetricsPush, LogsPush: c.Telemetry.LogsPush,
		ExportInterval: c.Telemetry.ExportInterval, ExportTimeout: c.Telemetry.ExportTimeout,
	}, telemetry.Service{Name: c.ServiceName, Version: c.Version, Environment: string(c.Environment)}, logger)
	if err != nil {
		return err
	}
	ready := health.NewRegistry(health.WithLogger(logger), health.WithMeter(tel.Meter("health")))
	gate := &readiness{registry: ready}
	run, err := app.New(app.Config{ShutdownDelay: c.Shutdown.Delay, ShutdownTimeout: c.Shutdown.Timeout}, app.WithLogger(logger), app.WithReadinessGate(gate))
	if err != nil {
		return errors.Join(err, tel.Shutdown(ctx))
	}
	cleanup := []func(context.Context) error{tel.Shutdown}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for i := len(cleanup) - 1; i >= 0; i-- {
			_ = cleanup[i](closeCtx)
		}
	}()
	run.Add(app.Closer("telemetry", tel.Shutdown))
	open := func(ctx context.Context, url, name string) (*postgres.Pool, error) {
		return postgres.Open(ctx, postgres.Config{URL: url, ApplicationName: name, MaxConns: o.DBMaxConns},
			postgres.WithLogger(logger), postgres.WithTracerProvider(tel.TracerProvider()), postgres.WithMeterProvider(tel.MeterProvider()))
	}
	apiPool, err := open(ctx, o.DatabaseURL, "check-runner-api")
	if err != nil {
		return err
	}
	cleanup = append(cleanup, apiPool.Shutdown)
	run.Add(app.Closer("postgres-api", apiPool.Shutdown))
	batchPool, err := open(ctx, o.BatchDatabaseURL, "check-runner-leases")
	if err != nil {
		return err
	}
	cleanup = append(cleanup, batchPool.Shutdown)
	run.Add(app.Closer("postgres-batch", batchPool.Shutdown))
	ready.Register("postgres-api", apiPool.HealthCheck(), health.Periodic(5*time.Second))
	ready.Register("postgres-batch", batchPool.HealthCheck(), health.Periodic(5*time.Second))
	checks, err := store.New(apiPool, batchPool)
	if err != nil {
		return err
	}
	cp, err := grpcclient.New(o.ControlPlane, c.Environment, grpcclient.WithLogger(logger), grpcclient.WithUpstream("control-plane"),
		grpcclient.WithTracerProvider(tel.TracerProvider()), grpcclient.WithMeterProvider(tel.MeterProvider()),
		grpcclient.WithIdempotent(cpv1.AuthorizationService_CheckPermission_FullMethodName, cpv1.ResourceService_GetResource_FullMethodName))
	if err != nil {
		return err
	}
	closeCP := func(context.Context) error { return cp.Close() }
	cleanup = append(cleanup, closeCP)
	run.Add(app.Closer("control-plane", closeCP))
	client, err := kafka.NewClient(o.Kafka)
	if err != nil {
		return err
	}
	closeKafka := func(context.Context) error { client.Close(); return nil }
	cleanup = append(cleanup, closeKafka)
	run.Add(app.Closer("kafka", closeKafka))
	meter := tel.Meter("check-runner")
	if err := registerOutbox(checks, meter); err != nil {
		return err
	}
	scheduler, err := worker.New(checks, probe.New(policy), o.Workers, o.TenantConcurrency, logger, tel.Tracer("check-runner"), meter)
	if err != nil {
		return err
	}
	projector := projection.New(checks, o.Kafka, client, logger)
	publisher := worker.Publisher{Store: checks, Client: client, Logger: logger}
	run.Add(app.Func("health", ready.Run, nil), app.Func("resource-projection", projector.Run, nil),
		app.Func("result-outbox", publisher.Run, nil), app.Func("retention", retention(checks, logger), nil), scheduler)
	grpc, err := grpcserver.New(ctx, grpcserver.Config{
		Addr: c.GRPC.Addr, MaxRecvMsgBytes: c.GRPC.MaxRecvMsgBytes, ConnectionTimeout: c.GRPC.ConnectionTimeout,
		KeepaliveTime: c.GRPC.KeepaliveTime, KeepaliveTimeout: c.GRPC.KeepaliveTimeout, MaxConnectionAge: c.GRPC.MaxConnectionAge,
		MaxConnectionAgeGrace: c.GRPC.MaxConnectionAgeGrace, EnableReflection: c.GRPC.EnableReflection,
		MaxConcurrentStreams: c.GRPC.MaxConcurrentStreams,
		TLSCertFile:          c.GRPC.TLSCertFile, TLSKeyFile: c.GRPC.TLSKeyFile, TLSClientCAFile: c.GRPC.TLSClientCAFile,
	}, grpcserver.WithLogger(logger), grpcserver.WithTracerProvider(tel.TracerProvider()), grpcserver.WithMeterProvider(tel.MeterProvider()),
		grpcserver.WithPeerPolicy(grpcserver.PeerPolicy{
			Rules: map[string][]string{"/infrapilot.checks.v1.CheckService/*": o.EdgeIdentities}, InsecureIdentity: c.GRPC.InsecurePeerIdentity,
		}))
	if err != nil {
		return err
	}
	gate.server = grpc
	cleanup = append(cleanup, grpc.Shutdown)
	checksv1.RegisterCheckServiceServer(grpc.Registrar(), checkgrpc.New(checks, cp, logger))
	admin, err := httpserver.New(ctx, httpserver.Config{Addr: c.Admin.Addr, ReadHeaderTimeout: c.HTTP.ReadHeaderTimeout,
		ReadTimeout: c.HTTP.ReadTimeout, WriteTimeout: c.Admin.WriteTimeout, IdleTimeout: c.HTTP.IdleTimeout,
		MaxHeaderBytes: c.HTTP.MaxHeaderBytes}, httpserver.AdminHandler(ready.LivenessHandler(), ready.ReadinessHandler(), tel.MetricsHandler(), c.Admin.Pprof),
		httpserver.WithName("admin"), httpserver.WithLogger(logger))
	if err != nil {
		return err
	}
	run.Add(admin, grpc)
	cleanup = nil
	return run.Run(ctx)
}

type readiness struct {
	registry *health.Registry
	server   *grpcserver.Server
}

func (r *readiness) SetReady(ready bool) {
	r.registry.SetReady(ready)
	r.server.SetServing(ready)
}

func registerOutbox(s *store.Store, meter metric.Meter) error {
	pending, err := meter.Int64ObservableGauge("checkrunner_outbox_pending")
	if err != nil {
		return err
	}
	age, err := meter.Float64ObservableGauge("checkrunner_outbox_oldest_seconds", metric.WithUnit("s"))
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rows, err := s.PendingResults(ctx)
		if err != nil {
			return fmt.Errorf("observe check outbox: %w", err)
		}
		observer.ObserveInt64(pending, rows.Pending)
		observer.ObserveFloat64(age, rows.OldestSeconds)
		return nil
	}, pending, age)
	return err
}

func retention(s *store.Store, logger *slog.Logger) func(context.Context) error {
	return func(ctx context.Context) error {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for ctx.Err() == nil {
			if err := s.Prune(ctx); err != nil && ctx.Err() == nil {
				logger.WarnContext(ctx, "check-runner retention failed", slog.Any("error", err))
			}
			select {
			case <-ctx.Done():
			case <-ticker.C:
			}
		}
		return nil
	}
}
