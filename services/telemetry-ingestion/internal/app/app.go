// Package app wires the telemetry ingestion listeners and their dependencies.
package app

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	"github.com/bytepharaoh/infrapilot/pkg/app"
	"github.com/bytepharaoh/infrapilot/pkg/grpcclient"
	"github.com/bytepharaoh/infrapilot/pkg/grpcserver"
	"github.com/bytepharaoh/infrapilot/pkg/health"
	"github.com/bytepharaoh/infrapilot/pkg/httpserver"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/pkg/telemetry"
	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/receiver"
)

// Serve runs the public OTLP listeners until shutdown. Kafka outages surface as retryable OTLP
// errors; keeping healthy replicas reachable preserves the explicit backpressure signal.
func Serve(ctx context.Context, cfg *Config, logCfg log.Config, logger *slog.Logger) (err error) {
	c := cfg.Common
	var cleanup []func(context.Context) error
	defer func() {
		closeCtx, closeDone := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer closeDone()
		for i := len(cleanup) - 1; i >= 0; i-- {
			_ = cleanup[i](closeCtx)
		}
	}()

	tel, err := telemetry.Setup(ctx, telemetry.Config{
		OTLPEndpoint: c.Telemetry.ExporterOTLPEndpoint, OTLPInsecure: c.Telemetry.ExporterOTLPInsecure,
		TraceSampleRatio: c.Telemetry.TracesSamplerArg, MetricsPush: c.Telemetry.MetricsPush, LogsPush: c.Telemetry.LogsPush,
		ExportInterval: c.Telemetry.ExportInterval, ExportTimeout: c.Telemetry.ExportTimeout,
	}, telemetry.Service{Name: c.ServiceName, Version: c.Version, Environment: string(c.Environment)}, logger)
	if err != nil {
		return err
	}
	cleanup = append(cleanup, tel.Shutdown)
	if handler := tel.LogHandler(); handler != nil {
		logger, err = log.New(logCfg, os.Stdout, c.ServiceName, c.Version, string(c.Environment), handler)
		if err != nil {
			return err
		}
	}

	ropts := &redis.Options{
		Addr: cfg.Redis.Addr, Username: cfg.Redis.Username, Password: cfg.Redis.Password,
		DialTimeout: 500 * time.Millisecond, ReadTimeout: 500 * time.Millisecond, WriteTimeout: 500 * time.Millisecond,
		PoolTimeout: 500 * time.Millisecond, PoolSize: min(16, cfg.MaxInFlight), MaxActiveConns: min(16, cfg.MaxInFlight),
		MaxRetries: -1, ContextTimeoutEnabled: true,
	}
	if cfg.Redis.TLS {
		ropts.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	rdb := redis.NewClient(ropts)
	closeRedis := func(context.Context) error { return rdb.Close() }
	cleanup = append(cleanup, closeRedis)
	if instrumentErr := redisotel.InstrumentTracing(rdb, redisotel.WithTracerProvider(tel.TracerProvider())); instrumentErr != nil {
		return fmt.Errorf("configure Redis tracing: %w", instrumentErr)
	}
	cp, err := grpcclient.New(cfg.ControlPlane, c.Environment, grpcclient.WithLogger(logger), grpcclient.WithUpstream("control-plane"),
		grpcclient.WithTracerProvider(tel.TracerProvider()), grpcclient.WithMeterProvider(tel.MeterProvider()),
		grpcclient.WithIdempotent(cpv1.IngestionAuthorizationService_ValidateIngestResource_FullMethodName))
	if err != nil {
		return err
	}
	closeCP := func(context.Context) error { return cp.Close() }
	cleanup = append(cleanup, closeCP)
	producer, err := kafka.NewClient(kafka.Config{Brokers: cfg.Kafka.Brokers, ClientID: c.ServiceName,
		CAFile: cfg.Kafka.CAFile, CertFile: cfg.Kafka.CertFile, KeyFile: cfg.Kafka.KeyFile})
	if err != nil {
		return err
	}
	closeKafka := func(context.Context) error { producer.Close(); return nil }
	cleanup = append(cleanup, closeKafka)

	limits := cfg.exportLimits()
	meter := tel.Meter(c.ServiceName)
	exporter, err := receiver.New(limits, cpv1.NewIngestionAuthorizationServiceClient(cp),
		receiver.NewRedisQuota(rdb, cfg.PointsPerSec, cfg.BurstPoints), producer, meter,
		tel.TracerProvider().Tracer(c.ServiceName))
	if err != nil {
		return err
	}

	reg := health.NewRegistry(health.WithLogger(logger), health.WithMeter(tel.Meter("health")))
	httpCfg := httpserver.Config{
		Addr: c.HTTP.Addr, ReadHeaderTimeout: c.HTTP.ReadHeaderTimeout, ReadTimeout: c.HTTP.ReadTimeout,
		WriteTimeout: c.HTTP.WriteTimeout, IdleTimeout: c.HTTP.IdleTimeout, MaxHeaderBytes: min(c.HTTP.MaxHeaderBytes, 16<<10),
	}
	mw, err := httpserver.Standard(httpserver.Options{Logger: logger, TracerProvider: tel.TracerProvider(),
		MeterProvider: tel.MeterProvider(), ServerName: c.ServiceName, Edge: true})
	if err != nil {
		return err
	}
	public, err := httpserver.New(ctx, httpCfg, httpserver.Chain(exporter.HTTP(), mw...), httpserver.WithLogger(logger), httpserver.WithName("otlp-http"))
	if err != nil {
		return err
	}
	cleanup = append(cleanup, public.Shutdown)
	grpc, err := grpcserver.New(ctx, grpcserver.Config{
		Addr: c.GRPC.Addr, MaxRecvMsgBytes: limits.RequestBytes, MaxHeaderBytes: 16 << 10, ConnectionTimeout: c.GRPC.ConnectionTimeout,
		KeepaliveTime: c.GRPC.KeepaliveTime, KeepaliveTimeout: c.GRPC.KeepaliveTimeout,
		MaxConnectionAge: c.GRPC.MaxConnectionAge, MaxConnectionAgeGrace: c.GRPC.MaxConnectionAgeGrace,
		MaxConcurrentStreams: min(c.GRPC.MaxConcurrentStreams, 128), DefaultTimeout: limits.Deadline,
		TLSCertFile: c.GRPC.TLSCertFile, TLSKeyFile: c.GRPC.TLSKeyFile,
	}, grpcserver.WithLogger(logger), grpcserver.WithName("otlp-grpc"), grpcserver.WithTapHandle(exporter.Tap),
		grpcserver.WithTracerProvider(tel.TracerProvider()), grpcserver.WithMeterProvider(tel.MeterProvider()))
	if err != nil {
		return err
	}
	cleanup = append(cleanup, grpc.Shutdown)
	exporter.RegisterGRPC(grpc.Registrar())

	adminCfg := httpCfg
	adminCfg.Addr, adminCfg.WriteTimeout = c.Admin.Addr, c.Admin.WriteTimeout
	admin, err := httpserver.New(ctx, adminCfg,
		httpserver.AdminHandler(reg.LivenessHandler(), reg.ReadinessHandler(), tel.MetricsHandler(), c.Admin.Pprof),
		httpserver.WithName("admin"), httpserver.WithLogger(logger))
	if err != nil {
		return err
	}
	cleanup = append(cleanup, admin.Shutdown)

	gate := readiness{registry: reg, grpc: grpc}
	process, err := app.New(app.Config{ShutdownDelay: c.Shutdown.Delay, ShutdownTimeout: c.Shutdown.Timeout},
		app.WithLogger(logger), app.WithReadinessGate(gate))
	if err != nil {
		return err
	}
	process.Add(app.Closer("telemetry", tel.Shutdown), app.Closer("redis", closeRedis), app.Closer("control-plane", closeCP),
		app.Closer("kafka", closeKafka), app.Func("health", reg.Run, nil), admin, grpc, public)
	cleanup = nil
	return process.Run(ctx)
}

type readiness struct {
	registry *health.Registry
	grpc     *grpcserver.Server
}

func (r readiness) SetReady(ready bool) {
	r.registry.SetReady(ready)
	r.grpc.SetServing(ready)
}
