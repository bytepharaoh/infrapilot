// Package app wires the query-service authorization and stream read boundary.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	"github.com/bytepharaoh/infrapilot/pkg/app"
	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/grpcclient"
	"github.com/bytepharaoh/infrapilot/pkg/grpcserver"
	"github.com/bytepharaoh/infrapilot/pkg/health"
	"github.com/bytepharaoh/infrapilot/pkg/httpserver"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/pkg/telemetry"
	querygrpc "github.com/bytepharaoh/infrapilot/services/query-service/internal/transport/grpc"
)

type Config struct {
	config.Common
	ControlPlane    config.GRPCClient `envPrefix:"CONTROLPLANE_"`
	StreamProcessor config.GRPCClient `envPrefix:"STREAMPROCESSOR_"`
	EdgeIdentities  []string          `env:"EDGE_IDENTITIES" envSeparator:"," envDefault:""`
}

func (c *Config) Validate() error {
	err := []error{c.Common.Validate(), c.ControlPlane.ValidateFor(c.Environment), c.StreamProcessor.ValidateFor(c.Environment)}
	if c.GRPC.Disabled || len(c.EdgeIdentities) == 0 {
		err = append(err, errors.New("query-service requires internal gRPC and EDGE_IDENTITIES"))
	}
	for _, identity := range c.EdgeIdentities {
		if !strings.HasPrefix(identity, "spiffe://") {
			err = append(err, fmt.Errorf("invalid edge identity %q", identity))
		}
	}
	return errors.Join(err...)
}

type gates struct {
	reg *health.Registry
	rpc *grpcserver.Server
}

func (g gates) SetReady(ready bool) { g.reg.SetReady(ready); g.rpc.SetServing(ready) }

func Run(ctx context.Context, cfg Config) (err error) {
	logger, err := log.New(log.Config{Level: cfg.Log.Level, Format: cfg.Log.Format, AddSource: cfg.Log.AddSource},
		os.Stdout, cfg.ServiceName, cfg.Version, string(cfg.Environment))
	if err != nil {
		return err
	}
	var cleanup []func(context.Context) error
	defer func() {
		if err == nil {
			return
		}
		stop, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for i := len(cleanup) - 1; i >= 0; i-- {
			_ = cleanup[i](stop)
		}
	}()
	tel, err := telemetry.Setup(ctx, telemetry.Config{
		OTLPEndpoint: cfg.Telemetry.ExporterOTLPEndpoint, OTLPInsecure: cfg.Telemetry.ExporterOTLPInsecure,
		TraceSampleRatio: cfg.Telemetry.TracesSamplerArg, MetricsPush: cfg.Telemetry.MetricsPush,
		LogsPush: cfg.Telemetry.LogsPush, ExportInterval: cfg.Telemetry.ExportInterval,
		ExportTimeout: cfg.Telemetry.ExportTimeout,
	}, telemetry.Service{Name: cfg.ServiceName, Version: cfg.Version, Environment: string(cfg.Environment)}, logger)
	if err != nil {
		return err
	}
	cleanup = append(cleanup, tel.Shutdown)
	if bridge := tel.LogHandler(); bridge != nil {
		logger, err = log.New(log.Config{Level: cfg.Log.Level, Format: cfg.Log.Format, AddSource: cfg.Log.AddSource},
			os.Stdout, cfg.ServiceName, cfg.Version, string(cfg.Environment), bridge)
		if err != nil {
			return err
		}
	}
	cpConn, err := grpcclient.New(cfg.ControlPlane, cfg.Environment, grpcclient.WithLogger(logger),
		grpcclient.WithUpstream("control-plane"), grpcclient.WithTracerProvider(tel.TracerProvider()),
		grpcclient.WithMeterProvider(tel.MeterProvider()), grpcclient.WithIdempotent(cpv1.AuthorizationService_CheckPermission_FullMethodName))
	if err != nil {
		return err
	}
	closeCP := func(context.Context) error { return cpConn.Close() }
	cleanup = append(cleanup, closeCP)
	streamConn, err := grpcclient.New(cfg.StreamProcessor, cfg.Environment, grpcclient.WithLogger(logger),
		grpcclient.WithUpstream("stream-processor"), grpcclient.WithTracerProvider(tel.TracerProvider()),
		grpcclient.WithMeterProvider(tel.MeterProvider()), grpcclient.WithIdempotent(
			streamv1.StreamReadService_ListCheckResults_FullMethodName, streamv1.StreamReadService_ListMetrics_FullMethodName))
	if err != nil {
		return err
	}
	closeStream := func(context.Context) error { return streamConn.Close() }
	cleanup = append(cleanup, closeStream)
	reg := health.NewRegistry(health.WithLogger(logger), health.WithMeter(tel.Meter("health")))
	rpc, err := grpcserver.New(ctx, grpcserver.Config{
		Addr: cfg.GRPC.Addr, MaxRecvMsgBytes: cfg.GRPC.MaxRecvMsgBytes,
		ConnectionTimeout: cfg.GRPC.ConnectionTimeout, KeepaliveTime: cfg.GRPC.KeepaliveTime,
		KeepaliveTimeout: cfg.GRPC.KeepaliveTimeout, MaxConnectionAge: cfg.GRPC.MaxConnectionAge,
		MaxConnectionAgeGrace: cfg.GRPC.MaxConnectionAgeGrace, MaxConcurrentStreams: cfg.GRPC.MaxConcurrentStreams,
		TLSCertFile: cfg.GRPC.TLSCertFile, TLSKeyFile: cfg.GRPC.TLSKeyFile,
		TLSClientCAFile: cfg.GRPC.TLSClientCAFile, EnableReflection: cfg.GRPC.EnableReflection,
	}, grpcserver.WithLogger(logger), grpcserver.WithMeterProvider(tel.MeterProvider()),
		grpcserver.WithTracerProvider(tel.TracerProvider()), grpcserver.WithPeerPolicy(grpcserver.PeerPolicy{
			Rules:            map[string][]string{"/infrapilot.stream.v1.StreamReadService/*": cfg.EdgeIdentities},
			InsecureIdentity: cfg.GRPC.InsecurePeerIdentity,
		}))
	if err != nil {
		return err
	}
	cleanup = append(cleanup, rpc.Shutdown)
	streamv1.RegisterStreamReadServiceServer(rpc.Registrar(), &querygrpc.Server{
		Auth: cpv1.NewAuthorizationServiceClient(cpConn), Stream: streamv1.NewStreamReadServiceClient(streamConn),
	})
	admin, err := httpserver.New(ctx, httpserver.Config{
		Addr: cfg.Admin.Addr, ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout: cfg.HTTP.ReadTimeout, WriteTimeout: cfg.Admin.WriteTimeout,
		IdleTimeout: cfg.HTTP.IdleTimeout, MaxHeaderBytes: cfg.HTTP.MaxHeaderBytes,
	}, httpserver.AdminHandler(reg.LivenessHandler(), reg.ReadinessHandler(), tel.MetricsHandler(), cfg.Admin.Pprof),
		httpserver.WithName("admin"), httpserver.WithLogger(logger))
	if err != nil {
		return err
	}
	cleanup = append(cleanup, admin.Shutdown)
	runtime, err := app.New(app.Config{ShutdownDelay: cfg.Shutdown.Delay, ShutdownTimeout: cfg.Shutdown.Timeout},
		app.WithLogger(logger), app.WithReadinessGate(gates{reg: reg, rpc: rpc}))
	if err != nil {
		return err
	}
	runtime.Add(app.Closer("telemetry", tel.Shutdown), app.Closer("control-plane", closeCP),
		app.Closer("stream-processor", closeStream), admin, rpc)
	cleanup = nil
	return runtime.Run(ctx)
}
