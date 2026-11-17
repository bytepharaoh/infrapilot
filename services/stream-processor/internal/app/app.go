// Package app wires the stream processor's database, Kafka group, and read RPC.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	"github.com/bytepharaoh/infrapilot/pkg/app"
	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/grpcserver"
	"github.com/bytepharaoh/infrapilot/pkg/health"
	"github.com/bytepharaoh/infrapilot/pkg/httpserver"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/pkg/telemetry"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store"
	streamgrpc "github.com/bytepharaoh/infrapilot/services/stream-processor/internal/transport/grpc"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/transport/kafkaconsumer"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/migrations"
)

type Config struct {
	config.Common
	DatabaseURL          string   `env:"DATABASE_URL" envDefault:""`
	ReadDatabaseURL      string   `env:"READ_DATABASE_URL" envDefault:""`
	MigrationDatabaseURL string   `env:"MIGRATION_DATABASE_URL" envDefault:""`
	BatchDatabaseURL     string   `env:"BATCH_DATABASE_URL" envDefault:""`
	DBMaxConns           int32    `env:"DB_MAX_CONNS" envDefault:"6"`
	ReadDBMaxConns       int32    `env:"READ_DB_MAX_CONNS" envDefault:"6"`
	KafkaBrokers         []string `env:"KAFKA_BROKERS" envSeparator:"," envDefault:""`
	KafkaCAFile          string   `env:"KAFKA_CA_FILE" envDefault:""`
	KafkaCertFile        string   `env:"KAFKA_CERT_FILE" envDefault:""`
	KafkaKeyFile         string   `env:"KAFKA_KEY_FILE" envDefault:""`
	KafkaConsumerGroup   string   `env:"KAFKA_CONSUMER_GROUP" envDefault:"stream-processor-v1"`
	QueryIdentities      []string `env:"QUERY_IDENTITIES" envSeparator:"," envDefault:""`
}

func (c *Config) Validate() error {
	if err := c.Common.Validate(); err != nil {
		return err
	}
	if c.DBMaxConns < 2 || c.DBMaxConns > 32 || c.ReadDBMaxConns < 1 || c.ReadDBMaxConns > 32 {
		return errors.New("invalid stream database pool size")
	}
	return nil
}

func Run(ctx context.Context, cfg Config, mode string) error {
	logger, err := newLogger(cfg, nil)
	if err != nil {
		return err
	}
	switch mode {
	case "serve":
		return serve(ctx, cfg, logger)
	case "migrate":
		if cfg.MigrationDatabaseURL == "" {
			return errors.New("MIGRATION_DATABASE_URL is required")
		}
		bounded, cancel := context.WithTimeout(ctx, 9*time.Minute)
		defer cancel()
		return migrations.Migrate(bounded, cfg.MigrationDatabaseURL, logger)
	case "purge":
		bounded, cancel := context.WithTimeout(ctx, 50*time.Minute)
		defer cancel()
		return purge(bounded, cfg.BatchDatabaseURL, logger)
	default:
		return fmt.Errorf("unknown stream processor mode %q", mode)
	}
}

func newLogger(cfg Config, otlp slog.Handler) (*slog.Logger, error) {
	return log.New(log.Config{Level: cfg.Log.Level, Format: cfg.Log.Format, AddSource: cfg.Log.AddSource},
		os.Stdout, cfg.ServiceName, cfg.Version, string(cfg.Environment), otlp)
}

func purge(ctx context.Context, url string, logger *slog.Logger) error {
	if url == "" {
		return errors.New("BATCH_DATABASE_URL is required")
	}
	pool, err := postgres.Open(ctx, postgres.Config{URL: url, ApplicationName: "stream-processor-purge", MaxConns: 1},
		postgres.WithLogger(logger))
	if err != nil {
		return fmt.Errorf("open stream retention pool: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = pool.Shutdown(cleanup)
	}()
	return store.Purge(ctx, pool, logger)
}

type gates struct {
	reg *health.Registry
	rpc *grpcserver.Server
}

func (g gates) SetReady(ready bool) { g.reg.SetReady(ready); g.rpc.SetServing(ready) }

func serve(ctx context.Context, cfg Config, logger *slog.Logger) (err error) {
	if cfg.DatabaseURL == "" || cfg.ReadDatabaseURL == "" || cfg.DatabaseURL == cfg.ReadDatabaseURL {
		return errors.New("DATABASE_URL and distinct READ_DATABASE_URL are required")
	}
	if len(cfg.QueryIdentities) == 0 || cfg.GRPC.Disabled || strings.TrimSpace(cfg.KafkaConsumerGroup) == "" {
		return errors.New("QUERY_IDENTITIES, internal gRPC and KAFKA_CONSUMER_GROUP are required")
	}
	if (cfg.Environment == config.EnvProd || cfg.Environment == config.EnvStaging) && cfg.KafkaCertFile == "" {
		return errors.New("Kafka mTLS is required for production and staging")
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
		TraceSampleRatio: cfg.Telemetry.TracesSamplerArg, MetricsPush: cfg.Telemetry.MetricsPush, LogsPush: cfg.Telemetry.LogsPush,
		ExportInterval: cfg.Telemetry.ExportInterval, ExportTimeout: cfg.Telemetry.ExportTimeout,
	}, telemetry.Service{Name: cfg.ServiceName, Version: cfg.Version, Environment: string(cfg.Environment)}, logger)
	if err != nil {
		return err
	}
	cleanup = append(cleanup, tel.Shutdown)
	if bridge := tel.LogHandler(); bridge != nil {
		logger, err = newLogger(cfg, bridge)
		if err != nil {
			return err
		}
	}
	open := func(url, name string, maxConns int32) (*postgres.Pool, error) {
		pool, openErr := postgres.Open(ctx, postgres.Config{URL: url, ApplicationName: name, MaxConns: maxConns},
			postgres.WithLogger(logger), postgres.WithTracerProvider(tel.TracerProvider()), postgres.WithMeterProvider(tel.MeterProvider()))
		if openErr == nil {
			cleanup = append(cleanup, pool.Shutdown)
		}
		return pool, openErr
	}
	writer, err := open(cfg.DatabaseURL, "stream-processor-writer", cfg.DBMaxConns)
	if err != nil {
		return fmt.Errorf("open stream write pool: %w", err)
	}
	reader, err := open(cfg.ReadDatabaseURL, "stream-processor-reader", cfg.ReadDBMaxConns)
	if err != nil {
		return fmt.Errorf("open stream read pool: %w", err)
	}
	if err = ensureReaderRole(ctx, reader); err != nil {
		return err
	}

	kafkaCfg := kafka.Config{Brokers: cfg.KafkaBrokers, ClientID: cfg.ServiceName,
		CAFile: cfg.KafkaCAFile, CertFile: cfg.KafkaCertFile, KeyFile: cfg.KafkaKeyFile}
	producer, err := kafka.NewClient(kafkaCfg)
	if err != nil {
		return err
	}
	closeKafka := func(context.Context) error { producer.Close(); return nil }
	cleanup = append(cleanup, closeKafka)
	meter := tel.Meter("stream")
	consumer, err := kafkaconsumer.New(kafkaCfg, cfg.KafkaConsumerGroup, producer, store.Writer{Pool: writer},
		meter, tel.TracerProvider().Tracer("stream-kafka"), logger)
	if err != nil {
		return err
	}
	signals, err := kafkaconsumer.NewSignals(writer, producer, logger, meter)
	if err != nil {
		return err
	}

	reg := health.NewRegistry(health.WithLogger(logger), health.WithMeter(tel.Meter("health")))
	reg.Register("timescale-writer", writer.HealthCheck(), health.Periodic(5*time.Second))
	reg.Register("timescale-reader", reader.HealthCheck(), health.Periodic(5*time.Second))
	grpcCfg := grpcserver.Config{
		Addr: cfg.GRPC.Addr, MaxRecvMsgBytes: cfg.GRPC.MaxRecvMsgBytes, ConnectionTimeout: cfg.GRPC.ConnectionTimeout,
		KeepaliveTime: cfg.GRPC.KeepaliveTime, KeepaliveTimeout: cfg.GRPC.KeepaliveTimeout, MaxConnectionAge: cfg.GRPC.MaxConnectionAge,
		MaxConnectionAgeGrace: cfg.GRPC.MaxConnectionAgeGrace, MaxConcurrentStreams: cfg.GRPC.MaxConcurrentStreams,
		TLSCertFile: cfg.GRPC.TLSCertFile, TLSKeyFile: cfg.GRPC.TLSKeyFile, TLSClientCAFile: cfg.GRPC.TLSClientCAFile,
		EnableReflection: cfg.GRPC.EnableReflection,
	}
	rpc, err := grpcserver.New(ctx, grpcCfg, grpcserver.WithLogger(logger),
		grpcserver.WithMeterProvider(tel.MeterProvider()), grpcserver.WithTracerProvider(tel.TracerProvider()),
		grpcserver.WithPeerPolicy(grpcserver.PeerPolicy{
			Rules:            map[string][]string{"/infrapilot.stream.v1.StreamReadService/*": cfg.QueryIdentities},
			InsecureIdentity: cfg.GRPC.InsecurePeerIdentity,
		}))
	if err != nil {
		return err
	}
	cleanup = append(cleanup, rpc.Shutdown)
	streamv1.RegisterStreamReadServiceServer(rpc.Registrar(), &streamgrpc.Server{Reader: store.Reader{Pool: reader}, Logger: logger})

	admin, err := httpserver.New(ctx, httpserver.Config{
		Addr: cfg.Admin.Addr, ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout, ReadTimeout: cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.Admin.WriteTimeout, IdleTimeout: cfg.HTTP.IdleTimeout, MaxHeaderBytes: cfg.HTTP.MaxHeaderBytes,
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
	runtime.Add(app.Closer("telemetry", tel.Shutdown), app.Closer("timescale-writer", writer.Shutdown),
		app.Closer("timescale-reader", reader.Shutdown), app.Closer("kafka-producer", closeKafka),
		app.Func("health", reg.Run, nil), signals, consumer, admin, rpc)
	cleanup = nil
	return runtime.Run(ctx)
}
