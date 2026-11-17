// Command check-runner owns tenant check definitions and runs scheduled probes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/app"
	"github.com/bytepharaoh/infrapilot/services/check-runner/migrations"
)

type settings struct {
	config.Common
	ControlPlane      config.GRPCClient `envPrefix:"CONTROL_PLANE_"`
	DatabaseURL       string            `env:"DATABASE_URL"`
	BatchDatabaseURL  string            `env:"BATCH_DATABASE_URL"`
	MigrationURL      string            `env:"MIGRATION_DATABASE_URL"`
	MigrateTimeout    time.Duration     `env:"MIGRATE_TIMEOUT" envDefault:"9m"`
	DBMaxConns        int32             `env:"DB_MAX_CONNS" envDefault:"10"`
	Workers           int32             `env:"WORKERS" envDefault:"32"`
	TenantConcurrency int32             `env:"TENANT_CONCURRENCY" envDefault:"8"`
	EdgeIdentities    []string          `env:"EDGE_IDENTITIES" envSeparator:","`
	DenyCIDRs         []string          `env:"PROBE_DENY_CIDRS" envSeparator:","`
	Brokers           []string          `env:"KAFKA_BROKERS" envSeparator:","`
	KafkaCA           string            `env:"KAFKA_CA_FILE"`
	KafkaCert         string            `env:"KAFKA_CERT_FILE"`
	KafkaKey          string            `env:"KAFKA_KEY_FILE"`
}

func (c *settings) Validate() error {
	if c.MigrateTimeout <= 0 || c.DBMaxConns < 2 || c.DBMaxConns > 100 || c.Workers < 1 || c.Workers > 256 ||
		c.TenantConcurrency < 1 || c.TenantConcurrency > 256 {
		return errors.New("migration timeout, pool size or worker bounds are invalid")
	}
	return c.Common.Validate()
}

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		log.Fallback(os.Stderr).Error("check-runner exited with error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := config.Load[settings]()
	if err != nil {
		return err
	}
	logger, err := log.New(log.Config{Level: cfg.Log.Level, Format: cfg.Log.Format, AddSource: cfg.Log.AddSource},
		os.Stdout, cfg.ServiceName, cfg.Version, string(cfg.Environment), nil)
	if err != nil {
		return err
	}
	mode := "serve"
	if len(args) > 0 {
		mode = args[0]
	}
	switch mode {
	case "migrate":
		if cfg.MigrationURL == "" {
			return errors.New("MIGRATION_DATABASE_URL is required")
		}
		bounded, cancel := context.WithTimeout(ctx, cfg.MigrateTimeout)
		defer cancel()
		return migrations.Migrate(bounded, cfg.MigrationURL, logger)
	case "serve":
		return app.Serve(ctx, app.Options{Common: cfg.Common, ControlPlane: cfg.ControlPlane, DatabaseURL: cfg.DatabaseURL,
			BatchDatabaseURL: cfg.BatchDatabaseURL, DBMaxConns: cfg.DBMaxConns, Workers: cfg.Workers, TenantConcurrency: cfg.TenantConcurrency,
			EdgeIdentities: cfg.EdgeIdentities, DenyCIDRs: cfg.DenyCIDRs, Logger: logger,
			Kafka: kafka.Config{Brokers: cfg.Brokers, ClientID: "check-runner", CAFile: cfg.KafkaCA, CertFile: cfg.KafkaCert, KeyFile: cfg.KafkaKey}})
	default:
		return fmt.Errorf("unknown check-runner command %q", mode)
	}
}
