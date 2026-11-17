package app

import (
	"errors"
	"time"

	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/receiver"
)

// Config declares the receiver's network dependencies, limits and listen settings.
type Config struct {
	config.Common
	ControlPlane config.GRPCClient `envPrefix:"CONTROLPLANE_"`
	Redis        Redis             `envPrefix:"REDIS_"`
	Kafka        Kafka             `envPrefix:"KAFKA_"`

	MaxBatchBytes  int           `env:"INGEST_MAX_BATCH_BYTES" envDefault:"4194304"`
	MaxPoints      int           `env:"INGEST_MAX_POINTS" envDefault:"1024"`
	MaxInFlight    int           `env:"INGEST_MAX_IN_FLIGHT" envDefault:"32"`
	ExportDeadline time.Duration `env:"INGEST_EXPORT_DEADLINE" envDefault:"10s"`
	PointsPerSec   int           `env:"INGEST_POINTS_PER_SECOND" envDefault:"2000"`
	BurstPoints    int           `env:"INGEST_BURST_POINTS" envDefault:"4000"`
}

// Redis configures the quota store.
type Redis struct {
	Addr     string `env:"ADDR"`
	Username string `env:"USERNAME" envDefault:""`
	Password string `env:"PASSWORD" envDefault:""`
	TLS      bool   `env:"TLS" envDefault:"false"`
}

// Kafka configures the metric publisher.
type Kafka struct {
	Brokers  []string `env:"BROKERS" envSeparator:","`
	CAFile   string   `env:"TLS_CA_FILE" envDefault:""`
	CertFile string   `env:"TLS_CERT_FILE" envDefault:""`
	KeyFile  string   `env:"TLS_KEY_FILE" envDefault:""`
}

// Validate checks the shared configuration and the public OTLP listener, which uses one-way TLS
// in production. The internal control-plane connection follows the shared mTLS policy.
func (c *Config) Validate() error {
	common := c.Common
	common.GRPC.Disabled = true
	errs := []error{common.Validate(), c.ControlPlane.ValidateFor(c.Environment)}
	if c.GRPC.Disabled || c.GRPC.Addr == c.HTTP.Addr || c.GRPC.Addr == c.Admin.Addr {
		errs = append(errs, errors.New("OTLP requires distinct HTTP, gRPC and admin listeners"))
	}
	if c.GRPC.MaxRecvMsgBytes <= 0 || c.GRPC.MaxConcurrentStreams == 0 || c.GRPC.ConnectionTimeout <= 0 ||
		c.GRPC.KeepaliveTime <= 0 || c.GRPC.KeepaliveTimeout <= 0 || c.GRPC.MaxConnectionAge <= 0 || c.GRPC.MaxConnectionAgeGrace <= 0 {
		errs = append(errs, errors.New("OTLP gRPC limits and timeouts must be positive"))
	}
	if (c.GRPC.TLSCertFile == "") != (c.GRPC.TLSKeyFile == "") || c.GRPC.TLSClientCAFile != "" || c.GRPC.InsecurePeerIdentity != "" {
		errs = append(errs, errors.New("OTLP gRPC uses a server certificate and key together with API-key client authentication"))
	}
	if c.Environment == config.EnvProd || c.Environment == config.EnvStaging {
		if c.GRPC.TLSCertFile == "" {
			errs = append(errs, errors.New("OTLP gRPC TLS is required in staging and production"))
		}
		if c.Kafka.CAFile == "" || c.Kafka.CertFile == "" || c.Kafka.KeyFile == "" {
			errs = append(errs, errors.New("Kafka mTLS credentials are required in staging and production"))
		}
	}
	if c.Redis.Addr == "" || len(c.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("Redis and Kafka endpoints are required"))
	}
	if c.HTTP.MaxBodyBytes > 4<<20 || c.GRPC.MaxRecvMsgBytes > 4<<20 || c.MaxBatchBytes < 1024 || c.MaxBatchBytes > 16<<20 {
		errs = append(errs, errors.New("OTLP request byte limits must be at most 4 MiB and broker batch limits between 1 KiB and 16 MiB"))
	}
	if c.MaxPoints < 1 || c.MaxPoints > 4096 || c.MaxInFlight < 1 || c.MaxInFlight > 128 || c.ExportDeadline < 100*time.Millisecond || c.ExportDeadline > 30*time.Second {
		errs = append(errs, errors.New("invalid OTLP export limits"))
	}
	// An otherwise valid full-sized batch must be able to fit in a new tenant's quota bucket.
	if c.PointsPerSec < 1 || c.PointsPerSec > 100_000 || c.BurstPoints < c.MaxPoints || c.BurstPoints > 100_000 {
		errs = append(errs, errors.New("invalid OTLP tenant quota"))
	}
	return errors.Join(errs...)
}

func (c *Config) exportLimits() receiver.Limits {
	return receiver.Limits{
		RequestBytes: int(min(c.HTTP.MaxBodyBytes, int64(c.GRPC.MaxRecvMsgBytes))),
		BatchBytes:   c.MaxBatchBytes, PointCount: c.MaxPoints, InFlight: c.MaxInFlight, Deadline: c.ExportDeadline,
	}
}
