// Package kafka provides bounded Kafka clients and acknowledgment-aware publication.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrQueueFull means the producer cannot accept the batch. Some records may already have been sent.
var ErrQueueFull = errors.New("kafka producer queue is full")

const (
	maxBufferedRecords = 4096
	maxBufferedBytes   = 32 << 20
	operationTimeout   = 8 * time.Second
)

// Config describes the Kafka bootstrap and optional TLS credentials. A CA file enables TLS.
type Config struct {
	Brokers                   []string
	ClientID                  string
	CAFile, CertFile, KeyFile string
}

// NewClient bounds Kafka requests and buffers and requires acknowledgment from all in-sync replicas.
// Group callers must also set DisableAutoCommit and commit after durable processing.
func NewClient(cfg Config, options ...kgo.Opt) (*kgo.Client, error) {
	if len(cfg.Brokers) == 0 || strings.TrimSpace(cfg.ClientID) == "" {
		return nil, errors.New("kafka requires bootstrap brokers and a client ID")
	}
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) == "" {
			return nil, errors.New("kafka bootstrap broker is empty")
		}
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") || (cfg.CertFile != "" && cfg.CAFile == "") {
		return nil, errors.New("kafka mTLS requires CA, certificate and key files")
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID(cfg.ClientID), kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DialTimeout(3 * time.Second), kgo.RequestTimeoutOverhead(2 * time.Second), kgo.RetryTimeout(operationTimeout),
		kgo.ProduceRequestTimeout(4 * time.Second), kgo.RecordDeliveryTimeout(operationTimeout),
		kgo.MaxBufferedRecords(maxBufferedRecords), kgo.MaxBufferedBytes(maxBufferedBytes),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.FetchMaxBytes(8 << 20), kgo.FetchMaxPartitionBytes(1 << 20),
		kgo.MaxConcurrentFetches(2),
	}
	if cfg.CAFile != "" {
		ca, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read Kafka CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			return nil, errors.New("Kafka CA file contains no certificates")
		}
		tlsConfig := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
		if cfg.CertFile != "" {
			cert, certErr := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
			if certErr != nil {
				return nil, fmt.Errorf("load Kafka client certificate: %w", certErr)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	client, err := kgo.NewClient(append(opts, options...)...)
	if err != nil {
		return nil, fmt.Errorf("configure Kafka client: %w", err)
	}
	return client, nil
}

// Publish waits for every broker acknowledgment and returns early on cancellation or buffer pressure.
// Callers that retry nontransactional batches must use stable record identities.
func Publish(ctx context.Context, client *kgo.Client, records ...*kgo.Record) error {
	if len(records) > maxBufferedRecords {
		return ErrQueueFull
	}
	for _, record := range records {
		if record == nil || record.Topic == "" || len(record.Key) == 0 {
			return errors.New("Kafka record requires a topic and nonempty key")
		}
	}
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	results := make(chan error, len(records))
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("Kafka publication: %w", err)
		}
		client.TryProduce(ctx, record, func(_ *kgo.Record, err error) { results <- err })
	}
	for range records {
		select {
		case err := <-results:
			if errors.Is(err, kgo.ErrMaxBuffered) {
				return ErrQueueFull
			}
			if err != nil {
				return fmt.Errorf("Kafka acknowledgment: %w", err)
			}
		case <-ctx.Done():
			return fmt.Errorf("Kafka acknowledgment: %w", ctx.Err())
		}
	}
	return nil
}
