package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric"

	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/service"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store/db"
)

// Signals dispatches durable threshold transitions to Kafka after their source transaction commits.
type Signals struct {
	pool       *postgres.Pool
	kafka      *kgo.Client
	logger     *slog.Logger
	size       metric.Int64Gauge
	age        metric.Float64Gauge
	stop, done chan struct{}
	stopOnce   sync.Once
}

func NewSignals(pool *postgres.Pool, client *kgo.Client, logger *slog.Logger, meter metric.Meter) (*Signals, error) {
	size, sizeErr := meter.Int64Gauge("stream_processor_signal_outbox_pending")
	age, ageErr := meter.Float64Gauge("stream_processor_signal_outbox_oldest_seconds", metric.WithUnit("s"))
	if err := errors.Join(sizeErr, ageErr); err != nil {
		return nil, fmt.Errorf("register signal dispatcher metrics: %w", err)
	}
	return &Signals{pool: pool, kafka: client, logger: logger, size: size, age: age,
		stop: make(chan struct{}), done: make(chan struct{})}, nil
}

func (s *Signals) Name() string { return "signal-outbox-dispatcher" }

func (s *Signals) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stop) })
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain signal outbox: %w", ctx.Err())
	}
}

func (s *Signals) Run(ctx context.Context) error {
	defer close(s.done)
	nextMetrics := time.Now()
	delay := 300 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.stop:
			return nil
		default:
		}
		if now := time.Now(); !now.Before(nextMetrics) {
			scan, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := s.pool.WithSystemTx(scan, func(ctx context.Context, tx pgx.Tx) error {
				backlog, queryErr := db.New(tx).SignalBacklog(ctx)
				if queryErr == nil {
					s.size.Record(ctx, backlog.Messages)
					s.age.Record(ctx, backlog.OldestSeconds)
				}
				return queryErr
			}, postgres.ReadOnly()); err != nil {
				s.logger.WarnContext(scan, "measure signal backlog", slog.Any("error", err))
			}
			cancel()
			nextMetrics = now.Add(10 * time.Second)
		}
		attempt, cancel := context.WithTimeout(ctx, 9*time.Second)
		count, err := s.dispatch(attempt)
		cancel()
		if err == nil {
			delay = 300 * time.Millisecond
			if count == 32 {
				continue
			}
		} else {
			s.logger.WarnContext(ctx, "threshold signal left pending", slog.Any("error", err))
			delay = min(2*delay, 5*time.Second)
		}
		timer := time.NewTimer(delay + rand.N(delay/2))
		select {
		case <-ctx.Done():
		case <-s.stop:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (s *Signals) dispatch(ctx context.Context) (int, error) {
	count := 0
	err := s.pool.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		rows, err := q.ClaimSignals(ctx, 32)
		if err != nil {
			return fmt.Errorf("claim signal outbox rows: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		records := make([]*kgo.Record, len(rows))
		for i, row := range rows {
			record := &kgo.Record{Topic: service.SignalsTopic, Key: []byte(row.RoutingKey), Value: row.Payload}
			if row.TraceParent != "" {
				record.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte(row.TraceParent)}}
			}
			records[i] = record
		}
		if err = kafka.Publish(ctx, s.kafka, records...); err != nil {
			return fmt.Errorf("publish threshold signals: %w", err)
		}
		for _, row := range rows {
			if err = q.MarkSignalSent(ctx, row.ID); err != nil {
				return fmt.Errorf("mark threshold signal sent: %w", err)
			}
		}
		count = len(rows)
		return nil
	})
	return count, err
}
