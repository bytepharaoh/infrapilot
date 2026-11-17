// Package worker manages bounded lease holders and check-result publication.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/probe"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/store/db"
)

// Scheduler owns the goroutine and lease capacity in one replica.
type Scheduler struct {
	checks            *store.Store
	probes            *probe.Executor
	owner             uuid.UUID
	workers           int32
	tenantConcurrency int32
	log               *slog.Logger
	trace             trace.Tracer
	drift             metric.Float64Histogram
	fenced            metric.Int64Counter
	stop              chan struct{}
	done              chan struct{}
	stopOnce          sync.Once
}

// New creates a bounded scheduler. Database leases also enforce the shared tenant limit.
func New(checks *store.Store, probes *probe.Executor, workers, tenantConcurrency int32, logger *slog.Logger, tracer trace.Tracer, meter metric.Meter) (*Scheduler, error) {
	owner, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate scheduler owner: %w", err)
	}
	drift, err := meter.Float64Histogram("checkrunner_scheduling_drift_seconds", metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60))
	if err != nil {
		return nil, err
	}
	fenced, err := meter.Int64Counter("checkrunner_fenced_completions_total")
	if err != nil {
		return nil, err
	}
	return &Scheduler{checks: checks, probes: probes, owner: owner, workers: workers, tenantConcurrency: tenantConcurrency,
		log: logger, trace: tracer, drift: drift, fenced: fenced, stop: make(chan struct{}), done: make(chan struct{})}, nil
}

// Name identifies this component in service lifecycle logs.
func (s *Scheduler) Name() string { return "scheduler" }

// Shutdown stops claims and waits for existing probes and completion transactions.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() { close(s.stop) })
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("drain scheduler: %w", ctx.Err())
	}
}

// Run starts only as many probes as the claimed worker slots can hold.
func (s *Scheduler) Run(ctx context.Context) error {
	defer close(s.done)
	var workers sync.WaitGroup
	defer workers.Wait()
	active := make(chan struct{}, s.workers)
	var delay time.Duration
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.stop:
			return nil
		case <-time.After(delay):
		}
		free := cap(active) - len(active)
		if free == 0 {
			delay = 10 * time.Millisecond
			continue
		}
		started := time.Now()
		queryCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		rows, err := s.checks.Claim(queryCtx, s.owner, min(int32(free), s.workers), s.tenantConcurrency)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				s.log.WarnContext(ctx, "database lease claim failed", slog.Any("error", err))
			}
			delay = min(3*time.Second, max(100*time.Millisecond, 2*delay)) + time.Duration(rand.IntN(100))*time.Millisecond
			continue
		}
		if len(rows) == 0 {
			delay = 75 * time.Millisecond
			continue
		}
		delay = 0
		for _, row := range rows {
			active <- struct{}{}
			workers.Go(func() {
				defer func() { <-active }()
				s.runCheck(ctx, row, started)
			})
		}
	}
}

func (s *Scheduler) runCheck(ctx context.Context, row db.Check, claimStarted time.Time) {
	// The DB owns lease expiration. This conservative elapsed timer covers the
	// five-second margin without comparing the database and pod wall clocks.
	if ctx.Err() != nil || time.Since(claimStarted) >= 5*time.Second {
		return
	}
	ctx, span := s.trace.Start(ctx, "check.run")
	defer span.End()
	s.drift.Record(ctx, max(0, time.Since(row.NextRunAt).Seconds()))
	measurement := s.probes.Run(ctx, store.Definition(row))
	final, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ok, err := s.checks.Complete(final, row, measurement)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			s.log.WarnContext(ctx, "check completion failed", slog.Any("error", err))
		}
		return
	}
	if !ok {
		s.fenced.Add(ctx, 1)
	}
}

// Publisher dispatches committed results with a bounded broker timeout and exponential backoff.
type Publisher struct {
	Store  *store.Store
	Client *kgo.Client
	Logger *slog.Logger
}

// Run dispatches until service stop. A crash may duplicate an envelope with its original ID.
func (p Publisher) Run(ctx context.Context) error {
	var delay time.Duration
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 9*time.Second)
		n, err := p.Store.PublishResults(attempt, p.Client)
		cancel()
		switch {
		case err != nil && ctx.Err() == nil:
			p.Logger.WarnContext(ctx, "check result publication failed", slog.Any("error", err))
			delay = min(8*time.Second, max(200*time.Millisecond, 2*delay)) + time.Duration(rand.IntN(100))*time.Millisecond
		case n == 0:
			delay = 100 * time.Millisecond
		default:
			delay = 0
		}
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
	}
	return nil
}
