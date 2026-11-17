// Package outbox delivers committed control-plane resource lifecycle events to Kafka.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/services/control-plane/internal/store/db"
)

const (
	resourceStateTopic = "resource.state.v1"
	partitionCount     = 12
	prepareLimit       = 64
	dispatchLimit      = 32
	transactionBudget  = 12 * time.Second
)

// ResourceState describes an outbox worker. Every replica uses the same deployment identifier.
type ResourceState struct {
	pool          *postgres.Pool
	kafka         kafka.Config
	deployment    string
	log           *slog.Logger
	pending       metric.Int64Gauge
	oldestSeconds metric.Float64Gauge
	acknowledged  metric.Int64Counter
	failures      metric.Int64Counter
}

func New(pool *postgres.Pool, cfg kafka.Config, deployment string, logger *slog.Logger, meter metric.Meter) (*ResourceState, error) {
	if strings.TrimSpace(deployment) != deployment || deployment == "" || len(deployment) > 96 {
		return nil, errors.New("resource outbox: KAFKA_DEPLOYMENT_ID must be 1–96 characters without edge whitespace")
	}
	client, err := kafka.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	client.Close()
	pending, pendingErr := meter.Int64Gauge("controlplane_resource_outbox_pending")
	oldest, oldestErr := meter.Float64Gauge("controlplane_resource_outbox_oldest_seconds", metric.WithUnit("s"))
	acknowledged, ackErr := meter.Int64Counter("controlplane_resource_outbox_acknowledged_total")
	failures, failuresErr := meter.Int64Counter("controlplane_resource_outbox_failures_total")
	if err := errors.Join(pendingErr, oldestErr, ackErr, failuresErr); err != nil {
		return nil, fmt.Errorf("resource outbox metrics: %w", err)
	}
	return &ResourceState{pool: pool, kafka: cfg, deployment: deployment, log: logger, pending: pending,
		oldestSeconds: oldest, acknowledged: acknowledged, failures: failures}, nil
}

// Run continuously prepares and publishes batches. Outages leave durable rows pending and keep the API
// ready to accept requests. Cancellation terminates polling and bounds the active broker transaction.
func (d *ResourceState) Run(ctx context.Context) error {
	var statsAt time.Time
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		if time.Since(statsAt) >= 5*time.Second {
			d.recordStats(ctx)
			statsAt = time.Now()
		}
		worked, err := d.step(ctx)
		if ctx.Err() != nil {
			break
		}
		delay := 100 * time.Millisecond
		switch {
		case err != nil:
			d.failures.Add(ctx, 1)
			d.log.WarnContext(ctx, "resource state outbox retry", slog.Any("error", err))
			delay = backoff + rand.N(backoff/2)
			backoff = min(backoff*2, 5*time.Second)
		case worked:
			backoff = 250 * time.Millisecond
			continue
		default:
			backoff = 250 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return nil
}

func (d *ResourceState) step(ctx context.Context) (bool, error) {
	prepared, err := d.prepare(ctx)
	if err != nil {
		return false, err
	}
	sent, err := d.send(ctx)
	if err != nil {
		return false, err
	}
	d.acknowledged.Add(ctx, sent)
	return prepared || sent != 0, nil
}

func (d *ResourceState) prepare(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var prepared bool
	err := d.pool.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		rows, err := q.LockUnpreparedResourceState(ctx, prepareLimit)
		if err != nil {
			return fmt.Errorf("select resource state: %w", err)
		}
		for _, row := range rows {
			if err := prepareEnvelope(ctx, q, row); err != nil {
				return fmt.Errorf("prepare resource event %d: %w", row.Revision, err)
			}
		}
		prepared = len(rows) > 0
		return nil
	})
	return prepared, err
}

func prepareEnvelope(ctx context.Context, q *db.Queries, row db.LockUnpreparedResourceStateRow) error {
	org, resource, valid := strings.Cut(row.RoutingKey, "/")
	if !valid || resource == "" {
		return errors.New("invalid resource state routing key")
	}
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": row.TraceParent})
	event, err := envelope.New(ctx, envelope.Params{
		Type: "controlplane.resource.changed", SchemaVersion: 1, OrganizationID: org, SubjectID: resource,
		OccurredAt: row.CapturedAt, Producer: "control-plane", Payload: &cpv1.ResourceStateChanged{
			ProjectId: row.ProjectID.String(), Status: cpv1.ResourceStateStatus(row.Status), Revision: uint64(row.Revision),
		},
	})
	if err != nil {
		return err
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialize envelope: %w", err)
	}
	partition := int16(crc32.ChecksumIEEE([]byte(row.RoutingKey)) % partitionCount)
	return q.PrepareResourceState(ctx, db.PrepareResourceStateParams{
		Revision: row.Revision, Payload: payload, KafkaPartition: pgtype.Int2{Int16: partition, Valid: true},
	})
}

func (d *ResourceState) send(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, transactionBudget)
	defer cancel()
	var sent int64
	err := d.pool.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		partition, err := q.LockResourceStatePartition(ctx)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("claim resource state partition: %w", err)
		}
		rows, err := q.ListResourceStateHeads(ctx, db.ListResourceStateHeadsParams{
			Partition: pgtype.Int2{Int16: partition, Valid: true}, Lim: dispatchLimit,
		})
		if err != nil {
			return fmt.Errorf("read pending resource events: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}
		if err := d.publishPartition(ctx, q, partition, rows); err != nil {
			return err
		}
		revisions := make([]int64, len(rows))
		for i, row := range rows {
			revisions[i] = row.Revision
		}
		sent, err = q.MarkResourceStateSent(ctx, revisions)
		if err != nil {
			return fmt.Errorf("mark resource state sent: %w", err)
		}
		if sent != int64(len(rows)) {
			return fmt.Errorf("resource outbox acknowledged %d rows but locked %d", sent, len(rows))
		}
		return nil
	})
	return sent, err
}

func (d *ResourceState) publishPartition(ctx context.Context, q *db.Queries, partition int16, rows []db.ListResourceStateHeadsRow) error {
	txnID := fmt.Sprintf("%s.control-plane.resource-state.%d", d.deployment, partition)
	client, err := kafka.NewClient(d.kafka, kgo.TransactionalID(txnID), kgo.TransactionTimeout(15*time.Second),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.BeginTransaction(); err != nil {
		return fmt.Errorf("start resource Kafka transaction: %w", err)
	}
	if _, _, err := client.ProducerID(ctx); err != nil {
		return fmt.Errorf("initialize resource producer epoch: %w", err)
	}
	// Confirm ownership on the same PostgreSQL session after the Kafka epoch is acquired.
	if err := q.ConfirmResourceStateLock(ctx, partition); err != nil {
		return fmt.Errorf("confirm resource partition lock: %w", err)
	}
	records := make([]*kgo.Record, len(rows))
	for i, row := range rows {
		record := &kgo.Record{Topic: resourceStateTopic, Partition: int32(partition), Key: []byte(row.RoutingKey)}
		if row.Status == int16(cpv1.ResourceStateStatus_RESOURCE_STATE_STATUS_PURGED) {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: "infrapilot-envelope", Value: row.Payload})
		} else {
			record.Value = row.Payload
		}
		if row.TraceParent != "" {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: "traceparent", Value: []byte(row.TraceParent)})
		}
		records[i] = record
	}
	if err := kafka.Publish(ctx, client, records...); err != nil {
		return fmt.Errorf("publish resource state: %w", err)
	}
	if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("commit resource Kafka transaction: %w", err)
	}
	return nil
}

func (d *ResourceState) recordStats(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var stats db.ResourceStateOutboxStatsRow
	err := d.pool.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) (err error) {
		stats, err = db.New(tx).ResourceStateOutboxStats(ctx)
		return err
	}, postgres.ReadOnly())
	if err != nil {
		d.log.WarnContext(ctx, "read resource outbox metrics", slog.Any("error", err))
		return
	}
	d.pending.Record(ctx, stats.Pending)
	d.oldestSeconds.Record(ctx, max(0, stats.OldestSeconds))
}
