// Package store implements the stream processor's tenant transactions.
package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/events/v1"
	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/postgres"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store/db"
)

// Writer applies finite batches to a single tenant.
type Writer struct{ Pool *postgres.Pool }

func (w Writer) Checks(ctx context.Context, org uuid.UUID, batch []domain.Check) error {
	ids := make([]uuid.UUID, len(batch))
	for i, check := range batch {
		if check.OrganizationID != org {
			return errors.New("check batch spans tenants")
		}
		ids[i] = check.CheckID
	}
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) })
	ids = slices.Compact(ids)
	return w.Pool.WithTx(ctx, org, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		// Lock in a stable order across batches, then apply records in original partition order.
		for _, id := range ids {
			if err := q.LockCheck(ctx, db.LockCheckParams{OrganizationID: org, CheckID: id}); err != nil {
				return fmt.Errorf("lock check stream: %w", err)
			}
		}
		for i := range batch {
			if err := writeCheck(ctx, q, &batch[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeCheck(ctx context.Context, q *db.Queries, check *domain.Check) error {
	prior, err := q.PriorCheck(ctx, db.PriorCheckParams{
		OrganizationID: check.OrganizationID, CheckID: check.CheckID, ScheduledAt: check.ScheduledAt,
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read existing check result: %w", err)
	}
	if err == nil && prior.LeaseEpoch >= check.LeaseEpoch {
		return nil
	}
	if saveErr := q.SaveCheck(ctx, db.SaveCheckParams{
		OrganizationID: check.OrganizationID, ProjectID: check.ProjectID, ResourceID: check.ResourceID,
		CheckID: check.CheckID, ScheduledAt: check.ScheduledAt, ObservedAt: check.ObservedAt, LeaseEpoch: check.LeaseEpoch,
		Succeeded: check.Succeeded, DurationMs: check.DurationMs, Payload: check.Payload,
	}); saveErr != nil {
		return fmt.Errorf("write check result: %w", saveErr)
	}
	delta := db.UpdateCheckRollupParams{
		OrganizationID: check.OrganizationID, ProjectID: check.ProjectID, ResourceID: check.ResourceID,
		CheckID: check.CheckID, ScheduledAt: check.ScheduledAt, ResultCount: 1, DurationDeltaMs: check.DurationMs,
	}
	if !check.Succeeded {
		delta.FailureCount++
	}
	if err == nil {
		delta.ResultCount = 0
		delta.DurationDeltaMs -= prior.DurationMs
		if !prior.Succeeded {
			delta.FailureCount--
		}
	}
	if updateErr := q.UpdateCheckRollup(ctx, delta); updateErr != nil {
		return fmt.Errorf("update check rollup: %w", updateErr)
	}
	for _, threshold := range domain.Thresholds(check.Result) {
		if thresholdErr := writeThreshold(ctx, q, check, threshold); thresholdErr != nil {
			return thresholdErr
		}
	}
	return nil
}

func writeThreshold(ctx context.Context, q *db.Queries, check *domain.Check, threshold domain.Threshold) error {
	previous, err := q.GetThresholdState(ctx, db.GetThresholdStateParams{
		OrganizationID: check.OrganizationID, CheckID: check.CheckID, ThresholdKind: threshold.Kind,
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("read threshold state: %w", err)
	}
	scheduled := check.ScheduledAt.Truncate(time.Microsecond)
	if previous.ScheduledAt.After(scheduled) ||
		previous.ScheduledAt.Equal(scheduled) && previous.LeaseEpoch >= check.LeaseEpoch {
		return nil
	}
	firing := threshold.Firing
	if !threshold.Known {
		firing = previous.Firing
	}
	if saveErr := q.SaveThresholdState(ctx, db.SaveThresholdStateParams{
		OrganizationID: check.OrganizationID, CheckID: check.CheckID, ThresholdKind: threshold.Kind,
		ScheduledAt: check.ScheduledAt, LeaseEpoch: check.LeaseEpoch, Firing: firing,
	}); saveErr != nil {
		return fmt.Errorf("write threshold state: %w", saveErr)
	}
	if previous.Firing == firing {
		return nil
	}
	return enqueueSignal(ctx, q, check, threshold, firing)
}

func enqueueSignal(ctx context.Context, q *db.Queries, check *domain.Check, threshold domain.Threshold, firing bool) error {
	org, resource := check.OrganizationID.String(), check.ResourceID.String()
	source := &eventsv1.Envelope{TraceParent: check.TraceParent}
	env, err := envelope.New(envelope.ContextWithTrace(ctx, source), envelope.Params{
		Type: "checks.threshold.changed", SchemaVersion: 1, OrganizationID: org, SubjectID: resource,
		OccurredAt: check.ObservedAt, Producer: "stream-processor",
		Payload: &streamv1.ThresholdSignal{
			ProjectId: check.ProjectID.String(), ResourceId: resource, CheckId: check.CheckID.String(),
			ScheduledAt: check.Result.GetScheduledAt(), Firing: firing, ThresholdKind: threshold.Kind,
			ObservedValue: threshold.Observed, ThresholdValue: threshold.Limit,
			ThresholdVersion: check.Result.GetThresholdVersion(), ThresholdRemoved: !threshold.Applied,
		},
	})
	if err != nil {
		return fmt.Errorf("encode threshold signal envelope: %w", err)
	}
	payload, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal threshold signal: %w", err)
	}
	if err = q.EnqueueSignal(ctx, db.EnqueueSignalParams{
		ID: uuid.MustParse(env.GetEventId()), RoutingKey: org + "/" + resource, Payload: payload, TraceParent: env.GetTraceParent(),
	}); err != nil {
		return fmt.Errorf("enqueue threshold signal: %w", err)
	}
	return nil
}

func (w Writer) Metrics(ctx context.Context, org uuid.UUID, batch []domain.Metric) error {
	rows := make([]db.SaveMetricParams, len(batch))
	for i, sample := range batch {
		if sample.OrganizationID != org {
			return errors.New("metric batch spans tenants")
		}
		var scalar pgtype.Float8
		if sample.Scalar != nil {
			scalar = pgtype.Float8{Float64: *sample.Scalar, Valid: true}
		}
		rows[i] = db.SaveMetricParams{
			OrganizationID: org, ProjectID: sample.ProjectID, ResourceID: sample.ResourceID,
			MetricName: sample.Name, ObservedAt: sample.ObservedAt, SeriesID: sample.SeriesID,
			SampleID: sample.SampleID, ScalarValue: scalar, Payload: sample.Payload,
		}
	}
	if len(rows) == 0 {
		return nil
	}
	return w.Pool.WithTx(ctx, org, func(ctx context.Context, tx pgx.Tx) error {
		var firstErr error
		results := db.New(tx).SaveMetric(ctx, rows)
		results.Exec(func(_ int, err error) {
			if firstErr == nil {
				firstErr = err
			}
		})
		if firstErr != nil {
			return fmt.Errorf("write metrics and rollups: %w", firstErr)
		}
		return nil
	})
}
