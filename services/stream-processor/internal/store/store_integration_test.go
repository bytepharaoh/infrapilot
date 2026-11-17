//go:build integration

package store_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	otlpmetrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	telemetryv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/telemetry/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/service"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/testdb"
)

func TestTimescaleRLSRollupsAndRetention_Integration(t *testing.T) {
	d := testdb.Start(t)
	ctx := t.Context()
	writer, reader := store.Writer{Pool: d.App}, store.Reader{Pool: d.Reader}
	owner := domain.Scope{OrganizationID: uuid.New(), ProjectID: uuid.New(), ResourceID: uuid.New()}
	other := owner
	other.OrganizationID = uuid.New()
	sampleTime := time.Now().UTC().Truncate(time.Hour)
	a := metric(t, owner, sampleTime, 9)
	b := metric(t, owner, sampleTime.Add(-2*time.Second), 3)
	alien := metric(t, other, sampleTime, 20)
	require.NoError(t, writer.Metrics(ctx, owner.OrganizationID, []domain.Metric{a, b, a}))
	require.NoError(t, writer.Metrics(ctx, other.OrganizationID, []domain.Metric{alien}))

	checkID := uuid.New()
	result := check(t, owner, checkID, sampleTime, 1, false, 70)
	newer := check(t, owner, checkID, sampleTime, 2, true, 20)
	require.NoError(t, writer.Checks(ctx, owner.OrganizationID, []domain.Check{result, newer, result}))
	rows, err := reader.Checks(ctx, store.Window{Scope: owner, Start: sampleTime.Add(-time.Hour), End: sampleTime.Add(time.Hour), Limit: 10}, checkID)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	var stored checksv1.CheckResult
	require.NoError(t, proto.Unmarshal(rows[0].Payload, &stored))
	require.Equal(t, uint64(2), stored.GetLeaseEpoch())

	window := store.Window{Scope: owner, Start: sampleTime.Add(-time.Hour), End: sampleTime.Add(time.Hour), Limit: 10}
	metricRows, err := reader.Metrics(ctx, window, a.Name)
	require.NoError(t, err)
	require.Len(t, metricRows, 2)
	require.Equal(t, b.Payload, metricRows[0].Payload)
	require.Equal(t, a.Payload, metricRows[1].Payload)
	window.OrganizationID = other.OrganizationID
	metricRows, err = reader.Metrics(ctx, window, a.Name)
	require.NoError(t, err)
	require.Len(t, metricRows, 1)
	require.Equal(t, alien.Payload, metricRows[0].Payload)

	require.NoError(t, d.Reader.WithSystemTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		var seen int
		if queryErr := tx.QueryRow(ctx, "SELECT count(*) FROM metrics").Scan(&seen); queryErr != nil {
			return queryErr
		}
		require.Zero(t, seen)
		return nil
	}))
	require.NoError(t, d.Reader.WithTx(ctx, owner.OrganizationID, func(ctx context.Context, tx pgx.Tx) error {
		var writable bool
		if queryErr := tx.QueryRow(ctx, "SELECT has_table_privilege(current_user, 'metrics', 'INSERT')").Scan(&writable); queryErr != nil {
			return queryErr
		}
		require.False(t, writable)
		return nil
	}))

	admin, err := pgx.Connect(ctx, d.MigratorURL)
	require.NoError(t, err)
	defer func() { _ = admin.Close(context.WithoutCancel(ctx)) }()
	var aggregateRows, total int
	require.NoError(t, admin.QueryRow(ctx, `SELECT count(*), sum(sample_count) FROM metric_rollups
        WHERE organization_id=$1 AND step_seconds=60`, owner.OrganizationID).Scan(&aggregateRows, &total))
	require.Equal(t, 2, aggregateRows)
	require.Equal(t, 2, total)
	var count, failed int
	var sum string
	require.NoError(t, admin.QueryRow(ctx, "SELECT result_count, failure_count, duration_sum_ms::text FROM check_rollups WHERE organization_id=$1",
		owner.OrganizationID).Scan(&count, &failed, &sum))
	require.Equal(t, 1, count)
	require.Zero(t, failed)
	require.Equal(t, "20", sum)
	var original, compressed int
	var codec string
	require.NoError(t, admin.QueryRow(ctx, `SELECT octet_length(payload),pg_column_size(payload),pg_column_compression(payload)
        FROM metrics WHERE organization_id=$1 LIMIT 1`, owner.OrganizationID).Scan(&original, &compressed, &codec))
	require.Less(t, compressed, original/4)
	require.Equal(t, "lz4", codec)
	t.Logf("Timescale metric payload: %d logical bytes, %d %s bytes", original, compressed, codec)

	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM timescaledb_information.jobs WHERE proc_name='policy_retention'").Scan(&total))
	require.Equal(t, 2, total)
	_, err = admin.Exec(ctx, "UPDATE metric_rollups SET bucket_start=bucket_start-interval '401 days' WHERE organization_id=$1", other.OrganizationID)
	require.NoError(t, err)
	require.NoError(t, store.Purge(ctx, d.Batch, slog.New(slog.DiscardHandler)))
	require.NoError(t, admin.QueryRow(ctx, "SELECT count(*) FROM metric_rollups WHERE organization_id=$1", other.OrganizationID).Scan(&total))
	require.Zero(t, total)
}

func metric(t *testing.T, scope domain.Scope, when time.Time, scalar float64) domain.Metric {
	t.Helper()
	seriesID, sampleID := sha256.Sum256([]byte("integration series")), sha256.Sum256([]byte(when.String()))
	data, err := proto.Marshal(&otlpmetrics.ResourceMetrics{Resource: &resourcev1.Resource{
		Attributes: []*commonv1.KeyValue{
			{Key: "compressed", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: bytes.Repeat([]byte("metric dimensions "), 2048)}}},
		},
	}})
	require.NoError(t, err)
	env, err := envelope.New(t.Context(), envelope.Params{
		Type: "telemetry.metrics.observed", SchemaVersion: 1, OrganizationID: scope.OrganizationID.String(),
		SubjectID: scope.ResourceID.String(), Producer: "telemetry-ingestion",
		Payload: &telemetryv1.MetricsSample{ProjectId: scope.ProjectID.String(), ResourceId: scope.ResourceID.String(),
			MetricName: "load", SeriesId: hex.EncodeToString(seriesID[:]), SampleId: hex.EncodeToString(sampleID[:]),
			ObservedAt: timestamppb.New(when), OtlpPayload: data,
			Value: &telemetryv1.MetricsSample_DoubleValue{DoubleValue: scalar}},
	})
	require.NoError(t, err)
	sample, err := service.DecodeMetric(env, scope.OrganizationID.String()+"/"+scope.ResourceID.String(), time.Now())
	require.NoError(t, err)
	return sample
}

func check(t *testing.T, scope domain.Scope, checkID uuid.UUID, when time.Time, epoch uint64, ok bool, duration float64) domain.Check {
	t.Helper()
	outcome := checksv1.CheckOutcome_CHECK_OUTCOME_FAILURE
	if ok {
		outcome = checksv1.CheckOutcome_CHECK_OUTCOME_SUCCESS
	}
	env, err := envelope.New(t.Context(), envelope.Params{
		Type: "checks.result.observed", SchemaVersion: 1, OrganizationID: scope.OrganizationID.String(),
		SubjectID: checkID.String(), Producer: "check-runner", Payload: &checksv1.CheckResult{
			ProjectId: scope.ProjectID.String(), ResourceId: scope.ResourceID.String(), CheckId: checkID.String(),
			ScheduledAt: timestamppb.New(when), ObservedAt: timestamppb.New(when), LeaseEpoch: epoch,
			Outcome: outcome, DurationMs: duration,
		},
	})
	require.NoError(t, err)
	result, err := service.DecodeCheck(env, scope.OrganizationID.String()+"/"+checkID.String(), time.Now())
	require.NoError(t, err)
	return result
}
