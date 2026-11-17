//go:build integration

package kafkaconsumer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	telemetryv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/telemetry/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/testutil/kafkatest"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/service"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/testdb"
)

func TestF06_StoreCommitBeforeOffsetReplaysOnce_Integration(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, baseline) })
	c, _, d, producer := harness(t)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	scope := domain.Scope{OrganizationID: uuid.New(), ProjectID: uuid.New(), ResourceID: uuid.New()}
	checkID := uuid.New()
	raw := checkRecord(t, scope, checkID)
	require.NoError(t, kafka.Publish(ctx, producer, raw))

	client, err := kafka.NewClient(c.config, kgo.ConsumerGroup(c.group), kgo.ConsumeTopics(service.ChecksTopic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	require.NoError(t, err)
	received := pollRecord(t, ctx, client)
	require.NoError(t, c.apply(ctx, received.Topic, []*kgo.Record{received}))
	// Close after the real TimescaleDB commit, without a Kafka offset commit.
	client.CloseAllowingRebalance()
	require.EqualValues(t, 1, count(t, d, "SELECT count(*) FROM check_results"))
	require.EqualValues(t, 1, count(t, d, "SELECT count(*) FROM signals_outbox"))

	retry, err := kafka.NewClient(c.config, kgo.ConsumerGroup(c.group), kgo.ConsumeTopics(service.ChecksTopic), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll())
	require.NoError(t, err)
	defer retry.CloseAllowingRebalance()
	repeated := pollRecord(t, ctx, retry)
	require.Equal(t, received.Offset, repeated.Offset)
	require.NoError(t, c.apply(ctx, repeated.Topic, []*kgo.Record{repeated}))
	require.NoError(t, retry.CommitRecords(ctx, repeated))
	require.EqualValues(t, 1, count(t, d, "SELECT count(*) FROM check_results"))
	require.EqualValues(t, 1, count(t, d, "SELECT count(*) FROM threshold_state WHERE firing"))
	require.EqualValues(t, 1, count(t, d, "SELECT count(*) FROM signals_outbox"))

	meter := metric.NewMeterProvider()
	defer func() { _ = meter.Shutdown(context.WithoutCancel(ctx)) }()
	signals, err := NewSignals(d.App, producer, slog.New(slog.DiscardHandler), meter.Meter("test"))
	require.NoError(t, err)
	dispatched, err := signals.dispatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, dispatched)
	dispatched, err = signals.dispatch(ctx)
	require.NoError(t, err)
	require.Zero(t, dispatched)
	subscriber, err := kafka.NewClient(c.config, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		service.SignalsTopic: {0: kgo.NewOffset().AtStart()},
	}))
	require.NoError(t, err)
	defer subscriber.Close()
	signalRecord := pollRecord(t, ctx, subscriber)
	env, err := service.DecodeEnvelope(signalRecord.Value)
	require.NoError(t, err)
	var signal streamv1.ThresholdSignal
	require.NoError(t, envelope.Unpack(env, &signal))
	require.True(t, signal.GetFiring())
	require.Equal(t, checkID.String(), signal.GetCheckId())
	require.Equal(t, "duration_ms", signal.GetThresholdKind())
	require.Equal(t, scope.OrganizationID.String()+"/"+scope.ResourceID.String(), string(signalRecord.Key))
}

func TestF07_LockedTimescalePausesAndDrains_Integration(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, baseline) })
	c, metrics, d, producer := harness(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection, err := pgx.Connect(ctx, d.MigratorURL)
	require.NoError(t, err)
	defer func() { _ = connection.Close(context.WithoutCancel(ctx)) }()
	tx, err := connection.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	_, err = tx.Exec(ctx, "LOCK TABLE metrics IN SHARE MODE")
	require.NoError(t, err)
	scope := domain.Scope{OrganizationID: uuid.New(), ProjectID: uuid.New(), ResourceID: uuid.New()}
	const size = 96
	backlog := make([]*kgo.Record, size)
	for i := range backlog {
		backlog[i] = metricRecord(t, scope, i)
	}
	require.NoError(t, kafka.Publish(ctx, producer, backlog...))

	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	defer func() {
		stop()
		require.NoError(t, <-done)
	}()
	poll := time.NewTicker(30 * time.Millisecond)
	defer poll.Stop()
	find := func(wantPaused, wantLag func(int64) bool) {
		t.Helper()
		for {
			var data metricdata.ResourceMetrics
			require.NoError(t, metrics.Collect(ctx, &data))
			var paused, lag int64
			sawPaused, sawLag := false, false
			for _, scopeMetrics := range data.ScopeMetrics {
				for _, m := range scopeMetrics.Metrics {
					gauge, ok := m.Data.(metricdata.Gauge[int64])
					if !ok || len(gauge.DataPoints) == 0 {
						continue
					}
					switch m.Name {
					case "stream_processor_consumer_partition_paused":
						paused, sawPaused = gauge.DataPoints[0].Value, true
					case "stream_processor_consumer_lag":
						lag, sawLag = gauge.DataPoints[0].Value, true
					}
				}
			}
			if sawPaused && sawLag && wantPaused(paused) && wantLag(lag) {
				return
			}
			select {
			case <-poll.C:
			case <-ctx.Done():
				t.Fatalf("paused=%d lag=%d: %v", paused, lag, ctx.Err())
			}
		}
	}
	find(func(v int64) bool { return v == 1 }, func(v int64) bool { return v >= size })
	require.Zero(t, count(t, d, "SELECT count(*) FROM metrics"), "the real store is blocked before commit")
	require.NoError(t, tx.Commit(ctx))
	find(func(v int64) bool { return v == 0 }, func(v int64) bool { return v == 0 })
	require.EqualValues(t, size, count(t, d, "SELECT count(*) FROM metrics"))
}

func harness(t *testing.T) (*Consumer, *metric.ManualReader, testdb.DB, *kgo.Client) {
	t.Helper()
	database := testdb.Start(t)
	broker := kafkatest.Start(t, kafkatest.Topic{Name: service.ChecksTopic, Partitions: 1},
		kafkatest.Topic{Name: service.MetricsTopic, Partitions: 1}, kafkatest.Topic{Name: service.SignalsTopic, Partitions: 1},
		kafkatest.Topic{Name: service.ChecksTopic + ".dlq", Partitions: 1}, kafkatest.Topic{Name: service.MetricsTopic + ".dlq", Partitions: 1})
	cfg := kafka.Config{Brokers: []string{broker.Address}, ClientID: "stream-integration"}
	producer, err := kafka.NewClient(cfg)
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.WithoutCancel(t.Context())) })
	c, err := New(cfg, "stream-"+uuid.NewString(), producer, store.Writer{Pool: database.App},
		mp.Meter("test"), noop.NewTracerProvider().Tracer("test"), slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	return c, reader, database, producer
}

func pollRecord(t *testing.T, ctx context.Context, client *kgo.Client) *kgo.Record {
	t.Helper()
	fetched := client.PollRecords(ctx, 1)
	require.Empty(t, fetched.Errors())
	require.NotEmpty(t, fetched.Records())
	return fetched.Records()[0]
}

func count(t *testing.T, db testdb.DB, query string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, db.MigratorURL)
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var total int64
	err = conn.QueryRow(ctx, query).Scan(&total)
	require.NoError(t, err)
	return total
}

func checkRecord(t *testing.T, scope domain.Scope, id uuid.UUID) *kgo.Record {
	t.Helper()
	now := timestamppb.Now()
	limit := float64(10)
	return makeRecord(t, service.ChecksTopic, envelope.Params{
		Type: "checks.result.observed", SchemaVersion: 1, OrganizationID: scope.OrganizationID.String(),
		SubjectID: id.String(), Producer: "check-runner", Payload: &checksv1.CheckResult{
			ProjectId: scope.ProjectID.String(), ResourceId: scope.ResourceID.String(), CheckId: id.String(),
			ScheduledAt: now, ObservedAt: now, LeaseEpoch: 1, Outcome: checksv1.CheckOutcome_CHECK_OUTCOME_SUCCESS,
			DurationMs: 15, Thresholds: &checksv1.Thresholds{MaxDurationMs: &limit}, ThresholdVersion: 2,
		},
	})
}

func metricRecord(t *testing.T, scope domain.Scope, n int) *kgo.Record {
	t.Helper()
	id, series := sha256.Sum256([]byte(strconv.Itoa(n))), sha256.Sum256([]byte("backlog"))
	return makeRecord(t, service.MetricsTopic, envelope.Params{
		Type: "telemetry.metrics.observed", SchemaVersion: 1, OrganizationID: scope.OrganizationID.String(),
		SubjectID: scope.ResourceID.String(), Producer: "telemetry-ingestion", Payload: &telemetryv1.MetricsSample{
			ProjectId: scope.ProjectID.String(), ResourceId: scope.ResourceID.String(),
			MetricName: "backlog", SeriesId: hex.EncodeToString(series[:]), SampleId: hex.EncodeToString(id[:]),
			ObservedAt: timestamppb.Now(), OtlpPayload: bytes.Repeat([]byte(fmt.Sprintf("sample %d", n)), 4096),
			Value: &telemetryv1.MetricsSample_IntValue{IntValue: int64(n)},
		},
	})
}

func makeRecord(t *testing.T, topic string, params envelope.Params) *kgo.Record {
	t.Helper()
	env, err := envelope.New(t.Context(), params)
	require.NoError(t, err)
	payload, err := proto.Marshal(env)
	require.NoError(t, err)
	return &kgo.Record{Topic: topic, Key: []byte(params.OrganizationID + "/" + params.SubjectID), Value: payload}
}
