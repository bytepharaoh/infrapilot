//go:build integration

package outbox

import (
	"context"
	"hash/crc32"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	eventsv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/events/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/testutil/kafkatest"
	"github.com/bytepharaoh/infrapilot/services/control-plane/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/control-plane/internal/service"
	"github.com/bytepharaoh/infrapilot/services/control-plane/internal/testdb"
)

func TestF08_ResourceOutboxSurvivesKafkaOutage_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	pg := testdb.Start(t)
	broker := kafkatest.Start(t, kafkatest.Topic{Name: resourceStateTopic, Partitions: partitionCount,
		Config: map[string]string{"cleanup.policy": "compact", "delete.retention.ms": "604800000"}})
	logger := slog.New(slog.DiscardHandler)
	meter := noop.NewMeterProvider().Meter("test")
	cfg := kafka.Config{Brokers: []string{broker.Address}, ClientID: "outbox-f08"}
	first, err := New(pg.App, cfg, "f08", logger, meter)
	require.NoError(t, err)
	require.NoError(t, broker.SetPaused(ctx, true))
	t.Cleanup(func() {
		cleanupCtx, finish := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer finish()
		_ = broker.SetPaused(cleanupCtx, false)
	})

	cp, err := service.New(pg.App)
	require.NoError(t, err)
	actor, err := cp.ResolveActor(ctx, service.Principal{Kind: domain.ActorUser, Subject: "outbox-owner",
		Email: "outbox-owner@example.com", EmailVerified: true, DisplayName: "Owner"})
	require.NoError(t, err)
	org, err := cp.CreateOrganization(ctx, actor, service.CreateOrganizationInput{Name: "Outbox Test"})
	require.NoError(t, err)
	project, err := cp.CreateProject(ctx, actor, org.ID, service.CreateProjectInput{Name: "Test", Slug: "test"})
	require.NoError(t, err)
	resource, err := cp.CreateResource(ctx, actor, org.ID, project.ID, service.CreateResourceInput{Kind: "service", Name: "Web"})
	require.NoError(t, err)
	require.NoError(t, cp.DeleteResource(ctx, actor, org.ID, project.ID, resource.ID))
	prepared, err := first.prepare(ctx)
	require.NoError(t, err)
	require.True(t, prepared)
	downCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	_, err = first.send(downCtx)
	stop()
	require.Error(t, err)

	inspect, err := pgx.Connect(ctx, pg.MigratorURL)
	require.NoError(t, err)
	defer func() { _ = inspect.Close(context.WithoutCancel(ctx)) }()
	key := org.ID.String() + "/" + resource.ID.String()
	rows, err := inspect.Query(ctx, `SELECT payload FROM resource_state_outbox
		WHERE routing_key = $1 AND sent_at IS NULL ORDER BY revision`, key)
	require.NoError(t, err)
	payloads, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	require.NoError(t, err)
	require.Len(t, payloads, 2)

	require.NoError(t, broker.SetPaused(ctx, false))
	restarted, err := New(pg.App, cfg, "f08", logger, meter)
	require.NoError(t, err)
	for range 2 {
		sent, sendErr := restarted.send(ctx)
		require.NoError(t, sendErr)
		require.EqualValues(t, 1, sent, "only the oldest pending revision for this key is sent in a transaction")
	}

	partition := int32(crc32.ChecksumIEEE([]byte(key)) % partitionCount)
	client, err := kafka.NewClient(cfg, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		resourceStateTopic: {partition: kgo.NewOffset().AtStart()},
	}))
	require.NoError(t, err)
	defer client.Close()
	readCtx, finishRead := context.WithTimeout(ctx, 12*time.Second)
	defer finishRead()
	var results []*kgo.Record
	for len(results) < 2 {
		fetches := client.PollRecords(readCtx, 2-len(results))
		require.NoError(t, fetches.Err())
		results = append(results, fetches.Records()...)
	}
	var previous uint64
	for i, status := range []cpv1.ResourceStateStatus{
		cpv1.ResourceStateStatus_RESOURCE_STATE_STATUS_ACTIVE, cpv1.ResourceStateStatus_RESOURCE_STATE_STATUS_INACTIVE,
	} {
		require.Equal(t, key, string(results[i].Key))
		require.Equal(t, payloads[i], results[i].Value, "retry kept the prepared envelope and UUID")
		var event eventsv1.Envelope
		var change cpv1.ResourceStateChanged
		require.NoError(t, proto.Unmarshal(results[i].Value, &event))
		require.NoError(t, envelope.Validate(&event))
		require.NoError(t, envelope.Unpack(&event, &change))
		require.Equal(t, status, change.GetStatus())
		require.Greater(t, change.GetRevision(), previous)
		previous = change.GetRevision()
	}
	var n int
	require.NoError(t, inspect.QueryRow(ctx, `SELECT count(*) FROM resource_state_outbox
		WHERE routing_key = $1 AND sent_at IS NOT NULL`, key).Scan(&n))
	require.Equal(t, 2, n)
}
