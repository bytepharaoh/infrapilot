//go:build integration

package app_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/twmb/franz-go/pkg/kgo"
	otlp "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"go.uber.org/goleak"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	eventsv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/events/v1"
	telemetryv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/telemetry/v1"
	"github.com/bytepharaoh/infrapilot/pkg/config"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/pkg/log"
	"github.com/bytepharaoh/infrapilot/pkg/testutil/kafkatest"
	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/app"
	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/receiver"
)

func TestF04_KafkaUnavailableOTLPMetrics_Integration(t *testing.T) {
	baseline := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, baseline) })
	cp, pg := controlPlane(t)
	owner := seedTenant(t, pg, []string{"ingest"})
	foreign := seedTenant(t, pg, []string{"ingest"})
	reader := seedTenant(t, pg, []string{"read"})
	redisAddress := startRedis(t)

	broker := kafkatest.Start(t, kafkatest.Topic{Name: domain.Topic, Partitions: 1})
	env := map[string]string{
		"SERVICE_NAME": "telemetry-ingestion", "ENVIRONMENT": "test", "HTTP_ADDR": freeAddress(t),
		"GRPC_ADDR": freeAddress(t), "ADMIN_ADDR": freeAddress(t), "SHUTDOWN_DELAY": "0s",
		"CONTROLPLANE_TARGET": cp.Target, "CONTROLPLANE_SERVER_NAME": cp.ServerName, "CONTROLPLANE_SERVER_IDENTITY": cp.ServerIdentity,
		"CONTROLPLANE_TLS_CA_FILE": cp.TLSCAFile, "CONTROLPLANE_TLS_CERT_FILE": cp.TLSCertFile, "CONTROLPLANE_TLS_KEY_FILE": cp.TLSKeyFile,
		"KAFKA_BROKERS": broker.Address, "REDIS_ADDR": redisAddress, "INGEST_MAX_IN_FLIGHT": "1", "INGEST_EXPORT_DEADLINE": "6s",
	}
	cfg, err := config.Load[app.Config](config.WithEnvironment(env))
	require.NoError(t, err)
	serveCtx, stop := context.WithCancel(t.Context())
	exit := make(chan error, 1)
	go func() {
		exit <- app.Serve(serveCtx, &cfg, log.Config{Level: "info", Format: "json"}, slog.New(slog.DiscardHandler))
	}()
	t.Cleanup(func() {
		stop()
		select {
		case serveErr := <-exit:
			require.NoError(t, serveErr)
		case <-time.After(8 * time.Second):
			t.Error("telemetry service did not stop")
		}
	})
	waitReady(t, "http://"+cfg.Admin.Addr+"/readyz")
	grpcConn, err := grpc.NewClient(cfg.GRPC.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = grpcConn.Close() })
	exporter := otlp.NewMetricsServiceClient(grpcConn)
	httpClient := &http.Client{Timeout: 12 * time.Second}
	t.Cleanup(httpClient.CloseIdleConnections)
	metricsURL := "http://" + cfg.HTTP.Addr + "/v1/metrics"

	req := exportRequest(foreign.org)
	wire, err := proto.Marshal(req)
	require.NoError(t, err)
	unauth, err := post(t.Context(), httpClient, metricsURL, tenant{}, wire, "application/x-protobuf", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, unauth.code)
	unscoped := grpcExport(t.Context(), exporter, reader, req)
	require.Equal(t, codes.PermissionDenied, status.Code(unscoped))
	crossTenant := owner
	crossTenant.resource = foreign.resource
	denied, err := post(t.Context(), httpClient, metricsURL, crossTenant, wire, "application/x-protobuf", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, denied.code)

	// Success is followed by a real Kafka fetch. The body deliberately names the foreign organization.
	first, err := post(t.Context(), httpClient, metricsURL, owner, wire, "application/x-protobuf", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, first.code)
	consumer, err := kafka.NewClient(kafka.Config{Brokers: []string{broker.Address}, ClientID: "f04-reader"},
		kgo.ConsumeTopics(domain.Topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	t.Cleanup(consumer.Close)
	before := fetchedSample(t, consumer, owner)
	require.Equal(t, int64(42), before.GetIntValue())

	require.NoError(t, dockerCommand(t.Context(), "pause", broker.Container.GetContainerID()))
	t.Cleanup(func() {
		_ = dockerCommand(context.WithoutCancel(t.Context()), "unpause", broker.Container.GetContainerID())
	})
	type attempt struct {
		response httpResult
		err      error
	}
	pending := make(chan attempt, 1)
	go func() {
		response, postErr := post(serveCtx, httpClient, metricsURL, owner, wire, "application/x-protobuf", "")
		pending <- attempt{response: response, err: postErr}
	}()
	metrics := "http://" + cfg.Admin.Addr + "/metrics"
	require.Eventually(t, func() bool { return scrapeInFlight(serveCtx, httpClient, metrics) == 1 }, 3*time.Second, 25*time.Millisecond)

	full, err := post(t.Context(), httpClient, metricsURL, owner, wire, "application/x-protobuf", "")
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, full.code)
	require.NotEmpty(t, full.retryAfter)
	grpcBusy := status.Convert(grpcExport(t.Context(), exporter, owner, req))
	require.Equal(t, codes.ResourceExhausted, grpcBusy.Code())
	require.Len(t, grpcBusy.Details(), 1)
	retry, ok := grpcBusy.Details()[0].(*errdetails.RetryInfo)
	require.True(t, ok)
	require.Positive(t, retry.GetRetryDelay().AsDuration())

	waiting := <-pending
	require.NoError(t, waiting.err)
	require.Equal(t, http.StatusServiceUnavailable, waiting.response.code, "an unacknowledged export cannot succeed")
	require.NotEmpty(t, waiting.response.retryAfter)
	require.NoError(t, dockerCommand(t.Context(), "unpause", broker.Container.GetContainerID()))

	// The same point keeps its identity across retry and across OTLP encodings.
	jsonWire, err := protojson.Marshal(req)
	require.NoError(t, err)
	var compressed bytes.Buffer
	gzipWriter := gzip.NewWriter(&compressed)
	_, err = gzipWriter.Write(jsonWire)
	require.NoError(t, err)
	require.NoError(t, gzipWriter.Close())
	require.Eventually(t, func() bool {
		recovered, postErr := post(t.Context(), httpClient, metricsURL, owner, compressed.Bytes(), "application/json", "gzip")
		return postErr == nil && recovered.code == http.StatusOK && recovered.mediaType == "application/json"
	}, 20*time.Second, 200*time.Millisecond)
	after := fetchedSample(t, consumer, owner)
	require.Equal(t, before.GetSampleId(), after.GetSampleId())
	require.NoError(t, grpcExport(t.Context(), exporter, owner, req, grpc.UseCompressor("gzip")))
	grpcSample := fetchedSample(t, consumer, owner)
	require.Equal(t, before.GetSampleId(), grpcSample.GetSampleId())

	quota := redis.NewClient(&redis.Options{Addr: redisAddress, MaxRetries: -1})
	t.Cleanup(func() { _ = quota.Close() })
	limit := receiver.NewRedisQuota(quota, 1, 2)
	a, b := uuid.New(), uuid.New()
	admitted, err := limit.Charge(t.Context(), a, 2)
	require.NoError(t, err)
	require.True(t, admitted)
	admitted, err = limit.Charge(t.Context(), a, 1)
	require.NoError(t, err)
	require.False(t, admitted)
	admitted, err = limit.Charge(t.Context(), b, 2)
	require.NoError(t, err)
	require.True(t, admitted)
}

func startRedis(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	container, err := tcredis.Run(ctx, "redis:7.4-alpine")
	if container != nil {
		testcontainers.CleanupContainer(t, container)
	}
	require.NoError(t, err)
	address, err := container.Endpoint(ctx, "")
	require.NoError(t, err)
	return address
}

func exportRequest(untrustedOrg uuid.UUID) *otlp.ExportMetricsServiceRequest {
	return &otlp.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
			Key: "organization_id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: untrustedOrg.String()}},
		}}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{Name: "integration"},
			Metrics: []*metricspb.Metric{{
				Name: "work", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
					TimeUnixNano: uint64(time.Now().UnixNano()), Value: &metricspb.NumberDataPoint_AsInt{AsInt: 42},
				}}}},
			}},
		}},
	}}}
}

type httpResult struct {
	code       int
	mediaType  string
	retryAfter string
}

func post(ctx context.Context, client *http.Client, endpoint string, id tenant, data []byte, media, encoding string) (httpResult, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return httpResult{}, err
	}
	if id.key != "" {
		request.Header.Set("Authorization", "Bearer "+id.key)
		request.Header.Set("x-infrapilot-resource-id", id.resource.String())
	}
	request.Header.Set("Content-Type", media)
	if encoding != "" {
		request.Header.Set("Content-Encoding", encoding)
	}
	response, err := client.Do(request)
	if err != nil {
		return httpResult{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		return httpResult{}, err
	}
	return httpResult{response.StatusCode, response.Header.Get("Content-Type"), response.Header.Get("Retry-After")}, nil
}

func grpcExport(ctx context.Context, client otlp.MetricsServiceClient, id tenant, req *otlp.ExportMetricsServiceRequest, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+id.key, "x-infrapilot-resource-id", id.resource.String())
	_, err := client.Export(ctx, req, opts...)
	return err
}

func dockerCommand(ctx context.Context, verb, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", verb, id).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w: %s", verb, err, out)
	}
	return nil
}

func scrapeInFlight(ctx context.Context, client *http.Client, endpoint string) int {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return -1
	}
	resp, err := client.Do(req)
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return -1
	}
	for line := range strings.SplitSeq(string(payload), "\n") {
		if !strings.HasPrefix(line, "telemetry_ingestion_in_flight") {
			continue
		}
		value := line[strings.LastIndexByte(line, ' ')+1:]
		n, parseErr := strconv.Atoi(value)
		if parseErr == nil {
			return n
		}
	}
	return -1
}

func fetchedSample(t *testing.T, consumer *kgo.Client, id tenant) *telemetryv1.MetricsSample {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	fetches := consumer.PollRecords(ctx, 1)
	require.Empty(t, fetches.Errors())
	records := fetches.Records()
	require.Len(t, records, 1)
	require.Equal(t, id.org.String()+"/"+id.resource.String(), string(records[0].Key))
	message := new(eventsv1.Envelope)
	require.NoError(t, proto.Unmarshal(records[0].Value, message))
	require.NoError(t, envelope.Validate(message))
	require.Equal(t, id.org.String(), message.GetOrganizationId())
	require.Equal(t, domain.EventType, message.GetEventType())
	sample := new(telemetryv1.MetricsSample)
	require.NoError(t, envelope.Unpack(message, sample))
	require.Equal(t, id.project.String(), sample.GetProjectId())
	require.Equal(t, id.resource.String(), sample.GetResourceId())
	return sample
}
