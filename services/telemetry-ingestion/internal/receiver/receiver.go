// Package receiver authenticates, limits and publishes OTLP metrics over HTTP and gRPC.
package receiver

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cpv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/controlplane/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/pkg/kafka"
	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/domain"
)

const resourceHeader = "x-infrapilot-resource-id"

var (
	errUnauthenticated = errors.New("a valid ingest credential is required")
	errForbidden       = errors.New("credential is not permitted for the resource")
	errBusy            = errors.New("telemetry ingestion is at capacity")
	errQuota           = errors.New("organization ingest quota exceeded")
	errUnavailable     = errors.New("telemetry ingestion is temporarily unavailable")
	errTooLarge        = errors.New("export exceeds the request byte limit")
)

// Limits apply across both transports.
type Limits struct {
	RequestBytes int
	BatchBytes   int
	PointCount   int
	InFlight     int
	Deadline     time.Duration
}

// Quota debits a single authenticated organization before any publication.
type Quota interface {
	Charge(ctx context.Context, org uuid.UUID, points int) (bool, error)
}

// Receiver owns the shared admission counter and OTLP export path.
type Receiver struct {
	metricspb.UnimplementedMetricsServiceServer
	limits   Limits
	active   chan struct{}
	auth     cpv1.IngestionAuthorizationServiceClient
	quota    Quota
	producer *kgo.Client
	tracer   trace.Tracer
	exports  metric.Int64Counter
	accepted metric.Int64Counter
	inFlight metric.Int64UpDownCounter
}

// New builds a receiver with the shared admission limits and dependency clients.
func New(limits Limits, auth cpv1.IngestionAuthorizationServiceClient, quota Quota, producer *kgo.Client, meter metric.Meter, tracer trace.Tracer) (*Receiver, error) {
	if limits.RequestBytes <= 0 || limits.BatchBytes <= 0 || limits.PointCount <= 0 || limits.InFlight <= 0 || limits.Deadline <= 0 {
		return nil, errors.New("ingestion limits must be positive")
	}
	exports, err := meter.Int64Counter("telemetry_ingestion_exports_total", metric.WithDescription("Completed OTLP export requests by outcome"))
	if err != nil {
		return nil, fmt.Errorf("ingestion metrics: %w", err)
	}
	accepted, err := meter.Int64Counter("telemetry_ingestion_accepted_points_total", metric.WithDescription("Metric points in fully acknowledged exports"))
	if err != nil {
		return nil, fmt.Errorf("ingestion metrics: %w", err)
	}
	inFlight, err := meter.Int64UpDownCounter("telemetry_ingestion_in_flight", metric.WithDescription("Admitted OTLP export requests in flight"))
	if err != nil {
		return nil, fmt.Errorf("ingestion metrics: %w", err)
	}
	return &Receiver{limits: limits, active: make(chan struct{}, limits.InFlight), auth: auth, quota: quota,
		producer: producer, tracer: tracer, exports: exports, accepted: accepted, inFlight: inFlight}, nil
}

func (r *Receiver) admit(ctx context.Context) (func(), error) {
	select {
	case r.active <- struct{}{}:
		r.inFlight.Add(ctx, 1)
		return func() {
			<-r.active
			r.inFlight.Add(ctx, -1)
		}, nil
	default:
		return nil, errBusy
	}
}

func (r *Receiver) authorize(ctx context.Context, authorization, resources []string) (domain.Binding, error) {
	if len(authorization) != 1 {
		return domain.Binding{}, errUnauthenticated
	}
	scheme, key, ok := strings.Cut(authorization[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return domain.Binding{}, errUnauthenticated
	}
	suffix, ok := strings.CutPrefix(key, "ipk_")
	if !ok || len(suffix) != 43 {
		return domain.Binding{}, errUnauthenticated
	}
	if _, err := base64.RawURLEncoding.Strict().DecodeString(suffix); err != nil {
		return domain.Binding{}, errUnauthenticated
	}
	if len(resources) != 1 {
		return domain.Binding{}, fmt.Errorf("%w: exactly one resource identifier is required", domain.ErrInvalid)
	}
	resource, err := parseUUID(resources[0])
	if err != nil {
		return domain.Binding{}, fmt.Errorf("%w: invalid resource identifier", domain.ErrInvalid)
	}

	digest := sha256.Sum256([]byte(key))
	out, err := r.auth.ValidateIngestResource(ctx, &cpv1.ValidateIngestResourceRequest{KeySha256: digest[:], ResourceId: resource.String()})
	if err != nil {
		switch status.Code(err) {
		case codes.Unauthenticated:
			return domain.Binding{}, errUnauthenticated
		case codes.NotFound, codes.PermissionDenied:
			return domain.Binding{}, errForbidden
		default:
			return domain.Binding{}, errUnavailable
		}
	}
	org, orgErr := parseUUID(out.GetOrganizationId())
	project, projectErr := parseUUID(out.GetProjectId())
	if orgErr != nil || projectErr != nil {
		return domain.Binding{}, errUnavailable
	}
	return domain.Binding{OrganizationID: org, ProjectID: project, ResourceID: resource}, nil
}

func parseUUID(raw string) (uuid.UUID, error) {
	u, err := uuid.Parse(raw)
	if err != nil || u == uuid.Nil || u.String() != raw {
		return uuid.Nil, errors.New("not a canonical UUID")
	}
	return u, nil
}

func (r *Receiver) publish(ctx context.Context, req *metricspb.ExportMetricsServiceRequest, identity domain.Binding) error {
	samples, err := domain.Samples(req.GetResourceMetrics(), identity, time.Now(), r.limits.PointCount, r.limits.BatchBytes)
	if err != nil || len(samples) == 0 {
		return err
	}
	admitted, err := r.quota.Charge(ctx, identity.OrganizationID, len(samples))
	if err != nil {
		return errUnavailable
	}
	if !admitted {
		return errQuota
	}

	ctx, span := r.tracer.Start(ctx, "publish telemetry.metrics.v1", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	key := []byte(identity.OrganizationID.String() + "/" + identity.ResourceID.String())
	records := make([]*kgo.Record, 0, len(samples))
	batchBytes := 0
	for _, sample := range samples {
		message, makeErr := envelope.New(ctx, envelope.Params{
			Type: domain.EventType, SchemaVersion: 1, OrganizationID: identity.OrganizationID.String(),
			SubjectID: identity.ResourceID.String(), OccurredAt: sample.GetObservedAt().AsTime(),
			Producer: "telemetry-ingestion", Payload: sample,
		})
		if makeErr != nil {
			return errUnavailable
		}
		value, marshalErr := proto.Marshal(message)
		if marshalErr != nil {
			return errUnavailable
		}
		batchBytes += len(key) + len(value) + len(message.GetTraceParent()) + len("traceparent")
		if batchBytes > r.limits.BatchBytes {
			return fmt.Errorf("%w: export exceeds broker batch limit", domain.ErrInvalid)
		}
		record := &kgo.Record{Topic: domain.Topic, Key: key, Value: value}
		if parent := message.GetTraceParent(); parent != "" {
			record.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte(parent)}}
		}
		records = append(records, record)
	}
	if err := kafka.Publish(ctx, r.producer, records...); err != nil {
		span.RecordError(err)
		if errors.Is(err, kafka.ErrQueueFull) {
			return errBusy
		}
		return errUnavailable
	}
	r.accepted.Add(ctx, int64(len(samples)))
	return nil
}

func (r *Receiver) recordOutcome(ctx context.Context, err error) {
	outcome := "accepted"
	switch {
	case errors.Is(err, errBusy):
		outcome = "busy"
	case errors.Is(err, errQuota):
		outcome = "quota"
	case errors.Is(err, errUnavailable):
		outcome = "unavailable"
	case errors.Is(err, errUnauthenticated), errors.Is(err, errForbidden):
		outcome = "denied"
	case err != nil:
		outcome = "invalid"
	}
	r.exports.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}
