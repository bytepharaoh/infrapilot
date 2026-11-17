// Package domain validates OTLP points and assigns stable series and sample identities.
package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	telemetryv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/telemetry/v1"
)

const (
	// Topic carries the normalized metric envelopes.
	Topic = "telemetry.metrics.v1"
	// EventType identifies the typed observation in the Envelope.
	EventType     = "telemetry.metrics.observed"
	maxPointBytes = 512 << 10
	maxSampleAge  = 7 * 24 * time.Hour
	maxClockSkew  = 5 * time.Minute
)

// ErrInvalid marks exports whose payloads must be corrected before retry.
var ErrInvalid = errors.New("invalid metric export")

// Binding is the identity established by the control-plane ingest authorization RPC.
type Binding struct {
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	ResourceID     uuid.UUID
}

// Samples validates a request and extracts its metric points. Shared resource and scope descriptors
// are repeated in the preserved OTLP messages, so the output budget also bounds serialization.
func Samples(resources []*metricspb.ResourceMetrics, identity Binding, now time.Time, maxPoints, byteBudget int) ([]*telemetryv1.MetricsSample, error) {
	var samples []*telemetryv1.MetricsSample
	for _, resource := range resources {
		if resource == nil {
			return nil, invalid("empty resource metrics")
		}
		resourceAttrs, err := attributeBytes(resource.GetResource().GetAttributes())
		if err != nil {
			return nil, err
		}
		for _, scope := range resource.GetScopeMetrics() {
			if scope == nil {
				return nil, invalid("empty scope metrics")
			}
			scopeAttrs, attrErr := attributeBytes(scope.GetScope().GetAttributes())
			if attrErr != nil {
				return nil, attrErr
			}
			scopeName, scopeVersion := scope.GetScope().GetName(), scope.GetScope().GetVersion()
			for _, metric := range scope.GetMetrics() {
				name := metric.GetName()
				if name == "" || len(name) > 255 || !utf8.ValidString(name) || strings.ContainsFunc(name, unicode.IsControl) {
					return nil, invalid("metric name must have 1 to 255 UTF-8 bytes without control characters")
				}
				metricIdentity := func(kind string, temporality metricspb.AggregationTemporality, monotonic bool) []byte {
					var options [5]byte
					binary.BigEndian.PutUint32(options[:4], uint32(int64(temporality)&0xffffffff))
					options[4] = byteFor(monotonic)
					sum := sha256.New()
					writeIdentity(sum, []byte("infrapilot-series-v1"), identity.OrganizationID[:], identity.ProjectID[:],
						identity.ResourceID[:], resourceAttrs, []byte(scopeName), []byte(scopeVersion), scopeAttrs,
						[]byte(name), []byte(metric.GetUnit()), []byte(kind), options[:])
					return sum.Sum(nil)
				}
				emit := func(point dataPoint, single *metricspb.Metric, descriptor []byte) error {
					if len(samples) >= maxPoints {
						return invalid("too many metric points")
					}
					sample, sampleErr := makeSample(identity, now, resource, scope, single, point, descriptor)
					if sampleErr != nil {
						return sampleErr
					}
					// Reserve the measured Protobuf size plus transport envelope fields (IDs and trace context).
					byteBudget -= proto.Size(sample) + 384
					if byteBudget < 0 {
						return invalid("serialized metric export exceeds broker batch limit")
					}
					samples = append(samples, sample)
					return nil
				}
				if visitErr := visitMetric(metric, metricIdentity, emit); visitErr != nil {
					return nil, visitErr
				}
			}
		}
	}
	return samples, nil
}

type dataPoint interface {
	GetAttributes() []*commonpb.KeyValue
	GetFlags() uint32
	GetStartTimeUnixNano() uint64
	GetTimeUnixNano() uint64
}

func validTemporality(t metricspb.AggregationTemporality) bool {
	return t == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA ||
		t == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
}

func makeSample(identity Binding, now time.Time, resource *metricspb.ResourceMetrics, scope *metricspb.ScopeMetrics, metric *metricspb.Metric, point dataPoint, descriptor []byte) (*telemetryv1.MetricsSample, error) {
	ns := point.GetTimeUnixNano()
	start := point.GetStartTimeUnixNano()
	if ns == 0 || ns > math.MaxInt64 || start > ns {
		return nil, invalid("invalid metric time window")
	}
	observed := time.Unix(0, int64(ns))
	if observed.Before(now.Add(-maxSampleAge)) || observed.After(now.Add(maxClockSkew)) {
		return nil, invalid("metric observation is outside the accepted time range")
	}
	attrs, err := attributeBytes(point.GetAttributes())
	if err != nil {
		return nil, err
	}
	seriesHash := sha256.New()
	writeIdentity(seriesHash, descriptor, attrs)
	series := seriesHash.Sum(nil)
	// The source timestamp precision is preserved in the sample identity, including sub-microsecond readings.
	var window [16]byte
	binary.BigEndian.PutUint64(window[:8], start)
	binary.BigEndian.PutUint64(window[8:], ns)
	sampleHash := sha256.New()
	writeIdentity(sampleHash, []byte("infrapilot-sample-v1"), series, window[:])

	singleResource := &metricspb.ResourceMetrics{
		Resource:  resource.GetResource(),
		SchemaUrl: resource.GetSchemaUrl(),
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: scope.GetScope(), SchemaUrl: scope.GetSchemaUrl(), Metrics: []*metricspb.Metric{metric},
		}},
	}
	if proto.Size(singleResource) > maxPointBytes {
		return nil, invalid("serialized metric point exceeds broker record limit")
	}
	payload, err := proto.Marshal(singleResource)
	if err != nil {
		return nil, invalid("invalid metric point encoding")
	}
	sample := &telemetryv1.MetricsSample{
		ProjectId: identity.ProjectID.String(), ResourceId: identity.ResourceID.String(), MetricName: metric.GetName(),
		SeriesId: hex.EncodeToString(series), SampleId: hex.EncodeToString(sampleHash.Sum(nil)),
		ObservedAt: timestamppb.New(observed), OtlpPayload: payload,
	}
	if number, ok := point.(*metricspb.NumberDataPoint); ok {
		switch value := number.GetValue().(type) {
		case *metricspb.NumberDataPoint_AsInt:
			sample.Value = &telemetryv1.MetricsSample_IntValue{IntValue: value.AsInt}
		case *metricspb.NumberDataPoint_AsDouble:
			if !math.IsInf(value.AsDouble, 0) && !math.IsNaN(value.AsDouble) {
				sample.Value = &telemetryv1.MetricsSample_DoubleValue{DoubleValue: value.AsDouble}
			}
		}
		if point.GetFlags()&uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK) != 0 {
			sample.Value = nil
		} else if number.GetValue() == nil {
			return nil, invalid("scalar metric point has no value")
		}
	}
	return sample, nil
}

func visitMetric(m *metricspb.Metric, identity func(string, metricspb.AggregationTemporality, bool) []byte, emit func(dataPoint, *metricspb.Metric, []byte) error) error {
	newPointMetric := func() *metricspb.Metric {
		return &metricspb.Metric{Name: m.GetName(), Unit: m.GetUnit(), Description: m.GetDescription(), Metadata: m.GetMetadata()}
	}
	switch d := m.GetData().(type) {
	case *metricspb.Metric_Gauge:
		return visitPoints(d.Gauge.GetDataPoints(), identity("gauge", 0, false), func(p *metricspb.NumberDataPoint) *metricspb.Metric {
			single := newPointMetric()
			single.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{p}}}
			return single
		}, emit)
	case *metricspb.Metric_Sum:
		t, mono := d.Sum.GetAggregationTemporality(), d.Sum.GetIsMonotonic()
		if !validTemporality(t) {
			return invalid("sum metric has no valid aggregation temporality")
		}
		return visitPoints(d.Sum.GetDataPoints(), identity("sum", t, mono), func(p *metricspb.NumberDataPoint) *metricspb.Metric {
			single := newPointMetric()
			single.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{AggregationTemporality: t, IsMonotonic: mono, DataPoints: []*metricspb.NumberDataPoint{p}}}
			return single
		}, emit)
	case *metricspb.Metric_Histogram:
		t := d.Histogram.GetAggregationTemporality()
		if !validTemporality(t) {
			return invalid("histogram has no valid aggregation temporality")
		}
		return visitPoints(d.Histogram.GetDataPoints(), identity("histogram", t, false), func(p *metricspb.HistogramDataPoint) *metricspb.Metric {
			single := newPointMetric()
			single.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{AggregationTemporality: t, DataPoints: []*metricspb.HistogramDataPoint{p}}}
			return single
		}, emit)
	case *metricspb.Metric_ExponentialHistogram:
		t := d.ExponentialHistogram.GetAggregationTemporality()
		if !validTemporality(t) {
			return invalid("exponential histogram has no valid aggregation temporality")
		}
		return visitPoints(d.ExponentialHistogram.GetDataPoints(), identity("exponential_histogram", t, false), func(p *metricspb.ExponentialHistogramDataPoint) *metricspb.Metric {
			single := newPointMetric()
			single.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
				AggregationTemporality: t, DataPoints: []*metricspb.ExponentialHistogramDataPoint{p},
			}}
			return single
		}, emit)
	case *metricspb.Metric_Summary:
		return visitPoints(d.Summary.GetDataPoints(), identity("summary", 0, false), func(p *metricspb.SummaryDataPoint) *metricspb.Metric {
			single := newPointMetric()
			single.Data = &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: []*metricspb.SummaryDataPoint{p}}}
			return single
		}, emit)
	default:
		return invalid("metric has no supported point kind")
	}
}

func visitPoints[P dataPoint](points []P, descriptor []byte, single func(P) *metricspb.Metric, emit func(dataPoint, *metricspb.Metric, []byte) error) error {
	for _, point := range points {
		if err := emit(point, single(point), descriptor); err != nil {
			return err
		}
	}
	return nil
}

func writeIdentity(h hash.Hash, fields ...[]byte) {
	for _, field := range fields {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(field)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(field)
	}
}

func byteFor(value bool) byte {
	if value {
		return 1
	}
	return 0
}

func invalid(message string) error { return fmt.Errorf("%w: %s", ErrInvalid, message) }
