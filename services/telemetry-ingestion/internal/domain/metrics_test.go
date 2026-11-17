package domain_test

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/domain"
)

func attr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}

func testMetric(at time.Time) (*metricspb.ResourceMetrics, *metricspb.NumberDataPoint) {
	point := &metricspb.NumberDataPoint{
		TimeUnixNano: uint64(at.UnixNano()), Value: &metricspb.NumberDataPoint_AsInt{AsInt: 9_007_199_254_740_993},
		Attributes: []*commonpb.KeyValue{attr("a", "a"), attr("b", "b")},
	}
	resource := &metricspb.ResourceMetrics{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{attr("service", "test"), attr("namespace", "samples")}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope: &commonpb.InstrumentationScope{Name: "checks", Version: "v1"},
			Metrics: []*metricspb.Metric{{
				Name: "latency", Unit: "ms", Description: "latency",
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{point}}},
			}},
		}},
	}
	return resource, point
}

func TestSamplesStableSeriesAndSampleIdentity(t *testing.T) {
	now := time.Now()
	resource, point := testMetric(now)
	identity := domain.Binding{OrganizationID: uuid.New(), ProjectID: uuid.New(), ResourceID: uuid.New()}
	batch := []*metricspb.ResourceMetrics{resource}
	first, err := domain.Samples(batch, identity, now, 4, 1<<20)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, int64(9_007_199_254_740_993), first[0].GetIntValue())
	require.Equal(t, identity.ResourceID.String(), first[0].GetResourceId())
	preserved := new(metricspb.ResourceMetrics)
	require.NoError(t, proto.Unmarshal(first[0].GetOtlpPayload(), preserved))
	require.True(t, proto.Equal(resource, preserved))

	slices.Reverse(point.Attributes)
	slices.Reverse(resource.Resource.Attributes)
	point.Value = &metricspb.NumberDataPoint_AsInt{AsInt: 42}
	resource.ScopeMetrics[0].Metrics[0].Description = "renamed documentation"
	retry, err := domain.Samples(batch, identity, now.Add(time.Second), 4, 1<<20)
	require.NoError(t, err)
	require.Equal(t, first[0].GetSeriesId(), retry[0].GetSeriesId())
	require.Equal(t, first[0].GetSampleId(), retry[0].GetSampleId())

	point.TimeUnixNano++
	next, err := domain.Samples(batch, identity, now, 4, 1<<20)
	require.NoError(t, err)
	require.Equal(t, first[0].GetSeriesId(), next[0].GetSeriesId())
	require.NotEqual(t, first[0].GetSampleId(), next[0].GetSampleId())

	identity.OrganizationID = uuid.New()
	foreign, err := domain.Samples(batch, identity, now, 4, 1<<20)
	require.NoError(t, err)
	require.NotEqual(t, next[0].GetSeriesId(), foreign[0].GetSeriesId())
}

func TestSamplesValidityAndLimits(t *testing.T) {
	now := time.Now()
	identity := domain.Binding{OrganizationID: uuid.New(), ProjectID: uuid.New(), ResourceID: uuid.New()}
	for _, test := range []struct {
		name           string
		edit           func(*metricspb.ResourceMetrics, *metricspb.NumberDataPoint)
		points, budget int
	}{
		{"duplicate attributes", func(_ *metricspb.ResourceMetrics, p *metricspb.NumberDataPoint) {
			p.Attributes = append(p.Attributes, p.Attributes[0])
		}, 1, 1 << 20},
		{"missing time", func(_ *metricspb.ResourceMetrics, p *metricspb.NumberDataPoint) { p.TimeUnixNano = 0 }, 1, 1 << 20},
		{"time outside retention", func(_ *metricspb.ResourceMetrics, p *metricspb.NumberDataPoint) {
			p.TimeUnixNano = uint64(now.Add(-8 * 24 * time.Hour).UnixNano())
		}, 1, 1 << 20},
		{"reversed window", func(_ *metricspb.ResourceMetrics, p *metricspb.NumberDataPoint) {
			p.StartTimeUnixNano = p.TimeUnixNano + 1
		}, 1, 1 << 20},
		{"point count", func(r *metricspb.ResourceMetrics, p *metricspb.NumberDataPoint) {
			gauge := r.ScopeMetrics[0].Metrics[0].GetGauge()
			gauge.DataPoints = append(gauge.DataPoints, p)
		}, 1, 1 << 20},
		{"serialized limit", func(*metricspb.ResourceMetrics, *metricspb.NumberDataPoint) {}, 1, 32},
	} {
		t.Run(test.name, func(t *testing.T) {
			resource, point := testMetric(now)
			test.edit(resource, point)
			_, err := domain.Samples([]*metricspb.ResourceMetrics{resource}, identity, now, test.points, test.budget)
			require.ErrorIs(t, err, domain.ErrInvalid)
		})
	}
}

func BenchmarkSamples(b *testing.B) {
	now := time.Now()
	resource, _ := testMetric(now)
	identity := domain.Binding{OrganizationID: uuid.New(), ProjectID: uuid.New(), ResourceID: uuid.New()}
	batch := []*metricspb.ResourceMetrics{resource}
	b.ReportAllocs()
	for b.Loop() {
		_, err := domain.Samples(batch, identity, now, 1, 1<<20)
		if err != nil {
			b.Fatal(err)
		}
	}
}
