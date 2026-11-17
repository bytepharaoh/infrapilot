// Package service validates production events and coordinates durable stream writes.
package service

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	eventsv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/events/v1"
	telemetryv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/telemetry/v1"
	"github.com/bytepharaoh/infrapilot/pkg/envelope"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/domain"
)

const (
	ChecksTopic     = "check.results.v1"
	MetricsTopic    = "telemetry.metrics.v1"
	SignalsTopic    = "signals.v1"
	MaxBatchRecords = 32
)

var ErrExpired = errors.New("stream record outside raw retention")

func DecodeEnvelope(raw []byte) (*eventsv1.Envelope, error) {
	var env eventsv1.Envelope
	if err := proto.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	if err := envelope.Validate(&env); err != nil {
		return nil, err
	}
	if env.GetSchemaVersion() != 1 {
		return nil, errors.New("unsupported stream envelope schema")
	}
	return &env, nil
}

func DecodeCheck(env *eventsv1.Envelope, key string, now time.Time) (domain.Check, error) {
	if env.GetEventType() != "checks.result.observed" || env.GetProducer() != "check-runner" {
		return domain.Check{}, errors.New("unexpected check result producer or event type")
	}
	var result checksv1.CheckResult
	if err := envelope.Unpack(env, &result); err != nil {
		return domain.Check{}, err
	}
	scope, err := domain.ParseScope(env.GetOrganizationId(), result.GetProjectId(), result.GetResourceId())
	if err != nil {
		return domain.Check{}, err
	}
	id, err := domain.ParseID(result.GetCheckId(), "check_id")
	if err != nil {
		return domain.Check{}, err
	}
	if env.GetSubjectId() != result.GetCheckId() || key != env.GetOrganizationId()+"/"+result.GetCheckId() {
		return domain.Check{}, errors.New("check result routing key mismatch")
	}
	if result.GetScheduledAt().CheckValid() != nil || result.GetObservedAt().CheckValid() != nil ||
		result.GetLeaseEpoch() == 0 || result.GetLeaseEpoch() > math.MaxInt64 ||
		result.GetDurationMs() < 0 || !finite(result.GetDurationMs()) ||
		len(env.GetPayload().GetValue()) > 16<<10 {
		return domain.Check{}, errors.New("invalid check result fields")
	}
	if t := result.GetThresholds(); t != nil && t.MaxDurationMs != nil && (!finite(t.GetMaxDurationMs()) || t.GetMaxDurationMs() < 0) {
		return domain.Check{}, errors.New("invalid check duration threshold")
	}
	succeeded := result.GetOutcome() == checksv1.CheckOutcome_CHECK_OUTCOME_SUCCESS
	if !succeeded && result.GetOutcome() != checksv1.CheckOutcome_CHECK_OUTCOME_FAILURE {
		return domain.Check{}, errors.New("invalid check outcome")
	}
	scheduled, observed := result.GetScheduledAt().AsTime(), result.GetObservedAt().AsTime()
	if scheduled.After(now.Add(5*time.Minute)) || observed.Before(scheduled) || observed.After(now.Add(5*time.Minute)) {
		return domain.Check{}, errors.New("invalid check result time")
	}
	if scheduled.Before(now.Add(-domain.MaxRange)) {
		return domain.Check{}, ErrExpired
	}
	return domain.Check{
		Scope: scope, CheckID: id, ScheduledAt: scheduled, ObservedAt: observed,
		LeaseEpoch: int64(result.GetLeaseEpoch()), Succeeded: succeeded, DurationMs: result.GetDurationMs(),
		Payload: env.GetPayload().GetValue(), Result: &result, TraceParent: env.GetTraceParent(),
	}, nil
}

func DecodeMetric(env *eventsv1.Envelope, key string, now time.Time) (domain.Metric, error) {
	if env.GetEventType() != "telemetry.metrics.observed" || env.GetProducer() != "telemetry-ingestion" {
		return domain.Metric{}, errors.New("unexpected metric producer or event type")
	}
	var sample telemetryv1.MetricsSample
	if err := envelope.Unpack(env, &sample); err != nil {
		return domain.Metric{}, err
	}
	scope, err := domain.ParseScope(env.GetOrganizationId(), sample.GetProjectId(), sample.GetResourceId())
	if err != nil {
		return domain.Metric{}, err
	}
	if env.GetSubjectId() != sample.GetResourceId() || key != env.GetOrganizationId()+"/"+sample.GetResourceId() {
		return domain.Metric{}, errors.New("metric routing key mismatch")
	}
	if len(sample.GetMetricName()) == 0 || len(sample.GetMetricName()) > 255 || strings.ContainsRune(sample.GetMetricName(), 0) ||
		sample.GetObservedAt().CheckValid() != nil || len(sample.GetOtlpPayload()) == 0 || len(sample.GetOtlpPayload()) > 512<<10 {
		return domain.Metric{}, errors.New("invalid metric fields")
	}
	series, seriesErr := hex.DecodeString(sample.GetSeriesId())
	id, idErr := hex.DecodeString(sample.GetSampleId())
	if seriesErr != nil || idErr != nil || len(series) != 32 || len(id) != 32 {
		return domain.Metric{}, errors.New("invalid metric identity")
	}
	observed := sample.GetObservedAt().AsTime()
	if observed.After(now.Add(5 * time.Minute)) {
		return domain.Metric{}, errors.New("metric time beyond allowed clock skew")
	}
	if observed.Before(now.Add(-domain.MaxRange)) {
		return domain.Metric{}, ErrExpired
	}
	var scalar *float64
	switch v := sample.GetValue().(type) {
	case *telemetryv1.MetricsSample_DoubleValue:
		if !finite(v.DoubleValue) {
			return domain.Metric{}, errors.New("non-finite metric scalar")
		}
		scalar = &v.DoubleValue
	case *telemetryv1.MetricsSample_IntValue:
		converted := float64(v.IntValue)
		scalar = &converted
	}
	return domain.Metric{Scope: scope, Name: sample.GetMetricName(), ObservedAt: observed, SeriesID: series, SampleID: id,
		Scalar: scalar, Payload: env.GetPayload().GetValue()}, nil
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
