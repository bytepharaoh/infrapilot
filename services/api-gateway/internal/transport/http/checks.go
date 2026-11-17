package http

import (
	"context"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	telemetryv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/telemetry/v1"
	"github.com/bytepharaoh/infrapilot/services/api-gateway/internal/transport/http/api"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var checkKindToProto = map[api.CheckKind]checksv1.CheckKind{
	"http": checksv1.CheckKind_CHECK_KIND_HTTP, "tcp": checksv1.CheckKind_CHECK_KIND_TCP,
	"tls": checksv1.CheckKind_CHECK_KIND_TLS, "dns": checksv1.CheckKind_CHECK_KIND_DNS,
	"icmp": checksv1.CheckKind_CHECK_KIND_ICMP,
}

var checkKindFromProto = map[checksv1.CheckKind]api.CheckKind{
	checksv1.CheckKind_CHECK_KIND_HTTP: "http", checksv1.CheckKind_CHECK_KIND_TCP: "tcp",
	checksv1.CheckKind_CHECK_KIND_TLS: "tls", checksv1.CheckKind_CHECK_KIND_DNS: "dns",
	checksv1.CheckKind_CHECK_KIND_ICMP: "icmp",
}

func checkSpec(in *api.CheckSpec) *checksv1.CheckSpec {
	if in == nil {
		return nil
	}
	out := &checksv1.CheckSpec{ResourceId: in.ResourceId.String(), Kind: checkKindToProto[in.Kind],
		Target: in.Target, IntervalSeconds: uint32(in.IntervalSeconds), TimeoutSeconds: uint32(in.TimeoutSeconds), Enabled: in.Enabled}
	if in.Port != nil {
		out.Port = uint32(*in.Port)
	}
	if in.Thresholds != nil {
		out.Thresholds = &checksv1.Thresholds{MaxDurationMs: in.Thresholds.MaxDurationMs}
		if in.Thresholds.MinTlsDays != nil {
			days := uint32(*in.Thresholds.MinTlsDays)
			out.Thresholds.MinTlsDays = &days
		}
	}
	return out
}

func checkFromProto(in *checksv1.Check) api.Check {
	spec := in.GetSpec()
	out := api.Check{Id: id(in.GetId()), OrganizationId: id(in.GetOrganizationId()), ProjectId: id(in.GetProjectId()),
		CreatedAt: in.GetCreatedAt().AsTime(), UpdatedAt: in.GetUpdatedAt().AsTime(), Spec: api.CheckSpec{
			ResourceId: id(spec.GetResourceId()), Kind: checkKindFromProto[spec.GetKind()], Target: spec.GetTarget(),
			IntervalSeconds: int32(spec.GetIntervalSeconds()), TimeoutSeconds: int32(spec.GetTimeoutSeconds()), Enabled: spec.GetEnabled(),
		}}
	if spec.GetPort() > 0 {
		port := int32(spec.GetPort())
		out.Spec.Port = &port
	}
	if x := spec.GetThresholds(); x != nil {
		out.Spec.Thresholds = &api.CheckThresholds{MaxDurationMs: x.MaxDurationMs}
		if x.MinTlsDays != nil {
			days := int32(x.GetMinTlsDays())
			out.Spec.Thresholds.MinTlsDays = &days
		}
	}
	return out
}

func (h *Handlers) ListChecks(ctx context.Context, r api.ListChecksRequestObject) (api.ListChecksResponseObject, error) {
	ctx, err := out(ctx)
	if err != nil {
		return nil, err
	}
	size, token := pageArgs(r.Params.Limit, r.Params.Cursor)
	resp, err := h.up.Checks.ListChecks(ctx, &checksv1.ListChecksRequest{OrganizationId: r.OrgId.String(), ProjectId: r.ProjectId.String(), PageSize: size, PageToken: token})
	if err != nil {
		return nil, err
	}
	items := make([]api.Check, 0, len(resp.GetChecks()))
	for _, item := range resp.GetChecks() {
		items = append(items, checkFromProto(item))
	}
	return api.ListChecks200JSONResponse{Body: api.CheckList{Items: items, Page: page(resp.GetNextPageToken())}}, nil
}

func (h *Handlers) CreateCheck(ctx context.Context, r api.CreateCheckRequestObject) (api.CreateCheckResponseObject, error) {
	ctx, err := out(withIdem(ctx, r.Params.IdempotencyKey))
	if err != nil {
		return nil, err
	}
	req := &checksv1.CreateCheckRequest{OrganizationId: r.OrgId.String(), ProjectId: r.ProjectId.String(), Spec: checkSpec(r.Body)}
	if r.Params.IdempotencyKey != nil {
		req.IdempotencyKey = *r.Params.IdempotencyKey
	}
	resp, err := h.up.Checks.CreateCheck(ctx, req)
	if err != nil {
		return nil, err
	}
	return api.CreateCheck201JSONResponse{Body: checkFromProto(resp.GetCheck())}, nil
}

func (h *Handlers) GetCheck(ctx context.Context, r api.GetCheckRequestObject) (api.GetCheckResponseObject, error) {
	ctx, err := out(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := h.up.Checks.GetCheck(ctx, &checksv1.GetCheckRequest{OrganizationId: r.OrgId.String(), ProjectId: r.ProjectId.String(), CheckId: r.CheckId.String()})
	if err != nil {
		return nil, err
	}
	return api.GetCheck200JSONResponse{Body: checkFromProto(resp.GetCheck())}, nil
}

func (h *Handlers) UpdateCheck(ctx context.Context, r api.UpdateCheckRequestObject) (api.UpdateCheckResponseObject, error) {
	ctx, err := out(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := h.up.Checks.UpdateCheck(ctx, &checksv1.UpdateCheckRequest{OrganizationId: r.OrgId.String(), ProjectId: r.ProjectId.String(), CheckId: r.CheckId.String(), Spec: checkSpec(r.Body)})
	if err != nil {
		return nil, err
	}
	return api.UpdateCheck200JSONResponse{Body: checkFromProto(resp.GetCheck())}, nil
}

func (h *Handlers) DeleteCheck(ctx context.Context, r api.DeleteCheckRequestObject) (api.DeleteCheckResponseObject, error) {
	ctx, err := out(ctx)
	if err != nil {
		return nil, err
	}
	_, err = h.up.Checks.DeleteCheck(ctx, &checksv1.DeleteCheckRequest{OrganizationId: r.OrgId.String(), ProjectId: r.ProjectId.String(), CheckId: r.CheckId.String()})
	if err != nil {
		return nil, err
	}
	return api.DeleteCheck204Response{}, nil
}

func checkResultFromProto(in *checksv1.CheckResult) api.CheckResult {
	out := api.CheckResult{ScheduledAt: in.GetScheduledAt().AsTime(), ObservedAt: in.GetObservedAt().AsTime(),
		DurationMs: in.GetDurationMs(), Detail: in.GetDetail(), Outcome: "failure"}
	if in.GetOutcome() == checksv1.CheckOutcome_CHECK_OUTCOME_SUCCESS {
		out.Outcome = "success"
	}
	if in.TlsDaysRemaining != nil {
		days := int32(in.GetTlsDaysRemaining())
		out.TlsDaysRemaining = &days
	}
	return out
}

func (h *Handlers) ListCheckResults(ctx context.Context, r api.ListCheckResultsRequestObject) (api.ListCheckResultsResponseObject, error) {
	ctx, err := out(ctx)
	if err != nil {
		return nil, err
	}
	size, token := pageArgs(r.Params.Limit, r.Params.Cursor)
	resp, err := h.up.Stream.ListCheckResults(ctx, &streamv1.ListCheckResultsRequest{
		OrganizationId: r.OrgId.String(), ProjectId: r.ProjectId.String(), ResourceId: r.ResourceId.String(), CheckId: r.CheckId.String(),
		StartTime: timestamppb.New(r.Params.StartTime), EndTime: timestamppb.New(r.Params.EndTime), PageSize: size, PageToken: token,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.CheckResult, 0, len(resp.GetResults()))
	for _, item := range resp.GetResults() {
		items = append(items, checkResultFromProto(item))
	}
	return api.ListCheckResults200JSONResponse{Body: api.CheckResultList{Items: items, Page: page(resp.GetNextPageToken())}}, nil
}

func metricFromProto(in *telemetryv1.MetricsSample) api.MetricSample {
	out := api.MetricSample{MetricName: in.GetMetricName(), SeriesId: in.GetSeriesId(), SampleId: in.GetSampleId(),
		ObservedAt: in.GetObservedAt().AsTime(), OtlpPayload: in.GetOtlpPayload()}
	switch in.GetValue().(type) {
	case *telemetryv1.MetricsSample_IntValue:
		value := in.GetIntValue()
		out.IntValue = &value
	case *telemetryv1.MetricsSample_DoubleValue:
		value := in.GetDoubleValue()
		out.DoubleValue = &value
	}
	return out
}

func (h *Handlers) ListMetricSamples(ctx context.Context, r api.ListMetricSamplesRequestObject) (api.ListMetricSamplesResponseObject, error) {
	ctx, err := out(ctx)
	if err != nil {
		return nil, err
	}
	size, token := pageArgs(r.Params.Limit, r.Params.Cursor)
	resp, err := h.up.Stream.ListMetrics(ctx, &streamv1.ListMetricsRequest{
		OrganizationId: r.OrgId.String(), ProjectId: r.ProjectId.String(), ResourceId: r.ResourceId.String(), MetricName: r.Params.MetricName,
		StartTime: timestamppb.New(r.Params.StartTime), EndTime: timestamppb.New(r.Params.EndTime), PageSize: size, PageToken: token,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.MetricSample, 0, len(resp.GetSamples()))
	for _, item := range resp.GetSamples() {
		items = append(items, metricFromProto(item))
	}
	return api.ListMetricSamples200JSONResponse{Body: api.MetricSampleList{Items: items, Page: page(resp.GetNextPageToken())}}, nil
}
