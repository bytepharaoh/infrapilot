package receiver

import (
	"context"
	"errors"
	"sync"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // Register OTLP/gRPC gzip decompression.
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/domain"
)

const retryDelay = time.Second

type releaseExportKey struct{}

// RegisterGRPC exposes the metrics-only OTLP service.
func (r *Receiver) RegisterGRPC(registrar grpc.ServiceRegistrar) {
	metricspb.RegisterMetricsServiceServer(registrar, r)
}

// Tap applies admission and a read deadline when the export headers arrive, before decompression.
func (r *Receiver) Tap(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info.FullMethodName != metricspb.MetricsService_Export_FullMethodName {
		return ctx, nil
	}
	if ctx.Err() != nil {
		return ctx, status.FromContextError(ctx.Err()).Err()
	}
	if len(info.Header.Get("authorization")) != 1 {
		r.recordOutcome(ctx, errUnauthenticated)
		return ctx, grpcError(errUnauthenticated)
	}
	release, err := r.admit(ctx)
	if err != nil {
		r.recordOutcome(ctx, err)
		return ctx, grpcError(err)
	}
	bounded, cancel := context.WithTimeout(ctx, r.limits.Deadline)
	releaseOnce := sync.OnceFunc(release)
	context.AfterFunc(bounded, func() {
		releaseOnce()
		cancel()
	})
	return context.WithValue(bounded, releaseExportKey{}, releaseOnce), nil
}

// Export authenticates the request and reports success after Kafka acknowledgment.
func (r *Receiver) Export(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) (*metricspb.ExportMetricsServiceResponse, error) {
	if release, ok := ctx.Value(releaseExportKey{}).(func()); ok {
		defer release()
	}
	err := r.exportGRPC(ctx, req)
	r.recordOutcome(ctx, err)
	if err != nil {
		return nil, grpcError(err)
	}
	return &metricspb.ExportMetricsServiceResponse{}, nil
}

func (r *Receiver) exportGRPC(ctx context.Context, req *metricspb.ExportMetricsServiceRequest) error {
	md, _ := metadata.FromIncomingContext(ctx)
	binding, err := r.authorize(ctx, md.Get("authorization"), md.Get(resourceHeader))
	if err != nil {
		return err
	}
	return r.publish(ctx, req, binding)
}

func grpcError(err error) error {
	var code codes.Code
	switch {
	case errors.Is(err, errUnauthenticated):
		code = codes.Unauthenticated
	case errors.Is(err, errForbidden):
		code = codes.PermissionDenied
	case errors.Is(err, domain.ErrInvalid), errors.Is(err, errUnsupported):
		code = codes.InvalidArgument
	case errors.Is(err, errTooLarge):
		code = codes.ResourceExhausted
	case errors.Is(err, errBusy), errors.Is(err, errQuota):
		code = codes.ResourceExhausted
		return retryStatus(code, err.Error())
	default:
		return retryStatus(codes.Unavailable, errUnavailable.Error())
	}
	return status.Error(code, err.Error())
}

func retryStatus(code codes.Code, message string) error {
	s := status.New(code, message)
	if withRetry, err := s.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(retryDelay)}); err == nil {
		return withRetry.Err()
	}
	return s.Err()
}
