// Package grpc exposes bounded reads of the owning service's tenant data.
package grpc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	checksv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/checks/v1"
	streamv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/stream/v1"
	telemetryv1 "github.com/bytepharaoh/infrapilot/gen/go/infrapilot/telemetry/v1"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/domain"
	"github.com/bytepharaoh/infrapilot/services/stream-processor/internal/store"
)

const (
	maxPage      = 500
	defaultPage  = 100
	cursorPrefix = 16
)

type Server struct {
	streamv1.UnimplementedStreamReadServiceServer
	Reader store.Reader
	Logger *slog.Logger
}

type queryFields interface {
	GetOrganizationId() string
	GetProjectId() string
	GetResourceId() string
	GetStartTime() *timestamppb.Timestamp
	GetEndTime() *timestamppb.Timestamp
	GetPageSize() int32
	GetPageToken() string
}

func window(req queryFields, filter string, idSize int) (store.Window, [cursorPrefix]byte, error) {
	scope, err := domain.ParseScope(req.GetOrganizationId(), req.GetProjectId(), req.GetResourceId())
	if err != nil {
		return store.Window{}, [cursorPrefix]byte{}, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.GetStartTime().CheckValid() != nil || req.GetEndTime().CheckValid() != nil {
		return store.Window{}, [cursorPrefix]byte{}, status.Error(codes.InvalidArgument, "start_time and end_time are required timestamps")
	}
	// Postgres comparison uses microsecond timestamps. Applying ceil at both edges preserves the half-open interval.
	start, end := ceilMicrosecond(req.GetStartTime().AsTime()), ceilMicrosecond(req.GetEndTime().AsTime())
	if !start.Before(end) || end.Sub(start) > domain.MaxRange || start.Before(time.Unix(0, 0)) ||
		end.After(time.Now().Add(5*time.Minute)) {
		return store.Window{}, [cursorPrefix]byte{}, status.Error(codes.InvalidArgument, "invalid time range; maximum interval is seven days")
	}
	size := req.GetPageSize()
	if size < 0 || size > maxPage {
		return store.Window{}, [cursorPrefix]byte{}, status.Error(codes.InvalidArgument, "page_size must be at most 500")
	}
	if size == 0 {
		size = defaultPage
	}
	w := store.Window{Scope: scope, Start: start, End: end, Limit: size + 1}
	hash := sha256.New()
	_, _ = hash.Write(scope.OrganizationID[:])
	_, _ = hash.Write(scope.ProjectID[:])
	_, _ = hash.Write(scope.ResourceID[:])
	_, _ = hash.Write(binary.BigEndian.AppendUint64(nil, uint64(start.UnixMicro())))
	_, _ = hash.Write(binary.BigEndian.AppendUint64(nil, uint64(end.UnixMicro())))
	_, _ = hash.Write([]byte(filter))
	fingerprint := [cursorPrefix]byte(hash.Sum(nil))
	if token := req.GetPageToken(); token != "" {
		if len(token) > 80 {
			return store.Window{}, fingerprint, status.Error(codes.InvalidArgument, "invalid page_token")
		}
		raw, decodeErr := base64.RawURLEncoding.Strict().DecodeString(token)
		if decodeErr != nil || len(raw) != cursorPrefix+8+idSize || [cursorPrefix]byte(raw) != fingerprint {
			return store.Window{}, fingerprint, status.Error(codes.InvalidArgument, "invalid page_token")
		}
		w.AfterAt = time.UnixMicro(int64(binary.BigEndian.Uint64(raw[cursorPrefix:])))
		if w.AfterAt.Before(w.Start) || !w.AfterAt.Before(w.End) {
			return store.Window{}, fingerprint, status.Error(codes.InvalidArgument, "invalid page_token range")
		}
		w.AfterID = raw[cursorPrefix+8:]
	}
	return w, fingerprint, nil
}

func ceilMicrosecond(t time.Time) time.Time {
	if micro := t.Truncate(time.Microsecond); !micro.Equal(t) {
		return micro.Add(time.Microsecond)
	}
	return t
}

func cursor(fingerprint [cursorPrefix]byte, t time.Time, id []byte) string {
	raw := binary.BigEndian.AppendUint64(fingerprint[:], uint64(t.UnixMicro()))
	return base64.RawURLEncoding.EncodeToString(append(raw, id...))
}

func (s *Server) ListCheckResults(ctx context.Context, req *streamv1.ListCheckResultsRequest) (*streamv1.ListCheckResultsResponse, error) {
	id, err := domain.ParseID(req.GetCheckId(), "check_id")
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	w, fingerprint, err := window(req, "check/"+id.String(), 0)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := s.Reader.Checks(ctx, w, id)
	if err != nil {
		return nil, s.readError(ctx, err)
	}
	response := &streamv1.ListCheckResultsResponse{}
	for i, row := range rows {
		if i == int(w.Limit)-1 || len(row.Payload) == 0 {
			if i > 0 {
				response.NextPageToken = cursor(fingerprint, rows[i-1].ScheduledAt, nil)
			}
			break
		}
		var check checksv1.CheckResult
		if decodeErr := proto.Unmarshal(row.Payload, &check); decodeErr != nil {
			return nil, s.readError(ctx, decodeErr)
		}
		response.Results = append(response.Results, &check)
	}
	return response, nil
}

func (s *Server) ListMetrics(ctx context.Context, req *streamv1.ListMetricsRequest) (*streamv1.ListMetricsResponse, error) {
	if name := req.GetMetricName(); name == "" || len(name) > 255 || strings.ContainsRune(name, 0) {
		return nil, status.Error(codes.InvalidArgument, "metric_name is required and must not exceed 255 bytes")
	}
	w, fingerprint, err := window(req, "metric/"+req.GetMetricName(), sha256.Size)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rows, err := s.Reader.Metrics(ctx, w, req.GetMetricName())
	if err != nil {
		return nil, s.readError(ctx, err)
	}
	response := &streamv1.ListMetricsResponse{}
	for i, row := range rows {
		if i == int(w.Limit)-1 || len(row.Payload) == 0 {
			if i > 0 {
				last := rows[i-1]
				response.NextPageToken = cursor(fingerprint, last.ObservedAt, last.SampleID)
			}
			break
		}
		var sample telemetryv1.MetricsSample
		if decodeErr := proto.Unmarshal(row.Payload, &sample); decodeErr != nil {
			return nil, s.readError(ctx, decodeErr)
		}
		response.Samples = append(response.Samples, &sample)
	}
	return response, nil
}

func (s *Server) readError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "read canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "read timed out")
	default:
		s.Logger.ErrorContext(ctx, "owning store read failed", slog.Any("error", err))
		return status.Error(codes.Unavailable, "stream data unavailable")
	}
}
