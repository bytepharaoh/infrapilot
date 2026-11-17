package receiver

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"

	metricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/bytepharaoh/infrapilot/services/telemetry-ingestion/internal/domain"
)

const (
	protobufMedia = "application/x-protobuf"
	jsonMedia     = "application/json"
	decodeDepth   = 64
)

var errUnsupported = errors.New("unsupported OTLP media type or content encoding")

// HTTP returns the OTLP/HTTP metrics handler. Keep the path and method fixed so that
// unrelated signals never appear accepted during the metrics-only milestone.
func (r *Receiver) HTTP() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		media, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
		if media != jsonMedia {
			media = protobufMedia
		}
		if req.URL.Path != "/v1/metrics" {
			writeProto(w, media, http.StatusNotFound, status.Convert(grpcError(errUnsupported)).Proto())
			return
		}
		if req.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeProto(w, media, http.StatusMethodNotAllowed, status.Convert(grpcError(errUnsupported)).Proto())
			return
		}
		err := r.exportHTTP(w, req)
		r.recordOutcome(req.Context(), err)
		if err != nil {
			writeFailure(w, media, err)
			return
		}
		writeProto(w, media, http.StatusOK, &metricspb.ExportMetricsServiceResponse{})
	})
}

func (r *Receiver) exportHTTP(w http.ResponseWriter, req *http.Request) error {
	media, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || (media != protobufMedia && media != jsonMedia) {
		return errUnsupported
	}
	release, err := r.admit(req.Context())
	if err != nil {
		return err
	}
	defer release()
	ctx, cancel := context.WithTimeout(req.Context(), r.limits.Deadline)
	defer cancel()
	if deadline, ok := ctx.Deadline(); ok {
		// A context deadline alone does not stop a slow request body from holding an admission slot.
		_ = http.NewResponseController(w).SetReadDeadline(deadline)
	}

	binding, err := r.authorize(ctx, req.Header.Values("Authorization"), req.Header.Values(resourceHeader))
	if err != nil {
		return err
	}
	body, err := readBody(w, req, int64(r.limits.RequestBytes))
	if err != nil {
		return err
	}
	export := new(metricspb.ExportMetricsServiceRequest)
	if media == jsonMedia {
		// The OTLP/JSON spec requires receivers to ignore unknown fields.
		err = (protojson.UnmarshalOptions{DiscardUnknown: true, RecursionLimit: decodeDepth}).Unmarshal(body, export)
	} else {
		err = (proto.UnmarshalOptions{RecursionLimit: decodeDepth}).Unmarshal(body, export)
	}
	if err != nil {
		return fmt.Errorf("%w: cannot decode OTLP metrics", domain.ErrInvalid)
	}
	return r.publish(ctx, export, binding)
}

func readBody(w http.ResponseWriter, req *http.Request, maxBytes int64) ([]byte, error) {
	if req.ContentLength > maxBytes {
		return nil, errTooLarge
	}
	bounded := http.MaxBytesReader(w, req.Body, maxBytes)
	defer func() { _ = bounded.Close() }()
	var reader io.Reader = bounded
	switch strings.ToLower(strings.TrimSpace(req.Header.Get("Content-Encoding"))) {
	case "", "identity":
	case "gzip":
		compressed, err := gzip.NewReader(reader)
		if err != nil {
			return nil, bodyError(err)
		}
		defer func() { _ = compressed.Close() }()
		reader = compressed
	default:
		return nil, errUnsupported
	}
	body, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, bodyError(err)
	}
	if int64(len(body)) > maxBytes {
		return nil, errTooLarge
	}
	return body, nil
}

func bodyError(err error) error {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		return errTooLarge
	}
	return fmt.Errorf("%w: cannot read OTLP metrics body", domain.ErrInvalid)
}

func writeFailure(w http.ResponseWriter, media string, err error) {
	code := http.StatusServiceUnavailable
	switch {
	case errors.Is(err, errUnsupported):
		code = http.StatusUnsupportedMediaType
	case errors.Is(err, errTooLarge):
		code = http.StatusRequestEntityTooLarge
	case errors.Is(err, domain.ErrInvalid):
		code = http.StatusBadRequest
	case errors.Is(err, errUnauthenticated):
		code = http.StatusUnauthorized
		w.Header().Set("WWW-Authenticate", "Bearer")
	case errors.Is(err, errForbidden):
		code = http.StatusForbidden
	case errors.Is(err, errQuota):
		code = http.StatusTooManyRequests
	}
	if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", strconv.Itoa(int(retryDelay.Seconds())))
	}
	writeProto(w, media, code, status.Convert(grpcError(err)).Proto())
}

func writeProto(w http.ResponseWriter, media string, code int, message proto.Message) {
	var data []byte
	var err error
	if media == jsonMedia {
		data, err = protojson.Marshal(message)
	} else {
		data, err = proto.Marshal(message)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", media)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_, _ = w.Write(data) //nolint:gosec // Protobuf or JSON status messages are served with type sniffing disabled.
}
