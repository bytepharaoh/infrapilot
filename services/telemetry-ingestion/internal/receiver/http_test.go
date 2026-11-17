package receiver

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPBodyExpandedLimit(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, err := writer.Write([]byte(strings.Repeat("o", 4096)))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	req := httptest.NewRequest(http.MethodPost, "/v1/metrics", &compressed)
	req.Header.Set("Content-Encoding", "gzip")
	_, err = readBody(httptest.NewRecorder(), req, 128)
	require.ErrorIs(t, err, errTooLarge)
}

func TestRetryHintsDistinguishOverloadFromByteLimit(t *testing.T) {
	overload := status.Convert(grpcError(errBusy))
	require.Equal(t, codes.ResourceExhausted, overload.Code())
	require.Len(t, overload.Details(), 1)
	hint, ok := overload.Details()[0].(*errdetails.RetryInfo)
	require.True(t, ok)
	require.Equal(t, retryDelay, hint.GetRetryDelay().AsDuration())
	tooLarge := status.Convert(grpcError(errTooLarge))
	require.Equal(t, codes.ResourceExhausted, tooLarge.Code())
	require.Empty(t, tooLarge.Details())
}
