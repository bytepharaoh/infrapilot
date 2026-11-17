// Package probe executes one read-only network check against a guarded target.
package probe

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bytepharaoh/infrapilot/pkg/ssrf"
	"github.com/bytepharaoh/infrapilot/services/check-runner/internal/domain"
)

const maxBody = 64 << 10

// Executor owns the policy and HTTP transport for probes.
type Executor struct {
	policy ssrf.Policy
	http   *http.Client
}

// New configures a transport that never uses environment proxies and rechecks
// the address at every connection, including redirects.
func New(policy ssrf.Policy) *Executor {
	transport := &http.Transport{
		DialContext: policy.DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
		DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 16 << 10,
	}
	return &Executor{policy: policy, http: &http.Client{Transport: transport, CheckRedirect: validateRedirect}}
}

func validateRedirect(req *http.Request, previous []*http.Request) error {
	if len(previous) > 5 {
		return errors.New("redirect limit exceeded")
	}
	if err := domain.ValidateHTTPURL(req.URL); err != nil {
		return err
	}
	if previous[len(previous)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
		return errors.New("HTTPS redirect cannot switch to plaintext")
	}
	req.Header.Del("Referer")
	return nil
}

// Run executes a probe with its own deadline and returns a sanitized result.
func (e *Executor) Run(ctx context.Context, def domain.Definition) domain.Measurement {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(def.TimeoutSeconds)*time.Second)
	defer cancel()
	var detail string
	var tlsDays *uint32
	var err error
	switch def.Kind {
	case domain.HTTP:
		detail, err = e.get(ctx, def.Target)
	case domain.TCP:
		var conn net.Conn
		conn, err = e.policy.DialContext(ctx, "tcp", net.JoinHostPort(def.Target, strconv.Itoa(int(def.Port))))
		if err == nil {
			_ = conn.Close()
		}
	case domain.TLS:
		tlsDays, err = e.tls(ctx, def)
	case domain.DNS:
		_, err = e.policy.Resolve(ctx, def.Target)
	case domain.ICMP:
		err = e.ping(ctx, def.Target)
	default:
		detail = "invalid_definition"
	}
	if err != nil {
		switch {
		case errors.Is(err, ssrf.ErrBlocked):
			detail = "blocked_destination"
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
			detail = "timeout"
		default:
			detail = "probe_failed"
		}
	}
	elapsed := time.Since(start)
	return domain.Measurement{ObservedAt: time.Now().UTC(), DurationMS: float64(elapsed) / float64(time.Millisecond), Success: detail == "", Detail: detail, TLSDays: tlsDays}
}

func (e *Executor) get(ctx context.Context, target string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", err
	}
	if err := domain.ValidateHTTPURL(req.URL); err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "InfraPilot-CheckRunner")
	resp, err := e.http.Do(req) //nolint:gosec // The guarded dialer accepts only validated public addresses.
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "http_status_" + strconv.Itoa(resp.StatusCode), nil
	}
	read, err := io.CopyN(io.Discard, resp.Body, maxBody+1)
	if read > maxBody {
		return "body_limit", nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	return "", nil
}

func (e *Executor) tls(ctx context.Context, def domain.Definition) (*uint32, error) {
	port := def.Port
	if port == 0 {
		port = 443
	}
	conn, err := e.policy.DialContext(ctx, "tcp", net.JoinHostPort(def.Target, strconv.Itoa(int(port))))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	secured := tls.Client(conn, &tls.Config{ServerName: strings.TrimSuffix(def.Target, "."), MinVersion: tls.VersionTLS12})
	if err := secured.HandshakeContext(ctx); err != nil {
		return nil, err
	}
	remaining := max(0, time.Until(secured.ConnectionState().PeerCertificates[0].NotAfter)/(24*time.Hour))
	days := uint32(min(remaining, 36500))
	return &days, nil
}
