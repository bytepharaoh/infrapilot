// Package domain contains the check configuration and its input constraints.
package domain

import (
	"encoding/binary"
	"errors"
	"math"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Kind is the probe protocol, numbered as the version 1 check contract.
type Kind int16

const (
	HTTP Kind = iota + 1
	TCP
	TLS
	DNS
	ICMP
)

// Definition is a tenant-selected monitoring target and its cadence.
type Definition struct {
	Resource                        uuid.UUID
	Kind                            Kind
	Target                          string
	Port                            int32
	IntervalSeconds, TimeoutSeconds int32
	Enabled                         bool
	MaxDurationMS                   *float64
	MinTLSDays                      *int32
}

// Validate rejects invalid check configuration before it is persisted.
func (d Definition) Validate() error {
	if d.Resource == uuid.Nil || d.IntervalSeconds < 30 || d.IntervalSeconds > 86400 || d.TimeoutSeconds < 1 || d.TimeoutSeconds > 15 {
		return errors.New("resource, cadence, or timeout is invalid")
	}
	if d.MaxDurationMS != nil && (math.IsNaN(*d.MaxDurationMS) || *d.MaxDurationMS <= 0 || *d.MaxDurationMS > 15000) {
		return errors.New("max duration must be greater than zero and at most 15000 milliseconds")
	}
	if d.MinTLSDays != nil && (d.Kind != TLS || *d.MinTLSDays < 1 || *d.MinTLSDays > 365) {
		return errors.New("min_tls_days applies to TLS and must be between 1 and 365")
	}
	if len(d.Target) == 0 || len(d.Target) > 2048 {
		return errors.New("target must have between 1 and 2048 bytes")
	}
	switch d.Kind {
	case HTTP:
		if d.Port != 0 {
			return errors.New("put the HTTP port in the URL")
		}
		u, err := url.Parse(d.Target)
		if err != nil {
			return errors.New("HTTP target is invalid")
		}
		return ValidateHTTPURL(u)
	case TCP, TLS:
		if d.Port < 0 || d.Port > 65535 || (d.Kind == TCP && d.Port == 0) || !ValidHost(d.Target) {
			return errors.New("host or port is invalid")
		}
	case DNS, ICMP:
		if d.Port != 0 || !ValidHost(d.Target) {
			return errors.New("target host is invalid")
		}
		if _, err := netip.ParseAddr(d.Target); d.Kind == DNS && err == nil {
			return errors.New("DNS checks require a hostname")
		}
	default:
		return errors.New("check kind is invalid")
	}
	return nil
}

// ValidateHTTPURL also applies to redirect destinations.
func ValidateHTTPURL(u *url.URL) error {
	if u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Opaque != "" || u.Fragment != "" || !ValidHost(u.Hostname()) {
		return errors.New("HTTP URL requires an HTTP(S) destination without credentials or fragments")
	}
	if port := u.Port(); port != "" {
		if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 {
			return errors.New("HTTP port is invalid")
		}
	}
	return nil
}

// ValidHost permits an IP literal or a multi-label ASCII hostname. IP zone IDs
// and search-relative single labels have no use in public monitoring.
func ValidHost(host string) bool {
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Zone() == ""
	}
	host = strings.TrimSuffix(host, ".")
	if len(host) > 253 || !strings.Contains(host, ".") {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
				return false
			}
		}
	}
	return true
}

// JitterMS separates checks that would otherwise run on the same clock tick.
func JitterMS(id uuid.UUID) int32 {
	return int32(binary.BigEndian.Uint32(id[12:]) % 1000)
}

// Measurement is safe to emit to storage and logs. Detail is a bounded service
// code; source response contents and connection error strings are omitted.
type Measurement struct {
	ObservedAt time.Time
	DurationMS float64
	Success    bool
	Detail     string
	TLSDays    *uint32
}
