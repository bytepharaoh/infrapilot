// Package ssrf restricts outbound connections to public IP addresses and configured
// deployment ranges. Callers keep the original hostname for HTTP and TLS verification.
package ssrf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"syscall"
)

// ErrBlocked indicates an address is outside the permitted public network ranges.
var ErrBlocked = errors.New("destination address is blocked")

var special = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/3"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
}

var publicIPv6 = netip.MustParsePrefix("2000::/3")

// Resolver is the system DNS lookup operation used before a connection.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// Policy checks DNS answers once, pins the connection to the checked address, and
// also checks the numeric address at socket creation. A zero Policy is usable.
type Policy struct {
	denied   []netip.Prefix
	resolver Resolver
}

// New validates extra deployment CIDRs. A nil resolver uses the system resolver.
func New(denied []string, resolver Resolver) (Policy, error) {
	p := Policy{denied: make([]netip.Prefix, 0, len(denied)), resolver: resolver}
	for _, value := range denied {
		prefix, err := netip.ParsePrefix(value)
		if err != nil || prefix.Addr().Is4In6() {
			return Policy{}, fmt.Errorf("invalid denied CIDR %q", value)
		}
		p.denied = append(p.denied, prefix.Masked())
	}
	return p, nil
}

// ValidateAddress applies the public-network allow policy to a literal address.
func (p Policy) ValidateAddress(ip netip.Addr) error {
	if ip.Zone() != "" {
		return ErrBlocked
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || (ip.Is6() && !publicIPv6.Contains(ip)) {
		return ErrBlocked
	}
	for _, prefix := range special {
		if prefix.Contains(ip) {
			return ErrBlocked
		}
	}
	for _, prefix := range p.denied {
		if prefix.Contains(ip) {
			return ErrBlocked
		}
	}
	return nil
}

// Resolve validates every answer before returning any of the IP addresses. Rejecting
// mixed public and private answers avoids selecting another address after a failure.
func (p Policy) Resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("destination lookup requires a deadline")
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if validateErr := p.ValidateAddress(ip); validateErr != nil {
			return nil, validateErr
		}
		return []netip.Addr{ip.Unmap()}, nil
	}
	resolver := p.resolver
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	ips, err := resolver.LookupNetIP(ctx, "ip", strings.TrimSuffix(host, ".")+".")
	if err != nil {
		return nil, fmt.Errorf("resolve destination: %w", err)
	}
	if len(ips) == 0 || len(ips) > 16 {
		return nil, errors.New("destination returned an unsupported number of addresses")
	}
	for _, ip := range ips {
		if err := p.ValidateAddress(ip); err != nil {
			return nil, err
		}
	}
	return ips, nil
}

func (p Policy) control(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("inspect dial destination: %w", err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("inspect resolved dial destination: %w", err)
	}
	return p.ValidateAddress(ip)
}

// DialContext dials only validated numeric addresses and checks them again in
// net.Dialer's socket control hook. The supplied context must have a deadline.
func (p Policy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("parse dial destination: %w", err)
	}
	ips, err := p.Resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{Control: p.control}
	var failures []error
	for _, ip := range ips {
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.Unmap().String(), port))
		if dialErr == nil {
			return conn, nil
		}
		failures = append(failures, dialErr)
		if ctx.Err() != nil {
			break
		}
	}
	return nil, fmt.Errorf("dial destination: %w", errors.Join(failures...))
}
