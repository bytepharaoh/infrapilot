package probe

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/netip"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

func (e *Executor) ping(ctx context.Context, host string) error {
	addresses, err := e.policy.Resolve(ctx, host)
	if err != nil {
		return err
	}
	address := addresses[0].Unmap()
	network, bind := "udp4", "0.0.0.0"
	request, reply, protocol := icmp.Type(ipv4.ICMPTypeEcho), icmp.Type(ipv4.ICMPTypeEchoReply), 1
	if address.Is6() {
		network, bind = "udp6", "::"
		request, reply, protocol = ipv6.ICMPTypeEchoRequest, ipv6.ICMPTypeEchoReply, 58
	}
	conn, err := icmp.ListenPacket(network, bind)
	if err != nil {
		return fmt.Errorf("open datagram ping socket: %w", err)
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("set ping deadline: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("generate ping nonce: %w", err)
	}
	message, err := (&icmp.Message{Type: request, Body: &icmp.Echo{ID: 0, Seq: 1, Data: nonce[:]}}).Marshal(nil)
	if err != nil {
		return fmt.Errorf("encode ping: %w", err)
	}
	if err := e.policy.ValidateAddress(address); err != nil {
		return err
	}
	if _, err := conn.WriteTo(message, &net.UDPAddr{IP: address.AsSlice()}); err != nil {
		return fmt.Errorf("send datagram ping: %w", err)
	}
	var buffer [128]byte
	for range 4 {
		n, source, err := conn.ReadFrom(buffer[:])
		if err != nil {
			return fmt.Errorf("read datagram ping: %w", err)
		}
		peer, ok := source.(*net.UDPAddr)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(peer.IP)
		if !ok || ip.Unmap() != address {
			continue
		}
		response, err := icmp.ParseMessage(protocol, buffer[:n])
		if err == nil && response.Type == reply {
			echo, ok := response.Body.(*icmp.Echo)
			if ok && echo.Seq == 1 && bytes.Equal(echo.Data, nonce[:]) {
				return nil
			}
		}
	}
	return errors.New("no valid datagram ping response")
}
