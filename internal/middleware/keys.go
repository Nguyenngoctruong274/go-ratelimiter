// Package middleware rate-limits net/http handlers. This file decides who a
// request belongs to.
package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// ErrNoKey means a KeyFunc found nothing to identify the client by, for
// example a missing header. FirstOf moves on to the next KeyFunc.
var ErrNoKey = errors.New("no rate-limit key")

// KeyFunc picks the identity a request is limited by. Keys from different
// KeyFuncs carry distinct prefixes ("ip:", "h:", "u:"), so an API key can
// never collide with an IP address.
type KeyFunc func(r *http.Request) (string, error)

// FirstOf returns the key of the first KeyFunc that finds one: for example
// the API key when present, otherwise the client IP.
func FirstOf(fns ...KeyFunc) KeyFunc {
	return func(r *http.Request) (string, error) {
		for _, fn := range fns {
			key, err := fn(r)
			if errors.Is(err, ErrNoKey) {
				continue
			}
			return key, err
		}
		return "", ErrNoKey
	}
}

// IPOptions configures ByIP.
type IPOptions struct {
	// TrustedProxies are the networks of the load balancers and proxies in
	// front of the service. X-Forwarded-For is only believed for hops added
	// by them; anything a client writes into the header itself is ignored.
	TrustedProxies []netip.Prefix
	// IPv6Prefix groups IPv6 clients by network, because one subscriber
	// usually holds a whole /64 and could otherwise rotate addresses to
	// escape the limit. 0 means 64; 128 limits single addresses.
	IPv6Prefix int
}

// ByIP keys requests by client IP.
//
// Without trusted proxies it uses the TCP peer address. With them, it walks
// X-Forwarded-For from the right, skipping trusted hops, and takes the first
// address it did not add itself. Taking the leftmost entry instead, as many
// examples do, lets any client choose its own key by sending the header.
func ByIP(opts IPOptions) (KeyFunc, error) {
	bits := opts.IPv6Prefix
	if bits == 0 {
		bits = 64
	}
	if bits < 1 || bits > 128 {
		return nil, fmt.Errorf("IPv6 prefix /%d out of range 1..128", bits)
	}
	for _, p := range opts.TrustedProxies {
		if !p.IsValid() {
			return nil, fmt.Errorf("invalid trusted proxy prefix %q", p)
		}
	}
	trusted := func(a netip.Addr) bool {
		for _, p := range opts.TrustedProxies {
			if p.Contains(a) {
				return true
			}
		}
		return false
	}

	return func(r *http.Request) (string, error) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		client, err := netip.ParseAddr(host)
		if err != nil {
			return "", fmt.Errorf("parse remote address %q: %w", r.RemoteAddr, err)
		}
		client = client.Unmap()

		if trusted(client) {
			hops := forwardedFor(r.Header)
			for i := len(hops) - 1; i >= 0; i-- {
				hop, err := netip.ParseAddr(hops[i])
				if err != nil {
					// Garbage in the header: stop at the last hop we can vouch for.
					break
				}
				client = hop.Unmap()
				if !trusted(client) {
					break
				}
			}
		}

		if client.Is4() {
			return "ip:" + client.String(), nil
		}
		p, err := client.Prefix(bits)
		if err != nil {
			return "", fmt.Errorf("prefix of %s: %w", client, err)
		}
		return "ip:" + p.String(), nil
	}, nil
}

// forwardedFor returns the X-Forwarded-For hops in order, across repeated
// headers.
func forwardedFor(h http.Header) []string {
	var hops []string
	for _, v := range h.Values("X-Forwarded-For") {
		for _, part := range strings.Split(v, ",") {
			if part = strings.TrimSpace(part); part != "" {
				hops = append(hops, part)
			}
		}
	}
	return hops
}

// ByHeader keys requests by the value of a header such as X-API-Key. The
// value is hashed: keys end up in Redis and in logs, and credentials should
// not. An absent or empty header yields ErrNoKey.
func ByHeader(name string) KeyFunc {
	canonical := http.CanonicalHeaderKey(name)
	tag := "h:" + strings.ToLower(canonical) + ":"
	return func(r *http.Request) (string, error) {
		v := strings.TrimSpace(r.Header.Get(canonical))
		if v == "" {
			return "", ErrNoKey
		}
		sum := sha256.Sum256([]byte(v))
		return tag + hex.EncodeToString(sum[:12]), nil
	}
}

// ByUser keys requests by an authenticated user id, typically read from the
// request context by the caller's auth middleware. ok=false yields ErrNoKey.
func ByUser(id func(r *http.Request) (string, bool)) KeyFunc {
	return func(r *http.Request) (string, error) {
		uid, ok := id(r)
		if !ok || uid == "" {
			return "", ErrNoKey
		}
		return "u:" + uid, nil
	}
}
