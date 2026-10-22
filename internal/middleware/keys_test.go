package middleware

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
)

func TestByIP(t *testing.T) {
	lb := []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8"), netip.MustParsePrefix("fd00::/8")}

	tests := []struct {
		name    string
		opts    IPOptions
		remote  string
		xff     []string
		want    string
		wantErr bool
	}{
		{name: "direct IPv4", remote: "203.0.113.7:51000", want: "ip:203.0.113.7"},
		{name: "no proxies: the header is ignored", remote: "203.0.113.7:51000",
			xff: []string{"198.51.100.1"}, want: "ip:203.0.113.7"},
		{name: "untrusted peer cannot spoof", opts: IPOptions{TrustedProxies: lb}, remote: "203.0.113.7:51000",
			xff: []string{"198.51.100.1"}, want: "ip:203.0.113.7"},
		{name: "behind one load balancer", opts: IPOptions{TrustedProxies: lb}, remote: "10.1.2.3:443",
			xff: []string{"198.51.100.1"}, want: "ip:198.51.100.1"},
		{name: "client-supplied prefix is skipped", opts: IPOptions{TrustedProxies: lb}, remote: "10.1.2.3:443",
			xff: []string{"6.6.6.6, 198.51.100.1"}, want: "ip:198.51.100.1"},
		{name: "chain of trusted proxies", opts: IPOptions{TrustedProxies: lb}, remote: "10.1.2.3:443",
			xff: []string{"198.51.100.1, 10.9.9.9", "10.8.8.8"}, want: "ip:198.51.100.1"},
		{name: "garbage stops at the last trusted hop", opts: IPOptions{TrustedProxies: lb}, remote: "10.1.2.3:443",
			xff: []string{"not-an-ip, 10.9.9.9"}, want: "ip:10.9.9.9"},
		{name: "request from the proxy itself", opts: IPOptions{TrustedProxies: lb}, remote: "10.1.2.3:443",
			want: "ip:10.1.2.3"},
		{name: "IPv6 grouped by /64", remote: "[2001:db8:1:2:aaaa:bbbb:cccc:dddd]:443", want: "ip:2001:db8:1:2::/64"},
		{name: "IPv6 /128 when asked", opts: IPOptions{IPv6Prefix: 128}, remote: "[2001:db8::1]:443",
			want: "ip:2001:db8::1/128"},
		{name: "IPv4-mapped IPv6 is IPv4", remote: "[::ffff:203.0.113.7]:443", want: "ip:203.0.113.7"},
		{name: "IPv6 client behind IPv6 proxy", opts: IPOptions{TrustedProxies: lb}, remote: "[fd00::1]:443",
			xff: []string{"2001:db8:5:6::9"}, want: "ip:2001:db8:5:6::/64"},
		{name: "bad remote address", remote: "nonsense", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn, err := ByIP(tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = tt.remote
			for _, v := range tt.xff {
				r.Header.Add("X-Forwarded-For", v)
			}
			got, err := fn(r)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("got %q, want error", got)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestByIPRejectsBadOptions(t *testing.T) {
	if _, err := ByIP(IPOptions{IPv6Prefix: 129}); err == nil {
		t.Fatal("prefix /129 accepted")
	}
	if _, err := ByIP(IPOptions{TrustedProxies: []netip.Prefix{{}}}); err == nil {
		t.Fatal("zero prefix accepted")
	}
}

func TestByHeaderHashesTheValue(t *testing.T) {
	fn := ByHeader("x-api-key")
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if _, err := fn(r); !errors.Is(err, ErrNoKey) {
		t.Fatalf("missing header: %v", err)
	}

	r.Header.Set("X-Api-Key", " sk_live_secret ")
	k1, err := fn(r)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(k1, "h:x-api-key:") || strings.Contains(k1, "secret") || len(k1) != len("h:x-api-key:")+24 {
		t.Fatalf("key %q leaks or has the wrong shape", k1)
	}
	r.Header.Set("X-Api-Key", "sk_live_other")
	if k2, _ := fn(r); k2 == k1 {
		t.Fatal("different credentials share a key")
	}
}

func TestByUserAndFirstOf(t *testing.T) {
	user := ByUser(func(r *http.Request) (string, bool) {
		id := r.Header.Get("X-Test-User") // stands in for an auth context value
		return id, id != ""
	})
	ip, _ := ByIP(IPOptions{})
	key := FirstOf(user, ByHeader("X-API-Key"), ip)

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.7:1"
	if got, _ := key(r); got != "ip:203.0.113.7" {
		t.Fatalf("anonymous: %q", got)
	}
	r.Header.Set("X-API-Key", "k")
	if got, _ := key(r); !strings.HasPrefix(got, "h:x-api-key:") {
		t.Fatalf("api key: %q", got)
	}
	r.Header.Set("X-Test-User", "42")
	if got, _ := key(r); got != "u:42" {
		t.Fatalf("user: %q", got)
	}

	if _, err := FirstOf(ByHeader("X-None"))(r); !errors.Is(err, ErrNoKey) {
		t.Fatalf("nothing found: %v", err)
	}
	r.RemoteAddr = "bad"
	if _, err := FirstOf(ByHeader("X-None"), ip)(r); err == nil || errors.Is(err, ErrNoKey) {
		t.Fatalf("a real error must stop FirstOf: %v", err)
	}
}
