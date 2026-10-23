// Package policy turns a JSON file of rate-limit rules into a
// middleware.Resolver: which limits apply to a request, keyed by what, at
// which tier. A Store reloads the file while the service runs.
package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/algo"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/middleware"
)

// Algorithm names accepted in the "algorithm" field.
const (
	TokenBucket   = "token_bucket"
	GCRA          = "gcra"
	SlidingWindow = "sliding_window"
)

// checks holds each algorithm's limit validation, so a limit that the
// algorithm cannot enforce is rejected when the file is loaded rather than
// on the first request.
var checks = map[string]func(limiter.Limit, int64) error{
	TokenBucket:   limiter.Limit.Check,
	GCRA:          algo.CheckGCRA,
	SlidingWindow: algo.CheckSlidingWindow,
}

// File is the JSON document. See testdata/policies.json for an example.
type File struct {
	Policies []Rule `json:"policies"`
}

// Rule is one named policy.
type Rule struct {
	Name string `json:"name"`
	// Match selects requests; an empty match applies to every request.
	Match MatchConfig `json:"match"`
	// Key is one or more of "ip", "user", "header:<Name>", joined by "|":
	// the first that identifies the client wins. A request that none of
	// them identifies is not subject to this policy.
	Key string `json:"key"`
	// Algorithm is token_bucket (default), gcra or sliding_window.
	Algorithm string `json:"algorithm"`
	// Cost is charged per request; 0 or absent means 1.
	Cost int64 `json:"cost"`
	// Limit applies to clients without a tier or with an unlisted one.
	Limit *LimitConfig `json:"limit"`
	// Tiers override Limit per tier, e.g. "free", "pro".
	Tiers map[string]TierConfig `json:"tiers"`
}

// MatchConfig selects requests. All given fields must match.
type MatchConfig struct {
	Methods    []string `json:"methods"`
	Path       string   `json:"path"`        // exact path
	PathPrefix string   `json:"path_prefix"` // e.g. "/v1/"
	Host       string   `json:"host"`
}

// LimitConfig is limiter.Limit in JSON: {"rate": 100, "period": "1m", "burst": 20}.
// Burst defaults to Rate.
type LimitConfig struct {
	Rate   int64  `json:"rate"`
	Period string `json:"period"`
	Burst  int64  `json:"burst"`
}

// TierConfig is either a limit or "unlimited": true.
type TierConfig struct {
	Limit     *LimitConfig `json:"limit"`
	Unlimited bool         `json:"unlimited"`
}

// Parse decodes a policy file; Compile validates it. Unknown fields are
// errors, so a typo such as "burts" fails the load instead of silently
// using a default.
func Parse(r io.Reader) (*File, error) {
	var f File
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("decode policy file: %w", err)
	}
	if dec.More() {
		return nil, errors.New("decode policy file: trailing data after the JSON document")
	}
	return &f, nil
}

// ParseBytes is Parse over a byte slice.
func ParseBytes(b []byte) (*File, error) { return Parse(bytes.NewReader(b)) }

func (lc *LimitConfig) limit() (limiter.Limit, error) {
	if lc == nil {
		return limiter.Limit{}, errors.New("required")
	}
	d, err := time.ParseDuration(lc.Period)
	if err != nil {
		return limiter.Limit{}, fmt.Errorf("period: %w", err)
	}
	l := limiter.Limit{Rate: lc.Rate, Period: d, Burst: lc.Burst}
	if l.Burst == 0 {
		l.Burst = l.Rate
	}
	return l, nil
}

// compileKey turns "header:X-API-Key|ip" into a KeyFunc.
func compileKey(spec string, opts Options) (middleware.KeyFunc, error) {
	if spec == "" {
		return nil, errors.New("required")
	}
	var fns []middleware.KeyFunc
	for _, part := range strings.Split(spec, "|") {
		part = strings.TrimSpace(part)
		switch {
		case part == "ip":
			fn, err := middleware.ByIP(opts.IP)
			if err != nil {
				return nil, err
			}
			fns = append(fns, fn)
		case part == "user":
			if opts.User == nil {
				return nil, errors.New(`"user" needs Options.User`)
			}
			fns = append(fns, middleware.ByUser(opts.User))
		case strings.HasPrefix(part, "header:"):
			name := strings.TrimPrefix(part, "header:")
			if name == "" || strings.ContainsAny(name, " \t:") {
				return nil, fmt.Errorf("bad header name %q", name)
			}
			fns = append(fns, middleware.ByHeader(name))
		default:
			return nil, fmt.Errorf("unknown key %q (want ip, user or header:<Name>)", part)
		}
	}
	if len(fns) == 1 {
		return fns[0], nil
	}
	return middleware.FirstOf(fns...), nil
}

func validMethod(m string) bool {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return true
	}
	return false
}
