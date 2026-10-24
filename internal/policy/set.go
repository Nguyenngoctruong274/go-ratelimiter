package policy

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/middleware"
)

// Options supplies what a policy file refers to but cannot contain.
type Options struct {
	// Limiters by algorithm name. Only the algorithms the file uses are
	// required, e.g. {TokenBucket: redisstore.NewTokenBucket(rdb, "")}.
	Limiters map[string]limiter.Limiter
	// IP configures the "ip" key (trusted proxies, IPv6 grouping).
	IP middleware.IPOptions
	// User returns the authenticated user id for the "user" key.
	User func(r *http.Request) (string, bool)
	// Tier returns the client's tier ("free", "pro", ...) or "" if none,
	// typically from a claim set by the auth middleware.
	Tier func(r *http.Request) string
}

// Set is a compiled, immutable set of policies. It implements
// middleware.Resolver.
type Set struct {
	policies []compiled
	tier     func(r *http.Request) string
}

var _ middleware.Resolver = (*Set)(nil)

type compiled struct {
	name    string
	match   MatchConfig
	key     middleware.KeyFunc
	limiter limiter.Limiter
	cost    int64
	limit   limiter.Limit
	tiers   map[string]tierLimit
}

type tierLimit struct {
	limit     limiter.Limit
	unlimited bool
}

// Compile validates f against opts and builds a Set. Every problem in the
// file is reported, each prefixed with its location, e.g.
// "policies[2].tiers.pro.limit: invalid limit: burst must be positive".
func Compile(f *File, opts Options) (*Set, error) {
	var errs []error
	fail := func(where string, err error) { errs = append(errs, fmt.Errorf("%s: %w", where, err)) }

	set := &Set{tier: opts.Tier}
	seen := map[string]bool{}
	for i, pc := range f.Policies {
		at := fmt.Sprintf("policies[%d]", i)
		if pc.Name != "" {
			at = fmt.Sprintf("policies[%d] %q", i, pc.Name)
		}
		c := compiled{name: pc.Name, match: pc.Match, cost: pc.Cost, tiers: map[string]tierLimit{}}

		switch {
		case !middleware.ValidPolicyName(pc.Name):
			fail(at+".name", errors.New("must be 1-64 characters of [a-zA-Z0-9_.-]"))
		case seen[pc.Name]:
			fail(at+".name", errors.New("duplicate policy name"))
		}
		seen[pc.Name] = true

		for _, m := range pc.Match.Methods {
			if !validMethod(m) {
				fail(at+".match.methods", fmt.Errorf("unknown method %q", m))
			}
		}
		if pc.Match.Path != "" && pc.Match.PathPrefix != "" {
			fail(at+".match", errors.New("use path or path_prefix, not both"))
		}
		for _, p := range []string{pc.Match.Path, pc.Match.PathPrefix} {
			if p != "" && !strings.HasPrefix(p, "/") {
				fail(at+".match", fmt.Errorf("path %q must start with /", p))
			}
		}

		var err error
		if c.key, err = compileKey(pc.Key, opts); err != nil {
			fail(at+".key", err)
		}

		algoName := pc.Algorithm
		if algoName == "" {
			algoName = TokenBucket
		}
		check, known := checks[algoName]
		if !known {
			fail(at+".algorithm", fmt.Errorf("unknown algorithm %q", pc.Algorithm))
		} else if c.limiter = opts.Limiters[algoName]; c.limiter == nil {
			fail(at+".algorithm", fmt.Errorf("no limiter configured for %q", algoName))
		}

		if c.cost == 0 {
			c.cost = 1
		}
		if c.cost < 0 {
			fail(at+".cost", errors.New("must not be negative"))
		}

		checkLimit := func(where string, lc *LimitConfig) limiter.Limit {
			l, err := lc.limit()
			if err == nil && check != nil {
				err = check(l, c.cost)
			}
			if err != nil {
				fail(where, err)
			}
			return l
		}
		c.limit = checkLimit(at+".limit", pc.Limit)

		for _, name := range slices.Sorted(maps.Keys(pc.Tiers)) { // stable error order
			tc := pc.Tiers[name]
			where := at + ".tiers." + name
			switch {
			case name == "":
				fail(where, errors.New("empty tier name"))
			case tc.Unlimited && tc.Limit != nil:
				fail(where, errors.New("set limit or unlimited, not both"))
			case tc.Unlimited:
				c.tiers[name] = tierLimit{unlimited: true}
			default:
				c.tiers[name] = tierLimit{limit: checkLimit(where+".limit", tc.Limit)}
			}
		}
		if len(pc.Tiers) > 0 && opts.Tier == nil {
			fail(at+".tiers", errors.New("tiers need Options.Tier"))
		}
		set.policies = append(set.policies, c)
	}

	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("invalid policies: %w", err)
	}
	return set, nil
}

// Resolve returns the checks for r: one per matching policy, in file order,
// at the client's tier. A policy is skipped when its key finds nothing to
// identify the client (no API key, not logged in) or the tier is
// unlimited; use a fallback such as "header:X-API-Key|ip" when anonymous
// traffic must be limited too.
func (s *Set) Resolve(r *http.Request) ([]middleware.Check, error) {
	var (
		out      []middleware.Check
		tier     string
		tierRead bool
	)
	for i := range s.policies {
		p := &s.policies[i]
		if !p.match.matches(r) {
			continue
		}
		key, err := p.key(r)
		if errors.Is(err, middleware.ErrNoKey) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("policy %s: %w", p.name, err)
		}

		l := p.limit
		if len(p.tiers) > 0 {
			if !tierRead {
				tier, tierRead = s.tier(r), true
			}
			if t, ok := p.tiers[tier]; ok {
				if t.unlimited {
					continue
				}
				l = t.limit
			}
		}
		out = append(out, middleware.Check{Policy: p.name, Limiter: p.limiter, Limit: l, Key: key, Cost: p.cost})
	}
	return out, nil
}

// Policy is one named policy at one tier, for callers that name the policy
// and the client themselves instead of sending an HTTP request through
// Resolve, such as the /v1/check endpoint.
type Policy struct {
	Name    string
	Limiter limiter.Limiter
	Limit   limiter.Limit
	Cost    int64 // the policy's cost per request
	// Unlimited is set when the tier is exempt from the policy; Limit is
	// then zero and nothing should be checked.
	Unlimited bool
}

// Lookup returns the policy called name at tier. A tier the policy does not
// list gets its default limit, as in Resolve.
func (s *Set) Lookup(name, tier string) (Policy, bool) {
	for i := range s.policies {
		p := &s.policies[i]
		if p.name != name {
			continue
		}
		out := Policy{Name: p.name, Limiter: p.limiter, Limit: p.limit, Cost: p.cost}
		if t, ok := p.tiers[tier]; ok {
			out.Limit, out.Unlimited = t.limit, t.unlimited
		}
		return out, true
	}
	return Policy{}, false
}

// Names lists the policies in file order.
func (s *Set) Names() []string {
	names := make([]string, len(s.policies))
	for i, p := range s.policies {
		names[i] = p.name
	}
	return names
}

func (m MatchConfig) matches(r *http.Request) bool {
	if len(m.Methods) > 0 {
		ok := false
		for _, meth := range m.Methods {
			if r.Method == meth {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if m.Path != "" && r.URL.Path != m.Path {
		return false
	}
	if m.PathPrefix != "" && !strings.HasPrefix(r.URL.Path, m.PathPrefix) {
		return false
	}
	if m.Host != "" && !strings.EqualFold(hostOnly(r.Host), m.Host) {
		return false
	}
	return true
}

func hostOnly(h string) string {
	if i := strings.LastIndexByte(h, ':'); i >= 0 && !strings.Contains(h[i:], "]") {
		return h[:i]
	}
	return h
}
