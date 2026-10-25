package policy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/middleware"
)

// Store serves the policies of a file and reloads them when it changes.
//
// Each request resolves against one immutable Set, swapped atomically, so a
// reload never shows a request half of the old rules and half of the new.
// A file that fails to parse or validate is rejected as a whole and the
// last good Set stays in force: a typo in production must not remove every
// limit.
type Store struct {
	path   string
	opts   Options
	logger *slog.Logger

	current atomic.Pointer[Set]

	mu  sync.Mutex // serializes reloads
	sum [sha256.Size]byte

	applied, rejected atomic.Uint64
	failing           atomic.Bool
}

// ReloadStats counts the Sets installed (the first load included) and the
// files rejected. Failing reports that the last attempt was rejected: the
// file on disk is not the policy being enforced.
type ReloadStats struct {
	Applied  uint64
	Rejected uint64
	Failing  bool
}

// ReloadStats returns the counts so far.
func (s *Store) ReloadStats() ReloadStats {
	return ReloadStats{Applied: s.applied.Load(), Rejected: s.rejected.Load(), Failing: s.failing.Load()}
}

var _ middleware.Resolver = (*Store)(nil)

// Open loads path and returns a Store serving it. Unlike a reload, the
// first load must succeed: there is no previous Set to fall back to.
func Open(path string, opts Options, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Store{path: path, opts: opts, logger: logger}
	if _, err := s.Reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Resolve implements middleware.Resolver with the current Set.
func (s *Store) Resolve(r *http.Request) ([]middleware.Check, error) {
	return s.current.Load().Resolve(r)
}

// Lookup finds a policy in the current Set; see Set.Lookup.
func (s *Store) Lookup(name, tier string) (Policy, bool) {
	return s.current.Load().Lookup(name, tier)
}

// Current returns the Set in force.
func (s *Store) Current() *Set { return s.current.Load() }

// Reload reads the file and, if its content changed and is valid, swaps in
// the new Set. It reports whether a new Set was installed.
func (s *Store) Reload() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	changed, err := s.reloadLocked()
	switch {
	case err != nil:
		s.rejected.Add(1)
	case changed:
		s.applied.Add(1)
	}
	s.failing.Store(err != nil)
	return changed, err
}

func (s *Store) reloadLocked() (bool, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return false, fmt.Errorf("read policies: %w", err)
	}
	sum := sha256.Sum256(raw)
	if s.current.Load() != nil && sum == s.sum {
		return false, nil
	}
	f, err := Parse(bytes.NewReader(raw))
	if err != nil {
		return false, fmt.Errorf("%s: %w", s.path, err)
	}
	set, err := Compile(f, s.opts)
	if err != nil {
		return false, fmt.Errorf("%s: %w", s.path, err)
	}
	s.current.Store(set)
	s.sum = sum
	return true, nil
}

// Watch checks the file every interval until ctx ends, reloading on change.
// It compares content hashes rather than modification times, which are
// coarse on some filesystems and change when a ConfigMap is re-mounted with
// identical content. Failed reloads are logged and retried on the next
// tick, so fixing the file is enough to recover.
func (s *Store) Watch(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return errors.New("watch: interval must be positive")
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			changed, err := s.Reload()
			switch {
			case err != nil:
				s.logger.ErrorContext(ctx, "policy reload rejected; keeping the previous policies",
					slog.String("path", s.path), slog.Any("error", err))
			case changed:
				s.logger.InfoContext(ctx, "policies reloaded",
					slog.String("path", s.path), slog.Any("policies", s.Current().Names()))
			}
		}
	}
}
