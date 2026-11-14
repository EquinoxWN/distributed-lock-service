// Package memkv is an in-process stand-in for etcd with the same lease and revision rules.
//
// It is used for local runs and tests (no etcd cluster runs on the development machine); the etcd
// backend is checked against a real cluster in CI. Lease expiry uses an injected clock whose
// readings must only move forward (time.Now carries a monotonic reading), so a wall-clock jump on
// the host does not expire or extend a lease.
package memkv

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
)

// DefaultMaxLeases bounds memory: Grant fails once this many leases are live.
const DefaultMaxLeases = 100_000

type lease struct {
	ttl      time.Duration
	deadline time.Time
	keys     map[string]struct{}
}

// Store is a linearizable in-memory store guarded by one mutex.
type Store struct {
	mu        sync.Mutex
	now       func() time.Time
	rev       int64
	nextLease kv.LeaseID
	leases    map[kv.LeaseID]*lease
	keys      map[string]kv.KeyValue
	maxLeases int
	fault     func(op string) error
}

var _ kv.Backend = (*Store)(nil)

// New returns an empty store at revision 1; now defaults to time.Now.
func New(now func() time.Time) *Store {
	if now == nil {
		now = time.Now
	}
	return &Store{now: now, rev: 1, leases: map[kv.LeaseID]*lease{}, keys: map[string]kv.KeyValue{}, maxLeases: DefaultMaxLeases}
}

// SetMaxLeases changes the live-lease limit.
func (s *Store) SetMaxLeases(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxLeases = n
}

// SetFault installs a hook that can fail any operation by name (tests only); nil removes it.
func (s *Store) SetFault(f func(op string) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fault = f
}

// Revision returns the current store revision.
func (s *Store) Revision() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
	return s.rev
}

// LeaseCount returns the number of live leases.
func (s *Store) LeaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expireLocked()
	return len(s.leases)
}

// begin checks the context and fault hook and expires due leases; the caller holds mu.
func (s *Store) begin(ctx context.Context, op string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w: %w", op, kv.ErrUnavailable, err)
	}
	if s.fault != nil {
		if err := s.fault(op); err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}
	s.expireLocked()
	return nil
}

// expireLocked revokes every lease whose deadline has passed.
func (s *Store) expireLocked() {
	now := s.now()
	for id, l := range s.leases {
		if !now.Before(l.deadline) {
			s.revokeLocked(id)
		}
	}
}

// revokeLocked deletes a lease and its keys in one revision, as etcd does.
func (s *Store) revokeLocked(id kv.LeaseID) {
	l := s.leases[id]
	delete(s.leases, id)
	if len(l.keys) == 0 {
		return
	}
	s.rev++
	for k := range l.keys {
		delete(s.keys, k)
	}
}

// Grant creates a lease with the requested TTL.
func (s *Store) Grant(ctx context.Context, ttl time.Duration) (kv.LeaseID, time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx, "grant"); err != nil {
		return 0, 0, err
	}
	if ttl <= 0 {
		return 0, 0, fmt.Errorf("grant: ttl %v must be positive", ttl)
	}
	if len(s.leases) >= s.maxLeases {
		return 0, 0, fmt.Errorf("grant: %w: %d live leases", kv.ErrUnavailable, len(s.leases))
	}
	s.nextLease++
	s.leases[s.nextLease] = &lease{ttl: ttl, deadline: s.now().Add(ttl), keys: map[string]struct{}{}}
	return s.nextLease, ttl, nil
}

// KeepAlive pushes the lease deadline to now plus its TTL.
func (s *Store) KeepAlive(ctx context.Context, id kv.LeaseID) (time.Duration, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx, "keepalive"); err != nil {
		return 0, err
	}
	l, ok := s.leases[id]
	if !ok {
		return 0, fmt.Errorf("keepalive %d: %w", id, kv.ErrLeaseNotFound)
	}
	l.deadline = s.now().Add(l.ttl)
	return l.ttl, nil
}

// Revoke ends a lease and deletes its keys.
func (s *Store) Revoke(ctx context.Context, id kv.LeaseID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx, "revoke"); err != nil {
		return err
	}
	if _, ok := s.leases[id]; !ok {
		return fmt.Errorf("revoke %d: %w", id, kv.ErrLeaseNotFound)
	}
	s.revokeLocked(id)
	return nil
}

// CreateIfAbsent puts the key on the lease unless it already exists.
func (s *Store) CreateIfAbsent(ctx context.Context, key, value string, id kv.LeaseID) (bool, kv.KeyValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx, "create"); err != nil {
		return false, kv.KeyValue{}, err
	}
	if err := kv.ValidateKV(key, value); err != nil {
		return false, kv.KeyValue{}, fmt.Errorf("create: %w", err)
	}
	if cur, ok := s.keys[key]; ok {
		return false, cur, nil
	}
	l, ok := s.leases[id]
	if !ok {
		return false, kv.KeyValue{}, fmt.Errorf("create %q: %w", key, kv.ErrLeaseNotFound)
	}
	s.rev++
	e := kv.KeyValue{Key: key, Value: value, CreateRevision: s.rev, ModRevision: s.rev, Lease: id}
	s.keys[key] = e
	l.keys[key] = struct{}{}
	return true, e, nil
}

// DeleteIfLease deletes the key if the given lease holds it.
func (s *Store) DeleteIfLease(ctx context.Context, key string, id kv.LeaseID) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx, "delete"); err != nil {
		return false, err
	}
	cur, ok := s.keys[key]
	if !ok || cur.Lease != id {
		return false, nil
	}
	s.rev++
	delete(s.keys, key)
	if l, ok := s.leases[id]; ok {
		delete(l.keys, key)
	}
	return true, nil
}

// Get reads one key.
func (s *Store) Get(ctx context.Context, key string) (kv.KeyValue, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.begin(ctx, "get"); err != nil {
		return kv.KeyValue{}, false, err
	}
	cur, ok := s.keys[key]
	return cur, ok, nil
}
