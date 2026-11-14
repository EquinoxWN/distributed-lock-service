// Package kvtest is the contract every kv.Backend must pass: memkv locally, etcd in CI.
package kvtest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
)

// Config tells the suite how to build a backend and move time forward.
type Config struct {
	// New returns a backend; it may be shared between tests (keys are prefixed per test).
	New func(t *testing.T) kv.Backend
	// Advance moves the backend's clock forward (a fake clock, or time.Sleep for a real store).
	Advance func(d time.Duration)
	// TTL is the lease TTL to request; the store may grant more.
	TTL time.Duration
	// Prefix is put in front of every key.
	Prefix string
}

// Run runs the whole contract against cfg.
func Run(t *testing.T, cfg Config) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, b kv.Backend, cfg Config, key string)
	}{
		{"GrantKeepAliveRevoke", grantKeepAliveRevoke},
		{"CreateIfAbsentOnce", createIfAbsentOnce},
		{"CreateRevisionsOnlyIncrease", createRevisionsOnlyIncrease},
		{"EveryCreateGetsANewRevision", everyCreateGetsANewRevision},
		{"CreateNeedsLiveLease", createNeedsLiveLease},
		{"DeleteOnlyByHoldingLease", deleteOnlyByHoldingLease},
		{"RevokeDeletesAttachedKeys", revokeDeletesAttachedKeys},
		{"LeaseExpiresWithoutKeepAlive", leaseExpiresWithoutKeepAlive},
		{"KeepAliveExtendsLease", keepAliveExtendsLease},
		{"RejectsOversizedKey", rejectsOversizedKey},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key := cfg.Prefix + "/" + strings.ReplaceAll(t.Name(), "/", "_")
			c.fn(t, cfg.New(t), cfg, key)
		})
	}
}

// ctx returns a context that ends with the test.
func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// grant takes a lease or fails the test.
func grant(t *testing.T, b kv.Backend, cfg Config) (kv.LeaseID, time.Duration) {
	t.Helper()
	id, ttl, err := b.Grant(ctx(t), cfg.TTL)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if id == 0 || ttl < cfg.TTL {
		t.Fatalf("grant returned id %d ttl %v, want id != 0 and ttl >= %v", id, ttl, cfg.TTL)
	}
	return id, ttl
}

// create puts a key or fails the test.
func create(t *testing.T, b kv.Backend, key, value string, id kv.LeaseID) kv.KeyValue {
	t.Helper()
	ok, e, err := b.CreateIfAbsent(ctx(t), key, value, id)
	if err != nil || !ok {
		t.Fatalf("create %q: ok=%v err=%v", key, ok, err)
	}
	return e
}

func grantKeepAliveRevoke(t *testing.T, b kv.Backend, cfg Config, _ string) {
	id, ttl := grant(t, b, cfg)
	got, err := b.KeepAlive(ctx(t), id)
	if err != nil || got != ttl {
		t.Fatalf("keepalive: ttl %v err %v, want %v", got, err, ttl)
	}
	if err := b.Revoke(ctx(t), id); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := b.KeepAlive(ctx(t), id); !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("keepalive after revoke: %v, want ErrLeaseNotFound", err)
	}
	if err := b.Revoke(ctx(t), id); !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("second revoke: %v, want ErrLeaseNotFound", err)
	}
}

func createIfAbsentOnce(t *testing.T, b kv.Backend, cfg Config, key string) {
	a, _ := grant(t, b, cfg)
	c, _ := grant(t, b, cfg)
	first := create(t, b, key, "owner-a", a)
	if first.CreateRevision <= 0 || first.Lease != a || first.Value != "owner-a" || first.Key != key {
		t.Fatalf("created %+v", first)
	}
	ok, holder, err := b.CreateIfAbsent(ctx(t), key, "owner-c", c)
	if err != nil || ok {
		t.Fatalf("second create: ok=%v err=%v, want refused", ok, err)
	}
	if holder.CreateRevision != first.CreateRevision || holder.Lease != a || holder.Value != "owner-a" {
		t.Fatalf("holder %+v, want %+v", holder, first)
	}
	got, found, err := b.Get(ctx(t), key)
	if err != nil || !found || got.CreateRevision != first.CreateRevision || got.Lease != a {
		t.Fatalf("get: %+v found=%v err=%v", got, found, err)
	}
	if _, found, err := b.Get(ctx(t), key+"-absent"); err != nil || found {
		t.Fatalf("get absent: found=%v err=%v", found, err)
	}
}

func createRevisionsOnlyIncrease(t *testing.T, b kv.Backend, cfg Config, key string) {
	id, _ := grant(t, b, cfg)
	var last int64
	for i := range 5 {
		e := create(t, b, key, "v", id)
		if e.CreateRevision <= last {
			t.Fatalf("round %d: create revision %d not above %d", i, e.CreateRevision, last)
		}
		last = e.CreateRevision
		if ok, err := b.DeleteIfLease(ctx(t), key, id); err != nil || !ok {
			t.Fatalf("delete: ok=%v err=%v", ok, err)
		}
	}
}

func everyCreateGetsANewRevision(t *testing.T, b kv.Backend, cfg Config, key string) {
	id, _ := grant(t, b, cfg)
	var last int64
	for i := range 4 {
		e := create(t, b, key+"-"+string(rune('a'+i)), "v", id)
		if e.CreateRevision <= last {
			t.Fatalf("key %d: create revision %d not above %d (no delete in between)", i, e.CreateRevision, last)
		}
		last = e.CreateRevision
	}
}

func createNeedsLiveLease(t *testing.T, b kv.Backend, cfg Config, key string) {
	id, _ := grant(t, b, cfg)
	if err := b.Revoke(ctx(t), id); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	ok, _, err := b.CreateIfAbsent(ctx(t), key, "v", id)
	if ok || !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("create on revoked lease: ok=%v err=%v, want ErrLeaseNotFound", ok, err)
	}
	if _, found, _ := b.Get(ctx(t), key); found {
		t.Fatal("key exists after a failed create")
	}
}

func deleteOnlyByHoldingLease(t *testing.T, b kv.Backend, cfg Config, key string) {
	a, _ := grant(t, b, cfg)
	c, _ := grant(t, b, cfg)
	create(t, b, key, "a", a)
	if ok, err := b.DeleteIfLease(ctx(t), key, c); err != nil || ok {
		t.Fatalf("delete by other lease: ok=%v err=%v", ok, err)
	}
	if _, found, _ := b.Get(ctx(t), key); !found {
		t.Fatal("other lease deleted the key")
	}
	if ok, err := b.DeleteIfLease(ctx(t), key, a); err != nil || !ok {
		t.Fatalf("delete by holder: ok=%v err=%v", ok, err)
	}
	if ok, err := b.DeleteIfLease(ctx(t), key, a); err != nil || ok {
		t.Fatalf("second delete: ok=%v err=%v", ok, err)
	}
}

func revokeDeletesAttachedKeys(t *testing.T, b kv.Backend, cfg Config, key string) {
	id, _ := grant(t, b, cfg)
	first := create(t, b, key, "a", id)
	create(t, b, key+"-2", "a", id)
	if err := b.Revoke(ctx(t), id); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	for _, k := range []string{key, key + "-2"} {
		if _, found, _ := b.Get(ctx(t), k); found {
			t.Fatalf("%s still exists after revoke", k)
		}
	}
	next, _ := grant(t, b, cfg)
	if e := create(t, b, key, "b", next); e.CreateRevision <= first.CreateRevision {
		t.Fatalf("revision after revoke %d not above %d", e.CreateRevision, first.CreateRevision)
	}
}

func leaseExpiresWithoutKeepAlive(t *testing.T, b kv.Backend, cfg Config, key string) {
	id, ttl := grant(t, b, cfg)
	create(t, b, key, "a", id)
	cfg.Advance(ttl + ttl/2)
	if _, found, err := b.Get(ctx(t), key); err != nil || found {
		t.Fatalf("key after expiry: found=%v err=%v", found, err)
	}
	if _, err := b.KeepAlive(ctx(t), id); !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("keepalive after expiry: %v, want ErrLeaseNotFound", err)
	}
}

func keepAliveExtendsLease(t *testing.T, b kv.Backend, cfg Config, key string) {
	id, ttl := grant(t, b, cfg)
	create(t, b, key, "a", id)
	for range 3 {
		cfg.Advance(ttl / 2)
		if _, err := b.KeepAlive(ctx(t), id); err != nil {
			t.Fatalf("keepalive: %v", err)
		}
	}
	if _, found, err := b.Get(ctx(t), key); err != nil || !found {
		t.Fatalf("key lost while kept alive for 1.5 TTL: found=%v err=%v", found, err)
	}
}

func rejectsOversizedKey(t *testing.T, b kv.Backend, cfg Config, _ string) {
	id, _ := grant(t, b, cfg)
	ok, _, err := b.CreateIfAbsent(ctx(t), strings.Repeat("k", kv.MaxKeyLen+1), "v", id)
	if ok || err == nil {
		t.Fatalf("oversized key: ok=%v err=%v, want an error", ok, err)
	}
}
