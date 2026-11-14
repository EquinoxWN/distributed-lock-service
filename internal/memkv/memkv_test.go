package memkv

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/kv/kvtest"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *fakeClock) advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newFake() (*Store, *fakeClock) {
	c := &fakeClock{t: time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)}
	return New(c.now), c
}

func TestContract(t *testing.T) {
	s, c := newFake()
	kvtest.Run(t, kvtest.Config{New: func(*testing.T) kv.Backend { return s }, Advance: c.advance, TTL: time.Second, Prefix: "/memkv"})
}

func TestExpiryAtExactDeadline(t *testing.T) {
	s, c := newFake()
	ctx := context.Background()
	id, _, _ := s.Grant(ctx, time.Second)
	if _, _, err := s.CreateIfAbsent(ctx, "k", "v", id); err != nil {
		t.Fatal(err)
	}
	c.advance(time.Second - time.Nanosecond)
	if _, found, _ := s.Get(ctx, "k"); !found {
		t.Fatal("expired before the deadline")
	}
	c.advance(time.Nanosecond)
	if _, found, _ := s.Get(ctx, "k"); found {
		t.Fatal("still held at the deadline")
	}
}

func TestExpiryBumpsRevisionOnlyWithKeys(t *testing.T) {
	s, c := newFake()
	ctx := context.Background()
	if _, _, err := s.Grant(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	before := s.Revision()
	c.advance(2 * time.Second)
	if got := s.Revision(); got != before {
		t.Fatalf("empty lease expiry changed revision %d -> %d", before, got)
	}
	id, _, _ := s.Grant(ctx, time.Second)
	_, e, _ := s.CreateIfAbsent(ctx, "k", "v", id)
	c.advance(2 * time.Second)
	if got := s.Revision(); got != e.CreateRevision+1 {
		t.Fatalf("revision after expiry %d, want %d", got, e.CreateRevision+1)
	}
}

func TestBadInputAndLimits(t *testing.T) {
	s, _ := newFake()
	ctx := context.Background()
	if _, _, err := s.Grant(ctx, 0); err == nil {
		t.Fatal("zero ttl granted")
	}
	if _, _, err := s.Grant(ctx, -time.Second); err == nil {
		t.Fatal("negative ttl granted")
	}
	s.SetMaxLeases(2)
	for range 2 {
		if _, _, err := s.Grant(ctx, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := s.Grant(ctx, time.Second); !errors.Is(err, kv.ErrUnavailable) {
		t.Fatalf("third lease: %v, want ErrUnavailable", err)
	}
	id := kv.LeaseID(1)
	if _, _, err := s.CreateIfAbsent(ctx, "", "v", id); err == nil {
		t.Fatal("empty key accepted")
	}
	if _, _, err := s.CreateIfAbsent(ctx, "k", string(make([]byte, kv.MaxValueLen+1)), id); err == nil {
		t.Fatal("oversized value accepted")
	}
}

func TestCancelledContextAndFaults(t *testing.T) {
	s, _ := newFake()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.Grant(ctx, time.Second); !errors.Is(err, kv.ErrUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled grant: %v", err)
	}
	boom := errors.New("boom")
	s.SetFault(func(op string) error {
		if op == "keepalive" {
			return boom
		}
		return nil
	})
	id, _, err := s.Grant(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.KeepAlive(context.Background(), id); !errors.Is(err, boom) {
		t.Fatalf("faulted keepalive: %v", err)
	}
	s.SetFault(nil)
	if _, err := s.KeepAlive(context.Background(), id); err != nil {
		t.Fatalf("keepalive after fault removed: %v", err)
	}
}

func TestConcurrentCreateHasOneWinner(t *testing.T) {
	s := New(nil)
	ctx := context.Background()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			id, _, err := s.Grant(ctx, time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			ok, _, err := s.CreateIfAbsent(ctx, "lock", "x", id)
			if err != nil {
				t.Error(err)
			}
			if ok {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d goroutines created the key, want 1", wins.Load())
	}
}
