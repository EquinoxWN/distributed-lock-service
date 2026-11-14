package lock

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/memkv"
)

const ttl = 200 * time.Millisecond

func setup(t *testing.T) (*Client, *memkv.Store) {
	t.Helper()
	s := memkv.New(nil)
	c, err := New(Config{Backend: s, RetryMin: time.Millisecond, RetryMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

func mustAcquire(t *testing.T, c *Client, name, owner string) *Lock {
	t.Helper()
	l, err := c.TryAcquire(context.Background(), name, owner, ttl)
	if err != nil {
		t.Fatalf("acquire %s by %s: %v", name, owner, err)
	}
	t.Cleanup(func() { _ = l.Release(context.Background()) })
	return l
}

// waitDone waits for a lock to end or fails after d.
func waitDone(t *testing.T, l *Lock, d time.Duration) {
	t.Helper()
	select {
	case <-l.Done():
	case <-time.After(d):
		t.Fatalf("lock %s still alive after %v", l.Name, d)
	}
}

func TestValidate(t *testing.T) {
	bad := []struct {
		name, owner string
		ttl         time.Duration
	}{
		{"", "w", ttl}, {"a/b", "w", ttl}, {strings.Repeat("n", 129), "w", ttl}, {"a b", "w", ttl},
		{"n", "", ttl}, {"n", "w 1", ttl}, {"n", strings.Repeat("o", 129), ttl},
		{"n", "w", MinTTL - 1}, {"n", "w", MaxTTL + 1}, {"n", "w", 0},
	}
	for _, b := range bad {
		if err := Validate(b.name, b.owner, b.ttl); !errors.Is(err, ErrInvalid) {
			t.Errorf("Validate(%q, %q, %v) = %v, want ErrInvalid", b.name, b.owner, b.ttl, err)
		}
	}
	for _, ok := range []struct{ name, owner string }{{"orders.v1_2-x", "worker-1@host:42"}, {strings.Repeat("n", 128), "w"}} {
		if err := Validate(ok.name, ok.owner, MinTTL); err != nil {
			t.Errorf("Validate(%q, %q) = %v", ok.name, ok.owner, err)
		}
	}
	c, _ := setup(t)
	if _, err := c.TryAcquire(context.Background(), "bad name", "w", ttl); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid acquire: %v", err)
	}
	if _, _, err := c.Get(context.Background(), "bad/name"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid get: %v", err)
	}
}

func TestNewChecksConfig(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil backend: %v", err)
	}
	if _, err := New(Config{Backend: memkv.New(nil), RetryMin: time.Second, RetryMax: time.Millisecond}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("retry min above max: %v", err)
	}
	c, err := New(Config{Backend: memkv.New(nil)})
	if err != nil || c.Key("x") != DefaultPrefix+"x" || c.cfg.OpTimeout != DefaultOpTimeout {
		t.Fatalf("defaults: %+v %v", c, err)
	}
}

func TestTryAcquireReportsHolderAndRevokesItsLease(t *testing.T) {
	c, s := setup(t)
	a := mustAcquire(t, c, "orders", "worker-a")
	if a.Token <= 0 || !a.Valid() || a.TTL != ttl {
		t.Fatalf("lock %+v valid=%v", a, a.Valid())
	}
	_, err := c.TryAcquire(context.Background(), "orders", "worker-b", ttl)
	var held *HeldError
	if !errors.As(err, &held) || !errors.Is(err, ErrHeld) {
		t.Fatalf("second acquire: %v, want HeldError", err)
	}
	if held.Holder != (Holder{Owner: "worker-a", Token: a.Token, Lease: a.Lease}) {
		t.Fatalf("holder %+v", held.Holder)
	}
	if n := s.LeaseCount(); n != 1 {
		t.Fatalf("%d live leases, want 1 (the loser's lease must be revoked)", n)
	}
	h, ok, err := c.Get(context.Background(), "orders")
	if err != nil || !ok || h != held.Holder {
		t.Fatalf("get: %+v %v %v", h, ok, err)
	}
	if _, ok, err := c.Get(context.Background(), "free"); err != nil || ok {
		t.Fatalf("get free: %v %v", ok, err)
	}
}

func TestTokensStrictlyIncreaseAcrossNamesAndClients(t *testing.T) {
	s := memkv.New(nil)
	clients := make([]*Client, 3)
	for i := range clients {
		clients[i], _ = New(Config{Backend: s})
	}
	var last int64
	for i := range 60 {
		c := clients[i%3]
		name := []string{"a", "b"}[i%2]
		l, err := c.TryAcquire(context.Background(), name, "w", ttl)
		if err != nil {
			t.Fatal(err)
		}
		if l.Token <= last {
			t.Fatalf("acquisition %d: token %d not above %d", i, l.Token, last)
		}
		last = l.Token
		if err := l.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKeepAliveHoldsTheLockPastItsTTL(t *testing.T) {
	c, _ := setup(t)
	a := mustAcquire(t, c, "job", "a")
	end := time.Now().Add(5 * ttl)
	for time.Now().Before(end) {
		if !a.Valid() {
			t.Fatalf("lock invalid while renewed: %v", a.Err())
		}
		if _, err := c.TryAcquire(context.Background(), "job", "b", ttl); !errors.Is(err, ErrHeld) {
			t.Fatalf("other owner got the lock while it was renewed: %v", err)
		}
		time.Sleep(ttl / 4)
	}
	if err := a.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(a.Err(), ErrReleased) || a.Valid() {
		t.Fatalf("after release: err %v valid %v", a.Err(), a.Valid())
	}
	b := mustAcquire(t, c, "job", "b")
	if b.Token <= a.Token {
		t.Fatalf("token after release %d not above %d", b.Token, a.Token)
	}
}

func TestAbandonedLockExpiresAndIsTakenOver(t *testing.T) {
	c, s := setup(t)
	a := mustAcquire(t, c, "job", "a")
	start := time.Now()
	a.Abandon()
	if !errors.Is(a.Err(), ErrAbandoned) || a.Valid() {
		t.Fatalf("abandoned: err %v valid %v", a.Err(), a.Valid())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := c.Acquire(ctx, "job", "b", ttl)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Release(context.Background()) }()
	waited := time.Since(start)
	// The store frees the key one TTL after a's last renewal, which was at most TTL/3 before Abandon.
	if waited < ttl-ttl/3 || b.Token <= a.Token {
		t.Fatalf("taken over after %v with token %d (old %d); want >= %v and a larger token", waited, b.Token, a.Token, ttl-ttl/3)
	}
	if n := s.LeaseCount(); n != 1 {
		t.Fatalf("%d live leases, want 1", n)
	}
}

func TestLockIsLostWhenRenewalsFail(t *testing.T) {
	c, s := setup(t)
	a := mustAcquire(t, c, "job", "a")
	unreachable := errors.New("store unreachable")
	s.SetFault(func(op string) error {
		if op == "keepalive" {
			return unreachable
		}
		return nil
	})
	start := time.Now()
	waitDone(t, a, 3*ttl)
	if took := time.Since(start); took > ttl+ttl/2 {
		t.Fatalf("loss noticed after %v, want within about one TTL", took)
	}
	if !errors.Is(a.Err(), ErrLost) || !(errors.Is(a.Err(), unreachable) || errors.Is(a.Err(), kv.ErrUnavailable)) || a.Valid() {
		t.Fatalf("err %v valid %v", a.Err(), a.Valid())
	}
}

func TestLockIsLostWhenLeaseRevoked(t *testing.T) {
	c, s := setup(t)
	a := mustAcquire(t, c, "job", "a")
	if err := s.Revoke(context.Background(), a.Lease); err != nil {
		t.Fatal(err)
	}
	waitDone(t, a, 2*ttl)
	if !errors.Is(a.Err(), ErrLost) || !errors.Is(a.Err(), kv.ErrLeaseNotFound) {
		t.Fatalf("err %v", a.Err())
	}
}

// slowKeepAlive answers keep-alives only after their deadline, late by one TTL.
type slowKeepAlive struct{ *memkv.Store }

func (b slowKeepAlive) KeepAlive(ctx context.Context, _ kv.LeaseID) (time.Duration, error) {
	<-ctx.Done()
	time.Sleep(ttl)
	return 0, ctx.Err()
}

func TestValidIsFalsePastTheDeadlineBeforeRenewalsGiveUp(t *testing.T) {
	c, err := New(Config{Backend: slowKeepAlive{memkv.New(nil)}})
	if err != nil {
		t.Fatal(err)
	}
	l, err := c.TryAcquire(context.Background(), "job", "a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Release(context.Background()) }()
	time.Sleep(ttl + ttl/4)
	if l.Err() != nil {
		t.Fatalf("renewal goroutine ended early: %v", l.Err())
	}
	if l.Valid() {
		t.Fatal("Valid() is true after the lease deadline")
	}
	waitDone(t, l, 2*ttl)
	if !errors.Is(l.Err(), ErrLost) {
		t.Fatalf("err %v, want ErrLost", l.Err())
	}
}

func TestShortRenewalOutageIsRidden(t *testing.T) {
	c, s := setup(t)
	var fails atomic.Int32
	a := mustAcquire(t, c, "job", "a")
	s.SetFault(func(op string) error {
		if op == "keepalive" && fails.Add(1) <= 2 {
			return kv.ErrUnavailable
		}
		return nil
	})
	time.Sleep(3 * ttl)
	if !a.Valid() || fails.Load() < 3 {
		t.Fatalf("valid %v err %v after %d renewal calls", a.Valid(), a.Err(), fails.Load())
	}
}

func TestConcurrentAcquireIsMutuallyExclusive(t *testing.T) {
	c, s := setup(t)
	var inside, overlaps atomic.Int32
	var mu sync.Mutex
	var tokens []int64
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for range 10 {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				l, err := c.Acquire(ctx, "shared", "w"+string(rune('a'+w)), ttl)
				cancel()
				if err != nil {
					t.Error(err)
					return
				}
				if inside.Add(1) != 1 {
					overlaps.Add(1)
				}
				mu.Lock()
				tokens = append(tokens, l.Token)
				mu.Unlock()
				time.Sleep(time.Millisecond)
				inside.Add(-1)
				if err := l.Release(context.Background()); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if overlaps.Load() != 0 || len(tokens) != 80 {
		t.Fatalf("%d overlaps, %d acquisitions", overlaps.Load(), len(tokens))
	}
	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Fatalf("token %d at %d not above %d", tokens[i], i, tokens[i-1])
		}
	}
	if n := s.LeaseCount(); n != 0 {
		t.Fatalf("%d leases left after all releases", n)
	}
}

func TestAcquireGivesUpWhenContextEnds(t *testing.T) {
	c, s := setup(t)
	mustAcquire(t, c, "job", "a")
	ctx, cancel := context.WithTimeout(context.Background(), 3*ttl)
	defer cancel()
	_, err := c.Acquire(ctx, "job", "b", ttl)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v, want DeadlineExceeded", err)
	}
	if n := s.LeaseCount(); n != 1 {
		t.Fatalf("%d live leases, want 1 (the waiter's lease must be revoked)", n)
	}
}

func TestReleaseIsIdempotentAndNeverDeletesAnotherHolder(t *testing.T) {
	c, _ := setup(t)
	a := mustAcquire(t, c, "job", "a")
	a.Abandon()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	b, err := c.Acquire(ctx, "job", "b", ttl)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := a.Release(context.Background()); err != nil {
			t.Fatalf("stale release: %v", err)
		}
	}
	h, ok, err := c.Get(context.Background(), "job")
	if err != nil || !ok || h.Owner != "b" || h.Token != b.Token {
		t.Fatalf("holder after a stale release: %+v %v %v", h, ok, err)
	}
	if err := b.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Release(context.Background()); err != nil {
		t.Fatalf("second release: %v", err)
	}
}

// lostAnswer applies the first create but reports it as failed, like a reply lost in transit.
type lostAnswer struct {
	*memkv.Store
	dropped atomic.Bool
}

func (b *lostAnswer) CreateIfAbsent(ctx context.Context, key, value string, id kv.LeaseID) (bool, kv.KeyValue, error) {
	ok, e, err := b.Store.CreateIfAbsent(ctx, key, value, id)
	if err == nil && !b.dropped.Swap(true) {
		return false, kv.KeyValue{}, kv.ErrUnavailable
	}
	return ok, e, err
}

func TestRetryAfterALostCreateAnswerKeepsTheLock(t *testing.T) {
	b := &lostAnswer{Store: memkv.New(nil)}
	c, _ := New(Config{Backend: b, RetryMin: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l, err := c.Acquire(ctx, "job", "a", ttl)
	if err != nil {
		t.Fatalf("acquire after a lost answer: %v", err)
	}
	defer func() { _ = l.Release(context.Background()) }()
	h, ok, _ := c.Get(ctx, "job")
	if !ok || h.Lease != l.Lease || h.Token != l.Token {
		t.Fatalf("holder %+v, lock %+v", h, l)
	}
	if _, err := c.TryAcquire(ctx, "other", "a", ttl); err != nil {
		t.Fatalf("second lock: %v", err)
	}
}

func TestStoreErrorsAreReturned(t *testing.T) {
	c, s := setup(t)
	down := errors.New("down")
	s.SetFault(func(op string) error {
		if op == "grant" {
			return down
		}
		return nil
	})
	if _, err := c.TryAcquire(context.Background(), "job", "a", ttl); !errors.Is(err, down) {
		t.Fatalf("grant failure: %v", err)
	}
	s.SetFault(func(op string) error {
		if op == "create" {
			return down
		}
		return nil
	})
	if _, err := c.TryAcquire(context.Background(), "job", "a", ttl); !errors.Is(err, down) {
		t.Fatalf("create failure: %v", err)
	}
	s.SetFault(nil)
	if n := s.LeaseCount(); n != 0 {
		t.Fatalf("%d leases left after failed acquisitions", n)
	}
}
