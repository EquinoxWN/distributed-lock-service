//go:build etcd

// Integration test against a real etcd cluster (CI only: three local etcd members started by the
// workflow). Run with: DLS_ETCD_ENDPOINTS=http://127.0.0.1:2379,... go test -tags etcd ./internal/etcdkv
package etcdkv

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/kv/kvtest"
	"github.com/EquinoxWN/distributed-lock-service/internal/lock"
)

// endpoints reads DLS_ETCD_ENDPOINTS or fails: with the etcd tag the cluster is required.
func endpoints(t *testing.T) []string {
	v := os.Getenv("DLS_ETCD_ENDPOINTS")
	if v == "" {
		t.Fatal("DLS_ETCD_ENDPOINTS is not set (needed with -tags etcd)")
	}
	return strings.Split(v, ",")
}

func TestEtcdContract(t *testing.T) {
	c := newClient(t, Options{Endpoints: endpoints(t)})
	prefix := fmt.Sprintf("/dls-test/%d", time.Now().UnixNano())
	kvtest.Run(t, kvtest.Config{New: func(*testing.T) kv.Backend { return c }, Advance: time.Sleep, TTL: 2 * time.Second, Prefix: prefix})
}

func TestEtcdLocksAcrossMembers(t *testing.T) {
	eps := endpoints(t)
	prefix := fmt.Sprintf("/dls-test/%d/", time.Now().UnixNano())
	var inside, overlaps atomic.Int32
	var mu sync.Mutex
	var tokens []int64
	var wg sync.WaitGroup
	for w := range 6 {
		// Each client talks to a different member first.
		ep := append(append([]string{}, eps[w%len(eps):]...), eps[:w%len(eps)]...)
		c, err := lock.New(lock.Config{Backend: newClient(t, Options{Endpoints: ep}), Prefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			for range 5 {
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				l, err := c.Acquire(ctx, "shared", fmt.Sprintf("w%d", w), 2*time.Second)
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
				time.Sleep(5 * time.Millisecond)
				inside.Add(-1)
				if err := l.Release(context.Background()); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if overlaps.Load() != 0 || len(tokens) != 30 {
		t.Fatalf("%d overlaps, %d acquisitions", overlaps.Load(), len(tokens))
	}
	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Fatalf("token %d at %d not above %d", tokens[i], i, tokens[i-1])
		}
	}
	t.Logf("etcd: 30 acquisitions by 6 clients over %d members, 0 overlaps, tokens %d..%d", len(eps), tokens[0], tokens[len(tokens)-1])
}

func TestEtcdAbandonedLeaseExpires(t *testing.T) {
	c, err := lock.New(lock.Config{Backend: newClient(t, Options{Endpoints: endpoints(t)}), Prefix: fmt.Sprintf("/dls-test/%d/", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	a, err := c.TryAcquire(context.Background(), "crash", "a", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	a.Abandon()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	b, err := c.Acquire(ctx, "crash", "b", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = b.Release(context.Background()) }()
	if b.Token <= a.Token {
		t.Fatalf("token %d not above %d", b.Token, a.Token)
	}
	t.Logf("etcd: takeover after %v (granted TTL %v), token %d -> %d", time.Since(t0), a.TTL, a.Token, b.Token)
}
