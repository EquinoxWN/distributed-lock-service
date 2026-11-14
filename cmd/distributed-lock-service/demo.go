package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/lock"
	"github.com/EquinoxWN/distributed-lock-service/internal/memkv"
)

type demoConfig struct {
	clients int
	rounds  int
	ttl     time.Duration
}

// parseDemo reads and bounds the demo flags.
func parseDemo(args []string) (demoConfig, error) {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	c := demoConfig{}
	fs.IntVar(&c.clients, "clients", 8, "concurrent clients (2..64)")
	fs.IntVar(&c.rounds, "rounds", 25, "acquisitions per client (1..1000)")
	fs.DurationVar(&c.ttl, "ttl", 300*time.Millisecond, "lease TTL (100ms..10s)")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	switch {
	case fs.NArg() > 0:
		return c, fmt.Errorf("unexpected arguments %v", fs.Args())
	case c.clients < 2 || c.clients > 64:
		return c, fmt.Errorf("-clients %d not in 2..64", c.clients)
	case c.rounds < 1 || c.rounds > 1000:
		return c, fmt.Errorf("-rounds %d not in 1..1000", c.rounds)
	case c.ttl < lock.MinTTL || c.ttl > 10*time.Second:
		return c, fmt.Errorf("-ttl %v not in 100ms..10s", c.ttl)
	}
	return c, nil
}

// demo runs four scenarios against the in-process store and checks the lock's safety rules.
func demo(cfg demoConfig, out io.Writer) error {
	start := time.Now()
	store := memkv.New(nil)
	c, err := lock.New(lock.Config{Backend: store, RetryMin: time.Millisecond, RetryMax: 20 * time.Millisecond})
	if err != nil {
		return err
	}
	ctx := context.Background()
	var regressions, overlaps, steals, notLost, lateTakeover int

	// 1. Contention: every client acquires the same lock many times.
	var inside atomic.Int32
	var mu sync.Mutex
	var tokens []int64
	var waits []time.Duration
	var wg sync.WaitGroup
	errs := make(chan error, cfg.clients)
	for w := range cfg.clients {
		wg.Go(func() {
			for range cfg.rounds {
				t0 := time.Now()
				actx, cancel := context.WithTimeout(ctx, 30*time.Second)
				l, err := c.Acquire(actx, "contended", fmt.Sprintf("client-%d", w), cfg.ttl)
				cancel()
				if err != nil {
					errs <- err
					return
				}
				waited := time.Since(t0)
				if inside.Add(1) != 1 {
					mu.Lock()
					overlaps++
					mu.Unlock()
				}
				mu.Lock()
				tokens = append(tokens, l.Token)
				waits = append(waits, waited)
				mu.Unlock()
				time.Sleep(200 * time.Microsecond)
				inside.Add(-1)
				if err := l.Release(ctx); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return fmt.Errorf("contention: %w", err)
	}
	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			regressions++
		}
	}
	sort.Slice(waits, func(i, j int) bool { return waits[i] < waits[j] })
	fmt.Fprintf(out, "contention: %d clients x %d rounds = %d acquisitions, tokens %d..%d, wait p50 %v p99 %v\n",
		cfg.clients, cfg.rounds, len(tokens), tokens[0], tokens[len(tokens)-1],
		waits[len(waits)/2].Round(time.Microsecond), waits[len(waits)*99/100].Round(time.Microsecond))

	// 2. Renewal: the holder keeps renewing for 5 TTLs while another client keeps trying.
	h, err := c.TryAcquire(ctx, "renewed", "holder", cfg.ttl)
	if err != nil {
		return fmt.Errorf("renewal: %w", err)
	}
	tries := 0
	for end := time.Now().Add(5 * cfg.ttl); time.Now().Before(end); time.Sleep(cfg.ttl / 10) {
		tries++
		if other, err := c.TryAcquire(ctx, "renewed", "other", cfg.ttl); err == nil {
			steals++
			_ = other.Release(ctx) // only reached on a violation, which is already counted
		} else if !errors.Is(err, lock.ErrHeld) {
			return fmt.Errorf("renewal: %w", err)
		}
	}
	valid := h.Valid()
	if err := h.Release(ctx); err != nil {
		return fmt.Errorf("renewal: %w", err)
	}
	fmt.Fprintf(out, "renewal: held for %v (5 TTLs) with keep-alives; %d attempts by another client, %d succeeded; still valid: %v\n",
		5*cfg.ttl, tries, steals, valid)
	if !valid {
		notLost++
	}

	// 3. Crash: the holder stops renewing without releasing; the lease expires and a waiter takes over.
	a, err := c.TryAcquire(ctx, "crash", "crashed", cfg.ttl)
	if err != nil {
		return fmt.Errorf("crash: %w", err)
	}
	t0 := time.Now()
	a.Abandon()
	actx, cancel := context.WithTimeout(ctx, 10*cfg.ttl)
	b, err := c.Acquire(actx, "crash", "successor", cfg.ttl)
	cancel()
	if err != nil {
		return fmt.Errorf("crash: %w", err)
	}
	took := time.Since(t0)
	if took > cfg.ttl+cfg.ttl/2 || b.Token <= a.Token {
		lateTakeover++
	}
	if err := b.Release(ctx); err != nil {
		return fmt.Errorf("crash: %w", err)
	}
	fmt.Fprintf(out, "crash: holder stopped renewing; successor acquired after %v (TTL %v), token %d -> %d\n",
		took.Round(time.Millisecond), cfg.ttl, a.Token, b.Token)

	// 4. Store outage: renewals fail; the holder must see the loss by its own deadline.
	o, err := c.TryAcquire(ctx, "outage", "holder", cfg.ttl)
	if err != nil {
		return fmt.Errorf("outage: %w", err)
	}
	store.SetFault(func(op string) error {
		if op == "keepalive" {
			return errors.New("injected: store unreachable")
		}
		return nil
	})
	t0 = time.Now()
	select {
	case <-o.Done():
	case <-time.After(3 * cfg.ttl):
	}
	noticed := time.Since(t0)
	store.SetFault(nil)
	lost := errors.Is(o.Err(), lock.ErrLost) && !o.Valid()
	if !lost {
		notLost++
	}
	if err := o.Release(ctx); err != nil {
		return fmt.Errorf("outage: %w", err)
	}
	fmt.Fprintf(out, "outage: renewals failing; holder saw the loss after %v (TTL %v): %v\n", noticed.Round(time.Millisecond), cfg.ttl, lost)

	fmt.Fprintf(out, "verify: overlaps=%d token_regressions=%d renewal_steals=%d missed_loss=%d late_takeover=%d leases_left=%d (%v)\n",
		overlaps, regressions, steals, notLost, lateTakeover, store.LeaseCount(), time.Since(start).Round(time.Millisecond))
	if overlaps+regressions+steals+notLost+lateTakeover+store.LeaseCount() != 0 {
		return errors.New("verify failed")
	}
	return nil
}
