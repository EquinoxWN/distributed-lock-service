// Package lock gives named locks held as leases with a TTL, renewed in the background, each
// acquisition carrying a fencing token that only ever increases.
//
// A lock is a key under Prefix created on the holder's lease. The key's create revision in the
// store is the fencing token: every write raises the store-wide revision, so a later acquisition
// always gets a larger token than any earlier one, across all clients and service instances.
package lock

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"sync"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
)

// Limits and defaults.
const (
	MinTTL           = 100 * time.Millisecond
	MaxTTL           = 10 * time.Minute
	DefaultPrefix    = "/dls/locks/"
	DefaultOpTimeout = 5 * time.Second
	DefaultRetryMin  = 10 * time.Millisecond
	DefaultRetryMax  = 250 * time.Millisecond
)

// Errors returned by this package.
var (
	// ErrHeld means another holder has the lock (see HeldError for who).
	ErrHeld = errors.New("lock held by another owner")
	// ErrLost means the lease could not be renewed before it expired.
	ErrLost = errors.New("lock lost: lease expired or revoked")
	// ErrReleased means the holder released the lock.
	ErrReleased = errors.New("lock released")
	// ErrAbandoned means the holder stopped renewing without releasing.
	ErrAbandoned = errors.New("lock abandoned: renewals stopped")
	// ErrInvalid wraps every input validation failure.
	ErrInvalid = errors.New("invalid argument")
)

// Holder describes who holds a lock.
type Holder struct {
	Owner string     `json:"owner"`
	Token int64      `json:"token"`
	Lease kv.LeaseID `json:"lease_id,string"`
}

// HeldError is returned by TryAcquire when someone else holds the lock.
type HeldError struct{ Holder Holder }

// Error describes the holder.
func (e *HeldError) Error() string {
	return fmt.Sprintf("%v: owner %q, token %d", ErrHeld, e.Holder.Owner, e.Holder.Token)
}

// Is makes errors.Is(err, ErrHeld) true.
func (e *HeldError) Is(target error) bool { return target == ErrHeld }

var (
	nameRe  = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	ownerRe = regexp.MustCompile(`^[A-Za-z0-9._:@-]{1,128}$`)
)

// Validate checks a lock name, owner and TTL.
func Validate(name, owner string, ttl time.Duration) error {
	switch {
	case !nameRe.MatchString(name):
		return fmt.Errorf("%w: name must match %s", ErrInvalid, nameRe)
	case !ownerRe.MatchString(owner):
		return fmt.Errorf("%w: owner must match %s", ErrInvalid, ownerRe)
	case ttl < MinTTL || ttl > MaxTTL:
		return fmt.Errorf("%w: ttl %v not in %v..%v", ErrInvalid, ttl, MinTTL, MaxTTL)
	}
	return nil
}

// Config configures a Client.
type Config struct {
	Backend   kv.Backend
	Prefix    string        // key prefix, default DefaultPrefix
	OpTimeout time.Duration // per store call
	RetryMin  time.Duration // first wait in Acquire
	RetryMax  time.Duration // longest wait in Acquire
}

// Client acquires locks from one store.
type Client struct {
	cfg Config
}

// New checks the config and fills defaults.
func New(cfg Config) (*Client, error) {
	if cfg.Backend == nil {
		return nil, fmt.Errorf("%w: nil backend", ErrInvalid)
	}
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultPrefix
	}
	if cfg.OpTimeout == 0 {
		cfg.OpTimeout = DefaultOpTimeout
	}
	if cfg.RetryMin == 0 {
		cfg.RetryMin = DefaultRetryMin
	}
	if cfg.RetryMax == 0 {
		cfg.RetryMax = DefaultRetryMax
	}
	if cfg.OpTimeout < 0 || cfg.RetryMin < 0 || cfg.RetryMax < cfg.RetryMin {
		return nil, fmt.Errorf("%w: timeouts %+v", ErrInvalid, cfg)
	}
	return &Client{cfg: cfg}, nil
}

// Key returns the store key for a lock name.
func (c *Client) Key(name string) string { return c.cfg.Prefix + name }

// Get reports the current holder of a lock, if any.
func (c *Client) Get(ctx context.Context, name string) (Holder, bool, error) {
	if !nameRe.MatchString(name) {
		return Holder{}, false, fmt.Errorf("%w: name must match %s", ErrInvalid, nameRe)
	}
	octx, cancel := context.WithTimeout(ctx, c.cfg.OpTimeout)
	defer cancel()
	e, ok, err := c.cfg.Backend.Get(octx, c.Key(name))
	if err != nil || !ok {
		return Holder{}, false, err
	}
	return Holder{Owner: e.Value, Token: e.CreateRevision, Lease: e.Lease}, true, nil
}

// TryAcquire takes the lock if it is free and returns a *HeldError if it is not.
func (c *Client) TryAcquire(ctx context.Context, name, owner string, ttl time.Duration) (*Lock, error) {
	return c.acquire(ctx, name, owner, ttl, false)
}

// Acquire waits until the lock is free or ctx ends.
func (c *Client) Acquire(ctx context.Context, name, owner string, ttl time.Duration) (*Lock, error) {
	return c.acquire(ctx, name, owner, ttl, true)
}

// acquire grants a lease, keeps it alive and tries to create the lock key on it.
func (c *Client) acquire(ctx context.Context, name, owner string, ttl time.Duration, wait bool) (*Lock, error) {
	if err := Validate(name, owner, ttl); err != nil {
		return nil, err
	}
	sent := time.Now()
	octx, cancel := context.WithTimeout(ctx, c.cfg.OpTimeout)
	id, granted, err := c.cfg.Backend.Grant(octx, ttl)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("acquire %s: grant: %w", name, err)
	}
	l := &Lock{Name: name, Owner: owner, Lease: id, TTL: granted, c: c, key: c.Key(name),
		until: sent.Add(granted), done: make(chan struct{}), stop: make(chan struct{}), stopped: make(chan struct{})}
	go l.keepAlive()
	delay := c.cfg.RetryMin
	for {
		octx, cancel := context.WithTimeout(ctx, c.cfg.OpTimeout)
		created, cur, err := c.cfg.Backend.CreateIfAbsent(octx, l.key, owner, id)
		cancel()
		switch {
		case err == nil && (created || cur.Lease == id):
			// cur.Lease == id: an earlier attempt succeeded but its answer was lost.
			l.Token = cur.CreateRevision
			return l, nil
		case err == nil && !wait:
			return nil, errors.Join(&HeldError{Holder: Holder{Owner: cur.Value, Token: cur.CreateRevision, Lease: cur.Lease}}, l.cleanup(ctx))
		case err != nil && (!wait || !errors.Is(err, kv.ErrUnavailable)):
			return nil, errors.Join(fmt.Errorf("acquire %s: %w", name, err), l.cleanup(ctx))
		}
		t := time.NewTimer(rand.N(delay) + 1)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, errors.Join(fmt.Errorf("acquire %s: %w", name, ctx.Err()), l.cleanup(ctx))
		case <-l.done:
			t.Stop()
			return nil, errors.Join(fmt.Errorf("acquire %s: %w", name, l.Err()), l.cleanup(ctx))
		case <-t.C:
		}
		delay = min(2*delay, c.cfg.RetryMax)
	}
}

// Lock is one held lock. Its fencing token goes with every write the holder makes.
type Lock struct {
	Name  string
	Owner string
	Token int64
	Lease kv.LeaseID
	TTL   time.Duration

	c       *Client
	key     string
	mu      sync.Mutex
	until   time.Time // deadline, guarded by mu
	err     error     // why the lock ended, guarded by mu
	once    sync.Once
	done    chan struct{}
	stop    chan struct{}
	stopped chan struct{}
	stopOne sync.Once
}

// Done is closed when the lock ends: lost, released or abandoned.
func (l *Lock) Done() <-chan struct{} { return l.done }

// Err says why the lock ended; nil while it is held.
func (l *Lock) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// Valid reports whether the lock is held and its lease deadline, as this client measures it, is ahead.
func (l *Lock) Valid() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err == nil && time.Now().Before(l.until)
}

// end records why the lock ended, once.
func (l *Lock) end(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.done)
	})
}

// keepAlive renews the lease every TTL/3 and ends the lock if its deadline passes without a renewal.
func (l *Lock) keepAlive() {
	defer close(l.stopped)
	next := l.TTL / 3
	for {
		t := time.NewTimer(next)
		select {
		case <-l.stop:
			t.Stop()
			return
		case <-t.C:
		}
		l.mu.Lock()
		until := l.until
		l.mu.Unlock()
		sent := time.Now()
		// The call may not outlive the deadline: at the deadline the lock is gone either way.
		callEnd := sent.Add(l.c.cfg.OpTimeout)
		if until.Before(callEnd) {
			callEnd = until
		}
		ctx, cancel := context.WithDeadline(context.Background(), callEnd)
		ttl, err := l.c.cfg.Backend.KeepAlive(ctx, l.Lease)
		cancel()
		switch {
		case err == nil:
			l.mu.Lock()
			l.until = sent.Add(ttl)
			l.mu.Unlock()
			next = ttl / 3
		case errors.Is(err, kv.ErrLeaseNotFound):
			l.end(fmt.Errorf("%w: %w", ErrLost, err))
			return
		case !time.Now().Before(until):
			l.end(fmt.Errorf("%w: no renewal before the deadline: %w", ErrLost, err))
			return
		default:
			next = max(l.TTL/12, time.Millisecond) // retry sooner while the deadline is ahead
		}
	}
}

// halt stops the renewal goroutine and waits for it.
func (l *Lock) halt() {
	l.stopOne.Do(func() { close(l.stop) })
	<-l.stopped
}

// cleanup stops renewals and revokes the lease after a failed acquisition.
func (l *Lock) cleanup(ctx context.Context) error {
	l.halt()
	l.end(ErrReleased)
	octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.c.cfg.OpTimeout)
	defer cancel()
	if err := l.c.cfg.Backend.Revoke(octx, l.Lease); err != nil && !errors.Is(err, kv.ErrLeaseNotFound) {
		return fmt.Errorf("revoke lease %d: %w", l.Lease, err)
	}
	return nil
}

// Release deletes the lock key if this lease still holds it, then revokes the lease; safe to call twice.
func (l *Lock) Release(ctx context.Context) error {
	l.halt()
	l.end(ErrReleased)
	octx, cancel := context.WithTimeout(ctx, l.c.cfg.OpTimeout)
	defer cancel()
	_, derr := l.c.cfg.Backend.DeleteIfLease(octx, l.key, l.Lease)
	if derr != nil {
		derr = fmt.Errorf("release %s: delete: %w", l.Name, derr)
	}
	rerr := l.c.cfg.Backend.Revoke(octx, l.Lease)
	if errors.Is(rerr, kv.ErrLeaseNotFound) {
		rerr = nil
	} else if rerr != nil {
		rerr = fmt.Errorf("release %s: revoke: %w", l.Name, rerr)
	}
	return errors.Join(derr, rerr)
}

// Abandon stops renewals without releasing, as a crashed holder would; the lease then expires.
func (l *Lock) Abandon() {
	l.halt()
	l.end(ErrAbandoned)
}
