package lock

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
	"github.com/EquinoxWN/distributed-lock-service/internal/memkv"
)

func TestGrantKeepAliveReleaseBySecret(t *testing.T) {
	c, s := setup(t)
	ctx := context.Background()
	g, err := c.Grant(ctx, "job", "a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := hex.DecodeString(g.Secret)
	if err != nil || len(raw) != SecretBytes || SecretBytes*8 < 128 {
		t.Fatalf("secret %q: %d bytes, err %v; want %d random bytes (>= 128 bits)", g.Secret, len(raw), err, SecretBytes)
	}
	if got, err := c.KeepAlive(ctx, "job", g.Secret); err != nil || got != ttl {
		t.Fatalf("keepalive: %v %v", got, err)
	}
	if _, err := c.Grant(ctx, "job", "b", ttl); !errors.Is(err, ErrHeld) {
		t.Fatalf("second grant: %v", err)
	}
	if err := c.Release(ctx, "job", g.Secret); err != nil {
		t.Fatal(err)
	}
	if err := c.Release(ctx, "job", g.Secret); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if _, err := c.KeepAlive(ctx, "job", g.Secret); !errors.Is(err, kv.ErrLeaseNotFound) {
		t.Fatalf("keepalive after release: %v", err)
	}
	if n := s.LeaseCount(); n != 0 {
		t.Fatalf("%d leases left", n)
	}
}

func TestWrongMissingOrGuessedSecretIsRejected(t *testing.T) {
	c, s := setup(t)
	ctx := context.Background()
	g, err := c.Grant(ctx, "job", "a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	other, err := c.Grant(ctx, "other", "b", ttl)
	if err != nil {
		t.Fatal(err)
	}
	flipped := []byte(g.Secret)
	flipped[len(flipped)-1] ^= 1
	bad := map[string]error{
		"":                                    ErrSecretRequired,
		strings.Repeat("0", 64):               ErrForbidden,
		string(flipped):                       ErrForbidden,
		other.Secret:                          ErrForbidden, // a valid secret of another lock
		strconv.FormatInt(int64(g.Lease), 10): ErrForbidden, // a guessed lease id
		g.Secret + "x":                        ErrForbidden,
		strings.Repeat("a", 4096):             ErrForbidden,
	}
	for secret, want := range bad {
		if err := c.Release(ctx, "job", secret); !errors.Is(err, want) {
			t.Errorf("release with %.12q: %v, want %v", secret, err, want)
		}
		if _, err := c.KeepAlive(ctx, "job", secret); !errors.Is(err, want) {
			t.Errorf("keepalive with %.12q: %v, want %v", secret, err, want)
		}
	}
	h, ok, err := c.Get(ctx, "job")
	if err != nil || !ok || h.Owner != "a" || h.Lease != g.Lease {
		t.Fatalf("holder after rejected calls: %+v %v %v", h, ok, err)
	}
	if n := s.LeaseCount(); n != 2 {
		t.Fatalf("%d leases, want 2", n)
	}
}

func TestStoredValueHoldsOnlyAHashOfTheSecret(t *testing.T) {
	c, s := setup(t)
	l := mustAcquire(t, c, "job", "a")
	e, ok, err := s.Get(context.Background(), c.Key("job"))
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if strings.Contains(e.Value, l.Secret) || !strings.Contains(e.Value, `"owner":"a"`) {
		t.Fatalf("stored value %q", e.Value)
	}
	if same, err := matches(e, l.Secret); err != nil || !same {
		t.Fatalf("own secret does not match: %v %v", same, err)
	}
	if _, err := c.KeepAlive(context.Background(), "job", l.Secret); err != nil {
		t.Fatalf("library lock's secret works by name: %v", err)
	}
}

func TestSecretsDifferPerAcquisition(t *testing.T) {
	c, _ := setup(t)
	seen := map[string]bool{}
	for i := range 50 {
		var secret string
		if i%2 == 0 {
			l := mustAcquire(t, c, "job", "a")
			secret = l.Secret
			if err := l.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
		} else {
			g, err := c.Grant(context.Background(), "job", "a", ttl)
			if err != nil {
				t.Fatal(err)
			}
			secret = g.Secret
			if err := c.Release(context.Background(), "job", secret); err != nil {
				t.Fatal(err)
			}
		}
		if len(secret) != 2*SecretBytes || seen[secret] {
			t.Fatalf("acquisition %d: secret %q repeated or wrong length", i, secret)
		}
		seen[secret] = true
	}
}

func TestForeignValueIsAnError(t *testing.T) {
	s := memkv.New(nil)
	c, _ := New(Config{Backend: s})
	ctx := context.Background()
	id, _, _ := s.Grant(ctx, time.Minute)
	if _, _, err := s.CreateIfAbsent(ctx, c.Key("job"), "plain owner", id); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Get(ctx, "job"); err == nil {
		t.Fatal("foreign value read as a holder")
	}
	if err := c.Release(ctx, "job", strings.Repeat("0", 64)); err == nil || errors.Is(err, ErrForbidden) {
		t.Fatalf("foreign value: %v", err)
	}
	if _, err := c.TryAcquire(ctx, "job", "a", ttl); err == nil || errors.Is(err, ErrHeld) {
		t.Fatalf("acquire over a foreign value: %v", err)
	}
	if _, err := c.Grant(ctx, "job", "a", ttl); err == nil || errors.Is(err, ErrHeld) {
		t.Fatalf("grant over a foreign value: %v", err)
	}
	if n := s.LeaseCount(); n != 1 {
		t.Fatalf("%d leases, want only the foreign one", n)
	}
}
