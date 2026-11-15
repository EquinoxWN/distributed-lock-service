package lock

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/EquinoxWN/distributed-lock-service/internal/kv"
)

// SecretBytes is the size of an owner secret before hex encoding (256 bits).
const SecretBytes = 32

// Errors for the owner secret.
var (
	// ErrSecretRequired means no owner secret was given.
	ErrSecretRequired = errors.New("owner secret required")
	// ErrForbidden means the owner secret does not match the current holder's.
	ErrForbidden = errors.New("owner secret does not match the lock holder")
)

// stored is the value of a lock key: the owner and the SHA-256 of its secret, never the secret.
type stored struct {
	Owner string `json:"owner"`
	Hash  string `json:"secret_sha256"`
}

// newSecret returns a fresh random owner secret, hex encoded.
func newSecret() string {
	b := make([]byte, SecretBytes)
	_, _ = rand.Read(b) // crypto/rand.Read never returns an error (Go 1.24+); it aborts the program instead
	return hex.EncodeToString(b)
}

// hashSecret returns the SHA-256 of a secret.
func hashSecret(secret string) []byte {
	h := sha256.Sum256([]byte(secret))
	return h[:]
}

// encodeValue builds the stored value for an owner and its secret.
func encodeValue(owner, secret string) string {
	b, _ := json.Marshal(stored{Owner: owner, Hash: hex.EncodeToString(hashSecret(secret))}) // two strings always encode
	return string(b)
}

// decodeValue parses a stored value.
func decodeValue(v string) (stored, error) {
	var s stored
	if err := json.Unmarshal([]byte(v), &s); err != nil || s.Owner == "" || len(s.Hash) != 2*sha256.Size {
		return stored{}, errors.New("lock key holds a value this service did not write")
	}
	return s, nil
}

// holderOf turns a stored key into a Holder (without the secret hash).
func holderOf(e kv.KeyValue) (Holder, error) {
	s, err := decodeValue(e.Value)
	if err != nil {
		return Holder{}, fmt.Errorf("%s: %w", e.Key, err)
	}
	return Holder{Owner: s.Owner, Token: e.CreateRevision, Lease: e.Lease}, nil
}

// matches compares a presented secret with the stored hash in constant time.
func matches(e kv.KeyValue, secret string) (bool, error) {
	s, err := decodeValue(e.Value)
	if err != nil {
		return false, fmt.Errorf("%s: %w", e.Key, err)
	}
	want, err := hex.DecodeString(s.Hash)
	if err != nil {
		return false, fmt.Errorf("%s: stored secret hash is not hex", e.Key)
	}
	return subtle.ConstantTimeCompare(hashSecret(secret), want) == 1, nil
}

// verified reads the lock key and checks the secret; found is false when nobody holds the lock.
func (c *Client) verified(ctx context.Context, name, secret string) (kv.KeyValue, bool, error) {
	if !nameRe.MatchString(name) {
		return kv.KeyValue{}, false, fmt.Errorf("%w: name must match %s", ErrInvalid, nameRe)
	}
	if secret == "" {
		return kv.KeyValue{}, false, ErrSecretRequired
	}
	if len(secret) > 4*SecretBytes {
		return kv.KeyValue{}, false, ErrForbidden
	}
	e, ok, err := c.cfg.Backend.Get(ctx, c.Key(name))
	if err != nil || !ok {
		return kv.KeyValue{}, false, err
	}
	same, err := matches(e, secret)
	if err != nil {
		return kv.KeyValue{}, false, err
	}
	if !same {
		return kv.KeyValue{}, false, fmt.Errorf("%s: %w", name, ErrForbidden)
	}
	return e, true, nil
}

// Granted is the result of a one-shot acquisition whose holder renews it by name and secret.
type Granted struct {
	Name   string
	Owner  string
	Token  int64
	Lease  kv.LeaseID
	TTL    time.Duration
	Secret string // returned once; needed for KeepAlive and Release
}

// Grant takes the lock if it is free, without background renewal; it returns a *HeldError if it is held.
func (c *Client) Grant(ctx context.Context, name, owner string, ttl time.Duration) (Granted, error) {
	if err := Validate(name, owner, ttl); err != nil {
		return Granted{}, err
	}
	octx, cancel := context.WithTimeout(ctx, c.cfg.OpTimeout)
	defer cancel()
	id, granted, err := c.cfg.Backend.Grant(octx, ttl)
	if err != nil {
		return Granted{}, fmt.Errorf("acquire %s: grant: %w", name, err)
	}
	secret := newSecret()
	ok, cur, err := c.cfg.Backend.CreateIfAbsent(octx, c.Key(name), encodeValue(owner, secret), id)
	if err == nil && (ok || cur.Lease == id) {
		return Granted{Name: name, Owner: owner, Token: cur.CreateRevision, Lease: id, TTL: granted, Secret: secret}, nil
	}
	if err == nil {
		var h Holder
		if h, err = holderOf(cur); err == nil {
			err = &HeldError{Holder: h}
		}
	} else {
		err = fmt.Errorf("acquire %s: %w", name, err)
	}
	rctx, rcancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.OpTimeout)
	defer rcancel()
	if rerr := c.cfg.Backend.Revoke(rctx, id); rerr != nil && !errors.Is(rerr, kv.ErrLeaseNotFound) {
		err = errors.Join(err, fmt.Errorf("revoke unused lease %d: %w", id, rerr))
	}
	return Granted{}, err
}

// KeepAlive renews the lease of the lock's current holder if the secret is the holder's.
func (c *Client) KeepAlive(ctx context.Context, name, secret string) (time.Duration, error) {
	octx, cancel := context.WithTimeout(ctx, c.cfg.OpTimeout)
	defer cancel()
	e, ok, err := c.verified(octx, name, secret)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("keepalive %s: lock not held: %w", name, kv.ErrLeaseNotFound)
	}
	return c.cfg.Backend.KeepAlive(octx, e.Lease)
}

// Release frees the lock if the secret is the holder's; a lock nobody holds is already released.
func (c *Client) Release(ctx context.Context, name, secret string) error {
	octx, cancel := context.WithTimeout(ctx, c.cfg.OpTimeout)
	defer cancel()
	e, ok, err := c.verified(octx, name, secret)
	if err != nil || !ok {
		return err
	}
	if _, err := c.cfg.Backend.DeleteIfLease(octx, e.Key, e.Lease); err != nil {
		return fmt.Errorf("release %s: delete: %w", name, err)
	}
	if err := c.cfg.Backend.Revoke(octx, e.Lease); err != nil && !errors.Is(err, kv.ErrLeaseNotFound) {
		return fmt.Errorf("release %s: revoke: %w", name, err)
	}
	return nil
}
