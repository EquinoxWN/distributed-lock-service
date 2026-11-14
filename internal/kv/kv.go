// Package kv defines the consensus store the lock service needs: leases and a compare-and-create.
//
// The operations mirror the etcd v3 API (lease grant, keep-alive and revoke; a transaction that
// creates a key only if it does not exist). Every write raises one store-wide revision, so the
// create revision of a lock key only ever increases: it is the fencing token.
package kv

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// LeaseID identifies a lease in the store.
type LeaseID int64

// KeyValue is one key as the store reports it.
type KeyValue struct {
	Key            string
	Value          string
	CreateRevision int64
	ModRevision    int64
	Lease          LeaseID
}

// Errors every backend maps its failures to.
var (
	// ErrLeaseNotFound means the lease expired, was revoked or never existed.
	ErrLeaseNotFound = errors.New("lease not found")
	// ErrUnavailable means the store could not be reached or did not answer in time.
	ErrUnavailable = errors.New("store unavailable")
)

// Backend is a linearizable key-value store with leases.
type Backend interface {
	// Grant creates a lease and returns its id and the TTL the store actually granted.
	Grant(ctx context.Context, ttl time.Duration) (LeaseID, time.Duration, error)
	// KeepAlive renews a lease and returns its new TTL, or ErrLeaseNotFound.
	KeepAlive(ctx context.Context, id LeaseID) (time.Duration, error)
	// Revoke ends a lease and deletes the keys attached to it.
	Revoke(ctx context.Context, id LeaseID) error
	// CreateIfAbsent puts key=value on the lease if the key does not exist; it returns whether it
	// did and the key as it now stands (ours, or the current holder's).
	CreateIfAbsent(ctx context.Context, key, value string, lease LeaseID) (bool, KeyValue, error)
	// DeleteIfLease deletes the key only if it is attached to the given lease.
	DeleteIfLease(ctx context.Context, key string, lease LeaseID) (bool, error)
	// Get reads one key with a linearizable read.
	Get(ctx context.Context, key string) (KeyValue, bool, error)
}

// Limits for keys and values, far below etcd's request limits.
const (
	MaxKeyLen   = 256
	MaxValueLen = 1024
)

// ValidateKV checks a key and value before they are sent to a store.
func ValidateKV(key, value string) error {
	if key == "" || len(key) > MaxKeyLen {
		return fmt.Errorf("key length %d not in 1..%d", len(key), MaxKeyLen)
	}
	if len(value) > MaxValueLen {
		return fmt.Errorf("value length %d over %d", len(value), MaxValueLen)
	}
	return nil
}
