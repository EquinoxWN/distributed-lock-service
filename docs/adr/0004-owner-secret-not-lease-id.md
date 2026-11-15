# ADR 0004: A random owner secret, not the lease id, proves who holds a lock

- **Status:** Accepted (replaces the lease-id routes of the first M1 version)

## Context

The first M1 version of the HTTP API took the lease id as the only proof of ownership:
`POST /v1/leases/{id}/keepalive` and `DELETE /v1/locks/{name}?lease_id=`. A security review found
this was a predictable capability. The in-process store hands out lease ids 1, 2, 3...; `GET` and the
409 answer show the holder's lease id to anyone; and etcd lease ids are not secrets either. Any caller
could release or keep renewing another holder's lock.

## Decision

- Each acquisition gets a fresh owner secret: 32 bytes (256 bits) from `crypto/rand`, hex encoded.
- The lock key's value stores the owner and the SHA-256 of the secret, never the secret
  (`{"owner":"...","secret_sha256":"..."}`), with both backends.
- The secret is returned once (the `secret` field of the acquire answer, or `Lock.Secret` /
  `Granted.Secret` in Go). Keep-alive and release by name require it: in Go,
  `Client.KeepAlive(name, secret)` and `Client.Release(name, secret)`; over HTTP,
  `POST /v1/locks/{name}/keepalive` and `DELETE /v1/locks/{name}` with a `Lock-Secret` header.
  The lease-id routes are removed.
- The presented secret is hashed and compared with `crypto/subtle.ConstantTimeCompare`. A missing
  secret is 401 (`ErrSecretRequired`), a wrong one 403 (`ErrForbidden`). The operation then acts on
  the lease stored with the key, never on a lease id from the caller.
- The secret is never logged: the server logs no request headers, error messages name the lock but
  never the secret, and answers carry `Cache-Control: no-store`.
- A `*lock.Lock` used inside the process that acquired it still releases by its own lease: the
  `*Lock` value is the capability there and its lease id never comes from outside.
- The fencing token is unchanged.

## Consequences

- Guessing a lease id, or reading it from `GET`, no longer gives control of a lock. Guessing a
  secret means guessing 256 random bits.
- A store that holds lock keys now holds a hash per lock, about 100 bytes. Lock keys written by other
  tools (a plain owner string) are reported as an error instead of being read as a holder.
- The secret is a bearer credential: a client that leaks it lets others release its lock. TLS between
  client and service is not part of M1 (the service binds to 127.0.0.1 by default).
- Check then act: keep-alive and release read the key, check the secret, then act on the stored
  lease. Between the read and the act the lock can expire and be re-acquired. The act still only
  touches the old lease: `DeleteIfLease` compares the lease, and a renewal of an expired lease fails
  with "lease not found". So a stale holder cannot affect the new holder.
- API change in M1 before any release: the old lease-id routes now answer 404 (tested).
- Held by `secret_test.go` (5 tests), the API tests for missing, wrong, other-lock and guessed
  secrets, a no-secret-in-logs test, the etcd integration test (CI), and 8 hand mutants that restore
  the old behaviour, all killed.
