# RFC 0001: distributed-lock-service design

- **Status:** Accepted for M1 (M2 and M3 sections are plans)
- **Author:** EquinoxWN
- **Created:** 2026-11-14

## Problem

Two workers both think they hold the lock and corrupt data. A lock held by a process that pauses
(GC, swap, a stopped VM) or crashes must be freed for others, and the freed holder must be able to
find out; any lock that frees itself on a timer also needs a way for storage to reject the old
holder's late writes. This service gives named locks held as leases with a time-to-live, renewed
by the holder, stored in etcd, and every acquisition returns a fencing token that only increases.

Users: backend engineers who run several workers or service instances against shared state (a job
that must run once, a single writer per shard) and want a lock with defined failure behaviour.

## Critical-to-quality measures (Define)

| CTQ | Target (M1) | How it is measured |
|---|---|---|
| Mutual exclusion | 0 overlapping holders | Concurrent tests and the demo count clients inside the critical section at once |
| Token order | Every acquisition's token is larger than every earlier one, across names and clients | Tests and the demo check the token sequence in acquisition order |
| Renewal | A holder that renews keeps the lock indefinitely; 0 steals over 5 TTLs | Keep-alive tests and the demo's renewal scenario |
| Crash recovery | A lock whose holder stops renewing is free within one TTL of its last renewal | Abandon tests; the demo measures takeover time |
| Loss detection | A holder whose renewals fail sees `ErrLost` by its own lease deadline, and `Valid()` is false after it | Outage tests; the demo measures time to notice |
| Leak-free | 0 leases left after releases and failed acquisitions | Lease counts in tests and the demo |

## Goals

- M1: clients acquire a lock as a lease with a TTL and must keep renewing it; state is in etcd
  (consensus) or, for local runs, an in-process store with the same rules.
- M1: every acquisition returns a fencing token that only ever increases.
- M2: protected storage rejects writes with an older token; tests freeze a client mid-write.
- M3: injected partitions and clock jumps; a naive lock without fencing runs alongside to count
  the corruptions it allows.

## Non-goals

- A hosted production service, authentication or multi-tenant quotas.
- Fair (FIFO) queueing of waiters; M1 waiters poll with jittered backoff (see Alternatives).
- Re-entrant locks, read/write locks, or semaphores.

## Design

```
client (Go library or HTTP) --> lock layer --> kv.Backend --> etcd v3 (JSON gateway)  [CI]
                                                         \--> memkv (in-process)      [local]
```

- **`kv.Backend`** is the smallest etcd-shaped interface the lock needs: lease grant, keep-alive,
  revoke; a transaction that creates a key only if its create revision is 0 (else returns the
  current key); a delete that compares the key's lease; a linearizable get.
- **Lock = key on a lease.** `Acquire(name, owner, ttl)` grants a lease of `ttl`, starts renewing it,
  then creates `/dls/locks/<name>` with the owner as value on that lease. If the key exists,
  `TryAcquire` revokes its lease and returns the holder; `Acquire` keeps renewing and retries with
  jittered exponential backoff (10 ms to 250 ms) until the key is free, the lease is lost or the
  context ends.
- **Fencing token = the key's create revision** (ADR 0002). etcd raises one cluster-wide revision on
  every write and Raft orders them, so a later create always has a larger revision, for any name and
  any client. memkv keeps the same single counter under one mutex.
- **Renewal.** A goroutine sends a keep-alive every TTL/3. The client's deadline is the time the
  keep-alive was *sent* plus the granted TTL, so the client believes its lease ends no later than the
  store does. A failed renewal is retried every TTL/12 while the deadline is ahead; each call's
  timeout is capped at the deadline. At the deadline (or on "lease not found") the lock ends with
  `ErrLost`, `Done()` closes and `Valid()` is false. Durations use Go's monotonic clock.
- **Release** deletes the key only if this lease holds it (so a stale holder cannot delete a
  successor's key), then revokes the lease. Repeating it is harmless.
- **Lost replies.** If a create succeeded but its reply was lost, the retry finds the key on its own
  lease and treats it as acquired.
- **HTTP API** for other languages: `POST /v1/locks/{name}` (acquire, 409 with the holder if held),
  `POST /v1/leases/{id}/keepalive`, `DELETE /v1/locks/{name}?lease_id=`, `GET /v1/locks/{name}`,
  `GET /healthz`. The service is stateless over the store, so several instances can share one etcd.
- **etcd access** is the v3 JSON gateway over `net/http`, standard library only (ADR 0003), with
  per-request timeouts, bounded retries with backoff, and rotation across endpoints.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Official etcd Go client (`clientv3` + `concurrency.Mutex`) | Correct and battle-tested, but brings gRPC and about 40 modules; the point of this repo is to show the lease and token mechanics, and the gateway keeps the dependency list empty (ADR 0003). Could replace `etcdkv` later behind the same interface. |
| Redis `SET NX PX` (single node or Redlock) | No consensus on one node; Redlock depends on bounded clock drift and has no monotonic token, which is the failure this project exists to show (M3 will run a naive lock for comparison). |
| Separate counter key for tokens (`INCR`-style) | Needs a second write in the same transaction and adds a key to keep consistent; the create revision is already monotonic and free (ADR 0002). |
| Watch the key instead of polling while waiting | Lower latency and fewer requests, and FIFO order with etcd's revision-ordered queue; needs a streaming watch through the gateway. Kept for M2; M1 polls with capped backoff. |
| One session lease shared by all locks of a client (etcd `concurrency.Session`) | Fewer leases, but losing one renewal loses every lock at once; M1 keeps one lease per lock so each lock has its own TTL. |

## Measurement plan

- M1 (done, see `docs/results/m1.md`): unit, contract, concurrency and end-to-end tests locally on
  the in-process store; the demo's four scenarios (contention, renewal, crash, outage) with a
  `verify:` line; hand mutation testing. Against real etcd: a contract run, six clients spread over
  three members, and a crash takeover, in CI only.
- M2/M3: a stale-writer test against fenced storage, then violation counts for a naive lock vs the
  fenced lock under injected pauses, partitions and clock jumps.

## Milestones

- **M1:** Clients acquire a lock as a lease with a TTL and must keep renewing it. Every acquisition
  returns a fencing token that only ever increases.
- **M2:** Protected storage rejects writes carrying an older token. Tests freeze a client mid-write
  (simulating a long GC pause) while another client acquires the lock.
- **M3:** Partitions and clock jumps are injected. A naive lock without fencing runs alongside to
  count the corruptions it allows.

## Risks and open questions

- **A lease is not mutual exclusion in time.** A holder paused longer than its TTL keeps running
  while a successor holds the lock; `Valid()` and `Done()` only help once it resumes. The fencing
  token is the defence, and storage that checks it is M2. Until then, M1 alone does not prevent a
  paused holder's late write.
- **Clock rate, not clock value.** The client's deadline uses its own monotonic clock; if it runs
  much slower than the store's, the client could think it holds a lease that has expired. etcd
  assumes bounded rate drift too.
- **The JSON gateway's shapes** are verified locally only against a fake that follows etcd's
  documented format; the real gateway is exercised by the CI job, which has not run yet.
- etcd's minimum lease TTL is a few seconds and TTLs are whole seconds; short TTLs are rounded up,
  and the granted TTL is the one the client uses.
