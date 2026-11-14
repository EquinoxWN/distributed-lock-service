# ADR 0003: Talk to etcd through its v3 JSON gateway with the standard library

- **Status:** Accepted

## Context

The lock needs five etcd operations: lease grant, keep-alive and revoke, and two small
transactions (create if absent, delete if the lease matches) plus a range read. The official Go
client (`go.etcd.io/etcd/client/v3`) does all of this over gRPC and includes a ready-made mutex, but
pulls in gRPC, protobuf and about 40 modules. etcd also serves the same API as JSON over HTTP
(`/v3/lease/grant`, `/v3/kv/txn`, ...).

## Decision

Implement `kv.Backend` for etcd with `net/http` and `encoding/json` against the JSON gateway
(`internal/etcdkv`). `go.mod` has no `require` lines. Each request has a timeout (3 s default);
transport errors and 5xx answers are retried at most 3 times with doubling backoff, rotating through
the endpoints; 4xx answers are not retried. Keep-alive sends one message per call.

## Consequences

- Nothing to vendor or update; govulncheck covers only the Go standard library. The flip side: a
  standard-library advisory hits directly (GO-2026-6859 in net/http is reported with Go 1.26.10 and
  allowed by ID in `scripts/vulncheck.sh` until CI moves to 1.26.11).
- One HTTP request per keep-alive instead of one long gRPC stream: more requests, simpler code. At a
  TTL of 10 s that is one request every 3.3 s per held lock.
- The request and response shapes are written by hand. Locally they are tested against a fake
  gateway that follows etcd's documented JSON format; the real gateway is tested only in CI (three
  etcd members), which has not run yet. If that run shows a mismatch, the fix stays inside `etcdkv`.
- Retried writes need care: a create whose reply was lost is recognised by its lease id at the lock
  layer; a retried grant can leave an unused lease that expires after its TTL.
- If watches (for FIFO waiting) are added in M2, the gateway's streaming watch or a move to
  `clientv3` behind the same interface will be reconsidered.
