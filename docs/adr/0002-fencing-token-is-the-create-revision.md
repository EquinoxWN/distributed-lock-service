# ADR 0002: The fencing token is the lock key's create revision

- **Status:** Accepted

## Context

Every acquisition needs a token that is larger than any token handed out before, for any lock name
and any client, so that storage can later reject writes from an older holder (M2). Options: a
counter key incremented in the acquiring transaction, a timestamp, or something the store already
orders.

## Decision

Use the create revision of the lock key. etcd keeps one revision for the whole keyspace, raises it
on every write, and Raft gives all members the same order, so the revision at which a key is created
is unique and later creates get larger ones. The in-process store keeps the same single counter,
raised on every put, delete and lease revocation that deletes keys.

## Consequences

- No extra key or write per acquisition, and no clock involved: tokens come from consensus order.
- Tokens are not contiguous (other writes raise the revision too) and are shared across names;
  storage must compare with "greater than the last token seen", never "equals last + 1".
- Tokens from different etcd clusters are not comparable; a restored backup or a new cluster starts
  lower. Moving a protected resource to another cluster needs a token floor (not in M1).
- The rule is held by tests (`EveryCreateGetsANewRevision`, `CreateRevisionsOnlyIncrease` in the
  backend contract, the token checks in the lock tests and the demo) and was checked with mutants
  that drop the revision bump.
