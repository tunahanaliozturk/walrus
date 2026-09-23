# 3. Ordering by lane, idempotency by each row's state, in the sink

Status: accepted

## Context

Every sink must apply one row's changes in the order they committed, apply different rows in parallel, and
survive redelivery: dispatch saves its position every 200 ms, so a crash repeats up to that much.

The design this started from kept a per-sink watermark keyed by a hash of the primary key, and skipped any change
whose commit LSN was at or below it. Two problems. Two rows whose keys hash alike share a watermark, so one row's
progress skips the other's changes. And a transaction that inserts a row and then updates it produces two changes
with the same commit LSN, so the update is skipped as already applied and the row keeps its inserted values.

## Decision

- **Ordering.** Every change to a row is routed to one of N lanes by a hash of the row's table and key, and a lane
  applies one batch at a time. A hash is fine here: two rows sharing a lane only means one waits for the other.
- **Idempotency.** Each sink keeps, per row, a `KeyState`: the newest delete, the newest write and its image, and for
  each column the newest write that touched it. Applying a change is a maximum over versions, which gives the same
  answer whatever order changes arrive in, however often, and however they are grouped. Those three properties are
  tested as properties with FsCheck, and the result is also compared with a reference computed from the whole
  history at once.
- **Versions** are (hybrid logical clock, source, ordinal within the transaction), a total order. The ordinal is
  what keeps an insert and an update in one transaction apart.
- **Where the state lives.** The Postgres sink writes each row's state in the sink database, in the same
  transaction as the row. A redelivered change reads the state, finds itself already absorbed, and writes
  nothing. The idempotency boundary is where the effect is, so nothing upstream has to be exactly once.

## Consequences

- Replaying a sink from the beginning changes nothing: the integration test counts writes to the sink table with
  a trigger and finds none. Rebuilding with `reset=true` gives the same rows.
- The soak and failover runs record every version the sink applies with a trigger, and count a row whose sequence
  ever goes down. The count has been zero in every run.
- The state costs about as much space as the row again, and a deleted row keeps its state as a tombstone so a
  late write from another source cannot resurrect it. Tombstones are not collected; see the known limitations.
- The webhook sink cannot hold state in someone else's system. It delivers at least once with an id per change
  that is stable across redeliveries, and deduplication is the receiver's job.

## Alternatives considered

**A watermark per hashed key.** Rejected for the two reasons above; both were reproduced as failing properties
before the version tuple replaced it.

**Exactly-once delivery through a distributed transaction to the sink.** Heavier, slower, and unavailable for most
sinks. At least once into state that cannot double-count gets the same effect.
