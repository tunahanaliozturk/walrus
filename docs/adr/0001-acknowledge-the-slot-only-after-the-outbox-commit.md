# 1. Acknowledge the slot only after the outbox commit

Status: accepted

## Context

A logical replication slot keeps every transaction after its confirmed position and sends them to whoever
attaches next. That position is the only durability the source offers a consumer: move it past a transaction,
and the source may throw the transaction away.

Capture reads transactions from the slot and writes them to the outbox, a table in Walrus's own database that
every sink reads from. Two writes to two databases, and no transaction spans both.

## Decision

A batch of whole source transactions is written to the outbox in one store transaction, and only after that
commits is the end of the last transaction acknowledged to the slot. The two lines sit next to each other in
`CaptureSession.PersistAsync`, in that order, with a comment saying why.

A crash between them costs a resend. The next session reads the checkpoint the outbox commit wrote, the source
resends from its own older position, and every transaction that ends at or before the checkpoint is skipped by
position. Capture writes one source's transactions in commit order and never splits one across outbox commits,
so "ends at or before the checkpoint" is exactly "already stored". A unique index on (source, commit LSN,
ordinal) backs that up: a bug in the position check becomes a failed write, not a duplicate.

The session also passes its checkpoint as the start position when it attaches, so the source skips most of what
the outbox already holds before sending it at all.

## Consequences

- No change is ever acknowledged before it is durable. `CaptureSessionTests` checks the order of every write and
  acknowledgement; the integration test runs a session that never acknowledges, restarts, and finds the slot
  still holding everything and the outbox holding each change once.
- A source whose captured tables are quiet produces nothing to acknowledge, so its slot would pin the log forever.
  A heartbeat row, updated every few seconds and filtered out of the outbox, gives every source a recent
  transaction to acknowledge.
- Batches are group commits with no linger: whatever queued while the last write was committing goes into the
  next one. An idle source gets small fast commits; a busy one gets large ones.

## Alternatives considered

**Acknowledge on receipt, as a message queue consumer might.** Simpler, and loses every transaction that was
received but not yet stored when the process dies.

**Keep the outbox in the source database and write it in the same transaction as the application.** That is the
transactional outbox pattern, and it is the right tool when you own the application. A CDC engine exists for the
case where you do not, and capturing from the log is the point.

**Deduplicate by looking up each change before inserting it.** A lookup per change, when the position check costs
one comparison per transaction.
