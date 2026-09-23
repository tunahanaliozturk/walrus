# 7. Park the refused row, retry the sink that is down

Status: accepted

## Context

A sink can fail in two ways that look alike from a catch block and are opposites in what to do. A database that
is restarting will accept the batch in ten seconds. A row that breaks a constraint in the sink will be refused
forever, and retrying it blocks every row behind it.

The specification asked for a sink that is "down past retries" to dead-letter too. Doing that would park every
change of every row while a database restarts, and turn a short outage into a manual cleanup of thousands of
letters.

## Decision

- **Transient failures are retried forever**, with backoff from 100 ms to 5 s. Connection errors, timeouts,
  shutdowns, serialization failures, HTTP 408, 429 and 5xx. The changes are safe in the outbox; the sink's state goes
  to Retrying, its lag grows, and the lag alert fires. Nothing is parked.
- **Permanent refusals are isolated.** Postgres SQLSTATE classes 22 (data), 23 (constraint) and 42 (schema), a column
  the sink table does not have, and any other 4xx from a webhook. A refused batch is split by row; the rows that
  apply are applied; within the refused row, the changes before the refused one are applied one at a time, and the
  refused change and every later change to that row are parked as dead letters.
- **Head of line per row, not per sink.** A row with an open letter parks every later change to it, because applying
  a later change over a missing earlier one is the reordering this service exists to prevent. Every other row keeps
  flowing.
- **Retry in the lane.** Retrying a sink's letters queues a retry item in each parked row's lane. The retry reads the
  row's letters when it runs, not when it was requested, so a letter parked by a batch ahead of it in the lane is
  included and nothing behind it can overtake. A row whose letters all apply is unblocked.

## Consequences

- The integration test gives the sink a check constraint that a source update breaks: the other row flows, the
  refused row stops at its last good version with two letters, the constraint is dropped, a retry applies both in
  order, and the sink matches the source.
- Parked changes are durable before their positions count as finished, so a restart never skips one.
- An operator has to act on a dead letter: fix the sink, then retry. The console shows them, with the reason; retrying
  needs the operator token.

## Alternatives considered

**Skip the refused change and carry on.** The sink silently diverges from the source.

**Stop the whole sink.** One bad row takes every row's replication down with it.
