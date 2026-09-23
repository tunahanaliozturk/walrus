# 2. One capture session per source: a lease to start, an epoch to finish

Status: accepted

## Context

The position check in ADR 0001 relies on one session writing a source's outbox rows at a time, in commit order.
Two sessions writing at once would interleave sequence numbers, and a dispatcher that had read up to sequence
number n could miss a row with a smaller number committed a moment later.

Two ways that happens: a second instance of the service starting while the first runs, and a session that has lost
its connection to the store without noticing and is still finishing a write after its replacement started.

## Decision

Two mechanisms, one for each case.

- **A lease to start.** A session takes a session-level Postgres advisory lock in the store before it does anything
  else, on a connection it holds for as long as it runs. A second instance fails to take it and waits as a
  standby. If the service dies, Postgres notices the connection is gone and the lock with it; nothing has to
  expire. The lease is released explicitly on shutdown, because a pooled connection would keep the server
  session, and the lock, alive.
- **An epoch to finish.** Taking the lease increments the source's epoch, and every outbox write first updates the
  checkpoint row where the epoch is still the session's own. A stale session updates nothing and its write is
  refused with `CaptureFencedException`.

The checkpoint update comes first in the write transaction, before a single sequence number is drawn. It takes the
row lock, so two writers can never hold interleaved ranges, and a reader that has seen sequence number n has seen
everything below it.

## Consequences

- Running two instances for availability works: one captures, the other waits, and takes over within one retry
  interval of the first dying.
- Dispatch can read the outbox by sequence number with no gaps to worry about except those left by rolled-back
  writes, which the dispatch watermark handles.
- The integration suite takes the lease twice and gets refused, releases it, takes it again with the next epoch,
  and checks that a write with the old epoch fails.

## Alternatives considered

**Rely on the replication slot's own exclusivity.** Postgres lets one session stream a slot at a time, but a session
attaches to the slot after reading its checkpoint, and a loser can have bumped state by then. It also says nothing
about a zombie finishing a write.

**A leader election service.** A dependency to operate for something the store database already does.
