# 6. A snapshot joins the stream at the exported consistent point

Status: accepted

## Context

A new sink often needs rows that were written before capture started, or before the outbox's retention. It needs
a copy of the tables and then the stream, with no row missing and none applied twice where the two meet.

## Decision

The snapshot comes from Postgres itself. Walrus creates a temporary logical slot with `EXPORT_SNAPSHOT` on a
replication connection, which returns a snapshot name and the slot's consistent point, and imports the snapshot
into a repeatable-read transaction on an ordinary connection. That transaction sees exactly the transactions that
committed before the consistent point. The temporary slot is dropped as soon as the snapshot is imported; the main
slot, which started earlier, carries every transaction after the point.

The sink is emptied, loaded from the copy, and its cursor set to deliver only outbox changes that committed at or
after the consistent point. Rows are read as `column::text`, which is the same output function `pgoutput` uses, so a
copied row and a streamed row with the same values are the same bytes.

A snapshot row has no clock stamp, since nobody knows when it was last written, so it takes the lowest version there
is. Any captured change outranks it.

## Consequences

- The seam test writes history, then registers a snapshot sink while a second workload is running, and compares the
  result with the source and with a sink that received the same writes as a stream from the start. They match.
- Snapshots are offered only for sinks that keep their state. An index sink rebuilds from the outbox on every start,
  and a snapshot loaded into it would be gone at the next restart.
- A row that exists in two regions' snapshots, and is not changed afterwards, resolves between the two snapshot
  rows by source name, not by when each was written, because that information does not exist. Any later change
  decides it properly. This is a known limitation of bootstrapping a multi-source sink from snapshots.

## Alternatives considered

**Copy with `SELECT` and start the stream from "now".** There is no "now" that lines up the two: changes committed
during the copy would either be missing or applied twice.

**Replay the whole outbox instead.** Fine when the outbox covers all history, which it does not once it has been
pruned, or for tables that predate capture.
