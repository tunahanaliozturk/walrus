# Changelog

## 1.0.0

The first release: capture, the outbox, dispatch to three kinds of sink, snapshot and replay, multi-source
conflict resolution, source failover, and the console, with the measurements in `docs/benchmark-results` taken on
this version.

### Capture

- Streams Postgres logical replication (`pgoutput`) from whichever node of a source is primary, and creates its slot
  with failover enabled.
- Writes whole source transactions to the outbox in group commits, and acknowledges the slot only after the outbox
  commit. Transactions the source resends after a restart are recognised by position.
- One session per source, through an advisory-lock lease and an epoch that fences a session that lost its lease.
- A heartbeat row per source keeps quiet slots from holding the log.
- Refuses to resume when the outbox holds positions the source has never written.
- Masks configured columns before the outbox: null, redact or keyed hash.
- An update that changes a primary key becomes a delete and an insert.

### Dispatch and sinks

- Per-row ordering through lanes, parallel across rows, with a contiguous watermark for the saved position.
- Postgres sinks keep each row's state in the sink database, written in the same transaction as the row, so
  redelivery changes nothing.
- An in-memory index sink that rebuilds from the outbox on every start, and a webhook sink with signed, at-least-once
  delivery to allowed hosts.
- Conflicts between sources resolved by hybrid logical clock, whole row or column by column.
- A sink that is down is retried without parking anything; a refused row is parked with everything after it, and other
  rows keep flowing. Parked rows are retried in order.
- Replay from any log position, with or without emptying the sink first, and bootstrap from a consistent snapshot.

### Operations

- Status, stats, dead letters, conflicts, search and a live event stream over HTTP, with read and operator tokens.
- OpenTelemetry metrics and traces on the `Walrus` meter and activity source.
- A replication console in Vue that holds no credential.
- A compose stack with a failover pair that rejoins itself with `pg_rewind`, and a harness for lag, failover and replay.
