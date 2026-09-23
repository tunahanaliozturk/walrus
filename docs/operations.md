# Operating Walrus

## What a source needs

| Requirement | Why |
|---|---|
| Postgres 17 or newer | Logical slots with `failover`, `sync_replication_slots` and `synchronized_standby_slots` |
| `wal_level = logical`, in `postgresql.conf` or `postgresql.auto.conf` | Logical decoding. It must be in a file, not only on the command line, or `pg_rewind` cannot recover a failed primary (ADR 0005) |
| A publication, created by the owner, `with (publish = 'insert, update, delete')` | Names the captured tables. Truncates are not captured |
| A primary key on every published table | Rows are ordered and deduplicated by key; capture stops with an error on a table without one |
| `REPLICA IDENTITY FULL` where before images matter | Needed for merge-columns tables and for before images in the feed and webhooks. Costs log volume |
| A heartbeat table in the publication, writable by the capture role | Keeps quiet slots moving (ADR 0001) |
| A role with `REPLICATION`, `SELECT` on published tables, write on the heartbeat | Least privilege (ADR 0009) |

For failover, additionally on every node: `hot_standby_feedback = on`, `sync_replication_slots = on`, a `dbname`
in `primary_conninfo`, `primary_slot_name` set, `synchronized_standby_slots` naming the standby's physical slot,
and `synchronous_standby_names` naming the physical standbys if acknowledged commits must survive a crash. Never
`'*'`: it matches capture's logical connection too, which then becomes the synchronous standby, so every commit waits
for Walrus and the physical standby is only a potential one. `compose.yaml` and
`docker/source/` are a working example.

## Configuration

Everything is under `Walrus`, and every value can come from an environment variable (`Walrus__Sources__0__Name`).
Secrets belong in environment variables or a secret store.

| Setting | Default | Meaning |
|---|---|---|
| `StoreConnection` | required | The store database. Migrated on start |
| `Sources:n:Name` | required | The source's name in change events and status |
| `Sources:n:Hosts:m` | required | Every node that can be primary, `host:port` |
| `Sources:n:Database`, `Username`, `Password` | required | Where and as whom to capture |
| `Sources:n:Publication` | `walrus` | The publication |
| `Sources:n:Slot` | `walrus_<name>` | The slot, created on first start with failover enabled |
| `Sources:n:HeartbeatTable` | `walrus.heartbeat` | The heartbeat row's table |
| `Sources:n:HeartbeatInterval` | `00:00:05` | How often it is written |
| `Sources:n:Tables:<schema.table>:Conflict` | `LastWriterWins` | Or `MergeColumns` (ADR 0004) |
| `Sources:n:Tables:<schema.table>:Masks:<column>` | none | `Null`, `Redact` or `Hash` (ADR 0009) |
| `SinkConnections:<name>` | none | Connection strings a Postgres sink may name |
| `WebhookAllowedHosts:n` | none | Hosts a webhook sink may call |
| `WebhookSigningKey` | required with webhooks | Base64, at least 32 bytes |
| `MaskingKey` | required | Base64, at least 32 bytes |
| `ReadToken` | required | At least 24 characters |
| `OperatorToken` | empty | At least 24 characters; empty disables every change |
| `OutboxRetention` | `7.00:00:00` | How long applied changes are kept for replay |
| `Capture:MaxBatchChanges` | `2000` | Changes per outbox commit |
| `Capture:PendingTransactions` | `10000` | Decoded transactions that may wait for the outbox |
| `Dispatch:Lanes` | `8` | Rows applied in parallel per sink |
| `Dispatch:MaxBatch` | `500` | Changes per apply |
| `Dispatch:ReadBatch` | `2000` | Outbox rows read at once |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | Exports traces, metrics and logs when set |

The service refuses to start on a configuration that would fail later: missing keys, a key shorter than 32
bytes, duplicate source names, short tokens.

## What to watch

| Signal | Where | Alert when | Because |
|---|---|---|---|
| Log kept for the slot | `/v1/stats` `walRetainedBytes`, console | above a quarter of the source's free disk | A stalled slot keeps every byte of log. This is how CDC fills a production disk |
| Capture attached | `attachedTo`, console "Capturing" | null for more than a minute | Nothing is being captured; the slot is growing |
| Failover readiness | `standbys[].synced`, console "Ready" | false for more than a few minutes | A failover now would have no slot on the new primary |
| Sink lag | `lagMilliseconds`, histogram `walrus.dispatch.lag` | p99 above 500 ms for 5 minutes | The sink is behind; see the sink's state |
| Sink state | `state` | `Retrying` for more than a minute | The sink is failing for a reason that may pass; `counters.lastError` says which |
| Dead letters | `walrus.dispatch.dead_letters`, `blockedRows` | any | A row is held until someone fixes the sink and retries |
| Resent transactions | `resent`, `walrus.capture.resent` | a steady rise | Normal after a restart or failover. Continuous resends mean acknowledgements are not reaching the source |
| Commits waiting for Walrus | `captureIsSynchronous`, console "Every commit waits for Walrus", a warning in the log | true | The source's synchronous standby setting matches capture. Fix it before anything else: see the runbook |

Metrics are on the `Walrus` meter: `walrus.capture.changes`, `walrus.capture.transactions`, `walrus.capture.resent`,
`walrus.dispatch.applied` (by sink and outcome), `walrus.dispatch.lag`, `walrus.dispatch.dead_letters`,
`walrus.dispatch.retries`, `walrus.dispatch.conflicts` (by sink and winning source).

## Runbooks

### The console says every commit waits for Walrus

The source's `synchronous_standby_names` is `'*'` or names `walrus-<source>`, so the source counts capture as a
synchronous standby. Every commit on the source now waits until Walrus has stored it, and if capture stops, commits
stop. Worse, the physical standby is only a potential standby, so a failover can lose commits the application saw
succeed. Name the physical standbys instead, for example `FIRST 1 (pg_a, pg_b)`, and reload. Walrus needs no change.

### The primary failed

Nothing to do in Walrus. Promote the standby with your usual tooling; capture finds the new primary, attaches to its
copy of the slot and resumes. Check afterwards that `attachedTo` names the new primary and that `resent` rose a
little, which is the new primary resending what the old one had not had acknowledged. Bring the old primary back as
a standby (`pg_rewind`) before the next failover: until then the source has no failover target, and with
synchronous commit its writes wait.

### Capture stops with "the outbox already holds changes up to …"

A standby was promoted without transactions capture had already taken, so the new primary is writing different
transactions at positions the outbox holds. Resuming would skip them, so capture refuses.

1. Find out why: `synchronized_standby_slots` missing or wrong on the old primary, or a standby promoted that was not
   the synchronised one.
2. The outbox rows above the new primary's position describe transactions that no longer exist. Delete them
   (`delete from walrus_outbox where source = '<name>' and commit_lsn > '<position>'`) and set the checkpoint back
   (`update walrus_capture_state set confirmed_lsn = '<position>' where source = '<name>'`).
3. Rebuild every sink of that source from a snapshot (`POST /v1/sinks/{id}/snapshot`), since they may hold rows the
   source no longer has.

### The slot is gone or invalidated

Dropped by hand, or invalidated by `max_slot_wal_keep_size`. Capture creates a new slot on start, positioned now:
whatever happened between the old slot's position and now is missing from the outbox. Rebuild every sink of that
source from a snapshot.

### A sink is Retrying

Its database or endpoint is failing for a reason that may pass. `counters.lastError` says what. Nothing is lost: the
changes wait in the outbox and are applied once the sink answers. If it will be down for longer than the outbox
retention, raise `OutboxRetention` or plan a snapshot afterwards.

### A sink has dead letters

`GET /v1/sinks/{id}/dead-letters` lists them with the reason. Fix the cause in the sink (a constraint, a column
type, a missing column), then `POST /v1/sinks/{id}/dead-letters/retry`. Rows whose letters all apply resume;
the rest stay parked with the attempt recorded.

### The outbox is growing

Pruning keeps changes that any durable sink still needs, so one sink stuck far behind holds the whole outbox. Find
it with `/v1/stats` and either fix it or delete it.

### Rotating the masking key

Hashed values computed with the new key differ from the old ones, so a column's values change for rows written after
the rotation. Rotate, then rebuild sinks from a snapshot if old and new rows must stay joinable.

## Known limitations

- Postgres is the only source and the only database sink.
- `TRUNCATE`, DDL and sequences are not captured. Schema changes need the sink's tables changed by hand, and a
  column the sink table lacks parks the row as a dead letter until it is added.
- A table without a primary key cannot be captured.
- Merge-columns tables need `REPLICA IDENTITY FULL`; without it they behave as last-writer-wins.
- Conflict resolution follows the regions' clocks, so skew between them decides races closer than the skew.
- Deleted rows keep their state in the sink as tombstones, which are never collected.
- A multi-source sink bootstrapped from snapshots resolves a row present in two regions' snapshots by source name
  until the row next changes (ADR 0006).
- The index sink is in memory and rebuilds from the outbox on every start, so it only covers the outbox's retention.
- One capture session per source at a time. Two service instances give availability, not throughput.
- The console is a read-only view; retrying dead letters and managing sinks is done through the API.
