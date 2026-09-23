# 5. Source failover through slot synchronisation, synchronous commit, and a refusal to guess

Status: accepted

## Context

When the source primary dies, capture has to carry on from the promoted standby with nothing lost and nothing
twice. Three things can go wrong:

1. The standby has no copy of the logical slot, so there is nowhere to resume from.
2. The standby has a copy, but capture had already taken transactions from the old primary that never reached
   the standby. The new primary then writes different transactions at the same log positions, and resuming by
   position skips them silently.
3. The application saw a commit succeed on the old primary that never reached the standby. That change is gone
   from the source itself.

## Decision

- **Postgres 17+ slot synchronisation.** Capture creates its slot with `failover => true`, and each standby runs
  with `sync_replication_slots = on`, `hot_standby_feedback = on` and a `dbname` in `primary_conninfo`. The
  standby keeps a copy of the slot, and a promotion finds it there. That answers 1.
- **`synchronized_standby_slots`** on the primary names the standby's physical slot, so the primary does not send
  a decoded transaction to capture until the standby has received it. That answers 2.
- **Synchronous commit** (`synchronous_standby_names = '*'`), so a commit the application saw succeed is on the
  standby. That answers 3, at the cost of a round trip per commit and of commits waiting while no standby is
  connected.
- **A refusal to guess.** Before resuming, capture compares the primary's current log position with its checkpoint.
  If the outbox holds positions the source has never written, capture stops with an error that points at the
  runbook, instead of resuming and skipping. With the settings above this cannot happen; the check turns a
  misconfiguration into a stopped pipeline rather than lost changes.
- **Finding the primary.** Capture is given every node of a source and asks each whether it is in recovery. A
  failover is a reconnect, not a configuration change.

In the compose stack each node decides its role at every start: if its peer is primary, it takes a base backup or
rewinds its own data onto the peer's timeline with `pg_rewind`, and starts as a standby. The chaos harness is then
only "kill the primary, promote the standby, start the old primary again".

## Consequences

- The failover run kills the primary under write load, promotes, and rejoins, repeatedly, then checks that every
  acknowledged write is in the source, that the sink equals the source, and that no row was ever applied out of
  order. Results are in `docs/benchmark-results/failover.md`.
- While the old primary is being rewound, the new one has no standby: synchronous commits wait, and capture waits
  for `synchronized_standby_slots`. Writes stop for a few seconds per failover instead of risking what they were
  acknowledged for. That is the trade, and it is measured.
- `pg_rewind` needs the old primary's log back to the last shared checkpoint. The crash recovery it runs first
  would recycle exactly those segments, so the nodes keep `wal_keep_size = 1GB`. Production would archive WAL and
  use `--restore-target-wal`. Two things about `pg_rewind` were found the hard way and are commented where they are
  handled: its single-user recovery reads only configuration files, not command-line settings, so `wal_level`
  lives in `postgresql.auto.conf`; and it refuses to run as root.

## Alternatives considered

**Recreate the slot on the new primary.** Its position starts at the promotion point, which silently drops whatever
capture had not yet taken from the old primary.

**Asynchronous replication and accept the loss.** Honest if documented, and wrong for a demo whose claim is zero
lost changes.

**Patroni or another cluster manager.** The right tool in production, and it would hide the thing this project is
meant to show.
