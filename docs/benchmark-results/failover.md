# Source failover under write load

```bash
dotnet run -c Release --project load/Walrus.Load -- failover --cycles 50 --rate 500
```

The eu source is a primary and a standby with synchronous commit, the capture slot created with failover enabled and
synchronised to the standby, and `synchronized_standby_slots` on each node naming the other. The harness writes
account upserts at 500 rows a second from eight connections and, fifty times over:

1. waits until the standby is streaming and holds a persisted, synchronised copy of the capture slot;
2. kills the primary's container outright, `docker kill`, with no shutdown;
3. promotes the standby with `pg_promote()`;
4. starts the old primary's container again, whose entry point rewinds it onto the new primary's timeline and starts it
   as a standby;
5. records when writes succeed again and when capture is attached to the new primary;
6. writes steadily for five more seconds before the next cycle.

Each key is written by one connection with a sequence number that rises by one per write, and the writer remembers the
highest sequence the source acknowledged for every key. At the end it checks three things: every acknowledged write is
in the source, the sink's apply log never shows a key's sequence going down, and the sink table hashes the same as the
source table.

The whole stack ran on one machine: Intel Core Ultra 7 255H, 16 cores, 32 GB, Windows 11, Docker Desktop with 16 vCPUs
and 15.3 GiB shared by every container.

## Results

| Measure | Value |
|---|---:|
| Failovers | 50 |
| Writes acknowledged | 809,126 |
| Writes that failed with the primary | 7,109 |
| **Acknowledged writes missing from the source** | **0** |
| **Rows applied out of order** | **0** |
| **Sink equal to the source afterwards** | **yes** |
| Writes resumed after, median / max | 1.5 s / 4.9 s |
| Capture resumed after, median / max | 1.6 s / 7.4 s |

The same run with three cycles is part of CI, on GitHub's standard Linux runner, where it also checks that the physical
standby, and not capture, is the synchronous one.

## Reading the numbers

**Nothing lost, nothing reordered, fifty times.** Every write the source acknowledged is in it and in the sink, each
key's changes reached the sink in the order they committed, and the two tables are identical. Writes that failed
because the primary died mid-commit may or may not have committed; the check is that the source holds at least the
acknowledged sequence for every key, and the sink holds exactly what the source holds.

**Writes come back in about a second and a half.** That is the promotion, plus the old primary rewinding and rejoining
as a standby, because commits are synchronous: until a standby is connected again the new primary makes them wait. The
slowest cycles are the ones where `pg_rewind` had more to copy. That wait is the price of an acknowledged write
surviving the primary dying, paid on every failover.

**Capture follows the writes.** In the median cycle it is attached to the new primary a tenth of a second after writes
resume.

## How the capture number got there

The first version resumed capture a median of 6.7 seconds after the kill, five seconds after writes did, in a run that
also had the synchronous standby misconfigured (ADR 0005). Three things held it back, each visible in the service's log
against the nodes' timestamps:

- A connection to a primary killed with its container fails only when something is written to it and the peer, back
  from the dead, answers with a reset. The only thing written was Npgsql's status update, every ten seconds. It now
  goes every second.
- The next session asked the nodes in configured order, and the first was the dead one, so it waited out a connection
  timeout before asking the node that was actually primary. All nodes are now asked at once and the first to answer as
  primary wins.
- Failed attempts doubled their wait up to five seconds, so after a few failures during the promotion capture sat
  waiting while the new primary was already usable. The cap is now one second.

A watch also ends a session as soon as another node answers as primary. In these runs the reset usually arrives first,
but a primary whose host disappears entirely never sends a reset, and without the watch the session would wait for the
replication timeout, a minute.

**Why capture cannot come back before the old primary rejoins.** `synchronized_standby_slots` makes the new primary hold
decoded changes back until its standby confirms them. With no standby connected there is nothing to confirm, so capture
waits for the old primary to rejoin, the same as synchronous commits do. That wait is what guarantees capture never takes
a change a later failover could lose.

**What a failover costs a sink.** Changes committed while capture is reconnecting wait in the source's log and arrive in
a burst when it resumes; the soak's lag measurements do not include failovers.
