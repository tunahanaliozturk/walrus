# Capture and replay of ten million changes

```bash
dotnet run -c Release --project load/Walrus.Load -- replay --changes 10000000
```

Ten million rows inserted into the eu primary in transactions of 250,000, captured into the outbox, and then replayed
into a sink registered afterwards with `"start": "Beginning"`, so every change is delivered from the outbox rather
than from the source. The sink applies through the same Postgres writer as the live sinks: each row and its state in
one transaction per batch.

The whole stack ran on one machine: Intel Core Ultra 7 255H, 16 cores, 32 GB, Windows 11, Docker Desktop with
16 vCPUs and 15.3 GiB shared by every container. Postgres 18 with default settings apart from those in
`compose.yaml`; the eu source was the synchronous failover pair.

| Measure | Value |
|---|---:|
| Changes captured | 10,000,000 |
| Inserts committed at the source | 25 s |
| Capture, first insert to the last change in the outbox | 145 s, 69,135 changes/s |
| Replay into a new sink from the outbox | 268 s, 37,372 changes/s |
| Rows in the sink / in the source | 10,000,000 / 10,000,000 |

The plan asked for a replay of ten million events in under twenty minutes, 8,300 a second. It took four and a half.

## Reading the numbers

**Here capture is the bottleneck, and the source is not.** The source committed ten million rows in 25 seconds and
capture finished two minutes later. Decoding runs on one connection per source, and each outbox commit is one binary
`COPY` of up to 2,000 changes, so 69,000 changes a second is what a single slot and a single writer sustain on this
machine. The source does not wait for it: the log is kept until capture confirms it, and nothing else.

**Replay is the sink's apply rate.** 37,000 changes a second is eight lanes each applying batches of 500 in one
transaction: a state lookup for the batch, then an upsert of the row and of its state for each change, in one round
trip. Every one of these ten million is a first write to its row, so every change moves state and writes twice. A
replay over rows the sink already has is faster, because a change the state has absorbed writes nothing at all.

**An earlier run measured something else.** With `synchronous_standby_names = '*'`, capture was the source's
synchronous standby (ADR 0005), so every insert waited for capture to store it. The inserts then took 209 seconds and
capture looked as if it kept pace with the writes, at 47,576 changes a second, because it was setting their pace.
Replay in that run measured 29,195 a second. Nothing in the replay path changed between the two runs, and the
difference was not investigated, so treat the pair as the spread on this machine.

**It runs beside the live stream.** A replay is the sink's own reader over the outbox, so other sinks, and capture,
keep going. During this run the live replica sink was idle, so this measures replay alone; its effect on another
sink's lag under load is not measured here.
