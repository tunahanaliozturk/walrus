# Commit-to-apply lag and throughput

```bash
dotnet run -c Release --project load/Walrus.Load -- soak --rate 3000 --seconds 600
dotnet run -c Release --project load/Walrus.Load -- soak --rate 5000 --seconds 600 --connections 24 --rows 20
```

The harness writes account upserts to the eu primary at a fixed rate from several connections, for ten minutes. Every
write stamps the row with `clock_timestamp()`. The sink database records every version of every row it applies, with its
own `clock_timestamp()`, through a trigger. Lag is the difference: from the moment the source wrote the row to the moment
the sink applied it, so it includes the source commit and its synchronous wait for the standby, logical decoding,
capture, the outbox commit, dispatch and the sink's transaction. Both clocks belong to Postgres containers on the same
machine.

Each key is written by one connection with a sequence number that rises by one per write. A row whose recorded sequence
ever goes down in the sink's apply log was applied out of order. At the end the two tables are hashed in key order and
compared.

The whole stack ran on one machine: Intel Core Ultra 7 255H, 16 cores, 32 GB, Windows 11, Docker Desktop with 16 vCPUs
and 15.3 GiB shared by every container. The eu source is the synchronous failover pair from `compose.yaml`, and the sink is
a database in the store's Postgres. The harness ran on the Windows host.

## Results

| | 3,000 a second | 5,000 a second |
|---|---:|---:|
| Rows per transaction, connections | 5, 16 | 20, 24 |
| Rows written and acknowledged | 1,800,005 | 3,000,240 |
| Achieved rate | 3,000/s | 5,000/s |
| Row versions applied by the sink | 1,799,753 | 2,999,090 |
| **Commit to apply p50** | **13.1 ms** | **30.4 ms** |
| **Commit to apply p95** | **51.5 ms** | **106.0 ms** |
| **Commit to apply p99** | **103.6 ms** | **269.3 ms** |
| Commit to apply max | 293.5 ms | 1,042.2 ms |
| **Rows applied out of order** | **0** | **0** |
| **Sink equal to the source afterwards** | **yes** | **yes** |
| Capture's share, commit to outbox p50 / p99 | 6.6 ms / 55.6 ms | 10.9 ms / 133.2 ms |

The plan's targets were a p99 under 500 ms at 3,000 changes a second, and at least 5,000 a second through the outbox for
ten minutes. Both hold with room to spare.

The same soak, one minute at 3,000 a second, runs in CI on GitHub's standard Linux runner, with every container sharing
four vCPUs. There the p99 has ranged from 366 ms to 902 ms across runs, so on a machine a quarter the size of the laptop,
with the same numbers of lanes and connections, the target holds some days and not others. Ordering and equality held in
every run.

## Reading the numbers

**Row versions applied is a little lower than rows written, on purpose.** When two changes to one row land in the same
batch, the sink writes only the row's final state. Both changes are absorbed; the row is written once.

**Capture takes the smaller share.** The outbox records when each change committed at the source and when capture
started the write that stored it, which splits the lag. Capture's share includes the time the primary holds a decoded
transaction back until the standby confirms it, which is what `synchronized_standby_slots` costs and what makes a
failover safe. The rest is dispatch: reading the outbox, and the sink's transaction per batch.

**An earlier configuration measured differently, and the difference was a bug.** The first soaks ran with
`synchronous_standby_names = '*'`, which made Walrus's own connection the source's synchronous standby, so every source
commit waited for capture to store it (ADR 0005). Writers were throttled by the pipeline they were measuring: a
5,000-a-second target reached only 3,615, and a 3,000-a-second run measured p99 285 ms. Those numbers are not published
as results because the configuration they measured was wrong.

**The writer's transaction size matters to the writer, not to Walrus.** At 5,000 a second the harness writes twenty rows
per transaction; with five, twenty-four connections from the Windows host through Docker Desktop's port forwarding
cannot commit that many transactions a second to a synchronous pair. Capture and dispatch batch across transactions
either way.

**The maximum at 5,000 a second.** One second at the extreme tail, against a p99 of 269 ms. The apply log does not say
which part of the pipeline it was spent in; the outbox's timestamps put capture's p99 at 133 ms, so most of it was
dispatch or the sink's database.
