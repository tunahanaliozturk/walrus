# What a change costs Walrus on the CPU

```bash
dotnet run -c Release --project benchmarks/Walrus.Benchmarks -- --filter "*"
```

The steps a change takes between the log and a sink, measured one at a time with no database in the way, so the
end-to-end numbers in [lag-and-throughput.md](lag-and-throughput.md) and [replay.md](replay.md) can be split into
what Walrus spends and what Postgres spends.

```
BenchmarkDotNet v0.15.8, Windows 11 (10.0.26100.9106/24H2)
Intel Core Ultra 7 255H 2.00GHz, 1 CPU, 16 logical and 16 physical cores
.NET SDK 10.0.401, .NET 10.0.12, X64 RyuJIT x86-64-v3
```

The machine was otherwise idle: no containers running, nothing building.

| Step | Mean | Allocated |
|---|---:|---:|
| Stamp and mask a transaction of 5 changes | 5,461 ns | 8,560 B |
| Apply an update to a row's state, last writer wins | 218 ns | 752 B |
| Apply an update to a row's state, merge columns | 68 ns | 416 B |
| Recognise a change already applied | 73 ns | 112 B |
| Materialise a row from its state | 123 ns | 328 B |
| Row image to JSON | 249 ns | 1,176 B |
| Row image from JSON | 359 ns | 1,048 B |
| Key state round trip through JSON | 6,957 ns | 8,832 B |
| Choose a lane | 111 ns | 776 B |
| Track 1,000 positions finishing out of order | 38,970 ns | 89,856 B |

## Reading the numbers

**Walrus is not what limits throughput.** Stamping costs about 1.1 microseconds a change, including hashing the
masked email with HMAC-SHA256. Applying a change to a row's state is a fifth of a microsecond. At the 37,000
changes a second of the replay, all of Walrus's own work on a change adds up to well under a core. The rest of the
time is round trips and Postgres writing rows.

**The most expensive step is the state's JSON.** Reading a row's state from the sink and writing it back costs
7 microseconds and 8.8 KB per change, more than everything else together. It is JSON because the state lives in a
column a person can read with `psql` when a sink looks wrong, and because it is not the bottleneck. If it became
one, a binary encoding would be the first change.

**Recognising a duplicate is cheap on purpose.** A resent change is checked against the row's state and dropped in
73 nanoseconds, allocating almost nothing, which is what makes resends after a restart or a failover harmless in
practice as well as in principle.

**The watermark is 39 nanoseconds a position,** even with every position finishing in reverse order, the worst
case for the structure that tracks which positions are done.
