using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Npgsql;

namespace Walrus.Load;

/// <summary>The measurements behind the README, each runnable against the compose stack.</summary>
internal static class Scenarios
{
    private const string Replica = "replica";

    /// <summary>
    /// Writes at a fixed rate for a fixed time, then reports lag from source commit to sink apply, whether any
    /// row was ever applied out of order, and whether the sink ends up equal to the source.
    /// </summary>
    public static async Task<int> SoakAsync(Stack stack)
    {
        int rate = stack.Option("rate", 3_000);
        int seconds = stack.Option("seconds", 600);
        var writer = new Writer(stack.Eu, stack.Option("keys", 20_000), stack.Option("connections", 16), stack.Option("rows", 5));

        await EnsureReplicaAsync(stack);
        await Stack.ExecuteAsync(stack.Sink, "truncate accounts_applied");

        Console.WriteLine($"Soak: {rate:N0} changes a second for {seconds} s into eu, applied to sink '{Replica}'.");

        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(seconds));
        var clock = Stopwatch.StartNew();
        await writer.RunAsync(rate, stop.Token);
        double written = clock.Elapsed.TotalSeconds;

        Console.WriteLine($"Wrote {writer.Rows:N0} rows in {written:N1} s. Waiting for the sink.");
        await stack.WaitForSinkAsync(Replica, TimeSpan.FromMinutes(10));

        Lag lag = await LagAsync(stack);
        long inversions = await InversionsAsync(stack);
        bool equal = await SinkEqualsSourceAsync(stack);

        Console.WriteLine();
        Console.WriteLine("| Measure | Value |");
        Console.WriteLine("|---|---:|");
        Console.WriteLine($"| Rows written and acknowledged | {writer.Rows:N0} |");
        Console.WriteLine($"| Achieved rate | {writer.Rows / written:N0} changes/s |");
        Console.WriteLine($"| Row versions applied by the sink | {lag.Count:N0} |");
        Console.WriteLine($"| Commit to apply p50 | {lag.P50:N1} ms |");
        Console.WriteLine($"| Commit to apply p95 | {lag.P95:N1} ms |");
        Console.WriteLine($"| Commit to apply p99 | {lag.P99:N1} ms |");
        Console.WriteLine($"| Commit to apply max | {lag.Max:N1} ms |");
        Console.WriteLine($"| Rows applied out of order | {inversions:N0} |");
        Console.WriteLine($"| Sink equals source afterwards | {(equal ? "yes" : "NO")} |");

        return inversions == 0 && equal ? 0 : 1;
    }

    /// <summary>
    /// Kills the eu primary under write load, promotes the standby, brings the old primary back as a standby,
    /// and repeats; then checks that no acknowledged write was lost and no row was reordered.
    /// </summary>
    public static async Task<int> FailoverAsync(Stack stack)
    {
        int cycles = stack.Option("cycles", 10);
        int rate = stack.Option("rate", 500);
        int keys = stack.Option("keys", 5_000);
        string project = stack.Option("project", "walrus");
        var writer = new Writer(stack.Eu, keys, stack.Option("connections", 8), stack.Option("rows", 2));
        List<(double Writes, double Capture)> outages = [];

        await EnsureReplicaAsync(stack);
        await Stack.ExecuteAsync(stack.Sink, "truncate accounts_applied");

        Console.WriteLine($"Failover: {cycles} cycles under {rate:N0} changes a second.");

        using var stop = new CancellationTokenSource();
        Task writing = writer.RunAsync(rate, stop.Token);

        for (int cycle = 1; cycle <= cycles; cycle++)
        {
            (string primary, string standby) = await RolesAsync(stack, project);
            await WaitForStandbyAsync(stack, project, standby);

            long before = writer.Rows;
            var outage = Stopwatch.StartNew();

            await Stack.DockerAsync("kill", Container(project, primary));
            await Stack.DockerAsync("exec", Container(project, standby), "psql", "-U", "postgres", "-d", "postgres", "-Atc", "select pg_promote(true, 60)");

            // The old primary comes back as a standby of the new one: the entry point rewinds it on start.
            await Stack.DockerAsync("start", Container(project, primary));

            double writesBack = await UntilAsync(() => Task.FromResult(writer.Rows > before + 10), outage);
            double captureBack = await UntilAsync(async () => await AttachedToAsync(stack) == $"{standby}:5432", outage);

            outages.Add((writesBack, captureBack));
            Console.WriteLine($"  cycle {cycle,3}: {primary} -> {standby}, writes back after {writesBack:N1} s, capture after {captureBack:N1} s");
        }

        await stop.CancelAsync();
        await writing;

        Console.WriteLine("Writes stopped. Waiting for the sink.");
        await stack.WaitForSinkAsync(Replica, TimeSpan.FromMinutes(10));

        long lost = await LostAcknowledgedAsync(stack, writer);
        long inversions = await InversionsAsync(stack);
        bool equal = await SinkEqualsSourceAsync(stack);
        double[] writes = [.. outages.Select(static outage => outage.Writes).Order()];
        double[] capture = [.. outages.Select(static outage => outage.Capture).Order()];

        Console.WriteLine();
        Console.WriteLine("| Measure | Value |");
        Console.WriteLine("|---|---:|");
        Console.WriteLine($"| Failovers | {cycles} |");
        Console.WriteLine($"| Writes acknowledged | {writer.Rows:N0} |");
        Console.WriteLine($"| Writes that failed with the primary | {writer.Failures:N0} |");
        Console.WriteLine($"| Acknowledged writes missing from the source | {lost:N0} |");
        Console.WriteLine($"| Rows applied out of order | {inversions:N0} |");
        Console.WriteLine($"| Sink equals source afterwards | {(equal ? "yes" : "NO")} |");
        Console.WriteLine($"| Writes resumed after, median / max | {Percentile(writes, 0.5):N1} s / {writes[^1]:N1} s |");
        Console.WriteLine($"| Capture resumed after, median / max | {Percentile(capture, 0.5):N1} s / {capture[^1]:N1} s |");

        return lost == 0 && inversions == 0 && equal ? 0 : 1;
    }

    /// <summary>
    /// Writes a large table in big transactions, times capture taking it all, then times a new sink replaying
    /// every change from the outbox.
    /// </summary>
    public static async Task<int> ReplayAsync(Stack stack)
    {
        int rows = stack.Option("changes", 10_000_000);
        const int Batch = 250_000;

        await Stack.ExecuteAsync(stack.Eu, """
            create table if not exists public.ledger (
                id      bigint primary key,
                account bigint not null,
                amount  numeric(18,2) not null,
                at      timestamptz not null);
            grant select on public.ledger to walrus_capture;
            """);

        if (!await Stack.ScalarAsync<bool>(stack.Eu, "select exists (select 1 from pg_publication_tables where pubname = 'walrus' and tablename = 'ledger')"))
        {
            await Stack.ExecuteAsync(stack.Eu, "alter publication walrus add table public.ledger");
        }

        await Stack.ExecuteAsync(stack.Sink, "create table if not exists public.ledger (id bigint primary key, account bigint not null, amount numeric(18,2) not null, at timestamptz not null)");
        long offset = await Stack.ScalarAsync<long>(stack.Eu, "select coalesce(max(id), 0) from ledger");

        Console.WriteLine($"Replay: writing {rows:N0} ledger rows to eu in transactions of {Batch:N0}.");
        var capture = Stopwatch.StartNew();

        for (long start = offset + 1; start <= offset + rows; start += Batch)
        {
            long end = Math.Min(start + Batch - 1, offset + rows);
            await Stack.ExecuteAsync(stack.Eu, $"insert into ledger select g, g % 10000, (g % 100000) / 100.0, now() from generate_series({start}, {end}) g");
        }

        double inserted = capture.Elapsed.TotalSeconds;
        string position = await Stack.ScalarAsync<string>(stack.Eu, "select pg_current_wal_lsn()::text");

        while (!await Stack.ScalarAsync<bool>(stack.Store, $"select coalesce(bool_or(confirmed_lsn >= '{position}'::pg_lsn), false) from walrus_capture_state where source = 'eu'"))
        {
            await Task.Delay(500);
        }

        double captured = capture.Elapsed.TotalSeconds;
        Console.WriteLine($"Inserted in {inserted:N0} s; captured into the outbox by {captured:N0} s.");

        await Stack.ExecuteAsync(stack.Sink, "truncate ledger");
        await Stack.ExecuteAsync(stack.Sink, "delete from walrus_sink_state where sink_id = 'ledger-replay'");

        var replay = Stopwatch.StartNew();
        await stack.RegisterAsync("ledger-replay", "Postgres", ["public.ledger"], Replica, "Beginning");
        await stack.WaitForSinkAsync("ledger-replay", TimeSpan.FromHours(2));
        double replayed = replay.Elapsed.TotalSeconds;

        long sinkRows = await Stack.ScalarAsync<long>(stack.Sink, "select count(*) from ledger");
        long sourceRows = await Stack.ScalarAsync<long>(stack.Eu, "select count(*) from ledger");

        Console.WriteLine();
        Console.WriteLine("| Measure | Value |");
        Console.WriteLine("|---|---:|");
        Console.WriteLine($"| Changes captured | {rows:N0} |");
        Console.WriteLine($"| Capture, source commit to outbox | {captured:N0} s, {rows / captured:N0} changes/s |");
        Console.WriteLine($"| Replay into a new sink from the outbox | {replayed:N0} s, {sourceRows / replayed:N0} changes/s |");
        Console.WriteLine($"| Rows in the sink / in the source | {sinkRows:N0} / {sourceRows:N0} |");

        return sinkRows == sourceRows ? 0 : 1;
    }

    /// <summary>A gentle, endless stream in both regions, with some rows written by both, for the console.</summary>
    public static async Task<int> TrafficAsync(Stack stack)
    {
        await EnsureReplicaAsync(stack);

        var eu = new Writer(stack.Eu, 2_000, 2, 2);
        var random = new Random(1);
        using var stop = new CancellationTokenSource();
        Console.CancelKeyPress += (_, arguments) =>
        {
            arguments.Cancel = true;
            stop.Cancel();
        };

        Console.WriteLine("Writing about 100 changes a second to eu, and profile edits in both regions. Ctrl+C stops.");
        Task accounts = eu.RunAsync(100, stop.Token);

        while (!stop.IsCancellationRequested)
        {
            int profile = random.Next(1, 200);
            string[] cities = ["Paris", "Lisbon", "Oslo", "Austin", "Denver", "Seoul"];
            string[] plans = ["free", "pro", "team"];

            // The same profile edited in both regions within a few milliseconds: a conflict every time.
            await Stack.ExecuteAsync(stack.Eu, $"insert into profiles values ({profile}, 'user {profile}', '{cities[random.Next(cities.Length)]}', 'free') on conflict (id) do update set city = excluded.city");
            await Stack.ExecuteAsync(stack.Us, $"insert into profiles values ({profile}, 'user {profile}', 'Austin', '{plans[random.Next(plans.Length)]}') on conflict (id) do update set plan = excluded.plan");

            try
            {
                await Task.Delay(250, stop.Token);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        await accounts;
        return 0;
    }

    private static async Task EnsureReplicaAsync(Stack stack)
    {
        HttpResponseMessage existing = await stack.Api.GetAsync($"/v1/sinks/{Replica}");

        if (!existing.IsSuccessStatusCode)
        {
            await stack.RegisterAsync(Replica, "Postgres", ["public.accounts", "public.profiles"], Replica, "Snapshot");
        }
    }

    private static async Task<Lag> LagAsync(Stack stack)
    {
        await using NpgsqlCommand command = stack.Sink.CreateCommand("""
            select count(*),
                   percentile_cont(0.5) within group (order by lag),
                   percentile_cont(0.95) within group (order by lag),
                   percentile_cont(0.99) within group (order by lag),
                   max(lag)
            from (select extract(epoch from applied_at - written_at) * 1000 as lag from accounts_applied) applied
            """);

        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();

        return new Lag(reader.GetInt64(0), reader.GetDouble(1), reader.GetDouble(2), reader.GetDouble(3), (double)reader.GetDecimal(4));
    }

    private static Task<long> InversionsAsync(Stack stack) =>
        Stack.ScalarAsync<long>(stack.Sink, """
            select count(*) from (
                select seq < lag(seq) over (partition by id order by n) as inverted from accounts_applied) applied
            where inverted
            """);

    /// <summary>Compares every row, bar the masked email, by hashing both tables in key order.</summary>
    private static async Task<bool> SinkEqualsSourceAsync(Stack stack)
    {
        const string Digest = "select md5(coalesce(string_agg(concat_ws('|', id, owner, balance, seq, written_at), ',' order by id), '')) from accounts";

        return string.Equals(
            await Stack.ScalarAsync<string>(stack.Eu, Digest),
            await Stack.ScalarAsync<string>(stack.Sink, Digest),
            StringComparison.Ordinal);
    }

    private static async Task<long> LostAcknowledgedAsync(Stack stack, Writer writer)
    {
        // Touch the primary first, so the pool has found it before the read below.
        await Stack.ScalarAsync<int>(stack.Eu, "select 1");
        Dictionary<long, long> stored = [];
        await using NpgsqlCommand command = stack.Eu.CreateCommand("select id, seq from accounts");
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            stored[reader.GetInt64(0)] = reader.GetInt64(1);
        }

        long lost = 0;

        for (int key = 1; key < writer.Acknowledged.Count; key++)
        {
            long acknowledged = writer.Acknowledged[key];

            if (acknowledged > 0 && stored.GetValueOrDefault(key) < acknowledged)
            {
                lost++;
            }
        }

        return lost;
    }

    private static async Task<(string Primary, string Standby)> RolesAsync(Stack stack, string project)
    {
        foreach ((string node, string peer) in (ValueTuple<string, string>[])[("pg-a", "pg-b"), ("pg-b", "pg-a")])
        {
            string recovering = await Stack.DockerAsync("exec", Container(project, node), "psql", "-U", "postgres", "-d", "postgres", "-Atc", "select pg_is_in_recovery()");

            if (recovering == "f")
            {
                return (node, peer);
            }
        }

        throw new InvalidOperationException("Neither eu node is a primary.");
    }

    /// <summary>
    /// Waits until the standby is streaming and holds a persisted, synchronised copy of the capture slot, so a
    /// promotion now would find it there.
    /// </summary>
    private static async Task WaitForStandbyAsync(Stack stack, string project, string standby)
    {
        var elapsed = Stopwatch.StartNew();

        while (true)
        {
            try
            {
                string ready = await Stack.DockerAsync("exec", Container(project, standby), "psql", "-U", "postgres", "-d", "postgres", "-Atc",
                    "select pg_is_in_recovery() and exists (select 1 from pg_replication_slots where slot_name = 'walrus_eu' and synced and not temporary and invalidation_reason is null) and exists (select 1 from pg_stat_wal_receiver where status = 'streaming')");

                if (ready == "t")
                {
                    return;
                }
            }
            catch (InvalidOperationException)
            {
                // Still starting.
            }

            Stack.Ensure(elapsed, TimeSpan.FromMinutes(3), $"{standby} to be a synchronised standby");
            await Task.Delay(500);
        }
    }

    private static async Task<string?> AttachedToAsync(Stack stack)
    {
        using JsonDocument stats = JsonDocument.Parse(await stack.Api.GetStringAsync("/v1/stats"));

        return stats.RootElement.GetProperty("sources").EnumerateArray()
            .First(static source => source.GetProperty("name").GetString() == "eu")
            .GetProperty("attachedTo").GetString();
    }

    private static async Task<double> UntilAsync(Func<Task<bool>> condition, Stopwatch since)
    {
        while (!await condition())
        {
            Stack.Ensure(since, TimeSpan.FromMinutes(3), "the system to recover");
            await Task.Delay(100);
        }

        return since.Elapsed.TotalSeconds;
    }

    private static string Container(string project, string service) => $"{project}-{service}-1";

    private static double Percentile(double[] sorted, double quantile) =>
        sorted.Length == 0 ? 0 : sorted[Math.Min(sorted.Length - 1, (int)Math.Ceiling(quantile * sorted.Length) - 1)];

    private sealed record Lag(long Count, double P50, double P95, double P99, double Max);
}
