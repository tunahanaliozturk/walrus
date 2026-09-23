using System.Net;
using System.Net.Http.Json;
using Npgsql;
using Walrus.Api;
using Walrus.Application.Sinks;
using Walrus.Domain;

namespace Walrus.IntegrationTests;

/// <summary>
/// The whole path, source commit to sink row, through the running host: sinks converge on the source, apply
/// each row's changes in order, shrug off redelivery, and join a snapshot to the stream without a seam.
/// </summary>
[Collection(SharedRig.Name)]
public sealed class DispatchTests(TestRig rig)
{
    [Fact]
    public async Task A_postgres_sink_converges_on_the_source_and_never_applies_a_row_out_of_order()
    {
        string table = rig.NewTable("converge");
        await rig.CreateTableAsync(table, Workload.Columns, ["eu"]);
        await Workload.RecordAppliesAsync(rig, table);
        await RegisterAsync(table, SinkKind.Postgres, "sink", SinkStartMode.Beginning);

        await new Workload(rig, table, keys: 40, seed: 1).RunAsync("eu", transactions: 1_500);

        await rig.WaitForSinkToMatchAsync(table);
        (await Workload.InversionsAsync(rig, table)).ShouldBe(0);
    }

    [Fact]
    public async Task Replaying_a_sink_from_the_beginning_changes_nothing_and_rebuilding_it_gives_the_same_rows()
    {
        string table = rig.NewTable("replayed");
        await rig.CreateTableAsync(table, Workload.Columns, ["eu"]);
        string sink = await RegisterAsync(table, SinkKind.Postgres, "sink", SinkStartMode.Beginning);

        await new Workload(rig, table, keys: 30, seed: 2).RunAsync("eu", transactions: 400);
        await rig.WaitForSinkToMatchAsync(table);
        IReadOnlyList<string> before = await rig.RowsAsync("sink", table);

        // Redeliver everything. The sink's per-row state recognises every change, so nothing is written.
        await using (NpgsqlConnection target = await rig.OpenAsync("sink"))
        {
            await TestRig.ExecuteAsync(target, $"create table public.{table}_writes (n bigserial); create function public.{table}_count() returns trigger language plpgsql as $$ begin insert into public.{table}_writes default values; return null; end $$; create trigger {table}_count after insert or update or delete on public.{table} for each row execute function public.{table}_count();");
        }

        (await rig.Client().PostAsync($"/v1/sinks/{sink}/replay", null)).StatusCode.ShouldBe(HttpStatusCode.NoContent);
        await WaitForCaughtUpAsync(sink);

        (await rig.RowsAsync("sink", table)).ShouldBe(before);
        (await CountAsync("sink", $"{table}_writes")).ShouldBe(0);

        // Now empty it and rebuild from the outbox.
        (await rig.Client().PostAsync($"/v1/sinks/{sink}/replay?reset=true", null)).StatusCode.ShouldBe(HttpStatusCode.NoContent);
        await rig.WaitForSinkToMatchAsync(table);
        (await rig.RowsAsync("sink", table)).ShouldBe(before);
    }

    [Fact]
    public async Task A_sink_bootstrapped_from_a_snapshot_under_load_matches_the_source_and_a_sink_that_saw_everything()
    {
        string table = rig.NewTable("seam");
        string streamed = rig.NewTable("seamstream");

        // Two copies of one workload: the snapshot sink reads table, the full-stream sink reads streamed, and
        // both run the same writes, so their final contents must be the same.
        await rig.CreateTableAsync(table, Workload.Columns, ["eu"]);
        await rig.CreateTableAsync(streamed, Workload.Columns, ["eu"]);
        await RegisterAsync(streamed, SinkKind.Postgres, "sink", SinkStartMode.Beginning);

        // History the snapshot has to carry, written before the sink exists.
        await new Workload(rig, table, keys: 50, seed: 3).RunAsync("eu", transactions: 300);
        await new Workload(rig, streamed, keys: 50, seed: 3).RunAsync("eu", transactions: 300);

        using var writing = new CancellationTokenSource();
        var load = new Workload(rig, table, keys: 50, seed: 4);
        var mirror = new Workload(rig, streamed, keys: 50, seed: 4);
        Task during = Task.Run(async () => await load.RunAsync("eu", transactions: 400, writing.Token));

        // Registered while writes are in flight, so some transactions commit around the snapshot's point.
        await RegisterAsync(table, SinkKind.Postgres, "sink", SinkStartMode.Snapshot);
        await during;
        await mirror.RunAsync("eu", transactions: 400);

        await rig.WaitForSinkToMatchAsync(table);
        await rig.WaitForSinkToMatchAsync(streamed);

        (await rig.RowsAsync("sink", table)).ShouldBe(await rig.RowsAsync("sink", streamed));
    }

    [Fact]
    public async Task Two_sources_writing_the_same_rows_converge_to_the_same_state_in_every_sink()
    {
        string table = rig.NewTable("contested");
        await rig.CreateTableAsync(table, Workload.Columns, TestRig.Sources);
        string postgres = await RegisterAsync(table, SinkKind.Postgres, "sink", SinkStartMode.Beginning);
        string index = await RegisterAsync(table, SinkKind.Index, null, SinkStartMode.Beginning);

        await Task.WhenAll(
            new Workload(rig, table, keys: 20, seed: 5).RunAsync("eu", transactions: 500),
            new Workload(rig, table, keys: 20, seed: 6).RunAsync("us", transactions: 500));

        await WaitForCaughtUpAsync(postgres);
        await WaitForCaughtUpAsync(index);

        Dictionary<string, RowImage> indexed = IndexRows(index);
        IReadOnlyList<string> rows = await rig.RowsAsync("sink", table);
        rows.Count.ShouldBe(indexed.Count);

        // Same rows, same values, in the Postgres sink and the index, whichever order each saw the changes in.
        await using NpgsqlConnection target = await rig.OpenAsync("sink");
        await using var read = new NpgsqlCommand($"select id::text, owner, balance::text, seq::text from public.{table}", target);
        await using NpgsqlDataReader reader = await read.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            RowImage row = indexed[$"public.{table} {{\"id\":\"{reader.GetString(0)}\"}}"];
            row.TryGetValue("owner", out string? owner);
            row.TryGetValue("balance", out string? balance);
            row.TryGetValue("seq", out string? seq);
            (owner, balance, seq).ShouldBe((reader.GetString(1), reader.GetString(2), reader.GetString(3)));
        }

        (await StatusAsync(postgres)).Counters.Conflicts.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task A_merge_table_keeps_both_sources_changes_to_different_columns()
    {
        const string table = "merged";
        await rig.CreateTableAsync(table, "id bigint primary key, name text, city text, plan text", TestRig.Sources);
        string sink = await RegisterAsync(table, SinkKind.Postgres, "sink", SinkStartMode.Beginning);

        await using (NpgsqlConnection eu = await rig.OpenAsync("eu"))
        {
            await TestRig.ExecuteAsync(eu, "insert into merged values (1, 'ada', 'London', 'free')");
        }

        await WaitForCaughtUpAsync(sink);

        // The same row in the other region, then each region changes a different column.
        await using (NpgsqlConnection us = await rig.OpenAsync("us"))
        {
            await TestRig.ExecuteAsync(us, "insert into merged values (1, 'ada', 'London', 'free')");
            await TestRig.ExecuteAsync(us, "update merged set plan = 'team' where id = 1");
        }

        await using (NpgsqlConnection eu = await rig.OpenAsync("eu"))
        {
            await TestRig.ExecuteAsync(eu, "update merged set city = 'Paris' where id = 1");
        }

        await TestRig.WaitForAsync(
            async () => (await rig.RowsAsync("sink", table)).SequenceEqual(["(1,ada,Paris,team)"]),
            TimeSpan.FromSeconds(30),
            "both regions' columns in the sink");
    }

    [Fact]
    public async Task A_row_the_sink_refuses_is_parked_with_everything_after_it_while_other_rows_flow()
    {
        string table = rig.NewTable("refused");
        await rig.CreateTableAsync(
            table,
            "id bigint primary key, owner text not null, balance numeric(18,2) not null, seq bigint not null",
            ["eu"],
            sinkColumns: "id bigint primary key, owner text not null, balance numeric(18,2) not null check (balance >= 0), seq bigint not null");

        string sink = await RegisterAsync(table, SinkKind.Postgres, "sink", SinkStartMode.Beginning);

        await using (NpgsqlConnection eu = await rig.OpenAsync("eu"))
        {
            await TestRig.ExecuteAsync(eu, $"insert into {table} values (1, 'ada', 10, 1), (2, 'grace', 20, 1)");
            await TestRig.ExecuteAsync(eu, $"update {table} set balance = -5, seq = 2 where id = 1");
            await TestRig.ExecuteAsync(eu, $"update {table} set owner = 'ada lovelace', seq = 3 where id = 1");
            await TestRig.ExecuteAsync(eu, $"update {table} set balance = 25, seq = 2 where id = 2");
        }

        await WaitForCaughtUpAsync(sink);

        // Row 2 flowed. Row 1 stopped at the change the sink refused, and the one after it queued behind.
        (await rig.RowsAsync("sink", table)).ShouldBe(["(1,ada,10.00,1)", "(2,grace,25.00,2)"]);

        IReadOnlyList<DeadLetterResponse>? letters = await rig.Client(TestRig.ReadToken)
            .GetFromJsonAsync<IReadOnlyList<DeadLetterResponse>>($"/v1/sinks/{sink}/dead-letters");

        // The refused change, and the change after it for the same row: parked with it if they arrived in one
        // batch, or queued behind it if they arrived in the next.
        letters!.Count.ShouldBe(2);
        letters[0].Error.ShouldContain("23514");
        letters[1].Error.ShouldMatch("23514|Queued behind");
        (await StatusAsync(sink)).BlockedRows.ShouldBe(1);

        // Fix the sink and retry: the parked changes apply in order and the row catches up.
        await using (NpgsqlConnection target = await rig.OpenAsync("sink"))
        {
            await TestRig.ExecuteAsync(target, $"alter table public.{table} drop constraint {table}_balance_check");
        }

        HttpResponseMessage retry = await rig.Client().PostAsync($"/v1/sinks/{sink}/dead-letters/retry", null);
        (await retry.Content.ReadFromJsonAsync<RetryResponse>()).ShouldBe(new RetryResponse(1, 0));

        await rig.WaitForSinkToMatchAsync(table);
        (await StatusAsync(sink)).BlockedRows.ShouldBe(0);
    }

    [Fact]
    public async Task The_api_refuses_callers_without_the_right_token()
    {
        (await rig.Client(null).GetAsync("/v1/stats")).StatusCode.ShouldBe(HttpStatusCode.Unauthorized);
        (await rig.Client("not-the-token").GetAsync("/v1/stats")).StatusCode.ShouldBe(HttpStatusCode.Unauthorized);
        (await rig.Client(TestRig.ReadToken).GetAsync("/v1/stats")).StatusCode.ShouldBe(HttpStatusCode.OK);

        // The read token can look but not change anything.
        HttpResponseMessage register = await rig.Client(TestRig.ReadToken).PostAsJsonAsync("/v1/sinks",
            new RegisterSinkRequest("nope", SinkKind.Index, ["public.nothing"], null, SinkStartMode.Now, null));

        register.StatusCode.ShouldBe(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task Registration_refuses_what_would_fail_later()
    {
        HttpClient client = rig.Client();

        (await client.PostAsJsonAsync("/v1/sinks", new RegisterSinkRequest("Bad Id", SinkKind.Index, ["public.x"], null, SinkStartMode.Now, null)))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);

        (await client.PostAsJsonAsync("/v1/sinks", new RegisterSinkRequest("no-such-connection", SinkKind.Postgres, ["public.x"], "elsewhere", SinkStartMode.Now, null)))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);

        (await client.PostAsJsonAsync("/v1/sinks", new RegisterSinkRequest("no-such-table", SinkKind.Postgres, ["public.does_not_exist"], "sink", SinkStartMode.Now, null)))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);

        (await client.PostAsJsonAsync("/v1/sinks", new RegisterSinkRequest("internal-host", SinkKind.Webhook, ["public.x"], "http://169.254.169.254/latest", SinkStartMode.Now, null)))
            .StatusCode.ShouldBe(HttpStatusCode.BadRequest);

        (await client.PostAsJsonAsync("/v1/sinks", new RegisterSinkRequest("twice", SinkKind.Index, ["public.x"], null, SinkStartMode.Now, null)))
            .StatusCode.ShouldBe(HttpStatusCode.Created);

        (await client.PostAsJsonAsync("/v1/sinks", new RegisterSinkRequest("twice", SinkKind.Index, ["public.x"], null, SinkStartMode.Now, null)))
            .StatusCode.ShouldBe(HttpStatusCode.Conflict);
    }

    private async Task<string> RegisterAsync(string table, SinkKind kind, string? target, SinkStartMode start)
    {
        string id = $"{table.Replace('_', '-')}-{kind.ToString().ToLowerInvariant()}";
        HttpResponseMessage response = await rig.Client().PostAsJsonAsync("/v1/sinks",
            new RegisterSinkRequest(id, kind, [$"public.{table}"], target, start, null));

        response.StatusCode.ShouldBe(HttpStatusCode.Created, await response.Content.ReadAsStringAsync());

        return id;
    }

    private async Task<SinkResponse> StatusAsync(string sink) =>
        (await rig.Client(TestRig.ReadToken).GetFromJsonAsync<SinkResponse>($"/v1/sinks/{sink}"))!;

    /// <summary>Waits until the sink has applied everything the outbox holds for it, from every source.</summary>
    private async Task WaitForCaughtUpAsync(string sink)
    {
        await rig.WaitForCaptureAsync(TestRig.Sources);
        await TestRig.WaitForAsync(
            async () =>
            {
                SinkResponse status = await StatusAsync(sink);
                return status.Sources.All(static source => source.PendingLsn is null) && status.State is SinkState.Running;
            },
            TimeSpan.FromSeconds(60),
            $"sink {sink} to catch up");
    }

    private Dictionary<string, RowImage> IndexRows(string sink) =>
        rig.Services.GetRequiredService<Walrus.Infrastructure.Sinks.IndexSinkRegistry>().Find(sink)!.Rows()
            .ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.Ordinal);

    private async Task<long> CountAsync(string database, string table)
    {
        await using NpgsqlConnection connection = await rig.OpenAsync(database);
        await using var count = new NpgsqlCommand($"select count(*) from public.{table}", connection);

        return (long)(await count.ExecuteScalarAsync())!;
    }
}
