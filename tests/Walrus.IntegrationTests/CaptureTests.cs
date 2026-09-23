using Npgsql;
using Walrus.Application.Capture;
using Walrus.Domain;
using Walrus.Infrastructure.Store;

namespace Walrus.IntegrationTests;

/// <summary>
/// Capture against a real slot: what <c>pgoutput</c> sends becomes the right change events, and nothing a
/// crash can do makes the outbox lose or repeat one.
/// </summary>
[Collection(SharedRig.Name)]
public sealed class CaptureTests(TestRig rig)
{
    [Fact]
    public async Task Inserts_updates_deletes_and_key_changes_arrive_in_commit_order_with_before_images()
    {
        string table = rig.NewTable("decoded");
        await CreateLabTableAsync(table, "id bigint primary key, owner text not null, note text");
        await using var lab = new Lab(rig);
        (Task<bool> run, CancellationTokenSource stop) = await lab.StartAsync();

        // Large enough to be stored out of line, and random enough not to compress back into the row, so an
        // update that leaves it alone makes Postgres omit it.
        string large = string.Concat(Enumerable.Range(0, 4_000).Select(static _ => Guid.NewGuid().ToString("N")));

        await using (NpgsqlConnection source = await rig.OpenAsync("lab"))
        {
            await TestRig.ExecuteAsync(source, $"insert into {table} values (1, 'ada', null), (2, 'grace', null)");
            await TestRig.ExecuteAsync(source, $"update {table} set owner = 'ada lovelace' where id = 1");
            await TestRig.ExecuteAsync(source, $"update {table} set id = 3 where id = 2");
            await TestRig.ExecuteAsync(source, $"update {table} set note = '{large}' where id = 3");
            await TestRig.ExecuteAsync(source, $"update {table} set owner = 'grace hopper' where id = 3");
            await TestRig.ExecuteAsync(source, $"delete from {table} where id = 1");
        }

        await TestRig.WaitForAsync(async () => (await lab.OutboxAsync(table)).Count == 8, TimeSpan.FromSeconds(30), "eight changes");
        await stop.CancelAsync();
        (await run.WaitAsync(TimeSpan.FromSeconds(30))).ShouldBeTrue();

        IReadOnlyList<OutboxRow> rows = await lab.OutboxAsync(table);
        ChangeEvent[] changes = [.. rows.Select(OutboxMapping.ToChange)];

        changes.Select(static change => (change.Operation, change.Key.ToJson())).ShouldBe(
        [
            (ChangeOperation.Insert, """{"id":"1"}"""),
            (ChangeOperation.Insert, """{"id":"2"}"""),
            (ChangeOperation.Update, """{"id":"1"}"""),

            // The key change becomes a delete of the old key and an insert of the new one.
            (ChangeOperation.Delete, """{"id":"2"}"""),
            (ChangeOperation.Insert, """{"id":"3"}"""),
            (ChangeOperation.Update, """{"id":"3"}"""),
            (ChangeOperation.Update, """{"id":"3"}"""),
            (ChangeOperation.Delete, """{"id":"1"}"""),
        ]);

        // Two changes in one transaction share its position and clock stamp and differ by ordinal.
        changes[0].CommitLsn.ShouldBe(changes[1].CommitLsn);
        changes[0].Hlc.ShouldBe(changes[1].Hlc);
        (changes[0].Ordinal, changes[1].Ordinal).ShouldBe((0, 1));

        // Across transactions both rise.
        for (int index = 2; index < changes.Length; index++)
        {
            changes[index].CommitLsn.ShouldBeGreaterThanOrEqualTo(changes[index - 1].CommitLsn);
            changes[index].Hlc.ShouldBeGreaterThanOrEqualTo(changes[index - 1].Hlc);
        }

        changes[2].Before!.ToJson().ShouldBe("""{"id":"1","owner":"ada","note":null}""");
        changes[2].After!.ToJson().ShouldBe("""{"id":"1","owner":"ada lovelace","note":null}""");
        changes[7].Before!.ToJson().ShouldBe("""{"id":"1","owner":"ada lovelace","note":null}""");
        changes[7].After.ShouldBeNull();

        // The update that left the large value alone does not carry it: absent, not null.
        changes[6].After!.Contains("note").ShouldBeFalse();
        changes[6].After!.TryGetValue("owner", out string? owner).ShouldBeTrue();
        owner.ShouldBe("grace hopper");
    }

    [Fact]
    public async Task Nothing_is_acknowledged_before_it_is_durable_and_a_restart_loses_and_repeats_nothing()
    {
        string table = rig.NewTable("unacknowledged");
        await CreateLabTableAsync(table, "id bigint primary key, value integer not null");
        await using var lab = new Lab(rig);

        // First session: persists every transaction but never tells the source, as if the process died after
        // each outbox commit and before the acknowledgement.
        (Task<bool> run, CancellationTokenSource stop) = await lab.StartAsync(logs: new SilentAcknowledgements(lab.Logs));
        Lsn acknowledgedBefore = await lab.SlotConfirmedAsync();
        await WriteAsync(table, transactions: 50);
        await TestRig.WaitForAsync(async () => (await lab.OutboxAsync(table)).Count == 50, TimeSpan.FromSeconds(30), "fifty changes");
        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(30));

        // The source was never told, so it still holds all fifty and would send them to anyone who asked.
        (await lab.SlotConfirmedAsync()).ShouldBe(acknowledgedBefore);
        (await lab.CheckpointAsync()).ShouldBeGreaterThan(acknowledgedBefore);

        // Second session: resumes from the store's checkpoint, so nothing is stored twice, and acknowledges.
        (run, stop) = await lab.StartAsync();
        await WriteAsync(table, transactions: 10, offset: 50);
        await TestRig.WaitForAsync(async () => (await lab.OutboxAsync(table)).Count == 60, TimeSpan.FromSeconds(30), "sixty changes");
        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(30));

        IReadOnlyList<OutboxRow> rows = await lab.OutboxAsync(table);
        rows.Select(static row => RowImage.FromJson(row.Key).Columns[0].Value).ShouldBe(Enumerable.Range(1, 60).Select(static id => id.ToString(System.Globalization.CultureInfo.InvariantCulture)));
        (await lab.SlotConfirmedAsync()).ShouldBe(await lab.CheckpointAsync());
    }

    [Fact]
    public async Task A_store_failure_mid_stream_loses_nothing_and_repeats_nothing()
    {
        string table = rig.NewTable("interrupted");
        await CreateLabTableAsync(table, "id bigint primary key, value integer not null");
        await using var lab = new Lab(rig);

        // Fails the first write that would take the outbox past a hundred of this test's changes, however the
        // two hundred transactions happen to be grouped into writes.
        var failing = new FailingStore(lab.Store, failPast: 100);
        (Task<bool> run, CancellationTokenSource stop) = await lab.StartAsync(store: failing);
        await WriteAsync(table, transactions: 200);

        await Should.ThrowAsync<InvalidOperationException>(() => run.WaitAsync(TimeSpan.FromSeconds(60)));
        stop.Dispose();
        (await lab.OutboxAsync(table)).Count.ShouldBeLessThanOrEqualTo(100);

        (run, stop) = await lab.StartAsync();
        await TestRig.WaitForAsync(async () => (await lab.OutboxAsync(table)).Count >= 200, TimeSpan.FromSeconds(30), "all two hundred changes");
        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(30));

        IReadOnlyList<OutboxRow> rows = await lab.OutboxAsync(table);
        rows.Count.ShouldBe(200);
        rows.Select(static row => RowImage.FromJson(row.Key).Columns[0].Value).Distinct().Count().ShouldBe(200);
    }

    [Fact]
    public async Task Only_one_session_leads_and_a_fenced_session_cannot_write()
    {
        await using var lab = new Lab(rig);

        ICaptureLease? first = await lab.Store.TryLeadAsync("lab", CancellationToken.None);
        first.ShouldNotBeNull();

        (await lab.Store.TryLeadAsync("lab", CancellationToken.None)).ShouldBeNull();

        long staleEpoch = first.Checkpoint.Epoch;
        await first.DisposeAsync();

        await using ICaptureLease? second = await lab.Store.TryLeadAsync("lab", CancellationToken.None);
        second.ShouldNotBeNull();
        second.Checkpoint.Epoch.ShouldBe(staleEpoch + 1);

        // A session that lost its lease without noticing tries to finish a write.
        await Should.ThrowAsync<CaptureFencedException>(() =>
            lab.Store.PersistAsync("lab", staleEpoch, [], second.Checkpoint.Confirmed, second.Checkpoint.LastHlc, CancellationToken.None));
    }

    [Fact]
    public async Task Masked_columns_never_reach_the_outbox()
    {
        string table = rig.NewTable("masked");
        await CreateLabTableAsync(table, "id bigint primary key, email text, phone text, city text");

        var masks = new Dictionary<string, ColumnMask>(StringComparer.Ordinal)
        {
            ["email"] = ColumnMask.Hash,
            ["phone"] = ColumnMask.Redact,
        };

        await using var lab = new Lab(rig, new Dictionary<string, TablePolicy>(StringComparer.Ordinal)
        {
            [$"public.{table}"] = new($"public.{table}", ConflictMode.LastWriterWins, masks),
        });

        (Task<bool> run, CancellationTokenSource stop) = await lab.StartAsync();

        await using (NpgsqlConnection source = await rig.OpenAsync("lab"))
        {
            await TestRig.ExecuteAsync(source, $"insert into {table} values (1, 'ada@example.com', '+44 20 7946 0000', 'London'), (2, 'ada@example.com', null, 'Paris')");
        }

        await TestRig.WaitForAsync(async () => (await lab.OutboxAsync(table)).Count == 2, TimeSpan.FromSeconds(30), "two changes");
        await stop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(30));

        IReadOnlyList<OutboxRow> rows = await lab.OutboxAsync(table);

        rows.ShouldAllBe(row => !row.After!.Contains("ada@example.com") && !row.After.Contains("7946"));

        RowImage first = RowImage.FromJson(rows[0].After!);
        RowImage second = RowImage.FromJson(rows[1].After!);
        first.TryGetValue("email", out string? firstEmail);
        second.TryGetValue("email", out string? secondEmail);
        first.TryGetValue("phone", out string? phone);
        second.TryGetValue("phone", out string? missingPhone);
        first.TryGetValue("city", out string? city);

        // Hashed values stay equal to each other, so the column can still be joined and counted distinct.
        firstEmail.ShouldBe(secondEmail);
        firstEmail!.Length.ShouldBe(64);
        phone.ShouldBe(ColumnMasker.RedactedMarker);
        missingPhone.ShouldBeNull();
        city.ShouldBe("London");
    }

    private async Task CreateLabTableAsync(string table, string columns)
    {
        await using NpgsqlConnection connection = await rig.OpenAsync("lab");
        await TestRig.ExecuteAsync(connection, $"""
            create table public.{table} ({columns});
            alter table public.{table} replica identity full;
            grant select on public.{table} to walrus_capture;
            alter publication walrus add table public.{table};
            """);
    }

    private async Task WriteAsync(string table, int transactions, int offset = 0)
    {
        await using NpgsqlConnection source = await rig.OpenAsync("lab");

        for (int index = 1; index <= transactions; index++)
        {
            await TestRig.ExecuteAsync(source, $"insert into {table} values ({offset + index}, {index})");
        }
    }

    /// <summary>A log that persists nothing to the source: acknowledgements go nowhere.</summary>
    private sealed class SilentAcknowledgements(ISourceLogFactory inner) : ISourceLogFactory
    {
        public async Task<ISourceLog> OpenAsync(string source, CancellationToken cancellationToken) =>
            new Silent(await inner.OpenAsync(source, cancellationToken));

        private sealed class Silent(ISourceLog inner) : ISourceLog
        {
            public string Host => inner.Host;

            public IAsyncEnumerable<DecodedTransaction> ReadAsync(Lsn after, CancellationToken cancellationToken) =>
                inner.ReadAsync(after, cancellationToken);

            public ValueTask AcknowledgeAsync(Lsn persisted, CancellationToken cancellationToken) => ValueTask.CompletedTask;

            public ValueTask DisposeAsync() => inner.DisposeAsync();
        }
    }

    /// <summary>A store whose connection drops once it has taken a given number of changes.</summary>
    private sealed class FailingStore(ICaptureStore inner, int failPast) : ICaptureStore
    {
        private int _changes;

        public Task<ICaptureLease?> TryLeadAsync(string source, CancellationToken cancellationToken) =>
            inner.TryLeadAsync(source, cancellationToken);

        public Task PersistAsync(string source, long epoch, IReadOnlyList<ChangeEvent> changes, Lsn confirmed, long lastHlc, CancellationToken cancellationToken) =>
            Interlocked.Add(ref _changes, changes.Count) > failPast
                ? throw new InvalidOperationException("The store connection dropped.")
                : inner.PersistAsync(source, epoch, changes, confirmed, lastHlc, cancellationToken);
    }
}
