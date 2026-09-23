using System.Collections.Concurrent;
using Microsoft.Extensions.Logging.Abstractions;
using Walrus.Application.Capture;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.UnitTests;

/// <summary>Dispatch against in-memory ports: ordering, isolation of a refused row, and patience with a sink that is down.</summary>
public sealed class DispatcherTests : IAsyncDisposable
{
    private static readonly SinkDefinition Sink = new("replica", SinkKind.Postgres, ["public.accounts"], "replica");

    private readonly FakeOutbox _outbox = new();
    private readonly FakeCursors _cursors = new();
    private readonly FakeDeadLetters _letters = new();
    private readonly FakeWriter _writer = new();
    private readonly RecordingDispatch _observed = new();
    private readonly OutboxSignal _signal = new();

    private SinkDispatcher Dispatcher() => new(
        Sink,
        ["eu", "us"],
        _writer,
        _outbox,
        _cursors,
        _letters,
        _signal,
        _observed,
        new DispatchOptions
        {
            Lanes = 4,
            MaxBatch = 16,
            ReadBatch = 64,
            IdleWait = TimeSpan.FromMilliseconds(20),
            CursorFlushInterval = TimeSpan.FromMilliseconds(10),
            FirstRetryDelay = TimeSpan.FromMilliseconds(1),
            MaxRetryDelay = TimeSpan.FromMilliseconds(5),
        },
        TimeProvider.System,
        NullLogger<SinkDispatcher>.Instance);

    [Fact]
    public async Task Each_rows_changes_reach_the_sink_in_capture_order_while_rows_run_in_parallel()
    {
        var random = new Random(7);
        Dictionary<int, int> versions = [];
        List<ChangeEvent> changes = [];

        for (int hlc = 1; hlc <= 3_000; hlc++)
        {
            int key = random.Next(1, 51);
            versions[key] = versions.GetValueOrDefault(key) + 1;
            changes.Add(Changes.Write("eu", hlc, key, versions[key]));
        }

        _outbox.Add(changes);

        await RunUntilAsync(() => _writer.Applied.Count >= changes.Count);

        // Per row, the versions arrive rising and without a gap.
        foreach (IGrouping<string, ChangeEvent> row in _writer.Applied.GroupBy(static change => change.EntityKey))
        {
            int[] seen = [.. row.Select(static change => int.Parse(Value(change, "version"), System.Globalization.CultureInfo.InvariantCulture))];
            seen.ShouldBe(Enumerable.Range(1, seen.Length).ToArray());
        }

        _cursors.Saved[("replica", "eu")].LastSeq.ShouldBe(_outbox.Head);
    }

    [Fact]
    public async Task A_refused_row_is_parked_with_everything_after_it_and_other_rows_keep_flowing()
    {
        _writer.Refuses = static change => Value(change, "value") == "poison";

        _outbox.Add(
        [
            Changes.Write("eu", 1, key: 1, version: 1),
            Changes.Write("eu", 2, key: 2, version: 1),
            Changes.Write("eu", 3, key: 1, version: 2, value: "poison"),
            Changes.Write("eu", 4, key: 2, version: 2),
            Changes.Write("eu", 5, key: 1, version: 3),
            Changes.Write("eu", 6, key: 3, version: 1),
        ]);

        await RunUntilAsync(() => _cursors.Saved.TryGetValue(("replica", "eu"), out SinkCursor? cursor) && cursor.LastSeq == 6);

        Value(_writer.Row("public.accounts {\"id\":\"1\"}")!, "version").ShouldBe("1");
        Value(_writer.Row("public.accounts {\"id\":\"2\"}")!, "version").ShouldBe("2");
        Value(_writer.Row("public.accounts {\"id\":\"3\"}")!, "version").ShouldBe("1");

        _letters.Open.Select(static letter => Value(letter.Change, "version")).ShouldBe(["2", "3"]);
        _observed.Parked.ShouldBe(2);
    }

    [Fact]
    public async Task Parked_rows_apply_in_order_once_the_sink_accepts_them()
    {
        _writer.Refuses = static change => Value(change, "value") == "poison";
        _outbox.Add([Changes.Write("eu", 1, 1, 1), Changes.Write("eu", 2, 1, 2, "poison"), Changes.Write("eu", 3, 1, 3)]);

        SinkDispatcher dispatcher = Dispatcher();
        using var stop = new CancellationTokenSource();
        Task run = dispatcher.RunAsync(stop.Token);

        await WaitAsync(() => _letters.Open.Count == 2);

        _writer.Refuses = static _ => false;
        (await dispatcher.RetryDeadLettersAsync(CancellationToken.None)).ShouldBe((1, 0));

        Value(_writer.Row("public.accounts {\"id\":\"1\"}")!, "version").ShouldBe("3");
        _letters.Open.ShouldBeEmpty();
        dispatcher.BlockedRows.ShouldBe(0);

        // And the row flows again.
        _outbox.Add([Changes.Write("eu", 4, 1, 4)]);
        _signal.Notify("eu");
        await WaitAsync(() => _writer.Row("public.accounts {\"id\":\"1\"}") is { } row && Value(row, "version") == "4");

        await stop.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => run);
    }

    [Fact]
    public async Task A_sink_that_is_down_is_retried_and_nothing_is_parked()
    {
        _writer.FailTransiently(5);
        _outbox.Add([.. Enumerable.Range(1, 20).Select(static index => Changes.Write("eu", index, index, 1))]);

        await RunUntilAsync(() => _writer.Applied.Count == 20);

        _observed.Retries.ShouldBeGreaterThanOrEqualTo(5);
        _letters.Open.ShouldBeEmpty();
    }

    [Fact]
    public async Task A_change_that_loses_to_another_source_is_reported_as_a_conflict()
    {
        _outbox.Add([Changes.Write("us", 20, 1, 1)]);
        await RunUntilAsync(() => _writer.Applied.Count == 1);

        _outbox.Add([Changes.Write("eu", 10, 1, 1)]);
        await RunUntilAsync(() => _writer.Applied.Count == 2);

        _observed.Conflicts.ShouldBe(1);
        _writer.Row("public.accounts {\"id\":\"1\"}").ShouldNotBeNull();
    }

    private async Task RunUntilAsync(Func<bool> done)
    {
        using var stop = new CancellationTokenSource();
        Task run = Dispatcher().RunAsync(stop.Token);

        await WaitAsync(done);
        await stop.CancelAsync();
        await Should.ThrowAsync<OperationCanceledException>(() => run);
    }

    private static async Task WaitAsync(Func<bool> done)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(20);

        while (!done())
        {
            DateTime.UtcNow.ShouldBeLessThan(deadline, "the dispatcher did not get there in time");
            await Task.Delay(10);
        }
    }

    public ValueTask DisposeAsync() => _writer.DisposeAsync();

    private static string Value(ChangeEvent change, string column) => Value(change.After!, column);

    private static string Value(RowImage row, string column)
    {
        row.TryGetValue(column, out string? value);
        return value!;
    }
}

/// <summary>The capture session's one ordering rule, and its duplicate check, against scripted ports.</summary>
public sealed class CaptureSessionTests
{
    private static DecodedTransaction Transaction(ulong commit, int changes) =>
        new(new Lsn(commit), new Lsn(commit + 10), (uint)commit, Changes.Commit,
            [.. Enumerable.Range(0, changes).Select(index => new DecodedChange(
                "public.accounts", ChangeOperation.Insert, Changes.Row(("id", $"{commit}-{index}")), null, Changes.Row(("id", $"{commit}-{index}"))))]);

    [Fact]
    public async Task Every_acknowledgement_follows_the_write_that_made_it_true()
    {
        ConcurrentQueue<string> events = new();
        DecodedTransaction[] script = [.. Enumerable.Range(1, 200).Select(index => Transaction((ulong)index * 100, index % 3))];
        var store = new RecordingStore(new CaptureCheckpoint(1, Lsn.Zero, 0), events);

        await RunAsync(store, new ScriptedLog(script, events), new CountingCaptureObserver(), () => events.Any(static e => e.StartsWith("ack", StringComparison.Ordinal))
            && events.Last() == $"ack {script[^1].EndLsn.Value}");

        string[] log = [.. events];
        ulong persisted = 0;

        foreach (string entry in log)
        {
            ulong position = ulong.Parse(entry[(entry.IndexOf(' ', StringComparison.Ordinal) + 1)..], System.Globalization.CultureInfo.InvariantCulture);

            if (entry.StartsWith("persist", StringComparison.Ordinal))
            {
                persisted = position;
            }
            else
            {
                position.ShouldBeLessThanOrEqualTo(persisted, "acknowledged a position before it was durable");
            }
        }

        store.Persisted.Count.ShouldBe(script.Sum(static transaction => transaction.Changes.Count));
    }

    [Fact]
    public async Task Transactions_the_store_already_has_are_skipped_by_position()
    {
        ConcurrentQueue<string> events = new();
        DecodedTransaction[] script = [.. Enumerable.Range(1, 10).Select(index => Transaction((ulong)index * 100, 1))];
        var observer = new CountingCaptureObserver();

        // The store already holds everything up to the end of the fourth transaction.
        var store = new RecordingStore(new CaptureCheckpoint(1, script[3].EndLsn, 0), events);

        await RunAsync(store, new ScriptedLog(script, events), observer, () => store.Persisted.Count == 6);

        observer.Resent.ShouldBe(4);
        store.Persisted.Select(static change => change.CommitLsn).ShouldBe(script.Skip(4).Select(static transaction => transaction.CommitLsn));
    }

    private static async Task RunAsync(ICaptureStore store, ScriptedLog log, ICaptureObserver observer, Func<bool> done)
    {
        var session = new CaptureSession(
            "eu",
            store,
            log,
            new TransactionStamper("eu", new Dictionary<string, TablePolicy>(), new ColumnMasker(new byte[32])),
            new OutboxSignal(),
            observer,
            new CaptureOptions(MaxBatchChanges: 7),
            NullLogger<CaptureSession>.Instance);

        using var stop = new CancellationTokenSource();
        Task<bool> run = session.RunAsync(stop.Token);
        DateTime deadline = DateTime.UtcNow.AddSeconds(20);

        while (!done())
        {
            DateTime.UtcNow.ShouldBeLessThan(deadline);
            await Task.Delay(10);
        }

        await stop.CancelAsync();
        (await run).ShouldBeTrue();
    }
}
