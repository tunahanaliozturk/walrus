using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Walrus.Application.Capture;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.UnitTests;

internal static class Changes
{
    public static readonly DateTimeOffset Commit = new(2026, 9, 23, 12, 0, 0, TimeSpan.Zero);

    public static RowImage Row(params (string Name, string? Value)[] columns) =>
        new(columns.Select(static column => new RowColumn(column.Name, column.Value)));

    /// <summary>An upsert of one row, numbered so a test can check the order a sink saw them in.</summary>
    public static ChangeEvent Write(string source, long hlc, int key, int version, string value = "ok") =>
        new(source, new Lsn((ulong)hlc), 0, 1, Commit, hlc, "public.accounts", ChangeOperation.Update,
            Row(("id", key.ToString(System.Globalization.CultureInfo.InvariantCulture))), null,
            Row(("id", key.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                ("version", version.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                ("value", value)),
            null);
}

internal sealed class FakeOutbox : IOutboxReader
{
    private readonly List<OutboxEntry> _entries = [];

    public void Add(IEnumerable<ChangeEvent> changes)
    {
        lock (_entries)
        {
            foreach (ChangeEvent change in changes)
            {
                _entries.Add(new OutboxEntry(_entries.Count + 1, change));
            }
        }
    }

    public long Head
    {
        get
        {
            lock (_entries)
            {
                return _entries.Count;
            }
        }
    }

    public Task<IReadOnlyList<OutboxEntry>> ReadAsync(string source, long afterSeq, Lsn fromLsn, IReadOnlyCollection<string> tables, int limit, CancellationToken cancellationToken)
    {
        lock (_entries)
        {
            return Task.FromResult<IReadOnlyList<OutboxEntry>>([.. _entries
                .Where(entry => entry.Seq > afterSeq && entry.Change.Source == source && entry.Change.CommitLsn >= fromLsn && tables.Contains(entry.Change.Table))
                .Take(limit)]);
        }
    }

    public Task<long> LastSeqBeforeAsync(string source, Lsn lsn, CancellationToken cancellationToken) => Task.FromResult(0L);

    public Task<long> HeadSeqAsync(string source, CancellationToken cancellationToken) => Task.FromResult(Head);

    public Task<OutboxPosition> PositionAsync(string source, long afterSeq, Lsn fromLsn, IReadOnlyCollection<string> tables, CancellationToken cancellationToken) =>
        Task.FromResult(new OutboxPosition(Head, Lsn.Zero, null, null));
}

internal sealed class FakeCursors : ISinkCursorStore
{
    public ConcurrentDictionary<(string, string), SinkCursor> Saved { get; } = new();

    public Task<SinkCursor> GetAsync(string sinkId, string source, CancellationToken cancellationToken) =>
        Task.FromResult(Saved.GetValueOrDefault((sinkId, source), SinkCursor.Beginning));

    public Task AdvanceAsync(string sinkId, string source, long lastSeq, CancellationToken cancellationToken)
    {
        Saved.AddOrUpdate((sinkId, source), _ => new SinkCursor(lastSeq, Lsn.Zero), (_, current) => current with { LastSeq = Math.Max(current.LastSeq, lastSeq) });
        return Task.CompletedTask;
    }

    public Task SetAsync(string sinkId, string source, SinkCursor cursor, CancellationToken cancellationToken)
    {
        Saved[(sinkId, source)] = cursor;
        return Task.CompletedTask;
    }
}

internal sealed class FakeDeadLetters : IDeadLetterStore
{
    private readonly List<(DeadLetter Letter, bool Resolved)> _letters = [];

    public IReadOnlyList<DeadLetter> Open
    {
        get
        {
            lock (_letters)
            {
                return [.. _letters.Where(static letter => !letter.Resolved).Select(static letter => letter.Letter)];
            }
        }
    }

    public Task AddAsync(string sinkId, IReadOnlyList<DeadLetterEntry> entries, CancellationToken cancellationToken)
    {
        lock (_letters)
        {
            foreach (DeadLetterEntry entry in entries)
            {
                _letters.Add((new DeadLetter(_letters.Count + 1, entry.Source, entry.Seq, entry.Change, entry.Error, 1, Changes.Commit), false));
            }
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> BlockedKeysAsync(string sinkId, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<string>>([.. Open.Select(static letter => letter.Change.EntityKey).Distinct()]);

    public Task<IReadOnlyList<DeadLetter>> OpenAsync(string sinkId, string? entityKey, int limit, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<DeadLetter>>([.. Open.Where(letter => entityKey is null || letter.Change.EntityKey == entityKey).Take(limit)]);

    public Task ResolveAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken)
    {
        lock (_letters)
        {
            for (int index = 0; index < _letters.Count; index++)
            {
                if (ids.Contains(_letters[index].Letter.Id))
                {
                    _letters[index] = (_letters[index].Letter, true);
                }
            }
        }

        return Task.CompletedTask;
    }

    public Task RecordAttemptAsync(IReadOnlyList<long> ids, string reason, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task ClearAsync(string sinkId, CancellationToken cancellationToken)
    {
        lock (_letters)
        {
            _letters.Clear();
        }

        return Task.CompletedTask;
    }
}

/// <summary>A sink that records what it applied, and fails on demand.</summary>
internal sealed class FakeWriter : ISinkWriter
{
    private readonly ConcurrentDictionary<string, KeyState> _states = new(StringComparer.Ordinal);
    private int _transientFailures;

    public bool IsDurable => true;

    /// <summary>Rows whose next change is refused while this returns true for it.</summary>
    public Func<ChangeEvent, bool> Refuses { get; set; } = static _ => false;

    public ConcurrentQueue<ChangeEvent> Applied { get; } = new();

    public int Calls;

    public void FailTransiently(int times) => _transientFailures = times;

    public Task<IReadOnlyList<ChangeOutcome>> ApplyAsync(IReadOnlyList<ChangeEvent> changes, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref Calls);

        if (Interlocked.Decrement(ref _transientFailures) >= 0)
        {
            throw new IOException("The sink is restarting.");
        }

        if (changes.Any(Refuses))
        {
            throw new SinkRejectedException("23514: the sink refused a value.");
        }

        var outcomes = new ChangeOutcome[changes.Count];

        for (int index = 0; index < changes.Count; index++)
        {
            ChangeEvent change = changes[index];
            KeyApplication application = _states.GetValueOrDefault(change.EntityKey, KeyState.Empty).Apply(change);
            _states[change.EntityKey] = application.State;
            outcomes[index] = new ChangeOutcome(application.Changed, application.State.LatestVersion);
            Applied.Enqueue(change);
        }

        return Task.FromResult<IReadOnlyList<ChangeOutcome>>(outcomes);
    }

    public RowImage? Row(string entityKey) => _states.GetValueOrDefault(entityKey)?.Materialize();

    public Task ResetAsync(CancellationToken cancellationToken)
    {
        _states.Clear();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class RecordingDispatch : IDispatchObserver
{
    public int Conflicts;
    public int Parked;
    public int Retries;

    public void Applied(string sinkId, IReadOnlyList<ChangeEvent> changes, IReadOnlyList<ChangeOutcome> outcomes)
    {
    }

    public void ConflictResolved(string sinkId, ChangeEvent loser, ChangeVersion winner) => Interlocked.Increment(ref Conflicts);

    public void DeadLettered(string sinkId, IReadOnlyList<DeadLetterEntry> entries) => Interlocked.Add(ref Parked, entries.Count);

    public void Retrying(string sinkId, Exception failure, TimeSpan delay) => Interlocked.Increment(ref Retries);
}

/// <summary>A source log that plays a fixed list of transactions and records every acknowledgement.</summary>
internal sealed class ScriptedLog(IReadOnlyList<DecodedTransaction> transactions, ConcurrentQueue<string> events) : ISourceLog, ISourceLogFactory
{
    public string Host => "scripted:5432";

    public async IAsyncEnumerable<DecodedTransaction> ReadAsync(Lsn after, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (DecodedTransaction transaction in transactions)
        {
            yield return transaction;
        }

        // A real stream never ends; this one parks until the session is stopped.
        await Task.Delay(Timeout.Infinite, cancellationToken);
    }

    public ValueTask AcknowledgeAsync(Lsn persisted, CancellationToken cancellationToken)
    {
        events.Enqueue($"ack {persisted.Value}");
        return ValueTask.CompletedTask;
    }

    public Task<ISourceLog> OpenAsync(string source, CancellationToken cancellationToken) => Task.FromResult<ISourceLog>(this);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal sealed class RecordingStore(CaptureCheckpoint checkpoint, ConcurrentQueue<string> events) : ICaptureStore
{
    public ConcurrentQueue<ChangeEvent> Persisted { get; } = new();

    public Task<ICaptureLease?> TryLeadAsync(string source, CancellationToken cancellationToken) =>
        Task.FromResult<ICaptureLease?>(new Lease(checkpoint));

    public Task PersistAsync(string source, long epoch, IReadOnlyList<ChangeEvent> changes, Lsn confirmed, long lastHlc, CancellationToken cancellationToken)
    {
        foreach (ChangeEvent change in changes)
        {
            Persisted.Enqueue(change);
        }

        events.Enqueue($"persist {confirmed.Value}");
        return Task.CompletedTask;
    }

    private sealed class Lease(CaptureCheckpoint checkpoint) : ICaptureLease
    {
        public CaptureCheckpoint Checkpoint { get; } = checkpoint;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

internal sealed class CountingCaptureObserver : ICaptureObserver
{
    public int Resent;

    public void Persisted(string source, IReadOnlyList<ChangeEvent> changes, int transactions)
    {
    }

    public void SkippedDuplicate(string source, DecodedTransaction transaction) => Interlocked.Increment(ref Resent);
}
