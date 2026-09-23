using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Walrus.Application.Capture;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure.Telemetry;

/// <summary>The meter and activity source every part of the service reports through.</summary>
public sealed class WalrusTelemetry : IDisposable
{
    /// <summary>The meter and activity source name.</summary>
    public const string Name = "Walrus";

    /// <summary>Creates the instruments.</summary>
    /// <param name="meters">The meter factory.</param>
    public WalrusTelemetry(IMeterFactory meters)
    {
        ArgumentNullException.ThrowIfNull(meters);

        Meter = meters.Create(Name);
        CapturedChanges = Meter.CreateCounter<long>("walrus.capture.changes", "{change}", "Row changes written to the outbox.");
        CapturedTransactions = Meter.CreateCounter<long>("walrus.capture.transactions", "{transaction}", "Source transactions persisted, heartbeats included.");
        ResentTransactions = Meter.CreateCounter<long>("walrus.capture.resent", "{transaction}", "Transactions the source sent again after a restart, recognised and skipped.");
        Applied = Meter.CreateCounter<long>("walrus.dispatch.applied", "{change}", "Changes delivered to a sink, by whether they changed it.");
        ApplyLag = Meter.CreateHistogram<double>("walrus.dispatch.lag", "ms", "Time from source commit to sink apply.");
        DeadLetters = Meter.CreateCounter<long>("walrus.dispatch.dead_letters", "{change}", "Changes a sink parked.");
        Retries = Meter.CreateCounter<long>("walrus.dispatch.retries", "{retry}", "Batches retried after a failure that may pass.");
        Conflicts = Meter.CreateCounter<long>("walrus.dispatch.conflicts", "{change}", "Changes that lost to a newer change from another source.");
    }

    /// <summary>Traces spanning capture, outbox and sink apply.</summary>
    public static ActivitySource Activities { get; } = new(Name);

    /// <summary>The meter.</summary>
    public Meter Meter { get; }

    /// <summary>Row changes written to the outbox.</summary>
    public Counter<long> CapturedChanges { get; }

    /// <summary>Source transactions persisted.</summary>
    public Counter<long> CapturedTransactions { get; }

    /// <summary>Resent transactions skipped.</summary>
    public Counter<long> ResentTransactions { get; }

    /// <summary>Changes delivered to sinks.</summary>
    public Counter<long> Applied { get; }

    /// <summary>Commit to apply time.</summary>
    public Histogram<double> ApplyLag { get; }

    /// <summary>Changes parked.</summary>
    public Counter<long> DeadLetters { get; }

    /// <summary>Retried batches.</summary>
    public Counter<long> Retries { get; }

    /// <summary>Lost conflicts.</summary>
    public Counter<long> Conflicts { get; }

    /// <inheritdoc />
    public void Dispose() => Meter.Dispose();
}

/// <summary>Running totals per source and per sink, for the stats endpoint and the console.</summary>
public sealed class PipelineCounters
{
    private readonly ConcurrentDictionary<string, SourceCounters> _sources = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SinkCounters> _sinks = new(StringComparer.Ordinal);

    /// <summary>A source's counters.</summary>
    /// <param name="source">The source.</param>
    public SourceCounters Source(string source) => _sources.GetOrAdd(source, static _ => new SourceCounters());

    /// <summary>A sink's counters.</summary>
    /// <param name="sink">The sink.</param>
    public SinkCounters Sink(string sink) => _sinks.GetOrAdd(sink, static _ => new SinkCounters());
}

/// <summary>What capture has done for one source since the service started.</summary>
public sealed class SourceCounters
{
    private long _changes;
    private long _transactions;
    private long _resent;
    private long _lastCommitTicks;

    /// <summary>Row changes persisted.</summary>
    public long Changes => Volatile.Read(ref _changes);

    /// <summary>Transactions persisted.</summary>
    public long Transactions => Volatile.Read(ref _transactions);

    /// <summary>Resent transactions skipped.</summary>
    public long Resent => Volatile.Read(ref _resent);

    /// <summary>The commit time of the newest change persisted, or null.</summary>
    public DateTimeOffset? LastCommit
    {
        get
        {
            long ticks = Volatile.Read(ref _lastCommitTicks);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    /// <summary>The node capture is attached to, or null when it is not attached.</summary>
    public string? Host { get; set; }

    /// <summary>Why capture last stopped, or null while it is running.</summary>
    public string? LastError { get; set; }

    internal void Persisted(int changes, int transactions, DateTimeOffset? lastCommit)
    {
        Interlocked.Add(ref _changes, changes);
        Interlocked.Add(ref _transactions, transactions);

        if (lastCommit is { } commit)
        {
            Volatile.Write(ref _lastCommitTicks, commit.UtcTicks);
        }
    }

    internal void Resend() => Interlocked.Increment(ref _resent);
}

/// <summary>What dispatch has done for one sink since the service started.</summary>
public sealed class SinkCounters
{
    private long _changed;
    private long _unchanged;
    private long _conflicts;
    private long _deadLetters;
    private long _retries;

    /// <summary>Changes that moved the sink's state.</summary>
    public long Changed => Volatile.Read(ref _changed);

    /// <summary>Changes already absorbed or superseded.</summary>
    public long Unchanged => Volatile.Read(ref _unchanged);

    /// <summary>Changes that lost to another source.</summary>
    public long Conflicts => Volatile.Read(ref _conflicts);

    /// <summary>Changes parked.</summary>
    public long DeadLetters => Volatile.Read(ref _deadLetters);

    /// <summary>Retried batches.</summary>
    public long Retries => Volatile.Read(ref _retries);

    /// <summary>The last failure that caused a retry, or null.</summary>
    public string? LastError { get; set; }

    internal void Applied(int changed, int unchanged)
    {
        Interlocked.Add(ref _changed, changed);
        Interlocked.Add(ref _unchanged, unchanged);
    }

    internal void Conflict() => Interlocked.Increment(ref _conflicts);

    internal void Parked(int count) => Interlocked.Add(ref _deadLetters, count);

    internal void Retry() => Interlocked.Increment(ref _retries);
}

/// <summary>Turns capture events into metrics, counters and a sampled live feed.</summary>
/// <param name="telemetry">The instruments.</param>
/// <param name="counters">The running totals.</param>
/// <param name="feed">The live feed.</param>
/// <param name="time">The clock.</param>
internal sealed class CaptureObserver(
    WalrusTelemetry telemetry,
    PipelineCounters counters,
    LiveFeed feed,
    TimeProvider time) : ICaptureObserver
{
    private const int SamplesPerSecond = 5;

    private readonly ConcurrentDictionary<string, (long Second, int Sent)> _sampling = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public void Persisted(string source, IReadOnlyList<ChangeEvent> changes, int transactions)
    {
        ArgumentNullException.ThrowIfNull(changes);

        var tag = new KeyValuePair<string, object?>("source", source);
        telemetry.CapturedChanges.Add(changes.Count, tag);
        telemetry.CapturedTransactions.Add(transactions, tag);
        counters.Source(source).Persisted(changes.Count, transactions, changes.Count > 0 ? changes[^1].CommitTimestamp : null);

        DateTimeOffset now = time.GetUtcNow();
        long second = now.ToUnixTimeSeconds();

        foreach (ChangeEvent change in changes)
        {
            (long Second, int Sent) window = _sampling.GetValueOrDefault(source);

            if (window.Second == second && window.Sent >= SamplesPerSecond)
            {
                break;
            }

            _sampling[source] = window.Second == second ? (second, window.Sent + 1) : (second, 1);
            feed.Publish(Entry(FeedKind.Change, now, change, null, null));
        }
    }

    /// <inheritdoc />
    public void SkippedDuplicate(string source, DecodedTransaction transaction)
    {
        telemetry.ResentTransactions.Add(1, new KeyValuePair<string, object?>("source", source));
        counters.Source(source).Resend();
    }

    internal static FeedEntry Entry(FeedKind kind, DateTimeOffset at, ChangeEvent change, string? sink, string? detail) =>
        new(kind, at, change.Source, change.Table, change.Operation.ToString().ToLowerInvariant(), change.Key.ToJson(),
            change.CommitLsn.ToString(), sink, detail);
}

/// <summary>Turns dispatch events into metrics, counters and feed entries.</summary>
/// <param name="telemetry">The instruments.</param>
/// <param name="counters">The running totals.</param>
/// <param name="feed">The live feed.</param>
/// <param name="time">The clock.</param>
internal sealed class DispatchObserver(
    WalrusTelemetry telemetry,
    PipelineCounters counters,
    LiveFeed feed,
    TimeProvider time) : IDispatchObserver
{
    /// <inheritdoc />
    public void Applied(string sinkId, IReadOnlyList<ChangeEvent> changes, IReadOnlyList<ChangeOutcome> outcomes)
    {
        ArgumentNullException.ThrowIfNull(changes);
        ArgumentNullException.ThrowIfNull(outcomes);

        DateTimeOffset now = time.GetUtcNow();
        var sink = new KeyValuePair<string, object?>("sink", sinkId);
        int changed = 0;

        for (int index = 0; index < changes.Count; index++)
        {
            if (outcomes[index].Changed)
            {
                changed++;
            }

            // Snapshot rows carry no commit time, and a lag measured from 1970 would swamp the histogram.
            if (changes[index].Hlc != 0)
            {
                telemetry.ApplyLag.Record((now - changes[index].CommitTimestamp).TotalMilliseconds, sink);
            }
        }

        telemetry.Applied.Add(changed, sink, new KeyValuePair<string, object?>("outcome", "changed"));
        telemetry.Applied.Add(changes.Count - changed, sink, new KeyValuePair<string, object?>("outcome", "unchanged"));
        counters.Sink(sinkId).Applied(changed, changes.Count - changed);
    }

    /// <inheritdoc />
    public void ConflictResolved(string sinkId, ChangeEvent loser, ChangeVersion winner)
    {
        telemetry.Conflicts.Add(1, new KeyValuePair<string, object?>("sink", sinkId), new KeyValuePair<string, object?>("winner", winner.Source));
        counters.Sink(sinkId).Conflict();
        feed.Publish(CaptureObserver.Entry(FeedKind.Conflict, time.GetUtcNow(), loser, sinkId,
            $"kept {winner.Source} at {winner.Hlc}/{winner.Ordinal} over {loser.Source} at {loser.Hlc}/{loser.Ordinal}"));
    }

    /// <inheritdoc />
    public void DeadLettered(string sinkId, IReadOnlyList<DeadLetterEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        telemetry.DeadLetters.Add(entries.Count, new KeyValuePair<string, object?>("sink", sinkId));
        counters.Sink(sinkId).Parked(entries.Count);
        DateTimeOffset now = time.GetUtcNow();

        foreach (DeadLetterEntry entry in entries)
        {
            feed.Publish(CaptureObserver.Entry(FeedKind.DeadLetter, now, entry.Change, sinkId, entry.Error));
        }
    }

    /// <inheritdoc />
    public void Retrying(string sinkId, Exception failure, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(failure);

        telemetry.Retries.Add(1, new KeyValuePair<string, object?>("sink", sinkId));
        SinkCounters sink = counters.Sink(sinkId);
        sink.Retry();
        sink.LastError = failure.Message;
    }
}
