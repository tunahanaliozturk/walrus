using System.Text.Json.Serialization;
using Walrus.Application.Capture;
using Walrus.Domain;

namespace Walrus.Application.Sinks;

/// <summary>Where a new sink starts reading.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<SinkStartMode>))]
public enum SinkStartMode
{
    /// <summary>From the first change the outbox holds.</summary>
    Beginning = 0,

    /// <summary>From the next change captured after registration.</summary>
    Now = 1,

    /// <summary>From the first change committed at or after a log position.</summary>
    Lsn = 2,

    /// <summary>
    /// From a consistent snapshot of the source tables, then from the snapshot's log position onward, with
    /// nothing missed and nothing repeated at the seam.
    /// </summary>
    Snapshot = 3,
}

/// <summary>A request to register a sink.</summary>
/// <param name="Sink">The sink.</param>
/// <param name="Start">Where it starts.</param>
/// <param name="StartLsn">The position, when <paramref name="Start"/> is <see cref="SinkStartMode.Lsn"/>.</param>
public sealed record SinkRegistration(SinkDefinition Sink, SinkStartMode Start, Lsn? StartLsn = null);

/// <summary>What happened to a request against a sink.</summary>
public enum SinkOperationOutcome
{
    /// <summary>Done.</summary>
    Done = 0,

    /// <summary>The request itself was wrong.</summary>
    Invalid = 1,

    /// <summary>No such sink.</summary>
    NotFound = 2,

    /// <summary>A sink with that id already exists.</summary>
    Conflict = 3,
}

/// <summary>The result of a request against a sink.</summary>
/// <param name="Outcome">What happened.</param>
/// <param name="Problems">What was wrong, for <see cref="SinkOperationOutcome.Invalid"/>.</param>
public sealed record SinkOperationResult(SinkOperationOutcome Outcome, IReadOnlyList<string> Problems)
{
    /// <summary>Done.</summary>
    public static SinkOperationResult Done { get; } = new(SinkOperationOutcome.Done, []);

    /// <summary>No such sink.</summary>
    public static SinkOperationResult NotFound { get; } = new(SinkOperationOutcome.NotFound, []);

    /// <summary>The id is taken.</summary>
    public static SinkOperationResult Conflict { get; } = new(SinkOperationOutcome.Conflict, []);

    /// <summary>The request was wrong.</summary>
    /// <param name="problems">What was wrong.</param>
    public static SinkOperationResult Invalid(params IReadOnlyList<string> problems) =>
        new(SinkOperationOutcome.Invalid, problems);
}

/// <summary>What a sink is doing.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<SinkState>))]
public enum SinkState
{
    /// <summary>Delivering.</summary>
    Running = 0,

    /// <summary>The sink is failing for a reason that may pass, and the last batch is being retried.</summary>
    Retrying = 1,

    /// <summary>Being reset, replayed or snapshotted.</summary>
    Rebuilding = 2,

    /// <summary>Not running, typically because dispatch failed and is waiting to restart.</summary>
    Stopped = 3,
}

/// <summary>Where a sink is in one source.</summary>
/// <param name="Source">The source.</param>
/// <param name="AppliedSeq">Everything up to here is applied or parked.</param>
/// <param name="HeadSeq">The newest change the outbox holds for the source.</param>
/// <param name="StartLsn">Nothing committed before this is delivered.</param>
/// <param name="PendingLsn">The commit position of the oldest change not yet applied, if any.</param>
/// <param name="LagMilliseconds">How long ago the oldest unapplied change committed at the source; zero when caught up.</param>
public sealed record SinkSourceStatus(
    string Source,
    long AppliedSeq,
    long HeadSeq,
    Lsn StartLsn,
    Lsn? PendingLsn,
    double LagMilliseconds);

/// <summary>A sink and where it is.</summary>
/// <param name="Sink">The definition.</param>
/// <param name="State">What it is doing.</param>
/// <param name="BlockedRows">Rows held behind a dead letter.</param>
/// <param name="Sources">Its position in each source.</param>
public sealed record SinkStatus(
    SinkDefinition Sink,
    SinkState State,
    int BlockedRows,
    IReadOnlyList<SinkSourceStatus> Sources);

/// <summary>Reads consistent snapshots of source tables.</summary>
public interface ISnapshotSource
{
    /// <summary>
    /// Copies the tables as of one instant and returns the log position of that instant: every transaction
    /// that committed before it is in the copy, and every one at or after it is not.
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="tables">The qualified tables to copy.</param>
    /// <param name="onRows">Receives the rows in batches, as inserts.</param>
    /// <param name="cancellationToken">Cancels the copy.</param>
    Task<Lsn> ExportAsync(
        string source,
        IReadOnlyList<string> tables,
        Func<IReadOnlyList<DecodedChange>, CancellationToken, Task> onRows,
        CancellationToken cancellationToken);
}

/// <summary>The configured sources and how each one's changes are stamped.</summary>
/// <param name="stampers">One stamper per source, keyed by source name.</param>
public sealed class CaptureSources(IReadOnlyDictionary<string, TransactionStamper> stampers)
{
    /// <summary>The source names.</summary>
    public IReadOnlyList<string> Names { get; } = [.. stampers.Keys.Order(StringComparer.Ordinal)];

    /// <summary>The stamper for a source.</summary>
    /// <param name="source">The source.</param>
    public TransactionStamper Stamper(string source) => stampers[source];
}
