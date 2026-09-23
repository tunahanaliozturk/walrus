using Walrus.Domain;

namespace Walrus.Application.Dispatch;

/// <summary>A change as the outbox holds it: its capture sequence number and the change.</summary>
/// <param name="Seq">Its position in the outbox, rising in capture order within a source.</param>
/// <param name="Change">The change.</param>
public readonly record struct OutboxEntry(long Seq, ChangeEvent Change);

/// <summary>How far the outbox has got for one source, and how far behind a reader at a given point is.</summary>
/// <param name="HeadSeq">The newest sequence number, or zero when the source has captured nothing.</param>
/// <param name="HeadLsn">The commit position of the newest change.</param>
/// <param name="OldestPendingCommit">
/// When the oldest change after the reader's position committed at the source, or null when the reader is
/// caught up. The age of this is the reader's lag.
/// </param>
/// <param name="PendingLsn">The commit position of that oldest pending change.</param>
public sealed record OutboxPosition(long HeadSeq, Lsn HeadLsn, DateTimeOffset? OldestPendingCommit, Lsn? PendingLsn);

/// <summary>Reads the outbox.</summary>
public interface IOutboxReader
{
    /// <summary>Reads the next changes after a sequence number, in sequence order.</summary>
    /// <param name="source">The source.</param>
    /// <param name="afterSeq">The last sequence number already read.</param>
    /// <param name="fromLsn">Changes that committed before this position are not returned.</param>
    /// <param name="tables">Only changes to these tables are returned.</param>
    /// <param name="limit">The most to return.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<IReadOnlyList<OutboxEntry>> ReadAsync(
        string source,
        long afterSeq,
        Lsn fromLsn,
        IReadOnlyCollection<string> tables,
        int limit,
        CancellationToken cancellationToken);

    /// <summary>The last sequence number of a change that committed before a position, or zero.</summary>
    /// <param name="source">The source.</param>
    /// <param name="lsn">The position.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<long> LastSeqBeforeAsync(string source, Lsn lsn, CancellationToken cancellationToken);

    /// <summary>The newest sequence number for a source, or zero.</summary>
    /// <param name="source">The source.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<long> HeadSeqAsync(string source, CancellationToken cancellationToken);

    /// <summary>Where the outbox is, relative to a reader at <paramref name="afterSeq"/>.</summary>
    /// <param name="source">The source.</param>
    /// <param name="afterSeq">The reader's position.</param>
    /// <param name="fromLsn">The reader's start position.</param>
    /// <param name="tables">The tables the reader receives.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<OutboxPosition> PositionAsync(
        string source,
        long afterSeq,
        Lsn fromLsn,
        IReadOnlyCollection<string> tables,
        CancellationToken cancellationToken);
}

/// <summary>What applying one change did to a sink.</summary>
/// <param name="Changed">Whether the sink's state moved.</param>
/// <param name="Winner">
/// The newest version the row holds afterwards. When a change did not move the state and the winner came from
/// another source, the change lost a conflict, and that is worth showing someone.
/// </param>
public readonly record struct ChangeOutcome(bool Changed, ChangeVersion? Winner);

/// <summary>Applies changes to one sink.</summary>
public interface ISinkWriter : IAsyncDisposable
{
    /// <summary>
    /// Whether the sink's state survives a restart. A durable sink resumes from its saved cursor; a volatile one
    /// rebuilds from its start position every time.
    /// </summary>
    bool IsDurable { get; }

    /// <summary>Applies a batch, all of it or none of it.</summary>
    /// <param name="changes">The changes, in delivery order. Changes to one row are in capture order.</param>
    /// <param name="cancellationToken">Cancels the apply.</param>
    /// <returns>One outcome per change, in the same order.</returns>
    /// <exception cref="SinkRejectedException">The batch can never apply, however often it is retried.</exception>
    Task<IReadOnlyList<ChangeOutcome>> ApplyAsync(IReadOnlyList<ChangeEvent> changes, CancellationToken cancellationToken);

    /// <summary>Removes everything the sink holds, so it can be rebuilt from a position.</summary>
    /// <param name="cancellationToken">Cancels the reset.</param>
    Task ResetAsync(CancellationToken cancellationToken);
}

/// <summary>Creates the writer for a sink.</summary>
public interface ISinkWriterFactory
{
    /// <summary>Creates the writer.</summary>
    /// <param name="sink">The sink.</param>
    /// <param name="validate">
    /// Whether to check the target now, which registration does so that a bad target is refused with a reason.
    /// A service starting up does not, so one sink whose database is down cannot stop the others starting.
    /// </param>
    /// <param name="cancellationToken">Cancels any preparation, such as creating the sink's state table.</param>
    /// <exception cref="ArgumentException">The target is not usable, when <paramref name="validate"/> is set.</exception>
    Task<ISinkWriter> CreateAsync(SinkDefinition sink, bool validate, CancellationToken cancellationToken);
}

/// <summary>A sink refused a batch for a reason retrying will not fix: a constraint, a type, a 4xx.</summary>
public sealed class SinkRejectedException : Exception
{
    /// <summary>Creates the exception.</summary>
    public SinkRejectedException()
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What the sink said.</param>
    public SinkRejectedException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What the sink said.</param>
    /// <param name="innerException">The cause.</param>
    public SinkRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>Where a sink is in one source's outbox.</summary>
/// <param name="LastSeq">Everything up to here has been applied or dead-lettered.</param>
/// <param name="StartLsn">Changes committed before this position are never delivered to the sink.</param>
public sealed record SinkCursor(long LastSeq, Lsn StartLsn)
{
    /// <summary>The start of the outbox.</summary>
    public static SinkCursor Beginning { get; } = new(0, Lsn.Zero);
}

/// <summary>Stores sink cursors.</summary>
public interface ISinkCursorStore
{
    /// <summary>Reads a cursor, or the beginning when none is stored.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="source">The source.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    Task<SinkCursor> GetAsync(string sinkId, string source, CancellationToken cancellationToken);

    /// <summary>Moves a cursor forward. Never moves it back.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="source">The source.</param>
    /// <param name="lastSeq">The new position.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task AdvanceAsync(string sinkId, string source, long lastSeq, CancellationToken cancellationToken);

    /// <summary>Sets a cursor to a position, backwards included, for replay and snapshot.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="source">The source.</param>
    /// <param name="cursor">The position.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task SetAsync(string sinkId, string source, SinkCursor cursor, CancellationToken cancellationToken);
}

/// <summary>A change a sink could not apply, parked with the reason.</summary>
/// <param name="Id">The letter's id, which also orders letters for one row.</param>
/// <param name="Source">The source.</param>
/// <param name="Seq">The change's outbox sequence number.</param>
/// <param name="Change">The change.</param>
/// <param name="Error">Why it was parked.</param>
/// <param name="Attempts">How many times it has been tried.</param>
/// <param name="DeadAt">When it was first parked.</param>
public sealed record DeadLetter(
    long Id,
    string Source,
    long Seq,
    ChangeEvent Change,
    string Error,
    int Attempts,
    DateTimeOffset DeadAt);

/// <summary>A change about to be parked.</summary>
/// <param name="Source">The source.</param>
/// <param name="Seq">Its outbox sequence number.</param>
/// <param name="Change">The change.</param>
/// <param name="Error">Why.</param>
public sealed record DeadLetterEntry(string Source, long Seq, ChangeEvent Change, string Error);

/// <summary>Stores dead letters.</summary>
public interface IDeadLetterStore
{
    /// <summary>Parks changes.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="entries">The changes, in delivery order.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task AddAsync(string sinkId, IReadOnlyList<DeadLetterEntry> entries, CancellationToken cancellationToken);

    /// <summary>The rows that have unresolved letters, and therefore queue everything behind them.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<IReadOnlyList<string>> BlockedKeysAsync(string sinkId, CancellationToken cancellationToken);

    /// <summary>Unresolved letters, oldest first, optionally for one row.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="entityKey">One row, or null for all.</param>
    /// <param name="limit">The most to return.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<IReadOnlyList<DeadLetter>> OpenAsync(string sinkId, string? entityKey, int limit, CancellationToken cancellationToken);

    /// <summary>Marks letters resolved.</summary>
    /// <param name="ids">The letters.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task ResolveAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken);

    /// <summary>Records another failed attempt.</summary>
    /// <param name="ids">The letters.</param>
    /// <param name="reason">The latest reason.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task RecordAttemptAsync(IReadOnlyList<long> ids, string reason, CancellationToken cancellationToken);

    /// <summary>Removes every letter for a sink, as part of rebuilding it.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task ClearAsync(string sinkId, CancellationToken cancellationToken);
}

/// <summary>Sees what dispatch does, for counters, the live feed and the conflict log.</summary>
public interface IDispatchObserver
{
    /// <summary>A batch was applied.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="changes">The changes.</param>
    /// <param name="outcomes">What each did.</param>
    void Applied(string sinkId, IReadOnlyList<ChangeEvent> changes, IReadOnlyList<ChangeOutcome> outcomes);

    /// <summary>A change lost to a newer change from another source.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="loser">The change that did not take effect.</param>
    /// <param name="winner">The version the row holds instead.</param>
    void ConflictResolved(string sinkId, ChangeEvent loser, ChangeVersion winner);

    /// <summary>Changes were parked.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="entries">The changes and why.</param>
    void DeadLettered(string sinkId, IReadOnlyList<DeadLetterEntry> entries);

    /// <summary>A batch failed for a reason that may pass, and will be retried.</summary>
    /// <param name="sinkId">The sink.</param>
    /// <param name="failure">The failure.</param>
    /// <param name="delay">How long until the retry.</param>
    void Retrying(string sinkId, Exception failure, TimeSpan delay);
}

/// <summary>Stores sink definitions.</summary>
public interface ISinkCatalog
{
    /// <summary>Every registered sink.</summary>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<IReadOnlyList<SinkDefinition>> ListAsync(CancellationToken cancellationToken);

    /// <summary>One sink, or null.</summary>
    /// <param name="id">The sink id.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    Task<SinkDefinition?> FindAsync(string id, CancellationToken cancellationToken);

    /// <summary>Adds a sink with its starting cursors, or returns false when the id is taken.</summary>
    /// <param name="sink">The sink.</param>
    /// <param name="cursors">Its cursor for each source.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<bool> AddAsync(SinkDefinition sink, IReadOnlyDictionary<string, SinkCursor> cursors, CancellationToken cancellationToken);

    /// <summary>Removes a sink with its cursors and letters, or returns false when there was none.</summary>
    /// <param name="id">The sink id.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    Task<bool> RemoveAsync(string id, CancellationToken cancellationToken);
}
