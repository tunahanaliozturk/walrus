using Walrus.Domain;

namespace Walrus.Application.Capture;

/// <summary>One row change as the source's log described it, before it is stamped and masked.</summary>
/// <param name="Table">The qualified table name.</param>
/// <param name="Operation">What happened.</param>
/// <param name="Key">The primary key after the change, or of the deleted row.</param>
/// <param name="Before">The row before the change, when the table's replica identity sends it.</param>
/// <param name="After">The row after the change. Null for a delete.</param>
/// <param name="PreviousKey">
/// The primary key before an update that changed it. Null when the key did not change, which is almost always.
/// </param>
public sealed record DecodedChange(
    string Table,
    ChangeOperation Operation,
    RowImage Key,
    RowImage? Before,
    RowImage? After,
    RowImage? PreviousKey = null);

/// <summary>One committed source transaction, whole.</summary>
/// <param name="CommitLsn">The log position of its commit record.</param>
/// <param name="EndLsn">
/// The position just past its commit record. Acknowledging this tells the source it never has to send the
/// transaction again, so it is only ever acknowledged once the transaction is in the outbox.
/// </param>
/// <param name="TransactionId">The source transaction id.</param>
/// <param name="CommitTimestamp">When the source says it committed.</param>
/// <param name="Changes">Its row changes, in the order the transaction made them. Empty for a heartbeat.</param>
public sealed record DecodedTransaction(
    Lsn CommitLsn,
    Lsn EndLsn,
    uint TransactionId,
    DateTimeOffset CommitTimestamp,
    IReadOnlyList<DecodedChange> Changes);

/// <summary>Where a capture session starts from, read back from the store as the session opens.</summary>
/// <param name="Epoch">This session's fencing token. Higher than every session before it.</param>
/// <param name="Confirmed">The end of the last transaction already in the outbox.</param>
/// <param name="LastHlc">The highest clock stamp already issued for this source.</param>
public sealed record CaptureCheckpoint(long Epoch, Lsn Confirmed, long LastHlc);

/// <summary>A replication session against one source, positioned by its slot.</summary>
public interface ISourceLog : IAsyncDisposable
{
    /// <summary>The host the session is attached to, for status and logs.</summary>
    string Host { get; }

    /// <summary>Streams committed transactions in commit order.</summary>
    /// <param name="after">The last position the store holds. The source may still resend earlier transactions.</param>
    /// <param name="cancellationToken">Stops the stream.</param>
    IAsyncEnumerable<DecodedTransaction> ReadAsync(Lsn after, CancellationToken cancellationToken);

    /// <summary>Tells the source everything up to <paramref name="persisted"/> is durable and need never be sent again.</summary>
    /// <param name="persisted">The end of the last transaction the outbox holds.</param>
    /// <param name="cancellationToken">Cancels sending the acknowledgement.</param>
    ValueTask AcknowledgeAsync(Lsn persisted, CancellationToken cancellationToken);
}

/// <summary>Opens replication sessions, finding the current primary each time.</summary>
public interface ISourceLogFactory
{
    /// <summary>Opens a session against the source's current primary.</summary>
    /// <param name="source">The configured source name.</param>
    /// <param name="cancellationToken">Cancels opening.</param>
    Task<ISourceLog> OpenAsync(string source, CancellationToken cancellationToken);
}

/// <summary>
/// The right to capture one source, held for as long as the session runs.
/// </summary>
/// <remarks>
/// Two things keep two capture sessions from writing one source's outbox at once, and they cover different
/// failures. The lease decides who may start: only one holder at a time, so a second instance of the service
/// waits as a standby instead of fighting for the slot. The epoch catches the case the lease cannot: a session
/// that lost its lease without noticing, because its connection to the store died, and is still finishing a
/// write. Its epoch is stale by then, and the write is refused.
/// </remarks>
public interface ICaptureLease : IAsyncDisposable
{
    /// <summary>Where this session resumes from, read after the lease was taken.</summary>
    CaptureCheckpoint Checkpoint { get; }
}

/// <summary>The durable side of capture: the outbox and the per-source checkpoint.</summary>
public interface ICaptureStore
{
    /// <summary>
    /// Takes the source's lease and starts a new epoch, or returns null when another session holds it.
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="cancellationToken">Cancels the call.</param>
    Task<ICaptureLease?> TryLeadAsync(string source, CancellationToken cancellationToken);

    /// <summary>
    /// Writes a batch of changes and moves the checkpoint in one transaction, provided the session still holds
    /// the latest epoch.
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="epoch">The session's epoch.</param>
    /// <param name="changes">The changes, in capture order.</param>
    /// <param name="confirmed">The end of the last transaction in the batch.</param>
    /// <param name="lastHlc">The highest stamp in the batch.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="CaptureFencedException">A newer session has started for this source.</exception>
    Task PersistAsync(
        string source,
        long epoch,
        IReadOnlyList<ChangeEvent> changes,
        Lsn confirmed,
        long lastHlc,
        CancellationToken cancellationToken);
}

/// <summary>Wakes dispatchers the moment a source has new changes, so an idle sink does not poll.</summary>
/// <remarks>
/// Versioned rather than a plain event. A reader takes the version, reads, finds nothing and waits for a
/// version newer than the one it took, so a notification that lands between its read and its wait still
/// wakes it. With a plain event that notification would be lost and the reader would sleep a full timeout
/// with a change sitting in the outbox.
/// </remarks>
public interface IOutboxSignal
{
    /// <summary>Records that the source has new changes.</summary>
    /// <param name="source">The source.</param>
    void Notify(string source);

    /// <summary>The source's current version, to pass to <see cref="WaitAsync"/> after a read.</summary>
    /// <param name="source">The source.</param>
    long Version(string source);

    /// <summary>Waits until the source moves past <paramref name="seen"/> or the timeout passes.</summary>
    /// <param name="source">The source.</param>
    /// <param name="seen">The version taken before the read that found nothing.</param>
    /// <param name="timeout">The longest wait.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    Task WaitAsync(string source, long seen, TimeSpan timeout, CancellationToken cancellationToken);
}

/// <summary>The in-process <see cref="IOutboxSignal"/>, for capture and dispatch running in one service.</summary>
public sealed class OutboxSignal : IOutboxSignal
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, (long Version, TaskCompletionSource Changed)> _sources = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public void Notify(string source)
    {
        TaskCompletionSource changed;

        lock (_gate)
        {
            (long version, changed) = Entry(source);
            _sources[source] = (version + 1, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        }

        changed.TrySetResult();
    }

    /// <inheritdoc />
    public long Version(string source)
    {
        lock (_gate)
        {
            return Entry(source).Version;
        }
    }

    /// <inheritdoc />
    public async Task WaitAsync(string source, long seen, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Task changed;

        lock (_gate)
        {
            (long version, TaskCompletionSource next) = Entry(source);

            if (version > seen)
            {
                return;
            }

            changed = next.Task;
        }

        await changed.WaitAsync(timeout, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        cancellationToken.ThrowIfCancellationRequested();
    }

    private (long Version, TaskCompletionSource Changed) Entry(string source)
    {
        if (!_sources.TryGetValue(source, out (long, TaskCompletionSource) entry))
        {
            entry = (0, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            _sources[source] = entry;
        }

        return entry;
    }
}

/// <summary>Sees each batch as it becomes durable, for the live feed and the counters.</summary>
public interface ICaptureObserver
{
    /// <summary>Called after a batch is persisted and acknowledged.</summary>
    /// <param name="source">The source.</param>
    /// <param name="changes">The changes.</param>
    /// <param name="transactions">How many source transactions the batch held, heartbeats included.</param>
    void Persisted(string source, IReadOnlyList<ChangeEvent> changes, int transactions);

    /// <summary>Called for a transaction the store already had, resent by the source after a restart.</summary>
    /// <param name="source">The source.</param>
    /// <param name="transaction">The transaction.</param>
    void SkippedDuplicate(string source, DecodedTransaction transaction);
}

/// <summary>A newer capture session has taken over the source.</summary>
public sealed class CaptureFencedException : Exception
{
    /// <summary>Creates the exception.</summary>
    public CaptureFencedException()
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">The message.</param>
    public CaptureFencedException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The cause.</param>
    public CaptureFencedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
