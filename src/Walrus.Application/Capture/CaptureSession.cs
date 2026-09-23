using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Walrus.Domain;

namespace Walrus.Application.Capture;

/// <summary>Tuning for a capture session.</summary>
/// <param name="MaxBatchChanges">
/// How many changes one outbox transaction takes before the rest wait for the next one. Whole source
/// transactions are never split, so a batch can exceed this by one transaction.
/// </param>
/// <param name="PendingTransactions">
/// How many decoded transactions may wait for the outbox. When full, reading from the source stops, and the
/// source holds the rest in its own log, which is where they are safest.
/// </param>
public sealed record CaptureOptions(int MaxBatchChanges = 2_000, int PendingTransactions = 10_000);

/// <summary>
/// Moves one source's committed transactions into the outbox, acknowledging each only once it is durable.
/// </summary>
/// <remarks>
/// <para>
/// The order of two lines in <see cref="PersistAsync"/> is the whole durability argument: the outbox commit,
/// then the acknowledgement. The source keeps every transaction after the last acknowledged position and sends
/// it again to the next session, so a crash between the two lines costs a resend, never a change. Swap them and
/// a crash in between loses a transaction that nothing will ever send again.
/// </para>
/// <para>
/// A resent transaction is recognised by position. One session writes a source at a time and writes whole
/// transactions in commit order, so everything the outbox holds ends at or before the checkpoint, and anything
/// that ends there or earlier is a transaction it already has.
/// </para>
/// <para>
/// There is no linger. The persister takes whatever has queued while the previous write was committing, so an
/// idle source gets one small commit per transaction, fast, and a busy one gets group commit for free.
/// </para>
/// </remarks>
public sealed partial class CaptureSession(
    string source,
    ICaptureStore store,
    ISourceLogFactory logs,
    TransactionStamper stamper,
    IOutboxSignal signal,
    ICaptureObserver observer,
    CaptureOptions options,
    ILogger<CaptureSession> logger)
{
    /// <summary>Runs until cancelled or until the source or the store fails.</summary>
    /// <param name="cancellationToken">Stops the session.</param>
    /// <returns>True when the session ran and was stopped; false when another session holds the lease.</returns>
    public async Task<bool> RunAsync(CancellationToken cancellationToken)
    {
        await using ICaptureLease? lease = await store.TryLeadAsync(source, cancellationToken);

        if (lease is null)
        {
            return false;
        }

        CaptureCheckpoint checkpoint = lease.Checkpoint;
        await using ISourceLog log = await logs.OpenAsync(source, cancellationToken);

        LogStarted(logger, source, log.Host, checkpoint.Epoch, checkpoint.Confirmed);

        Channel<DecodedTransaction> pending = Channel.CreateBounded<DecodedTransaction>(
            new BoundedChannelOptions(options.PendingTransactions)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
            });

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task reading = ReadAsync(log, checkpoint.Confirmed, pending.Writer, linked.Token);

        try
        {
            await PersistAsync(log, checkpoint, pending.Reader, linked.Token);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Stopped from outside, which is how a session is meant to end.
        }
        finally
        {
            await linked.CancelAsync();

            // The reader has either finished, which is how the persister got here, or is being cancelled. Its
            // own failure already reached the persister through the channel.
            await reading.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        return true;
    }

    private async Task ReadAsync(
        ISourceLog log,
        Lsn alreadyStored,
        ChannelWriter<DecodedTransaction> writer,
        CancellationToken cancellationToken)
    {
        try
        {
            await foreach (DecodedTransaction transaction in log.ReadAsync(alreadyStored, cancellationToken))
            {
                if (transaction.EndLsn <= alreadyStored)
                {
                    observer.SkippedDuplicate(source, transaction);
                    continue;
                }

                await writer.WriteAsync(transaction, cancellationToken);
            }

            writer.Complete();
        }
        catch (Exception exception)
        {
            writer.Complete(exception);
        }
    }

    private async Task PersistAsync(
        ISourceLog log,
        CaptureCheckpoint checkpoint,
        ChannelReader<DecodedTransaction> reader,
        CancellationToken cancellationToken)
    {
        var clock = new HybridLogicalClock(checkpoint.LastHlc);

        while (await reader.WaitToReadAsync(cancellationToken))
        {
            List<ChangeEvent> batch = [];
            Lsn end = Lsn.Zero;
            int transactions = 0;

            while (batch.Count < options.MaxBatchChanges && reader.TryRead(out DecodedTransaction? transaction))
            {
                batch.AddRange(stamper.Stamp(transaction, clock));
                end = transaction.EndLsn;
                transactions++;
            }

            // Durable first.
            await store.PersistAsync(source, checkpoint.Epoch, batch, end, clock.Last, cancellationToken);

            // Only then may the source forget it.
            await log.AcknowledgeAsync(end, cancellationToken);

            if (batch.Count > 0)
            {
                signal.Notify(source);
            }

            observer.Persisted(source, batch, transactions);
        }

        // WaitToReadAsync returned false: the reader completed the channel. Rethrow the reason it stopped, or
        // report that the source ended the stream, which a replication stream should never do on its own.
        await reader.Completion;

        throw new InvalidOperationException($"The replication stream for source '{source}' ended.");
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Capturing {Source} from {Host}, epoch {Epoch}, resuming after {Confirmed}.")]
    private static partial void LogStarted(ILogger logger, string source, string host, long epoch, Lsn confirmed);
}
