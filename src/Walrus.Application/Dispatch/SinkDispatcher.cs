using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Walrus.Application.Capture;
using Walrus.Domain;

namespace Walrus.Application.Dispatch;

/// <summary>Tuning for dispatch.</summary>
public sealed record DispatchOptions
{
    /// <summary>How many rows a sink applies in parallel. Changes to one row always share a lane.</summary>
    public int Lanes { get; init; } = 8;

    /// <summary>How many changes may queue in one lane before reading the outbox pauses.</summary>
    public int LaneCapacity { get; init; } = 2_000;

    /// <summary>The most changes a lane hands the sink in one apply.</summary>
    public int MaxBatch { get; init; } = 500;

    /// <summary>The most changes read from the outbox at once.</summary>
    public int ReadBatch { get; init; } = 2_000;

    /// <summary>How long an idle reader waits for a signal before looking anyway.</summary>
    public TimeSpan IdleWait { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>How often a durable sink's cursor is saved.</summary>
    public TimeSpan CursorFlushInterval { get; init; } = TimeSpan.FromMilliseconds(200);

    /// <summary>The first wait after a sink fails for a reason that may pass.</summary>
    public TimeSpan FirstRetryDelay { get; init; } = TimeSpan.FromMilliseconds(100);

    /// <summary>The longest wait between retries.</summary>
    public TimeSpan MaxRetryDelay { get; init; } = TimeSpan.FromSeconds(5);
}

/// <summary>
/// Delivers one sink's changes from the outbox: in order for each row, in parallel across rows, each change
/// applied or parked, never skipped.
/// </summary>
/// <remarks>
/// <para>
/// Every change to a row goes down the same lane, and a lane applies one batch at a time, so a row's changes
/// reach the sink in capture order. That is the ordering mechanism in full. Lanes run in parallel, which is where
/// the throughput comes from.
/// </para>
/// <para>
/// Two kinds of failure, handled differently on purpose. A sink that is down, slow or restarting is retried
/// forever with backoff: the changes are safe in the outbox, lag grows and alerts, and nothing is parked,
/// because parking every change while a database restarts would turn an outage into a manual cleanup. A batch
/// the sink refuses outright is split by row, the rows that apply are applied, and the row that cannot is
/// parked as a dead letter. Everything after it for that row is parked behind it, since applying a later change
/// over a missing earlier one would be exactly the reordering this service exists to prevent. Other rows keep
/// flowing.
/// </para>
/// </remarks>
public sealed partial class SinkDispatcher(
    SinkDefinition sink,
    IReadOnlyList<string> sources,
    ISinkWriter writer,
    IOutboxReader outbox,
    ISinkCursorStore cursors,
    IDeadLetterStore deadLetters,
    IOutboxSignal signal,
    IDispatchObserver observer,
    DispatchOptions options,
    TimeProvider time,
    ILogger<SinkDispatcher> logger)
{
    private const string QueuedBehind = "Queued behind an earlier dead letter for this row.";

    private readonly ConcurrentDictionary<string, byte> _blocked = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SourceProgress> _progress = new(StringComparer.Ordinal);
    private volatile Channel<Work>[]? _lanes;
    private volatile bool _retrying;

    /// <summary>The sink.</summary>
    public SinkDefinition Sink => sink;

    /// <summary>Whether the last apply failed and is waiting to be retried.</summary>
    public bool IsRetrying => _retrying;

    /// <summary>How many rows are held behind a dead letter.</summary>
    public int BlockedRows => _blocked.Count;

    /// <summary>For each source, the position below which everything is applied or parked.</summary>
    public IReadOnlyDictionary<string, long> Marks =>
        _progress.ToDictionary(pair => pair.Key, pair => pair.Value.Watermark.Mark, StringComparer.Ordinal);

    /// <summary>Runs until cancelled or until reading the outbox or a store fails.</summary>
    /// <param name="cancellationToken">Stops dispatch.</param>
    public async Task RunAsync(CancellationToken cancellationToken)
    {
        foreach (string key in await deadLetters.BlockedKeysAsync(sink.Id, cancellationToken))
        {
            _blocked[key] = 0;
        }

        Channel<Work>[] lanes = [.. Enumerable.Range(0, options.Lanes).Select(_ => Channel.CreateBounded<Work>(
            new BoundedChannelOptions(options.LaneCapacity)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
            }))];

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        CancellationToken token = linked.Token;
        List<Task> loops = [];

        foreach (string source in sources)
        {
            _progress[source] = await StartAsync(source, token);
        }

        _lanes = lanes;

        foreach (Channel<Work> lane in lanes)
        {
            loops.Add(RunLaneAsync(lane.Reader, token));
        }

        foreach (SourceProgress progress in _progress.Values)
        {
            loops.Add(FeedAsync(progress, lanes, token));
        }

        if (writer.IsDurable)
        {
            loops.Add(FlushLoopAsync(token));
        }

        Task first = await Task.WhenAny(loops);
        await linked.CancelAsync();
        await Task.WhenAll(loops).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        _lanes = null;

        if (writer.IsDurable)
        {
            // Whatever finished before the stop is kept, so a restart repeats as little as possible.
            await FlushAsync(CancellationToken.None);
        }

        cancellationToken.ThrowIfCancellationRequested();

        // Not cancelled from outside, so a loop failed. Surface its exception.
        await first;

        throw new InvalidOperationException($"A dispatch loop for sink '{sink.Id}' stopped without an error.");
    }

    /// <summary>
    /// Retries every parked row in its lane, oldest letter first. A row that applies is unblocked; one that is
    /// refused again stays parked with the attempt recorded.
    /// </summary>
    /// <param name="cancellationToken">Cancels waiting for the retries.</param>
    /// <returns>How many rows were unblocked, and how many are still parked.</returns>
    public async Task<(int Resolved, int StillBlocked)> RetryDeadLettersAsync(CancellationToken cancellationToken)
    {
        Channel<Work>[] lanes = _lanes
            ?? throw new InvalidOperationException($"Sink '{sink.Id}' is not running.");

        List<Task<bool>> retries = [];

        foreach (string key in _blocked.Keys)
        {
            var retry = new RetryLetters(key, new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
            await lanes[LaneRouter.LaneFor(key, lanes.Length)].Writer.WriteAsync(retry, cancellationToken);
            retries.Add(retry.Done.Task);
        }

        bool[] results = await Task.WhenAll(retries).WaitAsync(cancellationToken);

        return (results.Count(static resolved => resolved), results.Count(static resolved => !resolved));
    }

    private async Task<SourceProgress> StartAsync(string source, CancellationToken cancellationToken)
    {
        SinkCursor cursor = await cursors.GetAsync(sink.Id, source, cancellationToken);

        // A volatile sink holds nothing after a restart, so it starts over from where it was first positioned,
        // not from how far it had got.
        long start = writer.IsDurable
            ? cursor.LastSeq
            : await outbox.LastSeqBeforeAsync(source, cursor.StartLsn, cancellationToken);

        return new SourceProgress(source, cursor.StartLsn, new ContiguousWatermark(start), start);
    }

    private async Task FeedAsync(SourceProgress progress, Channel<Work>[] lanes, CancellationToken cancellationToken)
    {
        long after = progress.Watermark.Mark;

        while (true)
        {
            long version = signal.Version(progress.Source);
            IReadOnlyList<OutboxEntry> entries = await outbox.ReadAsync(
                progress.Source, after, progress.StartLsn, sink.Tables, options.ReadBatch, cancellationToken);

            if (entries.Count == 0)
            {
                await signal.WaitAsync(progress.Source, version, options.IdleWait, cancellationToken);
                continue;
            }

            foreach (OutboxEntry entry in entries)
            {
                progress.Watermark.Dispatched(entry.Seq);

                Channel<Work> lane = lanes[LaneRouter.LaneFor(entry.Change.EntityKey, lanes.Length)];
                await lane.Writer.WriteAsync(new Deliver(progress, entry), cancellationToken);
            }

            after = entries[^1].Seq;
        }
    }

    private async Task RunLaneAsync(ChannelReader<Work> reader, CancellationToken cancellationToken)
    {
        List<Deliver> batch = new(options.MaxBatch);

        while (await reader.WaitToReadAsync(cancellationToken))
        {
            while (batch.Count < options.MaxBatch && reader.TryPeek(out Work? next))
            {
                if (next is RetryLetters retry)
                {
                    // Deliver what came before the retry first, so the row's order holds.
                    if (batch.Count > 0)
                    {
                        break;
                    }

                    reader.TryRead(out _);
                    await RetryLettersAsync(retry, cancellationToken);
                    continue;
                }

                reader.TryRead(out _);
                batch.Add((Deliver)next);
            }

            if (batch.Count > 0)
            {
                await DeliverAsync(batch, cancellationToken);
                batch.Clear();
            }
        }
    }

    private async Task DeliverAsync(List<Deliver> batch, CancellationToken cancellationToken)
    {
        List<Deliver> queued = [];
        List<Deliver> ready = new(batch.Count);

        foreach (Deliver delivery in batch)
        {
            (_blocked.ContainsKey(delivery.Entry.Change.EntityKey) ? queued : ready).Add(delivery);
        }

        if (queued.Count > 0)
        {
            await ParkAsync(queued, QueuedBehind, cancellationToken);
        }

        if (ready.Count == 0)
        {
            return;
        }

        try
        {
            IReadOnlyList<ChangeOutcome> outcomes = await ApplyWithRetryAsync(ready, cancellationToken);
            Report(ready, outcomes);
            Finish(ready);
        }
        catch (SinkRejectedException)
        {
            // Something in the batch can never apply. Find out which row by applying each row on its own.
            foreach (List<Deliver> row in ready
                .GroupBy(static delivery => delivery.Entry.Change.EntityKey, StringComparer.Ordinal)
                .Select(static group => group.ToList()))
            {
                await ApplyRowAsync(row, cancellationToken);
            }
        }
    }

    private async Task ApplyRowAsync(List<Deliver> row, CancellationToken cancellationToken)
    {
        try
        {
            IReadOnlyList<ChangeOutcome> outcomes = await ApplyWithRetryAsync(row, cancellationToken);
            Report(row, outcomes);
            Finish(row);
            return;
        }
        catch (SinkRejectedException) when (row.Count > 1)
        {
            // Several changes to this row, and one of them is refused. Apply them one at a time, so the changes
            // before the refused one still land and only it and what follows it are parked.
        }
        catch (SinkRejectedException rejected)
        {
            LogParked(logger, sink.Id, row[0].Entry.Change.EntityKey, rejected.Message);
            await ParkAsync(row, rejected.Message, cancellationToken);
            return;
        }

        for (int index = 0; index < row.Count; index++)
        {
            List<Deliver> single = [row[index]];

            try
            {
                IReadOnlyList<ChangeOutcome> outcomes = await ApplyWithRetryAsync(single, cancellationToken);
                Report(single, outcomes);
                Finish(single);
            }
            catch (SinkRejectedException rejected)
            {
                LogParked(logger, sink.Id, row[index].Entry.Change.EntityKey, rejected.Message);
                await ParkAsync(single, rejected.Message, cancellationToken);

                if (index + 1 < row.Count)
                {
                    await ParkAsync(row[(index + 1)..], QueuedBehind, cancellationToken);
                }

                return;
            }
        }
    }

    private async Task<IReadOnlyList<ChangeOutcome>> ApplyWithRetryAsync(List<Deliver> deliveries, CancellationToken cancellationToken)
    {
        ChangeEvent[] changes = [.. deliveries.Select(static delivery => delivery.Entry.Change)];
        TimeSpan delay = options.FirstRetryDelay;

        while (true)
        {
            try
            {
                IReadOnlyList<ChangeOutcome> outcomes = await writer.ApplyAsync(changes, cancellationToken);
                _retrying = false;

                return outcomes;
            }
            catch (Exception exception) when (exception is not SinkRejectedException && !cancellationToken.IsCancellationRequested)
            {
                _retrying = true;
                observer.Retrying(sink.Id, exception, delay);
                LogRetrying(logger, sink.Id, delay.TotalMilliseconds, exception.Message);

                await Task.Delay(delay, time, cancellationToken);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, options.MaxRetryDelay.Ticks));
            }
        }
    }

    private async Task ParkAsync(List<Deliver> deliveries, string error, CancellationToken cancellationToken)
    {
        DeadLetterEntry[] entries = [.. deliveries.Select(delivery =>
            new DeadLetterEntry(delivery.Progress.Source, delivery.Entry.Seq, delivery.Entry.Change, error))];

        // Durable before the positions count as finished, or a restart could skip a change nobody parked.
        await deadLetters.AddAsync(sink.Id, entries, cancellationToken);

        foreach (Deliver delivery in deliveries)
        {
            _blocked[delivery.Entry.Change.EntityKey] = 0;
        }

        observer.DeadLettered(sink.Id, entries);
        Finish(deliveries);
    }

    private async Task RetryLettersAsync(RetryLetters retry, CancellationToken cancellationToken)
    {
        try
        {
            // Read here, in the lane, rather than when the retry was requested: anything parked for this row by
            // batches ahead of the retry in the lane is then included, and nothing behind it can overtake.
            IReadOnlyList<DeadLetter> letters = await deadLetters.OpenAsync(sink.Id, retry.EntityKey, 100_000, cancellationToken);
            long[] ids = [.. letters.Select(static letter => letter.Id)];

            if (letters.Count == 0)
            {
                _blocked.TryRemove(retry.EntityKey, out _);
                retry.Done.TrySetResult(true);
                return;
            }

            ChangeEvent[] changes = [.. letters.Select(static letter => letter.Change)];

            try
            {
                IReadOnlyList<ChangeOutcome> outcomes = await writer.ApplyAsync(changes, cancellationToken);
                await deadLetters.ResolveAsync(ids, cancellationToken);
                _blocked.TryRemove(retry.EntityKey, out _);
                observer.Applied(sink.Id, changes, outcomes);
                retry.Done.TrySetResult(true);
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                await deadLetters.RecordAttemptAsync(ids, exception.Message, cancellationToken);
                retry.Done.TrySetResult(false);
            }
        }
        catch (Exception exception)
        {
            retry.Done.TrySetException(exception);
            throw;
        }
    }

    private void Report(List<Deliver> deliveries, IReadOnlyList<ChangeOutcome> outcomes)
    {
        ChangeEvent[] changes = [.. deliveries.Select(static delivery => delivery.Entry.Change)];
        observer.Applied(sink.Id, changes, outcomes);

        for (int index = 0; index < changes.Length; index++)
        {
            ChangeOutcome outcome = outcomes[index];

            if (!outcome.Changed
                && outcome.Winner is { } winner
                && !string.Equals(winner.Source, changes[index].Source, StringComparison.Ordinal))
            {
                observer.ConflictResolved(sink.Id, changes[index], winner);
            }
        }
    }

    private static void Finish(List<Deliver> deliveries)
    {
        foreach (Deliver delivery in deliveries)
        {
            delivery.Progress.Watermark.Finished(delivery.Entry.Seq);
        }
    }

    private async Task FlushLoopAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(options.CursorFlushInterval, time);

        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            await FlushAsync(cancellationToken);
        }
    }

    private async Task FlushAsync(CancellationToken cancellationToken)
    {
        foreach (SourceProgress progress in _progress.Values)
        {
            long mark = progress.Watermark.Mark;

            if (mark > progress.Saved)
            {
                await cursors.AdvanceAsync(sink.Id, progress.Source, mark, cancellationToken);
                progress.Saved = mark;
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Sink {Sink} failed and will retry in {DelayMilliseconds} ms: {Error}")]
    private static partial void LogRetrying(ILogger logger, string sink, double delayMilliseconds, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Sink {Sink} refused row {EntityKey}; it is parked until retried: {Error}")]
    private static partial void LogParked(ILogger logger, string sink, string entityKey, string error);

    private sealed class SourceProgress(string source, Lsn startLsn, ContiguousWatermark watermark, long saved)
    {
        public string Source { get; } = source;

        public Lsn StartLsn { get; } = startLsn;

        public ContiguousWatermark Watermark { get; } = watermark;

        public long Saved { get; set; } = saved;
    }

    private abstract record Work;

    private sealed record Deliver(SourceProgress Progress, OutboxEntry Entry) : Work;

    private sealed record RetryLetters(string EntityKey, TaskCompletionSource<bool> Done) : Work;
}
