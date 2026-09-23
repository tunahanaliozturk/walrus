using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Walrus.Application.Capture;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Application.Sinks;

/// <summary>
/// Owns every sink's dispatcher: starts them, restarts them when they fail, and stops one while it is
/// registered, deleted, replayed or rebuilt from a snapshot.
/// </summary>
/// <remarks>
/// Operations on one sink are serialised and never overlap its dispatch, so a replay cannot race the dispatcher
/// it is repositioning. Operations on different sinks run independently: a snapshot of one sink taking ten
/// minutes does not hold up registering another.
/// </remarks>
public sealed partial class SinkSupervisor(
    ISinkCatalog catalog,
    ISinkWriterFactory writers,
    IOutboxReader outbox,
    ISinkCursorStore cursors,
    IDeadLetterStore deadLetters,
    ISnapshotSource snapshots,
    CaptureSources sources,
    IOutboxSignal signal,
    IDispatchObserver observer,
    DispatchOptions options,
    TimeProvider time,
    ILoggerFactory loggers) : IAsyncDisposable
{
    private static readonly TimeSpan MaxRestartDelay = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<string, RunningSink> _running = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new(StringComparer.Ordinal);
    private readonly TimeProvider _time = time;
    private readonly ILoggerFactory _loggers = loggers;
    private readonly ILogger _logger = loggers.CreateLogger<SinkSupervisor>();

    /// <summary>Starts every registered sink.</summary>
    /// <param name="cancellationToken">Cancels loading the catalog.</param>
    public async Task StartAllAsync(CancellationToken cancellationToken)
    {
        foreach (SinkDefinition sink in await catalog.ListAsync(cancellationToken))
        {
            ISinkWriter writer = await writers.CreateAsync(sink, validate: false, cancellationToken);
            Start(sink, writer);
        }
    }

    /// <summary>Registers a sink and starts it from the requested position.</summary>
    /// <param name="registration">The request.</param>
    /// <param name="cancellationToken">Cancels the registration.</param>
    public async Task<SinkOperationResult> RegisterAsync(SinkRegistration registration, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(registration);

        SinkDefinition sink = registration.Sink;
        List<string> problems = [.. sink.Problems()];

        if (registration.Start is SinkStartMode.Lsn && registration.StartLsn is null)
        {
            problems.Add("Starting from a log position needs the position.");
        }

        if (problems.Count > 0)
        {
            return SinkOperationResult.Invalid(problems);
        }

        SemaphoreSlim gate = Gate(sink.Id);
        await gate.WaitAsync(cancellationToken);

        try
        {
            ISinkWriter writer;

            try
            {
                // Creating the writer is where a sink's target is checked: a connection name that is not
                // configured, a webhook host that is not allowed. Better refused now than failing forever later.
                writer = await writers.CreateAsync(sink, validate: true, cancellationToken);
            }
            catch (ArgumentException invalid)
            {
                return SinkOperationResult.Invalid(invalid.Message);
            }

            if (registration.Start is SinkStartMode.Snapshot && !writer.IsDurable)
            {
                await writer.DisposeAsync();
                return SinkOperationResult.Invalid("A snapshot only makes sense for a sink that keeps its state across restarts.");
            }

            Dictionary<string, SinkCursor> start = new(StringComparer.Ordinal);

            foreach (string source in sources.Names)
            {
                start[source] = registration.Start switch
                {
                    SinkStartMode.Now => new SinkCursor(await outbox.HeadSeqAsync(source, cancellationToken), Lsn.Zero),
                    SinkStartMode.Lsn => await CursorAtAsync(source, registration.StartLsn!.Value, cancellationToken),
                    _ => SinkCursor.Beginning,
                };
            }

            if (!await catalog.AddAsync(sink, start, cancellationToken))
            {
                await writer.DisposeAsync();
                return SinkOperationResult.Conflict;
            }

            if (registration.Start is SinkStartMode.Snapshot)
            {
                await LoadSnapshotAsync(sink, writer, cancellationToken);
            }

            Start(sink, writer);
            LogRegistered(_logger, sink.Id, sink.Kind, registration.Start);

            return SinkOperationResult.Done;
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Stops and removes a sink, with its cursors and dead letters.</summary>
    /// <param name="id">The sink id.</param>
    /// <param name="cancellationToken">Cancels the removal.</param>
    public async Task<SinkOperationResult> DeleteAsync(string id, CancellationToken cancellationToken) =>
        await ExclusiveAsync(id, async (running, token) =>
        {
            await catalog.RemoveAsync(id, token);
            await running.Writer.DisposeAsync();

            return false;
        }, cancellationToken);

    /// <summary>
    /// Re-delivers a sink's changes from a log position, optionally clearing the sink first so it is rebuilt
    /// rather than re-applied.
    /// </summary>
    /// <param name="id">The sink id.</param>
    /// <param name="fromLsn">Where to start.</param>
    /// <param name="reset">Whether to empty the sink first.</param>
    /// <param name="cancellationToken">Cancels the repositioning. The replay itself runs in the background.</param>
    public async Task<SinkOperationResult> ReplayAsync(string id, Lsn fromLsn, bool reset, CancellationToken cancellationToken) =>
        await ExclusiveAsync(id, async (running, token) =>
        {
            if (reset)
            {
                await running.Writer.ResetAsync(token);
                await deadLetters.ClearAsync(id, token);
            }

            foreach (string source in sources.Names)
            {
                await cursors.SetAsync(id, source, await CursorAtAsync(source, fromLsn, token), token);
            }

            return true;
        }, cancellationToken);

    /// <summary>Rebuilds a sink from a consistent snapshot of the sources, then streams from the snapshot on.</summary>
    /// <param name="id">The sink id.</param>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    public async Task<SinkOperationResult> SnapshotAsync(string id, CancellationToken cancellationToken)
    {
        if (_running.TryGetValue(id, out RunningSink? existing) && !existing.Writer.IsDurable)
        {
            return SinkOperationResult.Invalid("A snapshot only makes sense for a sink that keeps its state across restarts.");
        }

        return await ExclusiveAsync(id, async (running, token) =>
        {
            await LoadSnapshotAsync(running.Sink, running.Writer, token);

            return true;
        }, cancellationToken);
    }

    /// <summary>Retries a sink's parked rows.</summary>
    /// <param name="id">The sink id.</param>
    /// <param name="cancellationToken">Cancels waiting for the retries.</param>
    public async Task<(int Resolved, int StillBlocked)?> RetryDeadLettersAsync(string id, CancellationToken cancellationToken) =>
        _running.TryGetValue(id, out RunningSink? running) && running.Dispatcher is { } dispatcher
            ? await dispatcher.RetryDeadLettersAsync(cancellationToken)
            : null;

    /// <summary>Every sink's status.</summary>
    /// <param name="cancellationToken">Cancels the queries.</param>
    public async Task<IReadOnlyList<SinkStatus>> StatusAsync(CancellationToken cancellationToken)
    {
        List<SinkStatus> statuses = [];

        foreach (RunningSink running in _running.Values.OrderBy(static running => running.Sink.Id, StringComparer.Ordinal))
        {
            statuses.Add(await StatusAsync(running, cancellationToken));
        }

        return statuses;
    }

    /// <summary>One sink's status, or null.</summary>
    /// <param name="id">The sink id.</param>
    /// <param name="cancellationToken">Cancels the queries.</param>
    public async Task<SinkStatus?> StatusAsync(string id, CancellationToken cancellationToken) =>
        _running.TryGetValue(id, out RunningSink? running) ? await StatusAsync(running, cancellationToken) : null;

    /// <summary>Stops every sink and releases its writer. Safe to call more than once.</summary>
    public async Task StopAllAsync()
    {
        foreach (string id in _running.Keys)
        {
            if (_running.TryRemove(id, out RunningSink? running))
            {
                await running.StopAsync();
                await running.Writer.DisposeAsync();
                running.Dispose();
            }
        }
    }

    /// <summary>Stops every sink.</summary>
    public async ValueTask DisposeAsync()
    {
        await StopAllAsync();

        foreach (SemaphoreSlim gate in _gates.Values)
        {
            gate.Dispose();
        }

        _gates.Clear();
    }

    private async Task<SinkStatus> StatusAsync(RunningSink running, CancellationToken cancellationToken)
    {
        SinkDispatcher? dispatcher = running.Dispatcher;
        IReadOnlyDictionary<string, long> marks = dispatcher?.Marks ?? new Dictionary<string, long>();
        List<SinkSourceStatus> perSource = [];
        DateTimeOffset now = _time.GetUtcNow();

        foreach (string source in sources.Names)
        {
            SinkCursor cursor = await cursors.GetAsync(running.Sink.Id, source, cancellationToken);
            long applied = marks.TryGetValue(source, out long mark) ? mark : cursor.LastSeq;
            OutboxPosition position = await outbox.PositionAsync(source, applied, cursor.StartLsn, running.Sink.Tables, cancellationToken);

            double lag = position.OldestPendingCommit is { } oldest
                ? Math.Max(0, (now - oldest).TotalMilliseconds)
                : 0;

            perSource.Add(new SinkSourceStatus(source, applied, position.HeadSeq, cursor.StartLsn, position.PendingLsn, lag));
        }

        SinkState state = running.Rebuilding ? SinkState.Rebuilding
            : dispatcher is null ? SinkState.Stopped
            : dispatcher.IsRetrying ? SinkState.Retrying
            : SinkState.Running;

        return new SinkStatus(running.Sink, state, dispatcher?.BlockedRows ?? 0, perSource);
    }

    private async Task<SinkCursor> CursorAtAsync(string source, Lsn lsn, CancellationToken cancellationToken) =>
        new(await outbox.LastSeqBeforeAsync(source, lsn, cancellationToken), lsn);

    private async Task LoadSnapshotAsync(SinkDefinition sink, ISinkWriter writer, CancellationToken cancellationToken)
    {
        await writer.ResetAsync(cancellationToken);
        await deadLetters.ClearAsync(sink.Id, cancellationToken);

        foreach (string source in sources.Names)
        {
            TransactionStamper stamper = sources.Stamper(source);
            long rows = 0;

            Lsn point = await snapshots.ExportAsync(
                source,
                sink.Tables,
                async (batch, token) =>
                {
                    await writer.ApplyAsync(stamper.StampSnapshot(batch), token);
                    rows += batch.Count;
                },
                cancellationToken);

            // The seam: the snapshot holds everything that committed before the point, and the stream delivers
            // everything from the point on.
            await cursors.SetAsync(sink.Id, source, await CursorAtAsync(source, point, cancellationToken), cancellationToken);
            LogSnapshot(_logger, sink.Id, source, rows, point);
        }
    }

    private async Task<SinkOperationResult> ExclusiveAsync(
        string id,
        Func<RunningSink, CancellationToken, Task<bool>> operation,
        CancellationToken cancellationToken)
    {
        SemaphoreSlim gate = Gate(id);
        await gate.WaitAsync(cancellationToken);

        try
        {
            if (!_running.TryGetValue(id, out RunningSink? running))
            {
                return SinkOperationResult.NotFound;
            }

            running.Rebuilding = true;
            await running.StopAsync();

            try
            {
                bool restart = await operation(running, cancellationToken);

                if (restart)
                {
                    running.Start();
                }
                else
                {
                    _running.TryRemove(id, out _);
                }
            }
            catch
            {
                // Whatever the operation left half done, the sink keeps dispatching from wherever its cursors
                // now say. An operator can run the operation again.
                running.Start();
                throw;
            }
            finally
            {
                running.Rebuilding = false;
            }

            return SinkOperationResult.Done;
        }
        finally
        {
            gate.Release();
        }
    }

    private SemaphoreSlim Gate(string id) => _gates.GetOrAdd(id, static _ => new SemaphoreSlim(1, 1));

    private void Start(SinkDefinition sink, ISinkWriter writer)
    {
        var running = new RunningSink(sink, writer, this);
        _running[sink.Id] = running;
        running.Start();
    }

    private SinkDispatcher CreateDispatcher(SinkDefinition sink, ISinkWriter writer) =>
        new(sink, sources.Names, writer, outbox, cursors, deadLetters, signal, observer, options, _time,
            _loggers.CreateLogger<SinkDispatcher>());

    [LoggerMessage(Level = LogLevel.Information, Message = "Registered {Kind} sink {Sink}, starting from {Start}.")]
    private static partial void LogRegistered(ILogger logger, string sink, SinkKind kind, SinkStartMode start);

    [LoggerMessage(Level = LogLevel.Information, Message = "Sink {Sink} loaded {Rows} rows from a snapshot of {Source} at {Point}.")]
    private static partial void LogSnapshot(ILogger logger, string sink, string source, long rows, Lsn point);

    [LoggerMessage(Level = LogLevel.Error, Message = "Dispatch for sink {Sink} failed; restarting in {DelaySeconds} s.")]
    private static partial void LogDispatchFailed(ILogger logger, Exception exception, string sink, double delaySeconds);

    private sealed class RunningSink(SinkDefinition sink, ISinkWriter writer, SinkSupervisor owner) : IDisposable
    {
        private CancellationTokenSource? _stop;
        private Task _loop = Task.CompletedTask;

        public SinkDefinition Sink { get; } = sink;

        public ISinkWriter Writer { get; } = writer;

        public SinkDispatcher? Dispatcher { get; private set; }

        public bool Rebuilding { get; set; }

        public void Dispose() => _stop?.Dispose();

        public void Start()
        {
            _stop = new CancellationTokenSource();
            _loop = RunAsync(_stop.Token);
        }

        public async Task StopAsync()
        {
            CancellationTokenSource? stop = Interlocked.Exchange(ref _stop, null);

            if (stop is null)
            {
                return;
            }

            await stop.CancelAsync();
            await _loop.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            stop.Dispose();
            Dispatcher = null;
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            TimeSpan delay = TimeSpan.FromSeconds(1);

            while (!cancellationToken.IsCancellationRequested)
            {
                SinkDispatcher dispatcher = owner.CreateDispatcher(Sink, Writer);
                Dispatcher = dispatcher;

                try
                {
                    await dispatcher.RunAsync(cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception exception)
                {
                    LogDispatchFailed(owner._logger, exception, Sink.Id, delay.TotalSeconds);
                }

                Dispatcher = null;
                await Task.Delay(delay, owner._time, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxRestartDelay.Ticks));
            }
        }
    }
}
