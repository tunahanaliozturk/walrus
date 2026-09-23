using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using Walrus.Application.Capture;
using Walrus.Application.Sinks;
using Walrus.Infrastructure.Source;
using Walrus.Infrastructure.Store;
using Walrus.Infrastructure.Telemetry;

namespace Walrus.Infrastructure.Hosting;

/// <summary>Brings the store schema up to date before anything else starts.</summary>
/// <param name="contexts">Creates contexts.</param>
internal sealed class StoreMigrator(IDbContextFactory<WalrusDbContext> contexts) : IHostedService
{
    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        await db.Database.MigrateAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>Runs one capture session per source, forever, restarting each after a failure.</summary>
/// <remarks>
/// A failover shows up here as a session failing: the replication connection drops, the next session finds the
/// new primary, attaches to its copy of the slot and resumes from the outbox's checkpoint. Nothing in this loop
/// knows a failover happened, which is the property worth having.
/// </remarks>
internal sealed partial class CaptureService(
    IOptions<WalrusOptions> options,
    ICaptureStore store,
    ISourceLogFactory logs,
    CaptureSources sources,
    IOutboxSignal signal,
    ICaptureObserver observer,
    PipelineCounters counters,
    TimeProvider time,
    ILoggerFactory loggers) : BackgroundService
{
    // Short on purpose. A failover makes a few attempts fail in a row while the standby is promoted, and every
    // second of backoff after the new primary is ready is a second of lag added for nothing. One connection attempt
    // a second to a source that is down for longer costs nothing worth saving.
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Healthy = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan WatchInterval = TimeSpan.FromSeconds(1);

    private readonly ILogger _logger = loggers.CreateLogger<CaptureService>();

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(sources.Names.Select(source => CaptureAsync(source, stoppingToken)));

    private async Task CaptureAsync(string source, CancellationToken stoppingToken)
    {
        var captureOptions = new CaptureOptions(options.Value.Capture.MaxBatchChanges, options.Value.Capture.PendingTransactions);
        SourceOptions configured = options.Value.Sources.First(candidate => string.Equals(candidate.Name, source, StringComparison.Ordinal));
        SourceCounters status = counters.Source(source);
        TimeSpan delay = TimeSpan.FromMilliseconds(250);

        while (!stoppingToken.IsCancellationRequested)
        {
            var session = new CaptureSession(
                source, store, logs, sources.Stamper(source), signal, observer, captureOptions,
                loggers.CreateLogger<CaptureSession>());

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            Task watching = WatchForPromotionAsync(configured, status, stop);
            DateTimeOffset started = time.GetUtcNow();
            bool superseded = false;

            try
            {
                if (!await session.RunAsync(stop.Token))
                {
                    status.LastError = "Another instance holds the capture lease; waiting as a standby.";
                }

                superseded = stop.IsCancellationRequested && !stoppingToken.IsCancellationRequested;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception)
            {
                status.LastError = exception.Message;
                LogSessionFailed(_logger, exception, source);
            }
            finally
            {
                status.Host = null;
                await stop.CancelAsync();
                await watching.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }

            // A session ended because another node was promoted, or one that ran for a while, deserves a quick retry.
            // One that keeps failing at once backs off, so a source that is down is not hammered.
            delay = superseded || time.GetUtcNow() - started > Healthy
                ? TimeSpan.FromMilliseconds(250)
                : TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxDelay.Ticks));

            await Task.Delay(delay, time, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    /// <summary>
    /// Ends a session as soon as another node of its source answers as primary.
    /// </summary>
    /// <remarks>
    /// A primary that dies with its host sends nothing to say so, and the replication connection only fails when
    /// something is next written to it and a reply never comes, which can take a minute. A promoted standby, on the
    /// other hand, says so at once. So the watch asks every node which one is primary, and when it is not the node the
    /// session is attached to, the session is over: it cannot receive another change from a node that has been
    /// replaced. Measured on the failover runs, this is the difference between capture resuming after about a second
    /// and after the next status update happened to hit a dead connection.
    /// </remarks>
    private async Task WatchForPromotionAsync(SourceOptions source, SourceCounters status, CancellationTokenSource session)
    {
        while (!session.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(WatchInterval, time, session.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (status.Host is not { } attached || source.Hosts.Count < 2)
            {
                continue;
            }

            try
            {
                string primary = await SourceConnections.FindPrimaryAsync(source, session.Token);

                if (!string.Equals(primary, attached, StringComparison.Ordinal))
                {
                    LogPromotion(_logger, source.Name, attached, primary);
                    await session.CancelAsync();
                    return;
                }
            }
            catch (Exception exception) when (exception is SourceUnavailableException or NpgsqlException or TimeoutException)
            {
                // No node answers as primary right now, typically mid-promotion. Keep watching.
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Source {Source} has a new primary, {Primary}; ending the session on {Attached}.")]
    private static partial void LogPromotion(ILogger logger, string source, string attached, string primary);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Capture session for {Source} ended; reconnecting.")]
    private static partial void LogSessionFailed(ILogger logger, Exception exception, string source);
}

/// <summary>Starts every registered sink after the store is migrated, and stops them on shutdown.</summary>
/// <param name="supervisor">The sinks.</param>
internal sealed class DispatchService(SinkSupervisor supervisor) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => supervisor.StartAllAsync(cancellationToken);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => supervisor.StopAllAsync();
}

/// <summary>
/// Writes each source's heartbeat row, so a slot always has a recent transaction to acknowledge.
/// </summary>
/// <remarks>
/// Without it, a source whose captured tables are quiet while other tables are busy never sends capture a
/// transaction, never gets an acknowledgement, and keeps every byte of log written since. That is the classic
/// way a logical slot fills a disk, and a heartbeat every few seconds is the classic fix.
/// </remarks>
internal sealed partial class HeartbeatService(
    IOptions<WalrusOptions> options,
    TimeProvider time,
    ILogger<HeartbeatService> logger) : BackgroundService
{
    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        Task.WhenAll(options.Value.Sources.Select(source => BeatAsync(source, stoppingToken)));

    private async Task BeatAsync(SourceOptions source, CancellationToken stoppingToken)
    {
        int dot = source.HeartbeatTable.IndexOf('.', StringComparison.Ordinal);
        string table = $"\"{source.HeartbeatTable[..dot]}\".\"{source.HeartbeatTable[(dot + 1)..]}\"";
        string upsert = $"insert into {table} (source, beat_at) values ($1, now()) on conflict (source) do update set beat_at = excluded.beat_at";

        using var timer = new PeriodicTimer(source.HeartbeatInterval, time);

        while (await Ticks.NextAsync(timer, stoppingToken))
        {
            try
            {
                string primary = await SourceConnections.FindPrimaryAsync(source, stoppingToken);
                await using var connection = new NpgsqlConnection(SourceConnections.ConnectionString(source, primary, pooled: false));
                await connection.OpenAsync(stoppingToken);
                await using var beat = new NpgsqlCommand(upsert, connection);
                beat.Parameters.Add(new NpgsqlParameter { Value = source.Name });
                await beat.ExecuteNonQueryAsync(stoppingToken);
            }
            catch (Exception exception) when (exception is NpgsqlException or SourceUnavailableException or TimeoutException)
            {
                LogBeatFailed(logger, source.Name, exception.Message);
            }
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Heartbeat for {Source} failed: {Error}")]
    private static partial void LogBeatFailed(ILogger logger, string source, string error);
}

/// <summary>Reads every source's slot health every few seconds.</summary>
/// <param name="options">The sources.</param>
/// <param name="monitor">Holds the readings.</param>
/// <param name="time">The clock.</param>
internal sealed class SourceMonitorService(IOptions<WalrusOptions> options, SourceMonitor monitor, TimeProvider time) : BackgroundService
{
    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5), time);

        do
        {
            foreach (SourceOptions source in options.Value.Sources)
            {
                await monitor.CheckAsync(source.Name, stoppingToken);
            }
        }
        while (await Ticks.NextAsync(timer, stoppingToken));
    }
}

/// <summary>Deletes outbox rows every durable sink has applied, once they are older than the retention.</summary>
/// <remarks>
/// Both conditions, not either. Age alone would delete changes a sink that has been down for a week still needs.
/// Cursors alone would delete history the moment every sink had it, and replay from an earlier position is the
/// reason the outbox keeps anything at all. Index sinks are left out of the cursor condition: they rebuild from
/// the outbox on every start and would otherwise pin it forever.
/// </remarks>
internal sealed partial class OutboxPruner(
    IOptions<WalrusOptions> options,
    IDbContextFactory<WalrusDbContext> contexts,
    TimeProvider time,
    ILogger<OutboxPruner> logger) : BackgroundService
{
    private const int BatchSize = 10_000;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMinutes(10), time);

        while (await Ticks.NextAsync(timer, stoppingToken))
        {
            foreach (SourceOptions source in options.Value.Sources)
            {
                try
                {
                    await PruneAsync(source.Name, stoppingToken);
                }
                catch (Exception exception) when (exception is NpgsqlException or DbUpdateException)
                {
                    LogPruneFailed(logger, exception, source.Name);
                }
            }
        }
    }

    private async Task PruneAsync(string source, CancellationToken cancellationToken)
    {
        DateTimeOffset cutoff = time.GetUtcNow() - options.Value.OutboxRetention;
        int deleted;

        do
        {
            await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

            deleted = await db.Database.ExecuteSqlAsync(
                $"""
                delete from walrus_outbox
                where seq in (
                    select seq from walrus_outbox
                    where source = {source}
                      and captured_at < {cutoff}
                      and seq <= coalesce((
                          select min(c.last_seq)
                          from walrus_sink_cursors c
                          join walrus_sinks s on s.id = c.sink_id
                          where c.source = {source} and s.kind <> 'index'), 9223372036854775807)
                    order by seq
                    limit {BatchSize})
                """,
                cancellationToken);
        }
        while (deleted == BatchSize && !cancellationToken.IsCancellationRequested);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Pruning the outbox for {Source} failed; will try again.")]
    private static partial void LogPruneFailed(ILogger logger, Exception exception, string source);
}

/// <summary>Waits for a timer's next tick, treating shutdown as the end of the ticks rather than an error.</summary>
internal static class Ticks
{
    /// <summary>True on a tick, false once the service is stopping.</summary>
    /// <param name="timer">The timer.</param>
    /// <param name="stoppingToken">Signals shutdown.</param>
    public static async Task<bool> NextAsync(PeriodicTimer timer, CancellationToken stoppingToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return false;
        }
    }
}
