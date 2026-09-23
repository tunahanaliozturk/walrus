using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace Walrus.Infrastructure.Source;

/// <summary>A standby's copy of the capture slot.</summary>
/// <param name="Host">The standby.</param>
/// <param name="Synced">Whether it holds a synchronised copy of the slot, and so could take over without losing position.</param>
/// <param name="ConfirmedFlushLsn">How far its copy has been confirmed.</param>
public sealed record StandbySlot(string Host, bool Synced, string? ConfirmedFlushLsn);

/// <summary>The health of one source as last checked.</summary>
/// <param name="Source">The source.</param>
/// <param name="Primary">The node that was primary, or null when none answered as one.</param>
/// <param name="WalRetainedBytes">
/// How much log the source keeps because of the capture slot. This is the number that fills a source's disk when
/// capture stops, and the one to alert on.
/// </param>
/// <param name="ConfirmedFlushLsn">The slot's acknowledged position.</param>
/// <param name="SlotActive">Whether a session is attached to the slot.</param>
/// <param name="Standbys">Each standby's copy of the slot.</param>
/// <param name="CaptureIsSynchronous">
/// Whether the source counts capture's own connection as a synchronous standby, so that every commit waits for
/// Walrus to store it. That happens when <c>synchronous_standby_names</c> is <c>*</c>, and it is almost never meant.
/// </param>
/// <param name="CheckedAt">When this was read.</param>
/// <param name="Error">What went wrong reading it, if anything.</param>
public sealed record SourceHealth(
    string Source,
    string? Primary,
    long? WalRetainedBytes,
    string? ConfirmedFlushLsn,
    bool SlotActive,
    IReadOnlyList<StandbySlot> Standbys,
    bool CaptureIsSynchronous,
    DateTimeOffset CheckedAt,
    string? Error);

/// <summary>Reads each source's slot from every node, for the status page, the metrics and the alerts.</summary>
/// <param name="connections">The configured sources.</param>
/// <param name="time">The clock.</param>
/// <param name="logger">Where a synchronous capture connection is reported.</param>
public sealed partial class SourceMonitor(SourceConnections connections, TimeProvider time, ILogger<SourceMonitor> logger)
{
    private readonly ConcurrentDictionary<string, SourceHealth> _health = new(StringComparer.Ordinal);

    /// <summary>The latest reading for every source.</summary>
    public IReadOnlyList<SourceHealth> All() => [.. _health.Values.OrderBy(static health => health.Source, StringComparer.Ordinal)];

    /// <summary>Reads one source now.</summary>
    /// <param name="source">The source.</param>
    /// <param name="cancellationToken">Cancels the reading.</param>
    public async Task<SourceHealth> CheckAsync(string source, CancellationToken cancellationToken)
    {
        SourceOptions options = connections.Get(source);
        SourceHealth health;

        try
        {
            string primary = await SourceConnections.FindPrimaryAsync(options, cancellationToken);
            (long? retained, string? confirmed, bool active, bool synchronous) = await ReadPrimaryAsync(options, primary, cancellationToken);
            List<StandbySlot> standbys = [];

            foreach (string node in options.Hosts.Where(node => !string.Equals(node, primary, StringComparison.Ordinal)))
            {
                standbys.Add(await ReadStandbyAsync(options, node, cancellationToken));
            }

            health = new SourceHealth(source, primary, retained, confirmed, active, standbys, synchronous, time.GetUtcNow(), null);

            if (synchronous && !(_health.TryGetValue(source, out SourceHealth? previous) && previous.CaptureIsSynchronous))
            {
                LogSynchronousCapture(logger, source, primary);
            }
        }
        catch (Exception exception) when (exception is NpgsqlException or SourceUnavailableException or TimeoutException)
        {
            health = new SourceHealth(source, null, null, null, false, [], false, time.GetUtcNow(), exception.Message);
        }

        _health[source] = health;

        return health;
    }

    private static async Task<(long? Retained, string? Confirmed, bool Active, bool Synchronous)> ReadPrimaryAsync(
        SourceOptions options,
        string node,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(SourceConnections.ConnectionString(options, node, pooled: false));
        await connection.OpenAsync(cancellationToken);
        await using var slot = new NpgsqlCommand(
            """
            select pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, confirmed_flush_lsn, active,
                   current_setting('synchronous_standby_names')
            from pg_replication_slots
            where slot_name = $1
            """,
            connection);

        slot.Parameters.Add(new NpgsqlParameter { Value = options.SlotName });
        await using NpgsqlDataReader reader = await slot.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, null, false, false);
        }

        return (
            reader.IsDBNull(0) ? null : reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<NpgsqlLogSequenceNumber>(1).ToString(),
            reader.GetBoolean(2),
            NamesCapture(reader.GetString(3), $"walrus-{options.Name}"));
    }

    /// <summary>
    /// Whether a <c>synchronous_standby_names</c> value would count capture's connection as a synchronous standby.
    /// </summary>
    /// <remarks>
    /// Read from the setting rather than from <c>pg_stat_replication</c>, whose sync state only a role with
    /// <c>pg_read_all_stats</c> can see, and capture's role should not need it. A logical replication connection is
    /// matched by its application name, like any standby, so the risk is a <c>*</c> or capture's own name anywhere in
    /// the list.
    /// </remarks>
    /// <param name="setting">The setting's value.</param>
    /// <param name="applicationName">Capture's application name.</param>
    internal static bool NamesCapture(string setting, string applicationName)
    {
        ArgumentNullException.ThrowIfNull(setting);

        foreach (string name in setting.Split([',', '(', ')', ' '], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            string unquoted = name.Trim('"');

            if (unquoted == "*" || string.Equals(unquoted, applicationName, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Source {Source} on {Primary} counts capture as a synchronous standby, so every commit waits for Walrus. Name the physical standbys in synchronous_standby_names instead of '*'.")]
    private static partial void LogSynchronousCapture(ILogger logger, string source, string primary);

    private static async Task<StandbySlot> ReadStandbyAsync(SourceOptions options, string node, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = new NpgsqlConnection(SourceConnections.ConnectionString(options, node, pooled: false));
            await connection.OpenAsync(cancellationToken);
            await using var slot = new NpgsqlCommand(
                "select synced and invalidation_reason is null, confirmed_flush_lsn from pg_replication_slots where slot_name = $1",
                connection);

            slot.Parameters.Add(new NpgsqlParameter { Value = options.SlotName });
            await using NpgsqlDataReader reader = await slot.ExecuteReaderAsync(cancellationToken);

            return await reader.ReadAsync(cancellationToken)
                ? new StandbySlot(node, reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetFieldValue<NpgsqlLogSequenceNumber>(1).ToString())
                : new StandbySlot(node, false, null);
        }
        catch (Exception exception) when (exception is NpgsqlException or TimeoutException)
        {
            return new StandbySlot(node, false, null);
        }
    }
}
