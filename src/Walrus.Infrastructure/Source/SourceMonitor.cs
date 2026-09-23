using System.Collections.Concurrent;
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
/// <param name="CheckedAt">When this was read.</param>
/// <param name="Error">What went wrong reading it, if anything.</param>
public sealed record SourceHealth(
    string Source,
    string? Primary,
    long? WalRetainedBytes,
    string? ConfirmedFlushLsn,
    bool SlotActive,
    IReadOnlyList<StandbySlot> Standbys,
    DateTimeOffset CheckedAt,
    string? Error);

/// <summary>Reads each source's slot from every node, for the status page, the metrics and the alerts.</summary>
/// <param name="connections">The configured sources.</param>
/// <param name="time">The clock.</param>
public sealed class SourceMonitor(SourceConnections connections, TimeProvider time)
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
            (long? retained, string? confirmed, bool active) = await ReadPrimaryAsync(options, primary, cancellationToken);
            List<StandbySlot> standbys = [];

            foreach (string node in options.Hosts.Where(node => !string.Equals(node, primary, StringComparison.Ordinal)))
            {
                standbys.Add(await ReadStandbyAsync(options, node, cancellationToken));
            }

            health = new SourceHealth(source, primary, retained, confirmed, active, standbys, time.GetUtcNow(), null);
        }
        catch (Exception exception) when (exception is NpgsqlException or SourceUnavailableException or TimeoutException)
        {
            health = new SourceHealth(source, null, null, null, false, [], time.GetUtcNow(), exception.Message);
        }

        _health[source] = health;

        return health;
    }

    private static async Task<(long? Retained, string? Confirmed, bool Active)> ReadPrimaryAsync(
        SourceOptions options,
        string node,
        CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(SourceConnections.ConnectionString(options, node, pooled: false));
        await connection.OpenAsync(cancellationToken);
        await using var slot = new NpgsqlCommand(
            """
            select pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint, confirmed_flush_lsn, active
            from pg_replication_slots
            where slot_name = $1
            """,
            connection);

        slot.Parameters.Add(new NpgsqlParameter { Value = options.SlotName });
        await using NpgsqlDataReader reader = await slot.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return (null, null, false);
        }

        return (
            reader.IsDBNull(0) ? null : reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetFieldValue<NpgsqlLogSequenceNumber>(1).ToString(),
            reader.GetBoolean(2));
    }

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
