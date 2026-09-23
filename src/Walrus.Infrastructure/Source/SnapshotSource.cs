using System.Data;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Walrus.Application.Capture;
using Walrus.Application.Sinks;
using Walrus.Domain;

namespace Walrus.Infrastructure.Source;

/// <summary>
/// Copies source tables as of one log position, using the snapshot Postgres exports when a replication slot is
/// created.
/// </summary>
/// <remarks>
/// <para>
/// A temporary slot is created only for its snapshot and its consistent point, and dropped when the replication
/// connection closes. The snapshot shows exactly the transactions that committed before the consistent point,
/// and the main slot's stream carries every transaction from that point on, so a sink that loads the copy and
/// then takes the stream from the point has everything once.
/// </para>
/// <para>
/// Values are read as <c>column::text</c>, which is the same output function <c>pgoutput</c> uses, so a copied
/// row and a streamed row with the same values are the same bytes.
/// </para>
/// </remarks>
/// <param name="connections">The configured sources.</param>
internal sealed class SnapshotSource(SourceConnections connections) : ISnapshotSource
{
    private const int BatchSize = 1_000;

    /// <inheritdoc />
    public async Task<Lsn> ExportAsync(
        string source,
        IReadOnlyList<string> tables,
        Func<IReadOnlyList<DecodedChange>, CancellationToken, Task> onRows,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(tables);
        ArgumentNullException.ThrowIfNull(onRows);

        SourceOptions options = connections.Get(source);
        string node = await SourceConnections.FindPrimaryAsync(options, cancellationToken);
        string connectionString = SourceConnections.ConnectionString(options, node, pooled: false);

        await using var reader = new NpgsqlConnection(connectionString);
        await reader.OpenAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await reader.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);

        Lsn point;

        await using (var replication = new LogicalReplicationConnection(connectionString))
        {
            await replication.Open(cancellationToken);

            PgOutputReplicationSlot slot = await replication.CreatePgOutputReplicationSlot(
                $"walrus_snapshot_{Guid.NewGuid():N}",
                temporarySlot: true,
                slotSnapshotInitMode: LogicalSlotSnapshotInitMode.Export,
                cancellationToken: cancellationToken);

            point = new Lsn((ulong)slot.ConsistentPoint);

            // The exported snapshot is only importable while the exporting session is idle, so it is imported
            // before anything else happens on the replication connection. Once imported, the transaction keeps
            // it, and the temporary slot can go.
            await using var import = new NpgsqlCommand($"set transaction snapshot '{slot.SnapshotName}'", reader, transaction);
            await import.ExecuteNonQueryAsync(cancellationToken);
        }

        foreach (string table in tables)
        {
            await CopyTableAsync(reader, transaction, table, onRows, cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);

        return point;
    }

    private static async Task CopyTableAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string table,
        Func<IReadOnlyList<DecodedChange>, CancellationToken, Task> onRows,
        CancellationToken cancellationToken)
    {
        (string[] columns, string[] keys) = await DescribeAsync(connection, transaction, table, cancellationToken);

        // A sink's tables need not exist in every source: a table only one region has is simply empty in the
        // others' snapshots.
        if (columns.Length == 0)
        {
            return;
        }

        string select = $"select {string.Join(", ", columns.Select(static column => $"{Quote(column)}::text"))} from {QuoteTable(table)}";
        await using var query = new NpgsqlCommand(select, connection, transaction);
        await using NpgsqlDataReader rows = await query.ExecuteReaderAsync(cancellationToken);

        List<DecodedChange> batch = new(BatchSize);

        while (await rows.ReadAsync(cancellationToken))
        {
            var values = new RowColumn[columns.Length];

            for (int index = 0; index < columns.Length; index++)
            {
                values[index] = new RowColumn(columns[index], rows.IsDBNull(index) ? null : rows.GetString(index));
            }

            var row = new RowImage(values);
            var key = new RowImage(keys.Select(name =>
            {
                row.TryGetValue(name, out string? value);
                return new RowColumn(name, value);
            }));

            batch.Add(new DecodedChange(table, ChangeOperation.Insert, key, null, row));

            if (batch.Count == BatchSize)
            {
                await onRows(batch, cancellationToken);
                batch = new List<DecodedChange>(BatchSize);
            }
        }

        if (batch.Count > 0)
        {
            await onRows(batch, cancellationToken);
        }
    }

    private static async Task<(string[] Columns, string[] Keys)> DescribeAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string table,
        CancellationToken cancellationToken)
    {
        int dot = table.IndexOf('.', StringComparison.Ordinal);

        await using var describe = new NpgsqlCommand(
            """
            select a.attname,
                   array_position(i.indkey::int2[], a.attnum) as key_position
            from pg_attribute a
            join pg_class c on c.oid = a.attrelid
            join pg_namespace n on n.oid = c.relnamespace
            left join pg_index i on i.indrelid = c.oid and i.indisprimary
            where n.nspname = $1 and c.relname = $2 and a.attnum > 0 and not a.attisdropped
            order by a.attnum
            """,
            connection,
            transaction);

        describe.Parameters.Add(new NpgsqlParameter { Value = table[..dot] });
        describe.Parameters.Add(new NpgsqlParameter { Value = table[(dot + 1)..] });

        List<string> columns = [];
        List<(string Name, int Position)> keys = [];

        await using NpgsqlDataReader reader = await describe.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            string name = reader.GetString(0);
            columns.Add(name);

            // indkey is an int2vector, which casts to a zero-based array: null means not part of the key.
            if (!reader.IsDBNull(1))
            {
                keys.Add((name, reader.GetInt32(1)));
            }
        }

        return ([.. columns], [.. keys.OrderBy(static key => key.Position).Select(static key => key.Name)]);
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static string QuoteTable(string table)
    {
        int dot = table.IndexOf('.', StringComparison.Ordinal);

        return Quote(table[..dot]) + "." + Quote(table[(dot + 1)..]);
    }
}
