using System.Text;
using Npgsql;
using NpgsqlTypes;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure.Sinks;

/// <summary>
/// Applies changes to tables in another Postgres database, exactly once in effect.
/// </summary>
/// <remarks>
/// <para>
/// Each row's <see cref="KeyState"/> lives in the sink database, in <c>walrus_sink_state</c>, and is written in
/// the same transaction as the row itself. Delivering a change twice, after a crash or a replay, reads the
/// state, finds the change already absorbed and writes nothing. Nothing outside the sink database has to agree
/// for that to hold, which is the point: the idempotency boundary is where the effect is.
/// </para>
/// <para>
/// Values travel as text with no declared type, so the sink column's own input function parses them, exactly as
/// the source's output function printed them. A numeric keeps every digit and a timestamp keeps its zone.
/// </para>
/// <para>
/// A deleted row keeps its state as a tombstone. Without it, a delayed older write from another source would
/// bring the row back.
/// </para>
/// </remarks>
internal sealed class PostgresSinkWriter(SinkDefinition sink, NpgsqlDataSource target) : ISinkWriter
{
    private readonly SemaphoreSlim _prepare = new(1, 1);
    private Dictionary<string, TargetTable>? _tables;

    /// <inheritdoc />
    public bool IsDurable => true;

    /// <summary>
    /// Creates the state table if needed and reads each target table's columns and key.
    /// </summary>
    /// <param name="cancellationToken">Cancels the preparation.</param>
    /// <exception cref="ArgumentException">A table the sink receives does not exist in the sink, or has no primary key.</exception>
    public async Task PrepareAsync(CancellationToken cancellationToken)
    {
        if (_tables is not null)
        {
            return;
        }

        await _prepare.WaitAsync(cancellationToken);

        try
        {
            if (_tables is not null)
            {
                return;
            }

            await using NpgsqlConnection connection = await target.OpenConnectionAsync(cancellationToken);

            await using (var create = new NpgsqlCommand(
                """
                create table if not exists walrus_sink_state (
                    sink_id    text not null,
                    entity_key text not null,
                    state      json not null,
                    primary key (sink_id, entity_key)
                )
                """,
                connection))
            {
                await create.ExecuteNonQueryAsync(cancellationToken);
            }

            Dictionary<string, TargetTable> tables = new(StringComparer.Ordinal);

            foreach (string table in sink.Tables)
            {
                tables[table] = await DescribeAsync(connection, table, cancellationToken);
            }

            _tables = tables;
        }
        finally
        {
            _prepare.Release();
        }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ChangeOutcome>> ApplyAsync(IReadOnlyList<ChangeEvent> changes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(changes);

        await PrepareAsync(cancellationToken);
        Dictionary<string, TargetTable> tables = _tables!;

        try
        {
            await using NpgsqlConnection connection = await target.OpenConnectionAsync(cancellationToken);
            await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);

            Dictionary<string, KeyState> states = await LoadStatesAsync(connection, transaction, changes, cancellationToken);
            var outcomes = new ChangeOutcome[changes.Count];
            Dictionary<string, ChangeEvent> touched = new(StringComparer.Ordinal);

            for (int index = 0; index < changes.Count; index++)
            {
                ChangeEvent change = changes[index];
                KeyState state = states.TryGetValue(change.EntityKey, out KeyState? known) ? known : KeyState.Empty;
                KeyApplication application = state.Apply(change);

                states[change.EntityKey] = application.State;
                outcomes[index] = new ChangeOutcome(application.Changed, application.State.LatestVersion);

                if (application.Changed)
                {
                    touched[change.EntityKey] = change;
                }
            }

            if (touched.Count > 0)
            {
                await using var batch = new NpgsqlBatch(connection, transaction);

                foreach ((string entityKey, ChangeEvent change) in touched)
                {
                    KeyState state = states[entityKey];
                    TargetTable table = tables[change.Table];
                    RowImage? row = state.Materialize();

                    batch.BatchCommands.Add(row is null ? table.Delete(change.Key) : table.Upsert(row));
                    batch.BatchCommands.Add(StateUpsert(entityKey, state));
                }

                await batch.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);

            return outcomes;
        }
        catch (PostgresException exception) when (IsPermanent(exception))
        {
            throw new SinkRejectedException($"{exception.SqlState}: {exception.MessageText}", exception);
        }
    }

    /// <inheritdoc />
    public async Task ResetAsync(CancellationToken cancellationToken)
    {
        await PrepareAsync(cancellationToken);

        await using NpgsqlConnection connection = await target.OpenConnectionAsync(cancellationToken);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using var batch = new NpgsqlBatch(connection, transaction);

        foreach (TargetTable table in _tables!.Values)
        {
            batch.BatchCommands.Add(new NpgsqlBatchCommand($"truncate {table.QuotedName}"));
        }

        var clear = new NpgsqlBatchCommand("delete from walrus_sink_state where sink_id = $1");
        clear.Parameters.Add(new NpgsqlParameter { Value = sink.Id });
        batch.BatchCommands.Add(clear);

        await batch.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _prepare.Dispose();
        return ValueTask.CompletedTask;
    }

    // Data exceptions, constraint violations and schema errors will fail the same way every time. Everything
    // else (connections, shutdowns, serialization failures, resources) may pass.
    private static bool IsPermanent(PostgresException exception) =>
        exception.SqlState.StartsWith("22", StringComparison.Ordinal)
        || exception.SqlState.StartsWith("23", StringComparison.Ordinal)
        || exception.SqlState.StartsWith("42", StringComparison.Ordinal);

    private async Task<Dictionary<string, KeyState>> LoadStatesAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<ChangeEvent> changes,
        CancellationToken cancellationToken)
    {
        string[] keys = [.. changes.Select(static change => change.EntityKey).Distinct(StringComparer.Ordinal)];

        await using var load = new NpgsqlCommand(
            "select entity_key, state from walrus_sink_state where sink_id = $1 and entity_key = any($2)",
            connection,
            transaction);

        load.Parameters.Add(new NpgsqlParameter { Value = sink.Id });
        load.Parameters.Add(new NpgsqlParameter { Value = keys, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });

        Dictionary<string, KeyState> states = new(keys.Length, StringComparer.Ordinal);
        await using NpgsqlDataReader reader = await load.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            states[reader.GetString(0)] = KeyState.FromJson(reader.GetString(1));
        }

        return states;
    }

    private NpgsqlBatchCommand StateUpsert(string entityKey, KeyState state)
    {
        var upsert = new NpgsqlBatchCommand(
            """
            insert into walrus_sink_state (sink_id, entity_key, state) values ($1, $2, $3)
            on conflict (sink_id, entity_key) do update set state = excluded.state
            """);

        upsert.Parameters.Add(new NpgsqlParameter { Value = sink.Id });
        upsert.Parameters.Add(new NpgsqlParameter { Value = entityKey });
        upsert.Parameters.Add(new NpgsqlParameter { Value = state.ToJson(), NpgsqlDbType = NpgsqlDbType.Json });

        return upsert;
    }

    private static async Task<TargetTable> DescribeAsync(NpgsqlConnection connection, string table, CancellationToken cancellationToken)
    {
        int dot = table.IndexOf('.', StringComparison.Ordinal);

        await using var describe = new NpgsqlCommand(
            """
            select a.attname, array_position(i.indkey::int2[], a.attnum)
            from pg_attribute a
            join pg_class c on c.oid = a.attrelid
            join pg_namespace n on n.oid = c.relnamespace
            left join pg_index i on i.indrelid = c.oid and i.indisprimary
            where n.nspname = $1 and c.relname = $2 and a.attnum > 0 and not a.attisdropped
            order by a.attnum
            """,
            connection);

        describe.Parameters.Add(new NpgsqlParameter { Value = table[..dot] });
        describe.Parameters.Add(new NpgsqlParameter { Value = table[(dot + 1)..] });

        List<string> columns = [];
        List<(string Name, int Position)> keys = [];

        await using (NpgsqlDataReader reader = await describe.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                columns.Add(reader.GetString(0));

                // indkey is an int2vector, which casts to a zero-based array: null means not part of the key,
                // and zero is the first key column.
                if (!reader.IsDBNull(1))
                {
                    keys.Add((reader.GetString(0), reader.GetInt32(1)));
                }
            }
        }

        if (columns.Count == 0)
        {
            throw new ArgumentException($"Table {table} does not exist in the sink database.");
        }

        if (keys.Count == 0)
        {
            throw new ArgumentException($"Table {table} in the sink database has no primary key to upsert on.");
        }

        return new TargetTable(table, [.. columns], [.. keys.OrderBy(static key => key.Position).Select(static key => key.Name)]);
    }

    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";

    private static NpgsqlParameter Text(string? value) =>
        new() { Value = (object?)value ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Unknown };

    private sealed class TargetTable(string name, string[] columns, string[] keys)
    {
        private readonly HashSet<string> _columns = new(columns, StringComparer.Ordinal);

        public string QuotedName { get; } = Quote(name[..name.IndexOf('.', StringComparison.Ordinal)])
            + "." + Quote(name[(name.IndexOf('.', StringComparison.Ordinal) + 1)..]);

        public NpgsqlBatchCommand Upsert(RowImage row)
        {
            foreach (RowColumn column in row.Columns)
            {
                if (!_columns.Contains(column.Name))
                {
                    // The sink's table does not match the source's. Every change to this row would fail the same
                    // way, so it is refused rather than retried.
                    throw new SinkRejectedException($"Column {column.Name} does not exist in sink table {name}.");
                }
            }

            var sql = new StringBuilder("insert into ").Append(QuotedName).Append(" (");
            var command = new NpgsqlBatchCommand();

            for (int index = 0; index < row.Columns.Count; index++)
            {
                sql.Append(index == 0 ? "" : ", ").Append(Quote(row.Columns[index].Name));
            }

            sql.Append(") values (");

            for (int index = 0; index < row.Columns.Count; index++)
            {
                sql.Append(index == 0 ? "$" : ", $").Append(index + 1);
                command.Parameters.Add(Text(row.Columns[index].Value));
            }

            sql.Append(") on conflict (").AppendJoin(", ", keys.Select(Quote)).Append(')');

            string[] updates = [.. row.Columns
                .Where(column => !keys.Contains(column.Name, StringComparer.Ordinal))
                .Select(column => $"{Quote(column.Name)} = excluded.{Quote(column.Name)}")];

            sql.Append(updates.Length == 0 ? " do nothing" : " do update set " + string.Join(", ", updates));
            command.CommandText = sql.ToString();

            return command;
        }

        public NpgsqlBatchCommand Delete(RowImage key)
        {
            var command = new NpgsqlBatchCommand();
            var sql = new StringBuilder("delete from ").Append(QuotedName).Append(" where ");

            for (int index = 0; index < keys.Length; index++)
            {
                key.TryGetValue(keys[index], out string? value);
                sql.Append(index == 0 ? "" : " and ").Append(Quote(keys[index])).Append(" = $").Append(index + 1);
                command.Parameters.Add(Text(value));
            }

            command.CommandText = sql.ToString();

            return command;
        }
    }
}
