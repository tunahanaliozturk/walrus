using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;
using Walrus.Application.Capture;
using Walrus.Domain;
using Walrus.Infrastructure.Telemetry;

namespace Walrus.Infrastructure.Source;

/// <summary>Opens <see cref="PgOutputLog"/> sessions against whichever node is primary.</summary>
/// <param name="connections">The configured sources.</param>
/// <param name="loggers">Creates loggers.</param>
/// <param name="counters">Where the attached node is reported.</param>
internal sealed partial class PgOutputLogFactory(
    SourceConnections connections,
    PipelineCounters counters,
    ILoggerFactory loggers) : ISourceLogFactory
{
    private readonly ILogger _logger = loggers.CreateLogger<PgOutputLogFactory>();

    /// <inheritdoc />
    public async Task<ISourceLog> OpenAsync(string source, CancellationToken cancellationToken)
    {
        SourceOptions options = connections.Get(source);
        string node = await SourceConnections.FindPrimaryAsync(options, cancellationToken);
        string connectionString = SourceConnections.ConnectionString(options, node, pooled: false);

        var catalog = new NpgsqlConnection(connectionString);
        LogicalReplicationConnection? replication = null;

        try
        {
            await catalog.OpenAsync(cancellationToken);
            await EnsureSlotAsync(catalog, options, cancellationToken);

            // Status every second rather than every ten. The flushed position is sent explicitly after each outbox
            // commit anyway; this is what makes a connection to a node that has gone away fail within a second of it
            // coming back, instead of whenever the next ten-second update happens to be due.
            replication = new LogicalReplicationConnection(connectionString)
            {
                WalReceiverStatusInterval = TimeSpan.FromSeconds(1),
            };
            await replication.Open(cancellationToken);

            SourceCounters status = counters.Source(source);
            status.Host = node;
            status.LastError = null;

            return new PgOutputLog(options, node, replication, catalog, loggers.CreateLogger<PgOutputLog>());
        }
        catch
        {
            if (replication is not null)
            {
                await replication.DisposeAsync();
            }

            await catalog.DisposeAsync();
            throw;
        }
    }

    private async Task EnsureSlotAsync(NpgsqlConnection catalog, SourceOptions options, CancellationToken cancellationToken)
    {
        await using (var level = new NpgsqlCommand("show wal_level", catalog))
        {
            if (await level.ExecuteScalarAsync(cancellationToken) is not "logical")
            {
                throw new InvalidOperationException(
                    $"Source '{options.Name}' runs with wal_level other than logical, so it cannot be captured.");
            }
        }

        await using (var exists = new NpgsqlCommand("select exists (select 1 from pg_replication_slots where slot_name = $1)", catalog))
        {
            exists.Parameters.Add(new NpgsqlParameter { Value = options.SlotName });

            if (await exists.ExecuteScalarAsync(cancellationToken) is true)
            {
                return;
            }
        }

        // Failover enabled, so a standby configured with sync_replication_slots keeps a copy of the slot and a
        // promotion does not leave capture with nowhere to resume from.
        await using var create = new NpgsqlCommand(
            "select pg_create_logical_replication_slot($1, 'pgoutput', failover => true)", catalog);
        create.Parameters.Add(new NpgsqlParameter { Value = options.SlotName });

        try
        {
            await create.ExecuteNonQueryAsync(cancellationToken);
            LogSlotCreated(_logger, options.SlotName, options.Name);
        }
        catch (PostgresException exception) when (exception.SqlState == PostgresErrorCodes.DuplicateObject)
        {
            // Another instance created it first, which is fine.
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Created replication slot {Slot} for source {Source}. Changes committed before now are not captured; register sinks with a snapshot to include them.")]
    private static partial void LogSlotCreated(ILogger logger, string slot, string source);
}

/// <summary>
/// One replication session: decodes <c>pgoutput</c> into whole transactions and acknowledges positions only
/// when told to.
/// </summary>
/// <remarks>
/// <para>
/// Npgsql reports the received position to the server on its own, which only lets the server know how far the
/// stream got. The flushed position, the one that lets the server discard its log, is set in exactly one place,
/// <see cref="AcknowledgeAsync"/>, which capture calls after the outbox commit.
/// </para>
/// <para>
/// Primary keys come from the source's catalog, not from the relation message. With <c>REPLICA IDENTITY FULL</c>,
/// which a before image needs, the relation message marks every column as part of the key.
/// </para>
/// </remarks>
internal sealed partial class PgOutputLog(
    SourceOptions source,
    string host,
    LogicalReplicationConnection replication,
    NpgsqlConnection catalog,
    ILogger<PgOutputLog> logger) : ISourceLog
{
    private readonly Dictionary<uint, Relation> _relations = [];

    /// <inheritdoc />
    public string Host => host;

    /// <inheritdoc />
    public async IAsyncEnumerable<DecodedTransaction> ReadAsync(
        Lsn after,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await RefuseIfBehindAsync(after, cancellationToken);

        var slot = new PgOutputReplicationSlot(source.SlotName);
        var options = new PgOutputReplicationOptions(source.Publication, PgOutputProtocolVersion.V1);
        Transaction? current = null;

        await foreach (PgOutputReplicationMessage message in replication.StartReplication(
            slot, options, cancellationToken, new NpgsqlLogSequenceNumber(after.Value)))
        {
            switch (message)
            {
                case BeginMessage begin:
                    current = new Transaction(
                        new Lsn((ulong)begin.TransactionFinalLsn),
                        begin.TransactionXid,
                        new DateTimeOffset(DateTime.SpecifyKind(begin.TransactionCommitTimestamp, DateTimeKind.Utc)));
                    break;

                case RelationMessage relation:
                    // Sent before a table's first change and again after its definition changes.
                    _relations.Remove(relation.RelationId);
                    break;

                case InsertMessage insert:
                    {
                        Relation relation = await RelationAsync(insert.Relation, cancellationToken);
                        RowImage row = await ReadRowAsync(insert.NewRow, relation, cancellationToken);
                        Add(current, relation, new DecodedChange(relation.Table, ChangeOperation.Insert, relation.KeyOf(row), null, row));
                        break;
                    }

                case FullUpdateMessage update:
                    {
                        Relation relation = await RelationAsync(update.Relation, cancellationToken);
                        RowImage before = await ReadRowAsync(update.OldRow, relation, cancellationToken);
                        RowImage row = await ReadRowAsync(update.NewRow, relation, cancellationToken);
                        Add(current, relation, new DecodedChange(
                            relation.Table, ChangeOperation.Update, relation.KeyOf(row), before, row, relation.KeyOf(before)));
                        break;
                    }

                case IndexUpdateMessage update:
                    {
                        Relation relation = await RelationAsync(update.Relation, cancellationToken);
                        RowImage oldKey = await ReadRowAsync(update.Key, relation, cancellationToken);
                        RowImage row = await ReadRowAsync(update.NewRow, relation, cancellationToken);
                        Add(current, relation, new DecodedChange(
                            relation.Table, ChangeOperation.Update, relation.KeyOf(row), null, row, relation.KeyOf(oldKey)));
                        break;
                    }

                case UpdateMessage update:
                    {
                        Relation relation = await RelationAsync(update.Relation, cancellationToken);
                        RowImage row = await ReadRowAsync(update.NewRow, relation, cancellationToken);
                        Add(current, relation, new DecodedChange(relation.Table, ChangeOperation.Update, relation.KeyOf(row), null, row));
                        break;
                    }

                case FullDeleteMessage delete:
                    {
                        Relation relation = await RelationAsync(delete.Relation, cancellationToken);
                        RowImage before = await ReadRowAsync(delete.OldRow, relation, cancellationToken);
                        Add(current, relation, new DecodedChange(relation.Table, ChangeOperation.Delete, relation.KeyOf(before), before, null));
                        break;
                    }

                case KeyDeleteMessage delete:
                    {
                        Relation relation = await RelationAsync(delete.Relation, cancellationToken);
                        RowImage key = await ReadRowAsync(delete.Key, relation, cancellationToken);
                        Add(current, relation, new DecodedChange(relation.Table, ChangeOperation.Delete, relation.KeyOf(key), null, null));
                        break;
                    }

                case CommitMessage commit when current is not null:
                    yield return new DecodedTransaction(
                        current.CommitLsn,
                        new Lsn((ulong)commit.TransactionEndLsn),
                        current.TransactionId,
                        current.CommitTimestamp,
                        current.Changes);

                    current = null;
                    break;

                case TruncateMessage:
                    LogTruncateIgnored(logger, source.Name);
                    break;
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask AcknowledgeAsync(Lsn persisted, CancellationToken cancellationToken)
    {
        replication.SetReplicationStatus(new NpgsqlLogSequenceNumber(persisted.Value));
        await replication.SendStatusUpdate(cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await replication.DisposeAsync();
        await catalog.DisposeAsync();
    }

    private void Add(Transaction? transaction, Relation relation, DecodedChange change)
    {
        if (transaction is null)
        {
            throw new InvalidOperationException("A row change arrived outside a transaction.");
        }

        // The heartbeat's changes carry nothing, but its transaction does: it moves the acknowledged position
        // forward when the captured tables are quiet.
        if (!string.Equals(relation.Table, source.HeartbeatTable, StringComparison.Ordinal))
        {
            transaction.Changes.Add(change);
        }
    }

    /// <summary>
    /// Refuses to resume when the outbox holds positions the source has never written.
    /// </summary>
    /// <remarks>
    /// That happens when a standby that had not received everything was promoted, and capture had already taken
    /// the missing transactions from the old primary. The new primary then writes different transactions at the
    /// positions the outbox already holds, and resuming by position would silently skip them. Postgres prevents it
    /// when <c>synchronized_standby_slots</c> is set, which is why the operations guide requires it; this check
    /// turns a misconfiguration into a stopped pipeline instead of lost changes.
    /// </remarks>
    private async Task RefuseIfBehindAsync(Lsn after, CancellationToken cancellationToken)
    {
        await using var current = new NpgsqlCommand("select pg_current_wal_lsn()", catalog);
        var position = (NpgsqlLogSequenceNumber)(await current.ExecuteScalarAsync(cancellationToken))!;

        if ((ulong)position < after.Value)
        {
            throw new InvalidOperationException(
                $"Source '{source.Name}' on {host} is at {position}, but the outbox already holds changes up to {after}. " +
                "A standby was promoted without transactions capture had already taken. Resuming would skip whatever " +
                "the new primary writes at those positions, so capture stops here. See the failover runbook.");
        }
    }

    private async ValueTask<Relation> RelationAsync(RelationMessage message, CancellationToken cancellationToken)
    {
        if (_relations.TryGetValue(message.RelationId, out Relation? known))
        {
            return known;
        }

        string table = $"{message.Namespace}.{message.RelationName}";
        List<string> keys = [];

        await using (var primaryKey = new NpgsqlCommand(
            """
            select a.attname
            from pg_index i
            cross join lateral unnest(i.indkey) with ordinality as k(attnum, position)
            join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum
            where i.indrelid = $1 and i.indisprimary
            order by k.position
            """,
            catalog))
        {
            primaryKey.Parameters.Add(new NpgsqlParameter { Value = message.RelationId, NpgsqlDbType = NpgsqlDbType.Oid });
            await using NpgsqlDataReader reader = await primaryKey.ExecuteReaderAsync(cancellationToken);

            while (await reader.ReadAsync(cancellationToken))
            {
                keys.Add(reader.GetString(0));
            }
        }

        if (keys.Count == 0 && !string.Equals(table, source.HeartbeatTable, StringComparison.Ordinal))
        {
            // A row without a key cannot be told apart from another with the same values, so there is nothing to
            // order its changes by or to key a sink's state on. Skipping the table would lose its changes quietly.
            throw new InvalidOperationException(
                $"Table {table} in publication '{source.Publication}' has no primary key, so it cannot be captured.");
        }

        var relation = new Relation(table, [.. message.Columns.Select(static column => column.ColumnName)], [.. keys]);
        _relations[message.RelationId] = relation;

        return relation;
    }

    private static async ValueTask<RowImage> ReadRowAsync(ReplicationTuple tuple, Relation relation, CancellationToken cancellationToken)
    {
        List<RowColumn> columns = new(relation.Columns.Length);
        int index = 0;

        await foreach (ReplicationValue value in tuple)
        {
            string name = relation.Columns[index++];

            switch (value.Kind)
            {
                case TupleDataKind.Null:
                    columns.Add(new RowColumn(name, null));
                    break;

                case TupleDataKind.UnchangedToastedValue:
                    // Postgres did not resend a large value the change left alone. Absent, not null: writing null
                    // here would destroy the value in every sink.
                    break;

                default:
                    columns.Add(new RowColumn(name, await value.Get<string>(cancellationToken)));
                    break;
            }
        }

        return new RowImage(columns);
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Source {Source} sent a TRUNCATE, which is not captured. Create the publication with publish = 'insert, update, delete'.")]
    private static partial void LogTruncateIgnored(ILogger logger, string source);

    private sealed class Transaction(Lsn commitLsn, uint transactionId, DateTimeOffset commitTimestamp)
    {
        public Lsn CommitLsn { get; } = commitLsn;

        public uint TransactionId { get; } = transactionId;

        public DateTimeOffset CommitTimestamp { get; } = commitTimestamp;

        public List<DecodedChange> Changes { get; } = [];
    }

    private sealed class Relation(string table, string[] columns, string[] keys)
    {
        public string Table { get; } = table;

        public string[] Columns { get; } = columns;

        public RowImage KeyOf(RowImage row)
        {
            var key = new RowColumn[keys.Length];

            for (int index = 0; index < keys.Length; index++)
            {
                if (!row.TryGetValue(keys[index], out string? value))
                {
                    throw new InvalidOperationException($"A change to {Table} arrived without key column {keys[index]}.");
                }

                key[index] = new RowColumn(keys[index], value);
            }

            return new RowImage(key);
        }
    }
}
