using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using NpgsqlTypes;
using Walrus.Application.Capture;
using Walrus.Domain;

namespace Walrus.Infrastructure.Store;

/// <summary>The outbox and the capture checkpoints, in the store database.</summary>
/// <param name="store">The store's data source, for the lease connection.</param>
/// <param name="contexts">Creates contexts for the writes.</param>
/// <param name="time">The clock.</param>
internal sealed class CaptureStore(
    NpgsqlDataSource store,
    IDbContextFactory<WalrusDbContext> contexts,
    TimeProvider time) : ICaptureStore
{
    private const string Copy = """
        copy walrus_outbox (source, commit_lsn, ordinal, xid, commit_ts, hlc, table_name, op, key, before, after, changed_columns)
        from stdin (format binary)
        """;

    /// <inheritdoc />
    public async Task<ICaptureLease?> TryLeadAsync(string source, CancellationToken cancellationToken)
    {
        // The lease is a session-level advisory lock, so it lives exactly as long as this connection. If the
        // service dies, Postgres notices the connection is gone and the lock with it; nothing needs to expire.
        NpgsqlConnection connection = await store.OpenConnectionAsync(cancellationToken);

        try
        {
            await using (var take = new NpgsqlCommand("select pg_try_advisory_lock(hashtextextended('walrus.capture:' || $1, 0))", connection))
            {
                take.Parameters.Add(new NpgsqlParameter { Value = source });

                if (await take.ExecuteScalarAsync(cancellationToken) is not true)
                {
                    await connection.DisposeAsync();
                    return null;
                }
            }

            await using var begin = new NpgsqlCommand(
                """
                insert into walrus_capture_state (source, epoch) values ($1, 1)
                on conflict (source) do update set epoch = walrus_capture_state.epoch + 1, updated_at = now()
                returning epoch, confirmed_lsn, last_hlc
                """,
                connection);

            begin.Parameters.Add(new NpgsqlParameter { Value = source });

            await using NpgsqlDataReader reader = await begin.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);

            var checkpoint = new CaptureCheckpoint(
                reader.GetInt64(0),
                new Lsn((ulong)reader.GetFieldValue<NpgsqlLogSequenceNumber>(1)),
                reader.GetInt64(2));

            return new Lease(connection, checkpoint);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <inheritdoc />
    public async Task PersistAsync(
        string source,
        long epoch,
        IReadOnlyList<ChangeEvent> changes,
        Lsn confirmed,
        long lastHlc,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(changes);

        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        await using IDbContextTransaction transaction = await db.Database.BeginTransactionAsync(cancellationToken);

        var position = new NpgsqlLogSequenceNumber(confirmed.Value);
        DateTimeOffset now = time.GetUtcNow();

        // The checkpoint moves first, which takes the row lock before a single sequence number is drawn. Two
        // writers can then never hold interleaved ranges of the outbox, and a reader that has seen sequence
        // number n has seen everything below it.
        int owned = await db.CaptureStates
            .Where(state => state.Source == source && state.Epoch == epoch)
            .ExecuteUpdateAsync(
                set => set
                    .SetProperty(state => state.ConfirmedLsn, position)
                    .SetProperty(state => state.LastHlc, lastHlc)
                    .SetProperty(state => state.UpdatedAt, now),
                cancellationToken);

        if (owned == 0)
        {
            throw new CaptureFencedException($"Capture of '{source}' at epoch {epoch} has been taken over by a newer session.");
        }

        if (changes.Count > 0)
        {
            // The hot path leaves LINQ: one binary COPY inside the context's own transaction, instead of an
            // INSERT per change through the change tracker.
            var connection = (NpgsqlConnection)db.Database.GetDbConnection();
            await using NpgsqlBinaryImporter importer = await connection.BeginBinaryImportAsync(Copy, cancellationToken);

            foreach (ChangeEvent change in changes)
            {
                await importer.StartRowAsync(cancellationToken);
                await importer.WriteAsync(change.Source, NpgsqlDbType.Text, cancellationToken);
                await importer.WriteAsync(new NpgsqlLogSequenceNumber(change.CommitLsn.Value), NpgsqlDbType.PgLsn, cancellationToken);
                await importer.WriteAsync(change.Ordinal, NpgsqlDbType.Integer, cancellationToken);
                await importer.WriteAsync((long)change.TransactionId, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(change.CommitTimestamp.ToUniversalTime(), NpgsqlDbType.TimestampTz, cancellationToken);
                await importer.WriteAsync(change.Hlc, NpgsqlDbType.Bigint, cancellationToken);
                await importer.WriteAsync(change.Table, NpgsqlDbType.Text, cancellationToken);
                await importer.WriteAsync(OutboxMapping.OpCode(change.Operation).ToString(), NpgsqlDbType.Char, cancellationToken);
                await importer.WriteAsync(change.Key.ToJson(), NpgsqlDbType.Json, cancellationToken);
                await WriteJsonAsync(importer, change.Before, cancellationToken);
                await WriteJsonAsync(importer, change.After, cancellationToken);

                if (change.ChangedColumns is null)
                {
                    await importer.WriteNullAsync(cancellationToken);
                }
                else
                {
                    await importer.WriteAsync(change.ChangedColumns.ToArray(), NpgsqlDbType.Array | NpgsqlDbType.Text, cancellationToken);
                }
            }

            await importer.CompleteAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    private static async Task WriteJsonAsync(NpgsqlBinaryImporter importer, RowImage? image, CancellationToken cancellationToken)
    {
        if (image is null)
        {
            await importer.WriteNullAsync(cancellationToken);
        }
        else
        {
            await importer.WriteAsync(image.ToJson(), NpgsqlDbType.Json, cancellationToken);
        }
    }

    private sealed class Lease(NpgsqlConnection connection, CaptureCheckpoint checkpoint) : ICaptureLease
    {
        public CaptureCheckpoint Checkpoint { get; } = checkpoint;

        // Returning a connection to the pool does not end its server session, and the lock belongs to the
        // session. Npgsql's reset only runs when the connection is next used, so a standby could wait for a lock
        // held by an idle pooled connection. The lock is released explicitly instead. If that fails the
        // connection is broken, and a broken connection's session is gone with its lock.
        public async ValueTask DisposeAsync()
        {
            try
            {
                await using var release = new NpgsqlCommand("select pg_advisory_unlock_all()", connection);
                await release.ExecuteNonQueryAsync();
            }
            catch (NpgsqlException)
            {
            }
            finally
            {
                await connection.DisposeAsync();
            }
        }
    }
}
