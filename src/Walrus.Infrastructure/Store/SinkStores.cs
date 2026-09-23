using Microsoft.EntityFrameworkCore;
using Npgsql;
using NpgsqlTypes;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure.Store;

/// <summary>Sink definitions in the store.</summary>
/// <param name="contexts">Creates contexts.</param>
internal sealed class SinkCatalog(IDbContextFactory<WalrusDbContext> contexts) : ISinkCatalog
{
    /// <inheritdoc />
    public async Task<IReadOnlyList<SinkDefinition>> ListAsync(CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        List<SinkRow> rows = await db.Sinks.AsNoTracking().OrderBy(row => row.Id).ToListAsync(cancellationToken);

        return [.. rows.Select(ToDefinition)];
    }

    /// <inheritdoc />
    public async Task<SinkDefinition?> FindAsync(string id, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        SinkRow? row = await db.Sinks.AsNoTracking().FirstOrDefaultAsync(row => row.Id == id, cancellationToken);

        return row is null ? null : ToDefinition(row);
    }

    /// <inheritdoc />
    public async Task<bool> AddAsync(
        SinkDefinition sink,
        IReadOnlyDictionary<string, SinkCursor> cursors,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(cursors);

        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        db.Sinks.Add(new SinkRow
        {
            Id = sink.Id,
            Kind = sink.Kind.ToString().ToLowerInvariant(),
            Tables = [.. sink.Tables],
            Target = sink.Target,
        });

        foreach ((string source, SinkCursor cursor) in cursors)
        {
            db.SinkCursors.Add(new SinkCursorRow
            {
                SinkId = sink.Id,
                Source = source,
                LastSeq = cursor.LastSeq,
                StartLsn = new NpgsqlLogSequenceNumber(cursor.StartLsn.Value),
            });
        }

        try
        {
            await db.SaveChangesAsync(cancellationToken);
            return true;
        }
        catch (DbUpdateException exception) when (exception.InnerException is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation })
        {
            return false;
        }
    }

    /// <inheritdoc />
    public async Task<bool> RemoveAsync(string id, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        // Cursors and dead letters go with it through their foreign keys.
        return await db.Sinks.Where(row => row.Id == id).ExecuteDeleteAsync(cancellationToken) > 0;
    }

    private static SinkDefinition ToDefinition(SinkRow row) =>
        new(row.Id, Enum.Parse<SinkKind>(row.Kind, ignoreCase: true), row.Tables, row.Target);
}

/// <summary>Sink cursors in the store.</summary>
/// <param name="contexts">Creates contexts.</param>
internal sealed class SinkCursorStore(IDbContextFactory<WalrusDbContext> contexts) : ISinkCursorStore
{
    /// <inheritdoc />
    public async Task<SinkCursor> GetAsync(string sinkId, string source, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        SinkCursorRow? row = await db.SinkCursors
            .AsNoTracking()
            .FirstOrDefaultAsync(row => row.SinkId == sinkId && row.Source == source, cancellationToken);

        return row is null ? SinkCursor.Beginning : new SinkCursor(row.LastSeq, new Lsn((ulong)row.StartLsn));
    }

    /// <inheritdoc />
    public async Task AdvanceAsync(string sinkId, string source, long lastSeq, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        // An upsert, because a source configured after the sink was registered has no cursor row yet, and
        // greatest(), because a cursor only ever moves forward on this path.
        await db.Database.ExecuteSqlAsync(
            $"""
            insert into walrus_sink_cursors (sink_id, source, last_seq, start_lsn)
            values ({sinkId}, {source}, {lastSeq}, '0/0')
            on conflict (sink_id, source) do update
            set last_seq = greatest(walrus_sink_cursors.last_seq, excluded.last_seq), updated_at = now()
            """,
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task SetAsync(string sinkId, string source, SinkCursor cursor, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(cursor);

        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        var start = new NpgsqlLogSequenceNumber(cursor.StartLsn.Value);

        await db.Database.ExecuteSqlAsync(
            $"""
            insert into walrus_sink_cursors (sink_id, source, last_seq, start_lsn)
            values ({sinkId}, {source}, {cursor.LastSeq}, {start})
            on conflict (sink_id, source) do update
            set last_seq = excluded.last_seq, start_lsn = excluded.start_lsn, updated_at = now()
            """,
            cancellationToken);
    }
}

/// <summary>Dead letters in the store.</summary>
/// <param name="contexts">Creates contexts.</param>
/// <param name="time">The clock.</param>
internal sealed class DeadLetterStore(IDbContextFactory<WalrusDbContext> contexts, TimeProvider time) : IDeadLetterStore
{
    /// <inheritdoc />
    public async Task AddAsync(string sinkId, IReadOnlyList<DeadLetterEntry> entries, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(entries);

        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        foreach (DeadLetterEntry entry in entries)
        {
            db.DeadLetters.Add(new DeadLetterRow
            {
                SinkId = sinkId,
                Source = entry.Source,
                Seq = entry.Seq,
                EntityKey = entry.Change.EntityKey,
                Event = OutboxMapping.ToJson(entry.Change),
                Error = entry.Error,
                Attempts = 1,
            });
        }

        await db.SaveChangesAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> BlockedKeysAsync(string sinkId, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        return await db.DeadLetters
            .Where(row => row.SinkId == sinkId && row.ResolvedAt == null)
            .Select(row => row.EntityKey)
            .Distinct()
            .ToListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<DeadLetter>> OpenAsync(
        string sinkId,
        string? entityKey,
        int limit,
        CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        IQueryable<DeadLetterRow> open = db.DeadLetters
            .AsNoTracking()
            .Where(row => row.SinkId == sinkId && row.ResolvedAt == null);

        if (entityKey is not null)
        {
            open = open.Where(row => row.EntityKey == entityKey);
        }

        List<DeadLetterRow> rows = await open.OrderBy(row => row.Id).Take(limit).ToListAsync(cancellationToken);

        return [.. rows.Select(static row => new DeadLetter(
            row.Id, row.Source, row.Seq, OutboxMapping.FromJson(row.Event), row.Error, row.Attempts, row.DeadAt))];
    }

    /// <inheritdoc />
    public async Task ResolveAsync(IReadOnlyList<long> ids, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        long[] resolved = [.. ids];
        DateTimeOffset now = time.GetUtcNow();

        await db.DeadLetters
            .Where(row => resolved.Contains(row.Id))
            .ExecuteUpdateAsync(set => set.SetProperty(row => row.ResolvedAt, now), cancellationToken);
    }

    /// <inheritdoc />
    public async Task RecordAttemptAsync(IReadOnlyList<long> ids, string reason, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        long[] attempted = [.. ids];

        await db.DeadLetters
            .Where(row => attempted.Contains(row.Id))
            .ExecuteUpdateAsync(
                set => set
                    .SetProperty(row => row.Attempts, row => row.Attempts + 1)
                    .SetProperty(row => row.Error, reason),
                cancellationToken);
    }

    /// <inheritdoc />
    public async Task ClearAsync(string sinkId, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        await db.DeadLetters.Where(row => row.SinkId == sinkId).ExecuteDeleteAsync(cancellationToken);
    }
}
