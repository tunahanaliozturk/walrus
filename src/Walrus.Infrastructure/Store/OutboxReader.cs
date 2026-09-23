using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using NpgsqlTypes;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure.Store;

/// <summary>Converts between outbox rows and change events.</summary>
internal static class OutboxMapping
{
    /// <summary>The one-letter code the outbox stores for an operation.</summary>
    /// <param name="operation">The operation.</param>
    public static char OpCode(ChangeOperation operation) => operation switch
    {
        ChangeOperation.Insert => 'c',
        ChangeOperation.Update => 'u',
        ChangeOperation.Delete => 'd',
        _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unknown operation."),
    };

    /// <summary>A row as a change event.</summary>
    /// <param name="row">The row.</param>
    public static ChangeEvent ToChange(OutboxRow row) => new(
        row.Source,
        new Lsn((ulong)row.CommitLsn),
        row.Ordinal,
        (uint)row.Xid,
        row.CommitTs,
        row.Hlc,
        row.TableName,
        row.Op switch
        {
            'c' => ChangeOperation.Insert,
            'u' => ChangeOperation.Update,
            'd' => ChangeOperation.Delete,
            _ => throw new InvalidOperationException($"Outbox row {row.Seq} has unknown operation '{row.Op}'."),
        },
        RowImage.FromJson(row.Key),
        row.Before is null ? null : RowImage.FromJson(row.Before),
        row.After is null ? null : RowImage.FromJson(row.After),
        row.ChangedColumns);

    /// <summary>A change as the JSON a dead letter stores.</summary>
    /// <param name="change">The change.</param>
    public static string ToJson(ChangeEvent change) => JsonSerializer.Serialize(change, DomainJson.Default.ChangeEvent);

    /// <summary>A change back from a dead letter's JSON.</summary>
    /// <param name="json">The JSON.</param>
    public static ChangeEvent FromJson(string json) =>
        JsonSerializer.Deserialize(json, DomainJson.Default.ChangeEvent)
        ?? throw new JsonException("A dead letter holds no change.");
}

/// <summary>Reads the outbox through the context, keyset-paged on the sequence number.</summary>
/// <param name="contexts">Creates contexts.</param>
internal sealed class OutboxReader(IDbContextFactory<WalrusDbContext> contexts) : IOutboxReader
{
    /// <inheritdoc />
    public async Task<IReadOnlyList<OutboxEntry>> ReadAsync(
        string source,
        long afterSeq,
        Lsn fromLsn,
        IReadOnlyCollection<string> tables,
        int limit,
        CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        var from = new NpgsqlLogSequenceNumber(fromLsn.Value);
        string[] names = [.. tables];

        List<OutboxRow> rows = await db.Outbox
            .AsNoTracking()
            .Where(row => row.Source == source && row.Seq > afterSeq && row.CommitLsn >= from && names.Contains(row.TableName))
            .OrderBy(row => row.Seq)
            .Take(limit)
            .ToListAsync(cancellationToken);

        return [.. rows.Select(static row => new OutboxEntry(row.Seq, OutboxMapping.ToChange(row)))];
    }

    /// <inheritdoc />
    public async Task<long> LastSeqBeforeAsync(string source, Lsn lsn, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        var before = new NpgsqlLogSequenceNumber(lsn.Value);

        // Ordered by position rather than taking max(seq): capture order is commit order, so the newest change
        // before the position is found by walking the position index backwards one step.
        return await db.Outbox
            .Where(row => row.Source == source && row.CommitLsn < before)
            .OrderByDescending(row => row.CommitLsn)
            .ThenByDescending(row => row.Seq)
            .Select(row => row.Seq)
            .FirstOrDefaultAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<long> HeadSeqAsync(string source, CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);

        return await db.Outbox
            .Where(row => row.Source == source)
            .OrderByDescending(row => row.Seq)
            .Select(row => row.Seq)
            .FirstOrDefaultAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<OutboxPosition> PositionAsync(
        string source,
        long afterSeq,
        Lsn fromLsn,
        IReadOnlyCollection<string> tables,
        CancellationToken cancellationToken)
    {
        await using WalrusDbContext db = await contexts.CreateDbContextAsync(cancellationToken);
        var from = new NpgsqlLogSequenceNumber(fromLsn.Value);
        string[] names = [.. tables];

        var head = await db.Outbox
            .Where(row => row.Source == source)
            .OrderByDescending(row => row.Seq)
            .Select(row => new { row.Seq, row.CommitLsn })
            .FirstOrDefaultAsync(cancellationToken);

        var pending = await db.Outbox
            .Where(row => row.Source == source && row.Seq > afterSeq && row.CommitLsn >= from && names.Contains(row.TableName))
            .OrderBy(row => row.Seq)
            .Select(row => new { row.CommitTs, row.CommitLsn })
            .FirstOrDefaultAsync(cancellationToken);

        return new OutboxPosition(
            head?.Seq ?? 0,
            head is null ? Lsn.Zero : new Lsn((ulong)head.CommitLsn),
            pending?.CommitTs,
            pending is null ? null : new Lsn((ulong)pending.CommitLsn));
    }
}
