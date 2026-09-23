using Walrus.Domain;

namespace Walrus.Application.Capture;

/// <summary>
/// Turns a decoded source transaction into the change events the outbox stores: stamped, numbered, masked.
/// </summary>
/// <remarks>
/// <para>
/// Every change in a transaction shares the transaction's clock stamp and differs by ordinal, so the order of
/// changes inside a transaction survives all the way to the sink.
/// </para>
/// <para>
/// An update that changes a primary key becomes a delete of the old key followed by an insert of the new one.
/// A sink keys its state by primary key, and a single event about two keys would have to be special-cased by
/// every sink; two ordinary events need nothing.
/// </para>
/// </remarks>
/// <param name="source">The source the transactions come from.</param>
/// <param name="policies">Configured tables by qualified name. Tables not listed get the default policy.</param>
/// <param name="masker">Applies column masks.</param>
public sealed class TransactionStamper(
    string source,
    IReadOnlyDictionary<string, TablePolicy> policies,
    ColumnMasker masker)
{
    /// <summary>Stamps one transaction.</summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="clock">The source's clock, advanced once per transaction that carries changes.</param>
    public IReadOnlyList<ChangeEvent> Stamp(DecodedTransaction transaction, HybridLogicalClock clock)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(clock);

        if (transaction.Changes.Count == 0)
        {
            return [];
        }

        long hlc = clock.Next(transaction.CommitTimestamp);
        List<ChangeEvent> events = new(transaction.Changes.Count);

        foreach (DecodedChange change in transaction.Changes)
        {
            TablePolicy policy = policies.TryGetValue(change.Table, out TablePolicy? configured)
                ? configured
                : TablePolicy.Default(change.Table);

            if (change.Operation is ChangeOperation.Update
                && change.PreviousKey is { } previous
                && !previous.Equals(change.Key))
            {
                events.Add(Build(transaction, hlc, events.Count, policy, ChangeOperation.Delete, previous, change.Before, null, null));
                events.Add(Build(transaction, hlc, events.Count, policy, ChangeOperation.Insert, change.Key, null, change.After, null));
                continue;
            }

            IReadOnlyList<string>? changed = policy.Conflict is ConflictMode.MergeColumns
                && change.Operation is ChangeOperation.Update
                    ? ChangedColumns(change.Before, change.After)
                    : null;

            events.Add(Build(transaction, hlc, events.Count, policy, change.Operation, change.Key, change.Before, change.After, changed));
        }

        return events;
    }

    /// <summary>Turns rows copied from a consistent snapshot into inserts every later change outranks.</summary>
    /// <remarks>
    /// A snapshot row has no clock stamp: nobody knows when it was last written. It takes the lowest version
    /// there is, so any change captured after the snapshot replaces it. Masks apply exactly as they do to
    /// captured changes, since a snapshot is just another way for a value to reach a sink.
    /// </remarks>
    /// <param name="rows">The rows, as inserts.</param>
    public IReadOnlyList<ChangeEvent> StampSnapshot(IReadOnlyList<DecodedChange> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var snapshot = new DecodedTransaction(Lsn.Zero, Lsn.Zero, 0, DateTimeOffset.UnixEpoch, rows);
        List<ChangeEvent> events = new(rows.Count);

        foreach (DecodedChange row in rows)
        {
            TablePolicy policy = policies.TryGetValue(row.Table, out TablePolicy? configured)
                ? configured
                : TablePolicy.Default(row.Table);

            events.Add(Build(snapshot, 0, 0, policy, ChangeOperation.Insert, row.Key, null, row.After, null));
        }

        return events;
    }

    /// <summary>
    /// The columns an update actually wrote, found by comparing the before and after images.
    /// </summary>
    /// <remarks>
    /// Without a before image there is nothing to compare, and every column the after image carries counts as
    /// written. That still converges; it just means a merge table without <c>REPLICA IDENTITY FULL</c> behaves
    /// like last-writer-wins, which the operations guide says.
    /// </remarks>
    /// <param name="before">The row before, or null.</param>
    /// <param name="after">The row after.</param>
    public static IReadOnlyList<string>? ChangedColumns(RowImage? before, RowImage? after)
    {
        if (before is null || after is null)
        {
            return null;
        }

        List<string> changed = [];

        foreach (RowColumn column in after.Columns)
        {
            if (!before.TryGetValue(column.Name, out string? previous)
                || !string.Equals(previous, column.Value, StringComparison.Ordinal))
            {
                changed.Add(column.Name);
            }
        }

        return changed;
    }

    private ChangeEvent Build(
        DecodedTransaction transaction,
        long hlc,
        int ordinal,
        TablePolicy policy,
        ChangeOperation operation,
        RowImage key,
        RowImage? before,
        RowImage? after,
        IReadOnlyList<string>? changed)
    {
        foreach (RowColumn column in key.Columns)
        {
            // A key masked to null or to a fixed marker would collapse every row into one. Hashing keeps keys
            // distinct, so it is the only mask a key column may carry.
            if (policy.Masks.TryGetValue(column.Name, out ColumnMask mask) && mask is not ColumnMask.Hash)
            {
                throw new InvalidOperationException(
                    $"Column {policy.Table}.{column.Name} is part of the primary key and can only be masked with Hash, not {mask}.");
            }
        }

        return new ChangeEvent(
            source,
            transaction.CommitLsn,
            ordinal,
            transaction.TransactionId,
            transaction.CommitTimestamp,
            hlc,
            policy.Table,
            operation,
            masker.Mask(key, policy.Masks)!,
            masker.Mask(before, policy.Masks),
            masker.Mask(after, policy.Masks),
            changed);
    }
}
