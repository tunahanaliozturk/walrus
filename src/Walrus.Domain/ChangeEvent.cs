using System.Text.Json.Serialization;

namespace Walrus.Domain;

/// <summary>What a change did to a row.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<ChangeOperation>))]
public enum ChangeOperation
{
    /// <summary>The row was inserted.</summary>
    Insert = 0,

    /// <summary>The row was updated.</summary>
    Update = 1,

    /// <summary>The row was deleted.</summary>
    Delete = 2,
}

/// <summary>
/// One committed change to one row, captured from a source database's write-ahead log.
/// </summary>
/// <remarks>
/// <para>
/// Identified by where it came from rather than by a generated id: the source, the commit position of its
/// transaction, and its ordinal inside that transaction. Reading the same transaction from the log twice, which
/// is exactly what happens after a crash, produces the same three values, and that is what lets the outbox
/// refuse the second copy with a unique constraint instead of a lookup.
/// </para>
/// <para>
/// <see cref="ChangedColumns"/> is null for a plain change, meaning every column present in
/// <see cref="After"/> was written. It is a narrower list only for tables configured to merge columns
/// independently, where two sources changing different columns of the same row should both survive.
/// </para>
/// </remarks>
/// <param name="Source">The source database, as configured.</param>
/// <param name="CommitLsn">The log position of the transaction's commit record.</param>
/// <param name="Ordinal">This change's position inside its transaction, starting at zero.</param>
/// <param name="TransactionId">The source transaction id, for correlating with the source's own logs.</param>
/// <param name="CommitTimestamp">When the source says the transaction committed.</param>
/// <param name="Hlc">The hybrid logical clock stamp of the transaction.</param>
/// <param name="Table">The qualified table name, <c>schema.table</c>.</param>
/// <param name="Operation">What happened to the row.</param>
/// <param name="Key">The primary key columns.</param>
/// <param name="Before">The row before the change, when the table's replica identity provides it.</param>
/// <param name="After">The row after the change. Null for a delete.</param>
/// <param name="ChangedColumns">The columns this change wrote, or null for all of them.</param>
public sealed record ChangeEvent(
    string Source,
    Lsn CommitLsn,
    int Ordinal,
    uint TransactionId,
    DateTimeOffset CommitTimestamp,
    long Hlc,
    string Table,
    ChangeOperation Operation,
    RowImage Key,
    RowImage? Before,
    RowImage? After,
    IReadOnlyList<string>? ChangedColumns)
{
    /// <summary>Where this change sits in the order every sink agrees on.</summary>
    [JsonIgnore]
    public ChangeVersion Version => new(Hlc, Source, Ordinal);

    /// <summary>The row this change is about, across every source: table plus primary key.</summary>
    /// <remarks>
    /// Deliberately not a hash. Two rows whose keys hash alike must never share a watermark, or one of them
    /// has its changes skipped because the other is further ahead. Hashing is fine for choosing a worker and
    /// wrong for deciding whether something was already applied.
    /// </remarks>
    [JsonIgnore]
    public string EntityKey => Table + " " + Key.ToJson();

    /// <summary>A stable identifier for this change, suitable as an idempotency key downstream.</summary>
    [JsonIgnore]
    public string EventId => $"{Source}/{CommitLsn}/{Ordinal}";
}
