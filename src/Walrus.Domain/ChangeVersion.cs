namespace Walrus.Domain;

/// <summary>
/// The total order every sink uses to decide which of two changes to the same row is newer.
/// </summary>
/// <remarks>
/// <para>
/// Hybrid clock first, then source, then the change's position inside its transaction. The clock carries the
/// order within a source and the last-writer-wins order across sources. The source name breaks the tie when
/// two databases issue the same stamp, which is rare and has to be deterministic rather than impossible. The
/// ordinal separates changes inside one transaction, which all share the transaction's stamp.
/// </para>
/// <para>
/// The ordinal matters more than it looks. A transaction that inserts a row and then updates it produces two
/// changes with the same commit position. A watermark compared on commit position alone treats the update as
/// already applied and skips it, and the row in the sink keeps its inserted values forever. That is what the
/// original design for this service did, and it is why the version is a tuple rather than a single number.
/// </para>
/// </remarks>
/// <param name="Hlc">The hybrid logical clock stamp of the transaction.</param>
/// <param name="Source">Which source database produced the change.</param>
/// <param name="Ordinal">The change's position within its transaction, starting at zero.</param>
public readonly record struct ChangeVersion(long Hlc, string Source, int Ordinal) : IComparable<ChangeVersion>
{
    /// <inheritdoc />
    public int CompareTo(ChangeVersion other)
    {
        int byClock = Hlc.CompareTo(other.Hlc);

        if (byClock != 0)
        {
            return byClock;
        }

        int bySource = string.CompareOrdinal(Source, other.Source);

        return bySource != 0 ? bySource : Ordinal.CompareTo(other.Ordinal);
    }

    /// <summary>Whether this version is newer.</summary>
    /// <param name="left">The first version.</param>
    /// <param name="right">The second version.</param>
    public static bool operator >(ChangeVersion left, ChangeVersion right) => left.CompareTo(right) > 0;

    /// <summary>Whether this version is older.</summary>
    /// <param name="left">The first version.</param>
    /// <param name="right">The second version.</param>
    public static bool operator <(ChangeVersion left, ChangeVersion right) => left.CompareTo(right) < 0;

    /// <summary>Whether this version is at least as new.</summary>
    /// <param name="left">The first version.</param>
    /// <param name="right">The second version.</param>
    public static bool operator >=(ChangeVersion left, ChangeVersion right) => left.CompareTo(right) >= 0;

    /// <summary>Whether this version is at most as new.</summary>
    /// <param name="left">The first version.</param>
    /// <param name="right">The second version.</param>
    public static bool operator <=(ChangeVersion left, ChangeVersion right) => left.CompareTo(right) <= 0;

    /// <summary>The newer of two versions, treating a missing one as older than anything.</summary>
    /// <param name="left">The first version.</param>
    /// <param name="right">The second version.</param>
    public static ChangeVersion? Max(ChangeVersion? left, ChangeVersion? right) =>
        left is null ? right
        : right is null ? left
        : left.Value >= right.Value ? left : right;
}
