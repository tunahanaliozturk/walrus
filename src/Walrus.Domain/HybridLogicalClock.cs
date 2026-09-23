namespace Walrus.Domain;

/// <summary>
/// Stamps changes with a timestamp that is ordered like a clock and never runs backwards.
/// </summary>
/// <remarks>
/// <para>
/// Two things need an order that the log sequence number cannot give. Changes from two different source
/// databases have unrelated LSNs, so comparing them says nothing. And a commit timestamp is not monotonic
/// in commit order even within one database: the timestamp is taken before the commit record is written, so
/// two concurrent transactions can commit in one order and carry timestamps in the other.
/// </para>
/// <para>
/// A hybrid logical clock fixes both. Its physical half follows the commit timestamps, so across sources the
/// later write by wall clock wins, within whatever skew the clocks have. Its logical half is a counter that
/// breaks ties and absorbs a timestamp that went backwards, so within one source the stamps rise strictly in
/// the order the log delivered the transactions. That second property is what lets a single ordering rule
/// serve both the single-source pipeline and multi-source conflict resolution.
/// </para>
/// <para>
/// Packed into one 64-bit value: milliseconds since the Unix epoch in the upper 48 bits and the counter in the
/// lower 16. Forty-eight bits of milliseconds lasts until the year 10889, and sixty-five thousand changes
/// sharing one millisecond is handled by borrowing the next millisecond rather than overflowing.
/// </para>
/// </remarks>
public sealed class HybridLogicalClock
{
    /// <summary>How many bits hold the logical counter.</summary>
    public const int LogicalBits = 16;

    private const long LogicalMask = (1L << LogicalBits) - 1;

    private long _last;

    /// <summary>Starts a clock that will never issue a stamp at or below <paramref name="floor"/>.</summary>
    /// <param name="floor">
    /// The highest stamp already issued, typically read back from storage on start. Resuming from anything
    /// lower would let a restarted capture stamp a new change below one it had already persisted.
    /// </param>
    public HybridLogicalClock(long floor = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(floor);

        _last = floor;
    }

    /// <summary>The most recent stamp issued, or the floor it started from.</summary>
    public long Last => _last;

    /// <summary>Issues the next stamp for a change that committed at <paramref name="physical"/>.</summary>
    /// <param name="physical">The commit timestamp the source reported.</param>
    public long Next(DateTimeOffset physical)
    {
        long wall = Math.Max(0, physical.ToUnixTimeMilliseconds());
        long lastWall = PhysicalOf(_last);

        long next = wall > lastWall
            ? Pack(wall, 0)

            // The timestamp did not move forward, either because two transactions share a millisecond or
            // because this one carries an earlier timestamp than the one before it. Either way the counter
            // advances, and a full counter borrows the next millisecond rather than wrapping to zero.
            : LogicalOf(_last) == LogicalMask
                ? Pack(lastWall + 1, 0)
                : _last + 1;

        _last = next;

        return next;
    }

    /// <summary>Combines a physical time and a counter into one stamp.</summary>
    /// <param name="physicalMilliseconds">Milliseconds since the Unix epoch.</param>
    /// <param name="logical">The counter.</param>
    public static long Pack(long physicalMilliseconds, int logical)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(physicalMilliseconds);
        ArgumentOutOfRangeException.ThrowIfNegative(logical);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(logical, (int)LogicalMask);

        return (physicalMilliseconds << LogicalBits) | (long)logical;
    }

    /// <summary>The physical half of a stamp, in milliseconds since the Unix epoch.</summary>
    /// <param name="stamp">The stamp.</param>
    public static long PhysicalOf(long stamp) => stamp >> LogicalBits;

    /// <summary>The logical half of a stamp.</summary>
    /// <param name="stamp">The stamp.</param>
    public static int LogicalOf(long stamp) => (int)(stamp & LogicalMask);
}
