namespace Walrus.Core;

/// <summary>Chooses which delivery lane carries changes for a row.</summary>
/// <remarks>
/// <para>
/// All changes to one row go down one lane, and a lane applies its changes one at a time, so a row's changes
/// reach a sink in the order they were captured. Different rows go down different lanes and are applied in
/// parallel. That is the whole ordering mechanism, and it needs no locks and no per-row bookkeeping.
/// </para>
/// <para>
/// A hash is the right tool here and the wrong tool for deduplication. If two rows share a lane, the cost is
/// that one waits for the other. If two rows shared a watermark, one would have changes skipped. The router
/// never decides whether something was applied; <see cref="KeyState"/> does that on the real key.
/// </para>
/// <para>
/// FNV-1a rather than <see cref="string.GetHashCode()"/>, which is randomised per process. Lane assignment does
/// not need to survive a restart, but a test that asserts two keys share a lane has to get the same answer on
/// every run.
/// </para>
/// </remarks>
public static class LaneRouter
{
    /// <summary>The lane for a row.</summary>
    /// <param name="entityKey">The row, as <see cref="ChangeEvent.EntityKey"/>.</param>
    /// <param name="laneCount">How many lanes there are.</param>
    public static int LaneFor(string entityKey, int laneCount)
    {
        ArgumentNullException.ThrowIfNull(entityKey);
        ArgumentOutOfRangeException.ThrowIfLessThan(laneCount, 1);

        return (int)(Fnv1a(entityKey) % (ulong)laneCount);
    }

    private static ulong Fnv1a(string value)
    {
        const ulong Offset = 14695981039346656037;
        const ulong Prime = 1099511628211;

        ulong hash = Offset;

        foreach (char character in value)
        {
            hash ^= character;
            hash *= Prime;
        }

        return hash;
    }
}
