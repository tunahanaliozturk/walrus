namespace Walrus.Application.Dispatch;

/// <summary>
/// The highest outbox position below which every dispatched change has finished, when changes finish out of
/// order.
/// </summary>
/// <remarks>
/// <para>
/// Lanes apply in parallel, so change 12 can finish before change 10. The saved cursor must not pass 10 until 10
/// is done, or a restart would skip it. It must also not wait for 11 if 11 was never dispatched: sequence
/// numbers have gaps wherever a capture transaction rolled back, and a filtered table's changes are never read.
/// So the watermark tracks what was actually handed out, in order, and advances over the finished prefix.
/// </para>
/// <para>
/// Everything is O(1) amortised: dispatch appends, completion marks, and the mark only ever walks forward over
/// each position once.
/// </para>
/// </remarks>
/// <param name="start">The position already known to be complete, typically the saved cursor.</param>
public sealed class ContiguousWatermark(long start)
{
    private readonly Lock _gate = new();
    private readonly Queue<long> _dispatched = new();
    private readonly HashSet<long> _finished = [];
    private long _mark = start;
    private long _last = start;

    /// <summary>Every dispatched position at or below this has finished.</summary>
    public long Mark
    {
        get
        {
            lock (_gate)
            {
                return _mark;
            }
        }
    }

    /// <summary>How many dispatched positions have not finished.</summary>
    public int InFlight
    {
        get
        {
            lock (_gate)
            {
                return _dispatched.Count;
            }
        }
    }

    /// <summary>Records a position being handed out. Positions must rise.</summary>
    /// <param name="seq">The position.</param>
    public void Dispatched(long seq)
    {
        lock (_gate)
        {
            if (seq <= _last)
            {
                throw new InvalidOperationException($"Position {seq} was dispatched after {_last}; positions must rise.");
            }

            _last = seq;
            _dispatched.Enqueue(seq);
        }
    }

    /// <summary>Records a position finishing, applied or parked.</summary>
    /// <param name="seq">The position.</param>
    public void Finished(long seq)
    {
        lock (_gate)
        {
            _finished.Add(seq);

            while (_dispatched.TryPeek(out long head) && _finished.Remove(head))
            {
                _dispatched.Dequeue();
                _mark = head;
            }
        }
    }
}
