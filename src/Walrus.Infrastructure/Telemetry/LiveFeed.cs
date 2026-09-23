using System.Text.Json.Serialization;
using System.Threading.Channels;

namespace Walrus.Infrastructure.Telemetry;

/// <summary>What a live feed entry is about.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<FeedKind>))]
public enum FeedKind
{
    /// <summary>A change was captured.</summary>
    Change = 0,

    /// <summary>A change lost to a newer change from another source.</summary>
    Conflict = 1,

    /// <summary>A sink parked a change.</summary>
    DeadLetter = 2,
}

/// <summary>One entry in the live feed.</summary>
/// <param name="Kind">What it is about.</param>
/// <param name="At">When it happened here.</param>
/// <param name="Source">The source of the change.</param>
/// <param name="Table">The table.</param>
/// <param name="Operation">insert, update or delete.</param>
/// <param name="Key">The primary key, as JSON.</param>
/// <param name="CommitLsn">Where the change committed.</param>
/// <param name="Sink">The sink, for conflicts and dead letters.</param>
/// <param name="Detail">The winning version for a conflict, the reason for a dead letter.</param>
public sealed record FeedEntry(
    FeedKind Kind,
    DateTimeOffset At,
    string Source,
    string Table,
    string Operation,
    string Key,
    string CommitLsn,
    string? Sink,
    string? Detail);

/// <summary>
/// Fans live events out to every connected console, and remembers the recent ones for a console that has just
/// opened.
/// </summary>
/// <remarks>
/// <para>
/// A slow viewer must not slow capture down, so each subscriber has a small buffer that drops its oldest entry
/// when full. That is the one place in this service where losing something is the intended behaviour: the feed
/// is a view, and the outbox is the record.
/// </para>
/// <para>
/// Captured changes are sampled to a few per second per source before they get here. At thousands of changes a
/// second nobody reads the stream; they read the counters, and the sample shows that data is moving and what
/// it looks like.
/// </para>
/// </remarks>
public sealed class LiveFeed
{
    private const int SubscriberBuffer = 256;
    private const int Remembered = 200;

    private readonly Lock _gate = new();
    private readonly List<Channel<FeedEntry>> _subscribers = [];
    private readonly Queue<FeedEntry> _recentChanges = new();
    private readonly Queue<FeedEntry> _recentConflicts = new();
    private readonly Queue<FeedEntry> _recentDeadLetters = new();

    /// <summary>Publishes an entry to every subscriber.</summary>
    /// <param name="entry">The entry.</param>
    public void Publish(FeedEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        lock (_gate)
        {
            Queue<FeedEntry> recent = entry.Kind switch
            {
                FeedKind.Conflict => _recentConflicts,
                FeedKind.DeadLetter => _recentDeadLetters,
                _ => _recentChanges,
            };

            recent.Enqueue(entry);

            if (recent.Count > Remembered)
            {
                recent.Dequeue();
            }

            foreach (Channel<FeedEntry> subscriber in _subscribers)
            {
                subscriber.Writer.TryWrite(entry);
            }
        }
    }

    /// <summary>The most recent entries of one kind, newest last.</summary>
    /// <param name="kind">The kind.</param>
    public IReadOnlyList<FeedEntry> Recent(FeedKind kind)
    {
        lock (_gate)
        {
            return kind switch
            {
                FeedKind.Conflict => [.. _recentConflicts],
                FeedKind.DeadLetter => [.. _recentDeadLetters],
                _ => [.. _recentChanges],
            };
        }
    }

    /// <summary>Streams entries from now until the caller stops reading.</summary>
    /// <param name="cancellationToken">Ends the subscription.</param>
    public async IAsyncEnumerable<FeedEntry> SubscribeAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        Channel<FeedEntry> channel = Channel.CreateBounded<FeedEntry>(new BoundedChannelOptions(SubscriberBuffer)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.DropOldest,
        });

        lock (_gate)
        {
            _subscribers.Add(channel);
        }

        try
        {
            await foreach (FeedEntry entry in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return entry;
            }
        }
        finally
        {
            lock (_gate)
            {
                _subscribers.Remove(channel);
            }
        }
    }
}
