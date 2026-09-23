using System.Collections.Concurrent;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure.Sinks;

/// <summary>A document the index holds: one row, as it currently is.</summary>
/// <param name="Table">The table.</param>
/// <param name="Key">The primary key.</param>
/// <param name="Row">The row.</param>
public sealed record IndexedRow(string Table, RowImage Key, RowImage Row);

/// <summary>
/// A search index over the captured rows, held in memory and rebuilt from the outbox whenever the service
/// starts.
/// </summary>
/// <remarks>
/// <para>
/// Deliberately the simplest index that behaves like one: lowercased word tokens, an inverted list per token,
/// and an AND of the query's tokens. What it demonstrates is not search quality but the third way of applying a
/// change: no transaction, no stored watermark, state that is lost on every restart and rebuilt by replay.
/// </para>
/// <para>
/// It keeps the same <see cref="KeyState"/> per row as the Postgres sink, so both sinks resolve conflicts
/// identically and can be compared row for row.
/// </para>
/// </remarks>
public sealed class IndexSinkWriter : ISinkWriter
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly Dictionary<string, HashSet<string>> _postings = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public bool IsDurable => false;

    /// <summary>How many rows currently exist in the index.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _entries.Values.Count(static entry => entry.Row is not null);
            }
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ChangeOutcome>> ApplyAsync(IReadOnlyList<ChangeEvent> changes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(changes);

        var outcomes = new ChangeOutcome[changes.Count];

        lock (_gate)
        {
            for (int index = 0; index < changes.Count; index++)
            {
                ChangeEvent change = changes[index];
                Entry current = _entries.TryGetValue(change.EntityKey, out Entry? known)
                    ? known
                    : new Entry(change.Table, change.Key, KeyState.Empty, null);

                KeyApplication application = current.State.Apply(change);
                outcomes[index] = new ChangeOutcome(application.Changed, application.State.LatestVersion);

                if (!application.Changed)
                {
                    continue;
                }

                RowImage? row = application.State.Materialize();
                Unindex(change.EntityKey, current.Row);
                Index(change.EntityKey, row);
                _entries[change.EntityKey] = current with { State = application.State, Row = row };
            }
        }

        return Task.FromResult<IReadOnlyList<ChangeOutcome>>(outcomes);
    }

    /// <summary>Rows containing every word of the query.</summary>
    /// <param name="query">The words.</param>
    /// <param name="table">Only rows of this table, or null for all.</param>
    /// <param name="limit">The most to return.</param>
    public IReadOnlyList<IndexedRow> Search(string query, string? table, int limit)
    {
        string[] tokens = [.. Tokens(query).Distinct(StringComparer.Ordinal)];

        if (tokens.Length == 0)
        {
            return [];
        }

        lock (_gate)
        {
            HashSet<string>? matches = null;

            // Start from the rarest token, so the intersection shrinks as fast as it can.
            foreach (string token in tokens.OrderBy(token => _postings.TryGetValue(token, out HashSet<string>? keys) ? keys.Count : 0))
            {
                if (!_postings.TryGetValue(token, out HashSet<string>? keys))
                {
                    return [];
                }

                if (matches is null)
                {
                    matches = new HashSet<string>(keys, StringComparer.Ordinal);
                }
                else
                {
                    matches.IntersectWith(keys);
                }
            }

            return [.. matches!
                .Select(key => _entries[key])
                .Where(entry => entry.Row is not null && (table is null || string.Equals(entry.Table, table, StringComparison.Ordinal)))
                .OrderBy(static entry => entry.Table, StringComparer.Ordinal)
                .ThenBy(static entry => entry.Key.ToJson(), StringComparer.Ordinal)
                .Take(limit)
                .Select(static entry => new IndexedRow(entry.Table, entry.Key, entry.Row!))];
        }
    }

    /// <summary>Every row the index holds, for comparing it with another sink.</summary>
    public IReadOnlyDictionary<string, RowImage> Rows()
    {
        lock (_gate)
        {
            return _entries
                .Where(static pair => pair.Value.Row is not null)
                .ToDictionary(static pair => pair.Key, static pair => pair.Value.Row!, StringComparer.Ordinal);
        }
    }

    /// <inheritdoc />
    public Task ResetAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _entries.Clear();
            _postings.Clear();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private void Index(string entityKey, RowImage? row)
    {
        foreach (string token in Words(row))
        {
            if (!_postings.TryGetValue(token, out HashSet<string>? keys))
            {
                keys = new HashSet<string>(StringComparer.Ordinal);
                _postings[token] = keys;
            }

            keys.Add(entityKey);
        }
    }

    private void Unindex(string entityKey, RowImage? row)
    {
        foreach (string token in Words(row))
        {
            if (_postings.TryGetValue(token, out HashSet<string>? keys) && keys.Remove(entityKey) && keys.Count == 0)
            {
                _postings.Remove(token);
            }
        }
    }

    private static IEnumerable<string> Words(RowImage? row) =>
        row is null ? [] : row.Columns.SelectMany(static column => Tokens(column.Value)).Distinct(StringComparer.Ordinal);

    private static IEnumerable<string> Tokens(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            yield break;
        }

        int start = -1;

        for (int index = 0; index <= text.Length; index++)
        {
            bool word = index < text.Length && char.IsLetterOrDigit(text[index]);

            if (word && start < 0)
            {
                start = index;
            }
            else if (!word && start >= 0)
            {
                yield return text[start..index].ToLowerInvariant();
                start = -1;
            }
        }
    }

    private sealed record Entry(string Table, RowImage Key, KeyState State, RowImage? Row);
}

/// <summary>The index sinks currently running, so the search endpoint can find them.</summary>
public sealed class IndexSinkRegistry
{
    private readonly ConcurrentDictionary<string, IndexSinkWriter> _indexes = new(StringComparer.Ordinal);

    /// <summary>Creates and registers an index for a sink, replacing any earlier one.</summary>
    /// <param name="sinkId">The sink.</param>
    public IndexSinkWriter Create(string sinkId)
    {
        var index = new IndexSinkWriter();
        _indexes[sinkId] = index;

        return index;
    }

    /// <summary>The index for a sink, or null.</summary>
    /// <param name="sinkId">The sink.</param>
    public IndexSinkWriter? Find(string sinkId) => _indexes.TryGetValue(sinkId, out IndexSinkWriter? index) ? index : null;
}
