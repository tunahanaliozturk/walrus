using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace Walrus.Domain;

/// <summary>The kinds of sink, each with a different way of applying a change.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<SinkKind>))]
public enum SinkKind
{
    /// <summary>
    /// Another Postgres database. A change and the sink's record of it commit in one transaction, so the sink
    /// is exactly-once in effect with no help from anything upstream.
    /// </summary>
    Postgres = 0,

    /// <summary>
    /// A search index held in memory. Nothing survives a restart, so it rebuilds itself from the outbox every
    /// time the service starts, which makes it the sink that exercises replay on every boot.
    /// </summary>
    Index = 1,

    /// <summary>
    /// An HTTP endpoint owned by someone else. Delivery is at least once, and each change carries an id the
    /// receiver can deduplicate on. Walrus cannot make a remote system idempotent; it can make that easy.
    /// </summary>
    Webhook = 2,
}

/// <summary>A sink as registered: what it is, what it receives, and where it writes.</summary>
/// <param name="Id">Lowercase letters, digits and hyphens, starting with a letter or digit.</param>
/// <param name="Kind">How changes are applied.</param>
/// <param name="Tables">The qualified source tables it receives.</param>
/// <param name="Target">
/// For a Postgres sink, the name of a configured connection, never the connection string itself: credentials
/// live in configuration or a secret store, not in the database that anyone with a read key can list. For a
/// webhook, the URL. Unused for an index.
/// </param>
public sealed partial record SinkDefinition(string Id, SinkKind Kind, IReadOnlyList<string> Tables, string? Target)
{
    /// <summary>Checks the definition on its own, without reference to what exists.</summary>
    /// <returns>Every problem found, or an empty list.</returns>
    public IReadOnlyList<string> Problems()
    {
        List<string> problems = [];

        if (string.IsNullOrEmpty(Id) || !IdPattern().IsMatch(Id))
        {
            problems.Add("The id must be 1 to 63 lowercase letters, digits or hyphens, starting with a letter or digit.");
        }

        if (Tables is null || Tables.Count == 0)
        {
            problems.Add("A sink must receive at least one table.");
        }
        else
        {
            foreach (string table in Tables)
            {
                if (!TablePattern().IsMatch(table))
                {
                    problems.Add($"'{table}' is not a qualified table name like public.accounts.");
                }
            }

            if (Tables.Distinct(StringComparer.Ordinal).Count() != Tables.Count)
            {
                problems.Add("A table is listed twice.");
            }
        }

        switch (Kind)
        {
            case SinkKind.Postgres when string.IsNullOrWhiteSpace(Target):
                problems.Add("A Postgres sink names the configured connection it writes to.");
                break;

            case SinkKind.Webhook when !Uri.TryCreate(Target, UriKind.Absolute, out Uri? url)
                || (url.Scheme != Uri.UriSchemeHttps && url.Scheme != Uri.UriSchemeHttp):
                problems.Add("A webhook sink needs an absolute http or https URL.");
                break;

            case SinkKind.Index when Target is not null:
                problems.Add("An index sink has no target.");
                break;
        }

        return problems;
    }

    [GeneratedRegex("^[a-z0-9][a-z0-9-]{0,62}$", RegexOptions.CultureInvariant)]
    private static partial Regex IdPattern();

    // Unquoted identifiers only. A quoted identifier would have to be quoted identically in the sink, and
    // supporting that is a class of bug with no user asking for it.
    [GeneratedRegex("^[a-z_][a-z0-9_]{0,62}\\.[a-z_][a-z0-9_]{0,62}$", RegexOptions.CultureInvariant)]
    private static partial Regex TablePattern();
}
