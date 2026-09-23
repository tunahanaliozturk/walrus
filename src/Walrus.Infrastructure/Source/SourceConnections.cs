using Microsoft.Extensions.Options;
using Npgsql;

namespace Walrus.Infrastructure.Source;

/// <summary>No node of a source is currently a primary.</summary>
public sealed class SourceUnavailableException : Exception
{
    /// <summary>Creates the exception.</summary>
    public SourceUnavailableException()
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was tried.</param>
    public SourceUnavailableException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">What was tried.</param>
    /// <param name="innerException">The last failure.</param>
    public SourceUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>Builds connections to source nodes and finds which one is the primary right now.</summary>
/// <param name="options">The configured sources.</param>
public sealed class SourceConnections(IOptions<WalrusOptions> options)
{
    /// <summary>A configured source.</summary>
    /// <param name="source">Its name.</param>
    public SourceOptions Get(string source) =>
        options.Value.Sources.FirstOrDefault(candidate => string.Equals(candidate.Name, source, StringComparison.Ordinal))
        ?? throw new InvalidOperationException($"Source '{source}' is not configured.");

    /// <summary>A connection string for one node of a source.</summary>
    /// <param name="source">The source.</param>
    /// <param name="node">The node, as <c>host:port</c>.</param>
    /// <param name="pooled">
    /// False for connections that must not be handed a pooled connection to a node that has since died or
    /// changed role, such as the probe that decides which node is primary.
    /// </param>
    public static string ConnectionString(SourceOptions source, string node, bool pooled = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(node);

        int colon = node.LastIndexOf(':');

        return new NpgsqlConnectionStringBuilder
        {
            Host = colon > 0 ? node[..colon] : node,
            Port = colon > 0 ? int.Parse(node[(colon + 1)..], System.Globalization.CultureInfo.InvariantCulture) : 5432,
            Database = source.Database,
            Username = source.Username,
            Password = source.Password,
            ApplicationName = $"walrus-{source.Name}",
            Timeout = 5,
            CommandTimeout = 30,
            Pooling = pooled,
        }.ConnectionString;
    }

    /// <summary>
    /// The node that is primary now. Asked of each node in turn, because after a failover the configured order
    /// says nothing about which one it is.
    /// </summary>
    /// <param name="source">The source.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    public static async Task<string> FindPrimaryAsync(SourceOptions source, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);

        Exception? last = null;

        foreach (string node in source.Hosts)
        {
            try
            {
                await using var connection = new NpgsqlConnection(ConnectionString(source, node, pooled: false));
                await connection.OpenAsync(cancellationToken);
                await using var probe = new NpgsqlCommand("select pg_is_in_recovery()", connection);

                if (await probe.ExecuteScalarAsync(cancellationToken) is false)
                {
                    return node;
                }
            }
            catch (Exception exception) when (exception is NpgsqlException or TimeoutException)
            {
                last = exception;
            }
        }

        string tried = string.Join(", ", source.Hosts);

        throw last is null
            ? new SourceUnavailableException($"None of {tried} is a primary for source '{source.Name}'.")
            : new SourceUnavailableException($"None of {tried} is a primary for source '{source.Name}'.", last);
    }
}
