using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Npgsql;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure.Sinks;

/// <summary>One data source per configured sink connection, shared by every sink that names it.</summary>
/// <param name="options">The configured connections.</param>
internal sealed class SinkDataSources(IOptions<WalrusOptions> options) : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, NpgsqlDataSource> _sources = new(StringComparer.Ordinal);

    /// <summary>The data source for a connection name, or null when no such connection is configured.</summary>
    /// <param name="name">The connection name.</param>
    public NpgsqlDataSource? Find(string name) =>
        options.Value.SinkConnections.TryGetValue(name, out string? connectionString)
            ? _sources.GetOrAdd(name, _ => NpgsqlDataSource.Create(connectionString))
            : null;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (NpgsqlDataSource source in _sources.Values)
        {
            await source.DisposeAsync();
        }
    }
}

/// <summary>Creates the writer for each kind of sink, checking its target on the way.</summary>
/// <param name="options">The configuration.</param>
/// <param name="dataSources">Sink database connections.</param>
/// <param name="http">Creates the webhook client.</param>
/// <param name="indexes">The running index sinks.</param>
/// <param name="time">The clock.</param>
internal sealed class SinkWriterFactory(
    IOptions<WalrusOptions> options,
    SinkDataSources dataSources,
    IHttpClientFactory http,
    IndexSinkRegistry indexes,
    TimeProvider time) : ISinkWriterFactory
{
    /// <summary>The named HTTP client webhooks use.</summary>
    public const string WebhookClient = "walrus-webhooks";

    /// <inheritdoc />
    public async Task<ISinkWriter> CreateAsync(SinkDefinition sink, bool validate, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(sink);

        switch (sink.Kind)
        {
            case SinkKind.Postgres:
                {
                    NpgsqlDataSource target = dataSources.Find(sink.Target!)
                        ?? throw new ArgumentException($"No sink connection named '{sink.Target}' is configured.");

                    var writer = new PostgresSinkWriter(sink, target);

                    if (validate)
                    {
                        await writer.PrepareAsync(cancellationToken);
                    }

                    return writer;
                }

            case SinkKind.Index:
                return indexes.Create(sink.Id);

            case SinkKind.Webhook:
                {
                    var url = new Uri(sink.Target!);

                    if (!options.Value.WebhookAllowedHosts.Contains(url.Host, StringComparer.OrdinalIgnoreCase))
                    {
                        throw new ArgumentException($"Webhooks to {url.Host} are not allowed. Add the host to Walrus:WebhookAllowedHosts.");
                    }

                    byte[] key = Convert.FromBase64String(options.Value.WebhookSigningKey);

                    return new WebhookSinkWriter(sink, http.CreateClient(WebhookClient), key, time);
                }

            default:
                throw new ArgumentException($"Unknown sink kind {sink.Kind}.");
        }
    }
}
