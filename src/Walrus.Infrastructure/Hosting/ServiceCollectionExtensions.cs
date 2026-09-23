using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Walrus.Application.Capture;
using Walrus.Application.Dispatch;
using Walrus.Application.Sinks;
using Walrus.Domain;
using Walrus.Infrastructure.Sinks;
using Walrus.Infrastructure.Source;
using Walrus.Infrastructure.Store;
using Walrus.Infrastructure.Telemetry;

namespace Walrus.Infrastructure.Hosting;

/// <summary>Wires the whole pipeline into a host.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// True when the host is being started by build-time tooling, such as the OpenAPI document generator, which
    /// builds the application to read its endpoints and must not start capturing a database while it does.
    /// </summary>
    public static bool IsBuildTimeTooling =>
        string.Equals(Assembly.GetEntryAssembly()?.GetName().Name, "GetDocument.Insider", StringComparison.Ordinal);

    /// <summary>Registers capture, dispatch, the store, the sinks and their telemetry.</summary>
    /// <param name="builder">The host builder.</param>
    public static IHostApplicationBuilder AddWalrus(this IHostApplicationBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        IServiceCollection services = builder.Services;

        OptionsBuilder<WalrusOptions> options = services.AddOptions<WalrusOptions>()
            .Bind(builder.Configuration.GetSection(WalrusOptions.Section));

        // Build-time tooling starts the host with no configuration at all, only to read its endpoints.
        if (!IsBuildTimeTooling)
        {
            options.ValidateOnStart();
        }

        services.AddSingleton<IValidateOptions<WalrusOptions>, WalrusOptionsValidator>();
        services.TryAddSingletonTimeProvider();

        // The store. A context is created per unit of work from a pooled factory, because the long-running
        // loops that use it are singletons and a scoped context would outlive its welcome.
        services.AddSingleton(provider =>
            NpgsqlDataSource.Create(provider.GetRequiredService<IOptions<WalrusOptions>>().Value.StoreConnection));

        services.AddPooledDbContextFactory<WalrusDbContext>((provider, options) => options
            .UseNpgsql(provider.GetRequiredService<NpgsqlDataSource>())
            .UseSnakeCaseNamingConvention());

        services.AddSingleton<ICaptureStore, CaptureStore>();
        services.AddSingleton<IOutboxReader, OutboxReader>();
        services.AddSingleton<ISinkCatalog, SinkCatalog>();
        services.AddSingleton<ISinkCursorStore, SinkCursorStore>();
        services.AddSingleton<IDeadLetterStore, DeadLetterStore>();

        // Sources.
        services.AddSingleton<SourceConnections>();
        services.AddSingleton<ISourceLogFactory, PgOutputLogFactory>();
        services.AddSingleton<ISnapshotSource, SnapshotSource>();
        services.AddSingleton<SourceMonitor>();
        services.AddSingleton(CreateSources);
        services.AddSingleton<IOutboxSignal, OutboxSignal>();

        // Sinks.
        services.AddHttpClient(SinkWriterFactory.WebhookClient, client => client.Timeout = TimeSpan.FromSeconds(10))
            .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler { AllowAutoRedirect = false });

        services.AddSingleton<SinkDataSources>();
        services.AddSingleton<IndexSinkRegistry>();
        services.AddSingleton<ISinkWriterFactory, SinkWriterFactory>();
        services.AddSingleton(provider => provider.GetRequiredService<IOptions<WalrusOptions>>().Value.Dispatch.ToOptions());
        services.AddSingleton<SinkSupervisor>();

        // Telemetry.
        services.AddSingleton<WalrusTelemetry>();
        services.AddSingleton<PipelineCounters>();
        services.AddSingleton<LiveFeed>();
        services.AddSingleton<ICaptureObserver, CaptureObserver>();
        services.AddSingleton<IDispatchObserver, DispatchObserver>();

        builder.AddWalrusTelemetry();

        if (!IsBuildTimeTooling)
        {
            // Registration order is start order: the schema first, then the sinks, then the sources feeding them.
            services.AddHostedService<StoreMigrator>();
            services.AddHostedService<DispatchService>();
            services.AddHostedService<CaptureService>();
            services.AddHostedService<HeartbeatService>();
            services.AddHostedService<SourceMonitorService>();
            services.AddHostedService<OutboxPruner>();
        }

        return builder;
    }

    private static void TryAddSingletonTimeProvider(this IServiceCollection services)
    {
        if (!services.Any(static descriptor => descriptor.ServiceType == typeof(TimeProvider)))
        {
            services.AddSingleton(TimeProvider.System);
        }
    }

    private static CaptureSources CreateSources(IServiceProvider provider)
    {
        WalrusOptions options = provider.GetRequiredService<IOptions<WalrusOptions>>().Value;
        var masker = new ColumnMasker(Convert.FromBase64String(options.MaskingKey));
        Dictionary<string, TransactionStamper> stampers = new(StringComparer.Ordinal);

        foreach (SourceOptions source in options.Sources)
        {
            Dictionary<string, TablePolicy> policies = new(StringComparer.Ordinal);

            foreach ((string table, TableOptions settings) in source.Tables)
            {
                policies[table] = new TablePolicy(
                    table,
                    settings.Conflict,
                    new Dictionary<string, ColumnMask>(settings.Masks, StringComparer.Ordinal));
            }

            stampers[source.Name] = new TransactionStamper(source.Name, policies, masker);
        }

        return new CaptureSources(stampers);
    }

    private static void AddWalrusTelemetry(this IHostApplicationBuilder builder)
    {
        builder.Logging.AddOpenTelemetry(logging =>
        {
            logging.IncludeFormattedMessage = true;
            logging.IncludeScopes = true;
        });

        builder.Services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService("walrus"))
            .WithMetrics(metrics => metrics
                .AddMeter(WalrusTelemetry.Name)
                .AddAspNetCoreInstrumentation()
                .AddRuntimeInstrumentation())
            .WithTracing(tracing => tracing
                .AddSource(WalrusTelemetry.Name)
                .AddAspNetCoreInstrumentation());

        // Exported only when an endpoint is configured, the standard way, so a laptop run has no collector to
        // start and a deployment needs one environment variable.
        if (!string.IsNullOrWhiteSpace(builder.Configuration["OTEL_EXPORTER_OTLP_ENDPOINT"]))
        {
            builder.Services.AddOpenTelemetry().UseOtlpExporter();
        }
    }
}

/// <summary>Refuses to start with a configuration that would fail later, or fail open.</summary>
internal sealed class WalrusOptionsValidator : IValidateOptions<WalrusOptions>
{
    private const int MinimumTokenLength = 24;

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, WalrusOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        List<string> problems = [];

        if (string.IsNullOrWhiteSpace(options.StoreConnection))
        {
            problems.Add("Walrus:StoreConnection is required.");
        }

        if (options.Sources.Count == 0)
        {
            problems.Add("At least one source is required under Walrus:Sources.");
        }

        foreach (SourceOptions source in options.Sources)
        {
            if (string.IsNullOrWhiteSpace(source.Name) || source.Hosts.Count == 0
                || string.IsNullOrWhiteSpace(source.Database) || string.IsNullOrWhiteSpace(source.Username))
            {
                problems.Add($"Source '{source.Name}' needs a name, at least one host, a database and a username.");
            }

            if (!source.HeartbeatTable.Contains('.', StringComparison.Ordinal))
            {
                problems.Add($"Source '{source.Name}' needs a qualified heartbeat table, like walrus.heartbeat.");
            }
        }

        if (options.Sources.Select(static source => source.Name).Distinct(StringComparer.Ordinal).Count() != options.Sources.Count)
        {
            problems.Add("Source names must be unique.");
        }

        if (!IsKey(options.MaskingKey))
        {
            problems.Add("Walrus:MaskingKey must be base64 for at least 32 bytes.");
        }

        if (options.WebhookAllowedHosts.Count > 0 && !IsKey(options.WebhookSigningKey))
        {
            problems.Add("Walrus:WebhookSigningKey must be base64 for at least 32 bytes when webhooks are allowed.");
        }

        if (options.ReadToken.Length < MinimumTokenLength)
        {
            problems.Add($"Walrus:ReadToken must be at least {MinimumTokenLength} characters.");
        }

        if (options.OperatorToken.Length is > 0 and < MinimumTokenLength)
        {
            problems.Add($"Walrus:OperatorToken must be at least {MinimumTokenLength} characters, or empty to disable changes.");
        }

        return problems.Count == 0 ? ValidateOptionsResult.Success : ValidateOptionsResult.Fail(problems);
    }

    private static bool IsKey(string value)
    {
        Span<byte> buffer = stackalloc byte[512];

        return Convert.TryFromBase64String(value, buffer, out int written) && written >= 32;
    }
}
