using System.Net.Http.Headers;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;
using Walrus.Application.Sinks;

namespace Walrus.IntegrationTests;

/// <summary>
/// One Postgres 18 cluster with logical decoding on, holding two source databases, the store and a sink
/// database, and the real Walrus host running against it.
/// </summary>
/// <remarks>
/// Tests share the cluster and the host but not tables: each test creates its own tables, adds them to the
/// sources' publication and registers its own sinks, so one test's rows never show up in another's
/// comparison. Capture and dispatch keep running across tests the way they would in production.
/// </remarks>
public sealed class TestRig : IAsyncLifetime
{
    public const string ReadToken = "read-token-for-integration-tests-only";
    public const string OperatorToken = "operator-token-for-integration-tests-only";
    public const string CapturePassword = "capture";

    public static readonly string[] Sources = ["eu", "us"];

    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:18-alpine")
        .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=40", "-c", "max_wal_senders=40", "-c", "max_connections=300")
        .Build();

    private WebApplicationFactory<Program>? _factory;
    private int _tables;

    public string Node { get; private set; } = "";

    public WebApplicationFactory<Program> Factory => _factory ?? throw new InvalidOperationException("The rig has not started.");

    public IServiceProvider Services => Factory.Services;

    public string MaskingKey { get; } = Convert.ToBase64String(Enumerable.Range(1, 32).Select(static value => (byte)value).ToArray());

    public string SigningKey { get; } = Convert.ToBase64String(Enumerable.Range(100, 32).Select(static value => (byte)value).ToArray());

    public async ValueTask InitializeAsync()
    {
        await _postgres.StartAsync();

        // localhost resolves to ::1 first on Windows, and the container port is published on IPv4.
        string host = _postgres.Hostname is "localhost" ? "127.0.0.1" : _postgres.Hostname;
        Node = $"{host}:{_postgres.GetMappedPublicPort(5432)}";

        await using (NpgsqlConnection admin = await OpenAsync("postgres"))
        {
            await ExecuteAsync(admin, $"create role walrus_capture login replication password '{CapturePassword}'");

            foreach (string database in (string[])[.. Sources, "lab", "walrus", "sink"])
            {
                await ExecuteAsync(admin, $"create database {database}");
            }
        }

        foreach (string source in (string[])[.. Sources, "lab"])
        {
            await using NpgsqlConnection connection = await OpenAsync(source);
            await ExecuteAsync(connection, """
                create schema walrus;
                create table walrus.heartbeat (source text primary key, beat_at timestamptz not null);
                grant usage on schema walrus to walrus_capture;
                grant select, insert, update on walrus.heartbeat to walrus_capture;
                create publication walrus for table walrus.heartbeat with (publish = 'insert, update, delete');
                """);
        }

        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseSetting("Walrus:StoreConnection", ConnectionString("walrus"));
            builder.UseSetting("Walrus:SinkConnections:sink", ConnectionString("sink"));
            builder.UseSetting("Walrus:MaskingKey", MaskingKey);
            builder.UseSetting("Walrus:WebhookSigningKey", SigningKey);
            builder.UseSetting("Walrus:WebhookAllowedHosts:0", "127.0.0.1");
            builder.UseSetting("Walrus:ReadToken", ReadToken);
            builder.UseSetting("Walrus:OperatorToken", OperatorToken);
            builder.UseSetting("Walrus:Dispatch:Lanes", "4");

            for (int index = 0; index < Sources.Length; index++)
            {
                string prefix = $"Walrus:Sources:{index}";
                builder.UseSetting($"{prefix}:Name", Sources[index]);
                builder.UseSetting($"{prefix}:Hosts:0", Node);
                builder.UseSetting($"{prefix}:Database", Sources[index]);
                builder.UseSetting($"{prefix}:Username", "walrus_capture");
                builder.UseSetting($"{prefix}:Password", CapturePassword);
                builder.UseSetting($"{prefix}:HeartbeatInterval", "00:00:01");
                builder.UseSetting($"{prefix}:Tables:public.merged:Conflict", "MergeColumns");
            }

            builder.ConfigureLogging(logging => logging.ClearProviders().AddSimpleConsole().SetMinimumLevel(LogLevel.Warning));
        });

        // Starting the server starts capture, which creates both slots.
        _ = Factory.Services;
        await WaitForAsync(async () => await SlotsExistAsync(), TimeSpan.FromSeconds(60), "capture to create its slots");
    }

    public async ValueTask DisposeAsync()
    {
        if (_factory is not null)
        {
            await _factory.DisposeAsync();
        }

        await _postgres.DisposeAsync();
    }

    public string ConnectionString(string database) =>
        new NpgsqlConnectionStringBuilder(_postgres.GetConnectionString())
        {
            Host = Node[..Node.LastIndexOf(':')],
            Port = int.Parse(Node[(Node.LastIndexOf(':') + 1)..], System.Globalization.CultureInfo.InvariantCulture),
            Database = database,
        }.ConnectionString;

    public async Task<NpgsqlConnection> OpenAsync(string database)
    {
        var connection = new NpgsqlConnection(ConnectionString(database));
        await connection.OpenAsync();

        return connection;
    }

    public HttpClient Client(string? token = OperatorToken)
    {
        HttpClient client = Factory.CreateClient();

        if (token is not null)
        {
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }

        return client;
    }

    public SinkSupervisor Supervisor => Services.GetRequiredService<SinkSupervisor>();

    /// <summary>A table name no other test uses.</summary>
    public string NewTable(string prefix) => $"{prefix}_{Interlocked.Increment(ref _tables)}";

    /// <summary>
    /// Creates a table in the given source databases, captured with before images, and the same table in the
    /// sink database, and adds it to the publications.
    /// </summary>
    public async Task CreateTableAsync(string name, string columns, IEnumerable<string> sources, string? sinkColumns = null)
    {
        foreach (string source in sources)
        {
            await using NpgsqlConnection connection = await OpenAsync(source);
            await ExecuteAsync(connection, $"""
                create table public.{name} ({columns});
                alter table public.{name} replica identity full;
                grant select on public.{name} to walrus_capture;
                alter publication walrus add table public.{name};
                """);
        }

        await using NpgsqlConnection sink = await OpenAsync("sink");
        await ExecuteAsync(sink, $"create table public.{name} ({sinkColumns ?? columns})");
    }

    public static async Task ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>Every row of a table as text, ordered by its first column, for comparing two databases.</summary>
    public async Task<IReadOnlyList<string>> RowsAsync(string database, string table)
    {
        await using NpgsqlConnection connection = await OpenAsync(database);
        await using var command = new NpgsqlCommand($"select t::text from public.{table} t order by 1", connection);
        await using NpgsqlDataReader reader = await command.ExecuteReaderAsync();
        List<string> rows = [];

        while (await reader.ReadAsync())
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    public static async Task WaitForAsync(Func<Task<bool>> condition, TimeSpan timeout, string what)
    {
        DateTime deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"Timed out after {timeout.TotalSeconds} s waiting for {what}.");
    }

    /// <summary>
    /// Waits until capture has persisted everything the sources had committed when this was called. The
    /// heartbeat writes to every source each second, so the checkpoint moves past this point even when the
    /// tables a test wrote to have gone quiet.
    /// </summary>
    public async Task WaitForCaptureAsync(params string[] sources)
    {
        foreach (string source in sources)
        {
            string now;

            await using (NpgsqlConnection connection = await OpenAsync(source))
            await using (var current = new NpgsqlCommand("select pg_current_wal_lsn()::text", connection))
            {
                now = (string)(await current.ExecuteScalarAsync())!;
            }

            await WaitForAsync(
                async () =>
                {
                    await using NpgsqlConnection store = await OpenAsync("walrus");
                    await using var confirmed = new NpgsqlCommand(
                        "select coalesce(bool_or(confirmed_lsn >= $2::pg_lsn), false) from walrus_capture_state where source = $1", store);
                    confirmed.Parameters.Add(new NpgsqlParameter { Value = source });
                    confirmed.Parameters.Add(new NpgsqlParameter { Value = now });

                    return (bool)(await confirmed.ExecuteScalarAsync())!;
                },
                TimeSpan.FromSeconds(60),
                $"capture of {source} to reach {now}");
        }
    }

    /// <summary>Waits until a table in the sink holds exactly what it holds in a source.</summary>
    public async Task WaitForSinkToMatchAsync(string table, string source = "eu", int seconds = 60) =>
        await WaitForAsync(
            async () => (await RowsAsync("sink", table)).SequenceEqual(await RowsAsync(source, table)),
            TimeSpan.FromSeconds(seconds),
            $"the sink's {table} to match {source}'s");

    private async Task<bool> SlotsExistAsync()
    {
        await using NpgsqlConnection connection = await OpenAsync("postgres");
        await using var command = new NpgsqlCommand("select count(*) from pg_replication_slots where slot_name in ('walrus_eu', 'walrus_us')", connection);

        return (long)(await command.ExecuteScalarAsync())! == 2;
    }
}

[CollectionDefinition(Name)]
public sealed class SharedRig : ICollectionFixture<TestRig>
{
    public const string Name = "rig";
}
