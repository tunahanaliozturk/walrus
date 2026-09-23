using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using Npgsql;

namespace Walrus.Load;

/// <summary>
/// Where the compose stack is, and the handful of things every scenario does to it. Defaults match compose.yaml,
/// so on a laptop no flags are needed.
/// </summary>
/// <remarks>
/// Addresses are 127.0.0.1, not localhost: on Windows .NET resolves localhost to ::1 first, Docker Desktop
/// publishes on IPv4, and the failed attempt costs tens of milliseconds a connection.
/// </remarks>
internal sealed class Stack : IAsyncDisposable
{
    public const string Password = "postgres-password-for-local-use-only";

    private readonly Dictionary<string, string> _options;

    public Stack(Dictionary<string, string> options)
    {
        _options = options;

        Api = new HttpClient(new SocketsHttpHandler { UseProxy = false })
        {
            BaseAddress = new Uri(Option("api", "http://127.0.0.1:5190")),
            Timeout = TimeSpan.FromMinutes(10),
        };

        Api.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Option("token", "walrus-operator-token-for-local-use-only"));

        // Both eu nodes, and only ever the primary: after a failover Npgsql finds the other one by itself.
        Eu = NpgsqlDataSource.Create(
            $"Host={Option("eu", "127.0.0.1:5433,127.0.0.1:5434")};Database=shop;Username=postgres;Password={Password};" +
            "Target Session Attributes=primary;Maximum Pool Size=64;Timeout=5;Command Timeout=60");

        Us = NpgsqlDataSource.Create($"Host={Option("us", "127.0.0.1:5435")};Database=shop;Username=postgres;Password={Password};Maximum Pool Size=64");
        Sink = NpgsqlDataSource.Create($"Host={Option("store", "127.0.0.1:5436")};Database=sink;Username=postgres;Password={Password};Command Timeout=300");
        Store = NpgsqlDataSource.Create($"Host={Option("store", "127.0.0.1:5436")};Database=walrus;Username=postgres;Password={Password};Command Timeout=300");
    }

    public HttpClient Api { get; }

    public NpgsqlDataSource Eu { get; }

    public NpgsqlDataSource Us { get; }

    public NpgsqlDataSource Sink { get; }

    public NpgsqlDataSource Store { get; }

    public string Option(string name, string fallback) => _options.GetValueOrDefault(name, fallback);

    public int Option(string name, int fallback) =>
        _options.TryGetValue(name, out string? value) ? int.Parse(value, CultureInfo.InvariantCulture) : fallback;

    /// <summary>
    /// Runs a query, retrying for a while if no node answers: straight after a failover, Npgsql's multi-host
    /// pool can still believe the new primary is a standby until it rechecks.
    /// </summary>
    public static async Task<T> ScalarAsync<T>(NpgsqlDataSource source, string sql)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await using NpgsqlCommand command = source.CreateCommand(sql);
                return (T)(await command.ExecuteScalarAsync())!;
            }
            catch (NpgsqlException) when (attempt < 60)
            {
                await Task.Delay(500);
            }
        }
    }

    public static async Task ExecuteAsync(NpgsqlDataSource source, string sql)
    {
        await using NpgsqlCommand command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>Registers a sink, replacing any earlier sink with the same id.</summary>
    public async Task RegisterAsync(string id, string kind, string[] tables, string? target, string start)
    {
        await Api.DeleteAsync($"/v1/sinks/{id}");

        HttpResponseMessage response = await Api.PostAsJsonAsync("/v1/sinks", new { id, kind, tables, target, start });

        if (!response.IsSuccessStatusCode)
        {
            throw new InvalidOperationException($"Registering {id} failed: {(int)response.StatusCode} {await response.Content.ReadAsStringAsync()}");
        }
    }

    /// <summary>Whether a sink has applied everything the outbox holds for it, in every source.</summary>
    public async Task<bool> CaughtUpAsync(string sink)
    {
        using JsonDocument status = JsonDocument.Parse(await Api.GetStringAsync($"/v1/sinks/{sink}"));

        return status.RootElement.GetProperty("state").GetString() == "Running"
            && status.RootElement.GetProperty("sources").EnumerateArray()
                .All(static source => source.GetProperty("pendingLsn").ValueKind == JsonValueKind.Null);
    }

    /// <summary>
    /// Waits until capture has persisted everything eu had committed when this was called, then until the sink
    /// has applied it.
    /// </summary>
    public async Task WaitForSinkAsync(string sink, TimeSpan timeout)
    {
        string position = await ScalarAsync<string>(Eu, "select pg_current_wal_lsn()::text");
        var deadline = Stopwatch.StartNew();

        while (!await ScalarAsync<bool>(Store, $"select coalesce(bool_or(confirmed_lsn >= '{position}'::pg_lsn), false) from walrus_capture_state where source = 'eu'"))
        {
            Ensure(deadline, timeout, "capture to catch up");
            await Task.Delay(200);
        }

        while (!await CaughtUpAsync(sink))
        {
            Ensure(deadline, timeout, $"sink {sink} to catch up");
            await Task.Delay(200);
        }
    }

    public static void Ensure(Stopwatch elapsed, TimeSpan timeout, string what)
    {
        if (elapsed.Elapsed > timeout)
        {
            throw new TimeoutException($"Gave up after {timeout.TotalSeconds:N0} s waiting for {what}.");
        }
    }

    /// <summary>Runs docker, failing loudly if it fails.</summary>
    public static async Task<string> DockerAsync(params string[] arguments)
    {
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        foreach (string argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        using Process process = Process.Start(start)!;
        string output = await process.StandardOutput.ReadToEndAsync();
        string error = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"docker {string.Join(' ', arguments)} failed: {error}");
        }

        return output.Trim();
    }

    public async ValueTask DisposeAsync()
    {
        Api.Dispose();
        await Eu.DisposeAsync();
        await Us.DisposeAsync();
        await Sink.DisposeAsync();
        await Store.DisposeAsync();
    }
}
