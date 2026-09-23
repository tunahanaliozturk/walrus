using System.Collections.Concurrent;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using Walrus.Application.Capture;
using Walrus.Domain;
using Walrus.Infrastructure;
using Walrus.Infrastructure.Source;
using Walrus.Infrastructure.Store;
using Walrus.Infrastructure.Telemetry;

namespace Walrus.IntegrationTests;

/// <summary>
/// A third source, <c>lab</c>, that the running host does not capture, so a test can drive capture sessions by
/// hand: start them, kill them between two steps, run two at once.
/// </summary>
internal sealed class Lab : IAsyncDisposable
{
    private readonly TestRig _rig;
    private readonly NpgsqlDataSource _store;
    private readonly List<(Task<bool> Run, CancellationTokenSource Stop)> _sessions = [];

    public Lab(TestRig rig, IReadOnlyDictionary<string, TablePolicy>? policies = null)
    {
        _rig = rig;
        _store = NpgsqlDataSource.Create(rig.ConnectionString("walrus"));

        var options = new WalrusOptions { StoreConnection = rig.ConnectionString("walrus") };
        var source = new SourceOptions
        {
            Name = "lab",
            Database = "lab",
            Username = "walrus_capture",
            Password = TestRig.CapturePassword,
        };

        source.Hosts.Add(rig.Node);
        options.Sources.Add(source);

        Contexts = rig.Services.GetRequiredService<IDbContextFactory<WalrusDbContext>>();
        Store = new CaptureStore(_store, Contexts, TimeProvider.System);
        Logs = new PgOutputLogFactory(new SourceConnections(Options.Create(options)), new PipelineCounters(), NullLoggerFactory.Instance);
        Stamper = new TransactionStamper(
            "lab",
            policies ?? new Dictionary<string, TablePolicy>(StringComparer.Ordinal),
            new ColumnMasker(Convert.FromBase64String(rig.MaskingKey)));
    }

    public IDbContextFactory<WalrusDbContext> Contexts { get; }

    public CaptureStore Store { get; }

    public PgOutputLogFactory Logs { get; }

    public TransactionStamper Stamper { get; }

    public Observer Seen { get; } = new();

    public CaptureSession Session(ICaptureStore? store = null, ISourceLogFactory? logs = null) =>
        new("lab", store ?? Store, logs ?? Logs, Stamper, new OutboxSignal(), Seen, new CaptureOptions(),
            NullLogger<CaptureSession>.Instance);

    /// <summary>Starts a session in the background and waits until it is streaming from the slot.</summary>
    /// <remarks>Every session started here is stopped when the lab is disposed, so a failing test cannot leave
    /// one holding the lease for the next.</remarks>
    public async Task<(Task<bool> Run, CancellationTokenSource Stop)> StartAsync(ICaptureStore? store = null, ISourceLogFactory? logs = null)
    {
        var stop = new CancellationTokenSource();
        Task<bool> run = Session(store, logs).RunAsync(stop.Token);
        _sessions.Add((run, stop));

        await TestRig.WaitForAsync(
            async () => run.IsCompleted || await SlotActiveAsync(),
            TimeSpan.FromSeconds(30),
            "the lab session to attach to its slot");

        return (run, stop);
    }

    public async Task<bool> SlotActiveAsync()
    {
        await using NpgsqlConnection connection = await _rig.OpenAsync("lab");
        await using var command = new NpgsqlCommand("select coalesce(bool_or(active), false) from pg_replication_slots where slot_name = 'walrus_lab'", connection);

        return (bool)(await command.ExecuteScalarAsync())!;
    }

    /// <summary>The lab's outbox rows for one table, in capture order.</summary>
    public async Task<IReadOnlyList<OutboxRow>> OutboxAsync(string table)
    {
        await using WalrusDbContext db = await Contexts.CreateDbContextAsync();
        string qualified = $"public.{table}";

        return await db.Outbox.AsNoTracking()
            .Where(row => row.Source == "lab" && row.TableName == qualified)
            .OrderBy(row => row.Seq)
            .ToListAsync();
    }

    /// <summary>The slot's acknowledged position, as the source sees it.</summary>
    public async Task<Lsn> SlotConfirmedAsync()
    {
        await using NpgsqlConnection connection = await _rig.OpenAsync("lab");
        await using var command = new NpgsqlCommand("select confirmed_flush_lsn::text from pg_replication_slots where slot_name = 'walrus_lab'", connection);

        return Lsn.Parse((string)(await command.ExecuteScalarAsync())!);
    }

    /// <summary>The end of the last transaction the store holds for the lab.</summary>
    public async Task<Lsn> CheckpointAsync()
    {
        await using WalrusDbContext db = await Contexts.CreateDbContextAsync();
        CaptureStateRow state = await db.CaptureStates.AsNoTracking().SingleAsync(row => row.Source == "lab");

        return new Lsn((ulong)state.ConfirmedLsn);
    }

    public async ValueTask DisposeAsync()
    {
        foreach ((Task<bool> run, CancellationTokenSource stop) in _sessions)
        {
            if (!stop.IsCancellationRequested)
            {
                try
                {
                    await stop.CancelAsync();
                }
                catch (ObjectDisposedException)
                {
                }
            }

            await ((Task)run).WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        await _store.DisposeAsync();
    }

    /// <summary>Records what capture reported.</summary>
    public sealed class Observer : ICaptureObserver
    {
        private int _resent;

        public int Resent => _resent;

        public ConcurrentQueue<ChangeEvent> Persisted { get; } = new();

        void ICaptureObserver.Persisted(string source, IReadOnlyList<ChangeEvent> changes, int transactions)
        {
            foreach (ChangeEvent change in changes)
            {
                Persisted.Enqueue(change);
            }
        }

        void ICaptureObserver.SkippedDuplicate(string source, DecodedTransaction transaction) =>
            Interlocked.Increment(ref _resent);
    }
}
