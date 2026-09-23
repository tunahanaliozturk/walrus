using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure;

/// <summary>
/// Everything configurable, bound from the <c>Walrus</c> section. Secrets (passwords, keys, tokens) are expected
/// from environment variables or a secret store, never from a committed file.
/// </summary>
public sealed class WalrusOptions
{
    /// <summary>The configuration section.</summary>
    public const string Section = "Walrus";

    /// <summary>The store database: outbox, checkpoints, sinks.</summary>
    public string StoreConnection { get; set; } = "";

    /// <summary>The databases to capture.</summary>
    public IList<SourceOptions> Sources { get; } = [];

    /// <summary>
    /// Connection strings a Postgres sink may name. A sink's definition holds the name, and only the name, so a
    /// read key that can list sinks cannot read a credential.
    /// </summary>
    public IDictionary<string, string> SinkConnections { get; } = new Dictionary<string, string>(StringComparer.Ordinal);

    /// <summary>
    /// Hosts a webhook sink may call. Anyone with the operator token can register a webhook, and without a list
    /// that is a way to make the service send requests to anything it can reach, internal networks included.
    /// </summary>
    public IList<string> WebhookAllowedHosts { get; } = [];

    /// <summary>The HMAC key webhook requests are signed with, base64, at least 32 bytes.</summary>
    public string WebhookSigningKey { get; set; } = "";

    /// <summary>The HMAC key for hashed column masks, base64, at least 32 bytes.</summary>
    public string MaskingKey { get; set; } = "";

    /// <summary>Required to register, change or delete sinks. Empty disables those endpoints.</summary>
    public string OperatorToken { get; set; } = "";

    /// <summary>Required to read status, stats and the live feed.</summary>
    public string ReadToken { get; set; } = "";

    /// <summary>How long the outbox keeps changes every durable sink has applied.</summary>
    public TimeSpan OutboxRetention { get; set; } = TimeSpan.FromDays(7);

    /// <summary>Capture tuning.</summary>
    public CaptureSettings Capture { get; set; } = new();

    /// <summary>Dispatch tuning.</summary>
    public DispatchSettings Dispatch { get; set; } = new();
}

/// <summary>One source database.</summary>
public sealed class SourceOptions
{
    /// <summary>The name changes carry, and the one a sink's status reports.</summary>
    public string Name { get; set; } = "";

    /// <summary>
    /// Every node that can be the primary, as <c>host:port</c>. Capture always attaches to whichever is primary
    /// now, so a failover is a reconnect, not a reconfiguration.
    /// </summary>
    public IList<string> Hosts { get; } = [];

    /// <summary>The database.</summary>
    public string Database { get; set; } = "";

    /// <summary>A role with REPLICATION and SELECT on the published tables, and nothing else.</summary>
    public string Username { get; set; } = "";

    /// <summary>Its password.</summary>
    public string Password { get; set; } = "";

    /// <summary>The publication naming the captured tables. Created by the database owner, not by Walrus.</summary>
    public string Publication { get; set; } = "walrus";

    /// <summary>The replication slot. Created by Walrus on first start, with failover enabled.</summary>
    public string Slot { get; set; } = "";

    /// <summary>
    /// A table in the publication that Walrus writes to every few seconds, so a source whose captured tables
    /// are quiet still produces something to acknowledge and the slot does not pin its log forever.
    /// </summary>
    public string HeartbeatTable { get; set; } = "walrus.heartbeat";

    /// <summary>How often the heartbeat is written.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Per-table settings, keyed by qualified name.</summary>
    public IDictionary<string, TableOptions> Tables { get; } = new Dictionary<string, TableOptions>(StringComparer.Ordinal);

    /// <summary>The slot name, defaulting to one derived from the source name.</summary>
    public string SlotName => string.IsNullOrEmpty(Slot) ? $"walrus_{Name.Replace('-', '_')}" : Slot;
}

/// <summary>Settings for one captured table.</summary>
public sealed class TableOptions
{
    /// <summary>How changes from different sources combine.</summary>
    public ConflictMode Conflict { get; set; } = ConflictMode.LastWriterWins;

    /// <summary>Masked columns.</summary>
    public IDictionary<string, ColumnMask> Masks { get; } = new Dictionary<string, ColumnMask>(StringComparer.Ordinal);
}

/// <summary>Capture tuning.</summary>
public sealed class CaptureSettings
{
    /// <summary>Changes per outbox transaction, before the rest wait for the next.</summary>
    public int MaxBatchChanges { get; set; } = 2_000;

    /// <summary>Decoded transactions that may wait for the outbox.</summary>
    public int PendingTransactions { get; set; } = 10_000;
}

/// <summary>Dispatch tuning.</summary>
public sealed class DispatchSettings
{
    /// <summary>Parallel lanes per sink.</summary>
    public int Lanes { get; set; } = 8;

    /// <summary>Changes per apply.</summary>
    public int MaxBatch { get; set; } = 500;

    /// <summary>Changes read from the outbox at once.</summary>
    public int ReadBatch { get; set; } = 2_000;

    /// <summary>The dispatch options these settings describe.</summary>
    public DispatchOptions ToOptions() => new()
    {
        Lanes = Lanes,
        MaxBatch = MaxBatch,
        ReadBatch = ReadBatch,
        LaneCapacity = Math.Max(MaxBatch * 4, 1_000),
    };
}
