using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Mvc;
using Walrus.Application.Sinks;
using Walrus.Domain;
using Walrus.Infrastructure.Telemetry;

namespace Walrus.Api;

/// <summary>A request to register a sink.</summary>
/// <param name="Id">Lowercase letters, digits and hyphens.</param>
/// <param name="Kind">Postgres, Index or Webhook.</param>
/// <param name="Tables">The qualified source tables it receives.</param>
/// <param name="Target">A configured connection name for Postgres, a URL for a webhook, nothing for an index.</param>
/// <param name="Start">Beginning, Now, Lsn or Snapshot.</param>
/// <param name="StartLsn">The position, for <c>Lsn</c>, as Postgres prints it: 0/16B3748.</param>
public sealed record RegisterSinkRequest(
    string Id,
    SinkKind Kind,
    IReadOnlyList<string> Tables,
    string? Target,
    SinkStartMode Start,
    string? StartLsn);

/// <summary>A sink's position in one source.</summary>
/// <param name="Source">The source.</param>
/// <param name="AppliedSeq">Everything up to this outbox position is applied or parked.</param>
/// <param name="HeadSeq">The newest outbox position for the source.</param>
/// <param name="StartLsn">Nothing that committed before this is delivered.</param>
/// <param name="PendingLsn">Where the oldest unapplied change committed, if any.</param>
/// <param name="LagMilliseconds">How long ago the oldest unapplied change committed; zero when caught up.</param>
public sealed record SinkSourceResponse(
    string Source,
    long AppliedSeq,
    long HeadSeq,
    string StartLsn,
    string? PendingLsn,
    double LagMilliseconds);

/// <summary>What a sink has done since the service started.</summary>
/// <param name="Changed">Changes that moved its state.</param>
/// <param name="Unchanged">Changes it already had, or that a newer change had superseded.</param>
/// <param name="Conflicts">Changes that lost to a newer change from another source.</param>
/// <param name="DeadLetters">Changes it parked.</param>
/// <param name="Retries">Batches retried after a failure that may pass.</param>
/// <param name="LastError">The last such failure.</param>
public sealed record SinkCountersResponse(
    long Changed,
    long Unchanged,
    long Conflicts,
    long DeadLetters,
    long Retries,
    string? LastError);

/// <summary>A sink and where it is.</summary>
/// <param name="Id">The id.</param>
/// <param name="Kind">The kind.</param>
/// <param name="Tables">The tables it receives.</param>
/// <param name="Target">The connection name or the webhook URL without its query string.</param>
/// <param name="State">Running, Retrying, Rebuilding or Stopped.</param>
/// <param name="BlockedRows">Rows held behind a dead letter.</param>
/// <param name="Sources">Its position in each source.</param>
/// <param name="Counters">What it has done.</param>
public sealed record SinkResponse(
    string Id,
    SinkKind Kind,
    IReadOnlyList<string> Tables,
    string? Target,
    SinkState State,
    int BlockedRows,
    IReadOnlyList<SinkSourceResponse> Sources,
    SinkCountersResponse Counters);

/// <summary>A standby's copy of the capture slot.</summary>
/// <param name="Host">The standby.</param>
/// <param name="Synced">Whether a promotion would find the slot there.</param>
/// <param name="ConfirmedFlushLsn">How far its copy is confirmed.</param>
public sealed record StandbyResponse(string Host, bool Synced, string? ConfirmedFlushLsn);

/// <summary>A source: where capture is attached, the slot's health, and what capture has done.</summary>
/// <param name="Name">The source.</param>
/// <param name="AttachedTo">The node capture is streaming from, or null while it is reconnecting.</param>
/// <param name="Primary">The node that answered as primary at the last check.</param>
/// <param name="WalRetainedBytes">Log the source keeps for the slot.</param>
/// <param name="ConfirmedFlushLsn">The slot's acknowledged position.</param>
/// <param name="SlotActive">Whether a session is attached.</param>
/// <param name="Standbys">Each standby's copy of the slot.</param>
/// <param name="CaptureIsSynchronous">
/// Whether the source makes every commit wait for Walrus, because its synchronous standby setting matches capture's
/// connection. Almost always a misconfiguration.
/// </param>
/// <param name="Changes">Row changes captured since the service started.</param>
/// <param name="Transactions">Transactions captured, heartbeats included.</param>
/// <param name="Resent">Transactions the source sent again after a restart, recognised and skipped.</param>
/// <param name="LastCommit">When the newest captured change committed.</param>
/// <param name="LastError">Why capture last stopped, if it has.</param>
/// <param name="CheckedAt">When the slot was last read.</param>
public sealed record SourceResponse(
    string Name,
    string? AttachedTo,
    string? Primary,
    long? WalRetainedBytes,
    string? ConfirmedFlushLsn,
    bool SlotActive,
    IReadOnlyList<StandbyResponse> Standbys,
    bool CaptureIsSynchronous,
    long Changes,
    long Transactions,
    long Resent,
    DateTimeOffset? LastCommit,
    string? LastError,
    DateTimeOffset? CheckedAt);

/// <summary>Everything the console shows, in one request.</summary>
/// <param name="At">When this was read.</param>
/// <param name="Sources">The sources.</param>
/// <param name="Sinks">The sinks.</param>
public sealed record StatsResponse(DateTimeOffset At, IReadOnlyList<SourceResponse> Sources, IReadOnlyList<SinkResponse> Sinks);

/// <summary>A parked change.</summary>
/// <param name="Id">The letter.</param>
/// <param name="Source">The source.</param>
/// <param name="Seq">Its outbox position.</param>
/// <param name="Table">The table.</param>
/// <param name="Operation">What the change did.</param>
/// <param name="Key">The row's primary key.</param>
/// <param name="Error">Why it is parked.</param>
/// <param name="Attempts">How many times it has been tried.</param>
/// <param name="DeadAt">When it was parked.</param>
public sealed record DeadLetterResponse(
    long Id,
    string Source,
    long Seq,
    string Table,
    ChangeOperation Operation,
    IReadOnlyList<ColumnValue> Key,
    string Error,
    int Attempts,
    DateTimeOffset DeadAt);

/// <summary>One column of a row, in the order the table declares it.</summary>
/// <param name="Name">The column.</param>
/// <param name="Value">Its value as Postgres prints it, or null.</param>
public sealed record ColumnValue(string Name, string? Value);

/// <summary>The result of retrying a sink's parked rows.</summary>
/// <param name="Resolved">Rows that applied and are flowing again.</param>
/// <param name="StillBlocked">Rows the sink refused again.</param>
public sealed record RetryResponse(int Resolved, int StillBlocked);

/// <summary>A row an index sink found.</summary>
/// <param name="Table">The table.</param>
/// <param name="Key">The primary key.</param>
/// <param name="Row">The row.</param>
public sealed record SearchHit(string Table, IReadOnlyList<ColumnValue> Key, IReadOnlyList<ColumnValue> Row);

/// <summary>An index sink's search results.</summary>
/// <param name="Rows">The rows, ordered by table and key.</param>
public sealed record SearchResponse(IReadOnlyList<SearchHit> Rows);

/// <summary>Source-generated JSON for every request and response.</summary>
[JsonSourceGenerationOptions(JsonSerializerDefaults.Web, UseStringEnumConverter = true)]
[JsonSerializable(typeof(RegisterSinkRequest))]
[JsonSerializable(typeof(SinkResponse))]
[JsonSerializable(typeof(IReadOnlyList<SinkResponse>))]
[JsonSerializable(typeof(IReadOnlyList<SourceResponse>))]
[JsonSerializable(typeof(StatsResponse))]
[JsonSerializable(typeof(IReadOnlyList<DeadLetterResponse>))]
[JsonSerializable(typeof(RetryResponse))]
[JsonSerializable(typeof(SearchResponse))]
[JsonSerializable(typeof(FeedEntry))]
[JsonSerializable(typeof(IReadOnlyList<FeedEntry>))]
[JsonSerializable(typeof(ProblemDetails))]
[JsonSerializable(typeof(HttpValidationProblemDetails))]
internal sealed partial class ApiJson : JsonSerializerContext;
