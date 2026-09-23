using Microsoft.AspNetCore.Http.HttpResults;
using Walrus.Application.Dispatch;
using Walrus.Application.Sinks;
using Walrus.Domain;
using Walrus.Infrastructure.Sinks;
using Walrus.Infrastructure.Source;
using Walrus.Infrastructure.Telemetry;

namespace Walrus.Api;

/// <summary>The HTTP surface: status for anyone holding the read token, changes for the operator.</summary>
internal static class Endpoints
{
    public static void MapWalrus(this WebApplication app)
    {
        RouteGroupBuilder read = app.MapGroup("/v1").RequireReader();

        read.MapGet("/stats", StatsAsync);
        read.MapGet("/sinks", async (SinkSupervisor sinks, PipelineCounters counters, CancellationToken cancellationToken) =>
            TypedResults.Ok<IReadOnlyList<SinkResponse>>([.. (await sinks.StatusAsync(cancellationToken)).Select(status => ToResponse(status, counters))]));

        read.MapGet("/sinks/{id}", async Task<Results<Ok<SinkResponse>, NotFound>> (
            string id, SinkSupervisor sinks, PipelineCounters counters, CancellationToken cancellationToken) =>
            await sinks.StatusAsync(id, cancellationToken) is { } status
                ? TypedResults.Ok(ToResponse(status, counters))
                : TypedResults.NotFound());

        read.MapGet("/sinks/{id}/dead-letters", async (string id, IDeadLetterStore letters, int? limit, CancellationToken cancellationToken) =>
        {
            IReadOnlyList<DeadLetter> open = await letters.OpenAsync(id, null, Math.Clamp(limit ?? 100, 1, 1_000), cancellationToken);

            return TypedResults.Ok<IReadOnlyList<DeadLetterResponse>>([.. open.Select(static letter => new DeadLetterResponse(
                letter.Id,
                letter.Source,
                letter.Seq,
                letter.Change.Table,
                letter.Change.Operation,
                Columns(letter.Change.Key),
                letter.Error,
                letter.Attempts,
                letter.DeadAt))]);
        });

        read.MapGet("/sinks/{id}/search", Results<Ok<SearchResponse>, NotFound> (
            string id, string q, string? table, int? limit, IndexSinkRegistry indexes) =>
            indexes.Find(id) is { } index
                ? TypedResults.Ok(new SearchResponse([.. index.Search(q, table, Math.Clamp(limit ?? 20, 1, 200)).Select(static hit =>
                    new SearchHit(hit.Table, Columns(hit.Key), Columns(hit.Row)))]))
                : TypedResults.NotFound());

        read.MapGet("/conflicts", (LiveFeed feed) => TypedResults.Ok(feed.Recent(FeedKind.Conflict)));
        read.MapGet("/events", (LiveFeed feed, CancellationToken cancellationToken) =>
            TypedResults.ServerSentEvents(feed.SubscribeAsync(cancellationToken)));

        RouteGroupBuilder operate = app.MapGroup("/v1").RequireOperator();

        operate.MapPost("/sinks", RegisterAsync);
        operate.MapDelete("/sinks/{id}", async (string id, SinkSupervisor sinks, CancellationToken cancellationToken) =>
            ToResult(await sinks.DeleteAsync(id, cancellationToken)));

        operate.MapPost("/sinks/{id}/replay", async (string id, string? fromLsn, bool? reset, SinkSupervisor sinks, CancellationToken cancellationToken) =>
        {
            Lsn from = Lsn.Zero;

            if (fromLsn is not null && !Lsn.TryParse(fromLsn, out from))
            {
                return ToResult(SinkOperationResult.Invalid($"'{fromLsn}' is not a log position like 0/16B3748."));
            }

            return ToResult(await sinks.ReplayAsync(id, from, reset ?? false, cancellationToken));
        });

        operate.MapPost("/sinks/{id}/snapshot", async (string id, SinkSupervisor sinks, CancellationToken cancellationToken) =>
            ToResult(await sinks.SnapshotAsync(id, cancellationToken)));

        operate.MapPost("/sinks/{id}/dead-letters/retry", async Task<Results<Ok<RetryResponse>, NotFound>> (
            string id, SinkSupervisor sinks, CancellationToken cancellationToken) =>
            await sinks.RetryDeadLettersAsync(id, cancellationToken) is { } result
                ? TypedResults.Ok(new RetryResponse(result.Resolved, result.StillBlocked))
                : TypedResults.NotFound());
    }

    private static async Task<IResult> RegisterAsync(RegisterSinkRequest request, SinkSupervisor sinks, CancellationToken cancellationToken)
    {
        Lsn? start = null;

        if (request.StartLsn is not null)
        {
            if (!Lsn.TryParse(request.StartLsn, out Lsn parsed))
            {
                return ToResult(SinkOperationResult.Invalid($"'{request.StartLsn}' is not a log position like 0/16B3748."));
            }

            start = parsed;
        }

        var sink = new SinkDefinition(request.Id, request.Kind, request.Tables ?? [], request.Target);
        SinkOperationResult result = await sinks.RegisterAsync(new SinkRegistration(sink, request.Start, start), cancellationToken);

        return result.Outcome is SinkOperationOutcome.Done
            ? TypedResults.Created($"/v1/sinks/{sink.Id}")
            : ToResult(result);
    }

    private static async Task<Ok<StatsResponse>> StatsAsync(
        SinkSupervisor sinks,
        SourceMonitor monitor,
        CaptureSources sources,
        PipelineCounters counters,
        TimeProvider time,
        CancellationToken cancellationToken)
    {
        Dictionary<string, SourceHealth> health = monitor.All().ToDictionary(static health => health.Source, StringComparer.Ordinal);
        List<SourceResponse> sourceResponses = [];

        foreach (string source in sources.Names)
        {
            SourceCounters captured = counters.Source(source);
            SourceHealth? slot = health.GetValueOrDefault(source);

            sourceResponses.Add(new SourceResponse(
                source,
                captured.Host,
                slot?.Primary,
                slot?.WalRetainedBytes,
                slot?.ConfirmedFlushLsn,
                slot?.SlotActive ?? false,
                [.. (slot?.Standbys ?? []).Select(static standby => new StandbyResponse(standby.Host, standby.Synced, standby.ConfirmedFlushLsn))],
                captured.Changes,
                captured.Transactions,
                captured.Resent,
                captured.LastCommit,
                captured.LastError ?? slot?.Error,
                slot?.CheckedAt));
        }

        IReadOnlyList<SinkStatus> statuses = await sinks.StatusAsync(cancellationToken);

        return TypedResults.Ok(new StatsResponse(
            time.GetUtcNow(),
            sourceResponses,
            [.. statuses.Select(status => ToResponse(status, counters))]));
    }

    private static SinkResponse ToResponse(SinkStatus status, PipelineCounters counters)
    {
        SinkCounters sink = counters.Sink(status.Sink.Id);

        return new SinkResponse(
            status.Sink.Id,
            status.Sink.Kind,
            status.Sink.Tables,
            PublicTarget(status.Sink),
            status.State,
            status.BlockedRows,
            [.. status.Sources.Select(static source => new SinkSourceResponse(
                source.Source,
                source.AppliedSeq,
                source.HeadSeq,
                source.StartLsn.ToString(),
                source.PendingLsn?.ToString(),
                Math.Round(source.LagMilliseconds, 1)))],
            new SinkCountersResponse(sink.Changed, sink.Unchanged, sink.Conflicts, sink.DeadLetters, sink.Retries, sink.LastError));
    }

    // A webhook URL can carry a token in its query string, and anyone with the read token can list sinks.
    private static string? PublicTarget(SinkDefinition sink) =>
        sink.Kind is SinkKind.Webhook && Uri.TryCreate(sink.Target, UriKind.Absolute, out Uri? url)
            ? url.GetLeftPart(UriPartial.Path)
            : sink.Target;

    private static ColumnValue[] Columns(RowImage image) =>
        [.. image.Columns.Select(static column => new ColumnValue(column.Name, column.Value))];

    private static IResult ToResult(SinkOperationResult result) => result.Outcome switch
    {
        SinkOperationOutcome.Done => TypedResults.NoContent(),
        SinkOperationOutcome.NotFound => TypedResults.NotFound(),
        SinkOperationOutcome.Conflict => TypedResults.Problem(statusCode: StatusCodes.Status409Conflict, title: "A sink with that id already exists."),
        _ => TypedResults.ValidationProblem(new Dictionary<string, string[]> { ["sink"] = [.. result.Problems] }),
    };
}
