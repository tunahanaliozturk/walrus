using Npgsql;
using Walrus.Api;
using Walrus.Infrastructure.Hosting;

WebApplicationBuilder builder = WebApplication.CreateSlimBuilder(args);

builder.Services.ConfigureHttpJsonOptions(options => options.SerializerOptions.TypeInfoResolverChain.Insert(0, ApiJson.Default));
builder.Services.AddProblemDetails();
builder.Services.AddOpenApi();
builder.AddWalrus();

WebApplication app = builder.Build();

app.UseExceptionHandler();
app.UseStatusCodePages();
app.MapOpenApi();

app.MapGet("/health/live", () => Results.Ok());

// Ready means the store answers. Capture and the sinks are deliberately left out: during a source failover
// capture reconnects for a few seconds, and pulling the API out of the load balancer for that would hide the
// status page exactly when someone wants to look at it.
app.MapGet("/health/ready", async (NpgsqlDataSource store, CancellationToken cancellationToken) =>
{
    try
    {
        await using NpgsqlCommand ping = store.CreateCommand("select 1");
        await ping.ExecuteScalarAsync(cancellationToken);

        return Results.Ok();
    }
    catch (NpgsqlException)
    {
        return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
});

app.MapWalrus();

await app.RunAsync();
