using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Options;
using Walrus.Infrastructure;

namespace Walrus.Api;

/// <summary>
/// Who is calling. Two credentials: the read token sees status, stats and the live feed, and the operator token
/// also registers, rebuilds and deletes sinks. Every route group says which it needs, and a request without it
/// never reaches a handler.
/// </summary>
internal static class Callers
{
    public static RouteGroupBuilder RequireReader(this RouteGroupBuilder group) =>
        group.AddEndpointFilter(async (context, next) =>
        {
            WalrusOptions options = context.HttpContext.RequestServices.GetRequiredService<IOptions<WalrusOptions>>().Value;
            string? presented = Bearer(context.HttpContext);

            return Matches(presented, options.ReadToken) || Matches(presented, options.OperatorToken)
                ? await next(context)
                : Unauthorized(context.HttpContext);
        });

    public static RouteGroupBuilder RequireOperator(this RouteGroupBuilder group) =>
        group.AddEndpointFilter(async (context, next) =>
        {
            string configured = context.HttpContext.RequestServices.GetRequiredService<IOptions<WalrusOptions>>().Value.OperatorToken;

            if (string.IsNullOrEmpty(configured))
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status403Forbidden,
                    title: "Changes are disabled because no operator token is configured.");
            }

            return Matches(Bearer(context.HttpContext), configured) ? await next(context) : Unauthorized(context.HttpContext);
        });

    /// <summary>
    /// Compares in constant time. Both sides are hashed first, so the comparison takes the same time whatever
    /// the lengths, and a wrong token's length leaks nothing either.
    /// </summary>
    private static bool Matches(string? presented, string configured) =>
        presented is not null
        && configured.Length > 0
        && CryptographicOperations.FixedTimeEquals(
            SHA256.HashData(Encoding.UTF8.GetBytes(presented)),
            SHA256.HashData(Encoding.UTF8.GetBytes(configured)));

    private static string? Bearer(HttpContext http)
    {
        string header = http.Request.Headers.Authorization.ToString();

        return header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) ? header[7..].Trim() : null;
    }

    private static IResult Unauthorized(HttpContext http)
    {
        http.Response.Headers.WWWAuthenticate = "Bearer";

        return Results.Problem(statusCode: StatusCodes.Status401Unauthorized, title: "A valid token for this endpoint is required.");
    }
}
