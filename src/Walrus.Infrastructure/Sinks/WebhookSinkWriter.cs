using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Infrastructure.Sinks;

/// <summary>One change as a webhook receives it.</summary>
/// <param name="Id">Stable across redeliveries: deduplicate on this.</param>
/// <param name="Source">The source database.</param>
/// <param name="Table">The qualified table.</param>
/// <param name="Operation">insert, update or delete.</param>
/// <param name="CommitLsn">The source commit position.</param>
/// <param name="CommitTimestamp">When it committed.</param>
/// <param name="Hlc">The clock stamp, for ordering changes from different sources.</param>
/// <param name="Key">The primary key.</param>
/// <param name="Before">The row before, if the table sends it.</param>
/// <param name="After">The row after, or null for a delete.</param>
public sealed record WebhookChange(
    string Id,
    string Source,
    string Table,
    ChangeOperation Operation,
    Lsn CommitLsn,
    DateTimeOffset CommitTimestamp,
    long Hlc,
    RowImage Key,
    RowImage? Before,
    RowImage? After);

/// <summary>The body of a webhook request.</summary>
/// <param name="Sink">The sink that sent it.</param>
/// <param name="Changes">The changes, in order for each row.</param>
public sealed record WebhookPayload(string Sink, IReadOnlyList<WebhookChange> Changes);

/// <summary>Source-generated serialisation for webhook bodies.</summary>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(WebhookPayload))]
public sealed partial class WebhookJson : JsonSerializerContext;

/// <summary>
/// Posts changes to an HTTP endpoint, signed, at least once, in order for each row.
/// </summary>
/// <remarks>
/// <para>
/// The receiver owns its state, so this sink cannot make delivery exactly once; it makes deduplication easy
/// instead. Every change carries an id that is the same on every redelivery, and the body is signed with
/// <c>Walrus-Signature: t=unix-seconds,v1=hex(hmac-sha256(key, t + "." + body))</c>, so a receiver can reject a
/// forged request and, by checking <c>t</c>, a replayed one.
/// </para>
/// <para>
/// A 408, a 429 or any 5xx is the endpoint having a bad moment and is retried. Any other 4xx means it will never
/// accept this body, and the row is parked. Redirects are not followed: a redirect is a way to reach a host the
/// allow-list never approved.
/// </para>
/// </remarks>
internal sealed class WebhookSinkWriter(SinkDefinition sink, HttpClient http, byte[] signingKey, TimeProvider time) : ISinkWriter
{
    /// <inheritdoc />
    public bool IsDurable => true;

    /// <inheritdoc />
    public async Task<IReadOnlyList<ChangeOutcome>> ApplyAsync(IReadOnlyList<ChangeEvent> changes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(changes);

        var payload = new WebhookPayload(sink.Id, [.. changes.Select(static change => new WebhookChange(
            change.EventId,
            change.Source,
            change.Table,
            change.Operation,
            change.CommitLsn,
            change.CommitTimestamp,
            change.Hlc,
            change.Key,
            change.Before,
            change.After))]);

        byte[] body = JsonSerializer.SerializeToUtf8Bytes(payload, WebhookJson.Default.WebhookPayload);
        string timestamp = time.GetUtcNow().ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture);

        using var request = new HttpRequestMessage(HttpMethod.Post, sink.Target);
        request.Content = new ByteArrayContent(body);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        request.Headers.Add("Walrus-Signature", $"t={timestamp},v1={Sign(signingKey, timestamp, body)}");

        using HttpResponseMessage response = await http.SendAsync(request, cancellationToken);

        if (response.IsSuccessStatusCode)
        {
            return [.. changes.Select(static _ => new ChangeOutcome(true, null))];
        }

        if (response.StatusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests || (int)response.StatusCode >= 500)
        {
            throw new HttpRequestException($"The endpoint answered {(int)response.StatusCode}.", null, response.StatusCode);
        }

        throw new SinkRejectedException($"The endpoint answered {(int)response.StatusCode} and will not accept this change.");
    }

    /// <summary>Signs a body the way a receiver should verify it.</summary>
    /// <param name="key">The signing key.</param>
    /// <param name="timestamp">The <c>t</c> value.</param>
    /// <param name="body">The body.</param>
    public static string Sign(byte[] key, string timestamp, byte[] body)
    {
        ArgumentNullException.ThrowIfNull(timestamp);
        ArgumentNullException.ThrowIfNull(body);

        byte[] prefix = Encoding.UTF8.GetBytes(timestamp + ".");
        byte[] signed = new byte[prefix.Length + body.Length];
        prefix.CopyTo(signed, 0);
        body.CopyTo(signed, prefix.Length);

        return Convert.ToHexStringLower(HMACSHA256.HashData(key, signed));
    }

    /// <inheritdoc />
    /// <remarks>Nothing to reset: the receiver's state is the receiver's.</remarks>
    public Task ResetAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
