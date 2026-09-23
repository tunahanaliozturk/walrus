using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Walrus.Domain;

/// <summary>A position in a Postgres write-ahead log.</summary>
/// <remarks>
/// Kept as a plain 64-bit value here rather than Npgsql's own type, so the rules that decide what a sink ends
/// up holding can be tested without a database driver in the dependency graph. Postgres prints an LSN as two
/// hexadecimal halves separated by a slash, and that is the form it takes anywhere a person reads it.
/// </remarks>
/// <param name="Value">The raw position.</param>
[JsonConverter(typeof(LsnJsonConverter))]
public readonly record struct Lsn(ulong Value) : IComparable<Lsn>
{
    /// <summary>The start of the log, and the position that sorts before every real change.</summary>
    public static Lsn Zero { get; } = new(0);

    /// <summary>Parses the <c>16/B374D848</c> form Postgres uses.</summary>
    /// <param name="text">The printed position.</param>
    public static Lsn Parse(string text) =>
        TryParse(text, out Lsn lsn)
            ? lsn
            : throw new FormatException($"'{text}' is not a log sequence number. Expected the form 16/B374D848.");

    /// <summary>Parses the <c>16/B374D848</c> form Postgres uses, without throwing.</summary>
    /// <param name="text">The printed position.</param>
    /// <param name="lsn">The position, when parsing succeeded.</param>
    public static bool TryParse(string? text, out Lsn lsn)
    {
        lsn = Zero;

        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        int slash = text.IndexOf('/', StringComparison.Ordinal);

        if (slash <= 0 || slash == text.Length - 1)
        {
            return false;
        }

        if (!uint.TryParse(text.AsSpan(0, slash), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out uint high)
            || !uint.TryParse(text.AsSpan(slash + 1), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out uint low))
        {
            return false;
        }

        lsn = new Lsn(((ulong)high << 32) | low);

        return true;
    }

    /// <inheritdoc />
    public int CompareTo(Lsn other) => Value.CompareTo(other.Value);

    /// <summary>Whether this position is before another.</summary>
    /// <param name="left">The first position.</param>
    /// <param name="right">The second position.</param>
    public static bool operator <(Lsn left, Lsn right) => left.Value < right.Value;

    /// <summary>Whether this position is after another.</summary>
    /// <param name="left">The first position.</param>
    /// <param name="right">The second position.</param>
    public static bool operator >(Lsn left, Lsn right) => left.Value > right.Value;

    /// <summary>Whether this position is at or before another.</summary>
    /// <param name="left">The first position.</param>
    /// <param name="right">The second position.</param>
    public static bool operator <=(Lsn left, Lsn right) => left.Value <= right.Value;

    /// <summary>Whether this position is at or after another.</summary>
    /// <param name="left">The first position.</param>
    /// <param name="right">The second position.</param>
    public static bool operator >=(Lsn left, Lsn right) => left.Value >= right.Value;

    /// <inheritdoc />
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"{(uint)(Value >> 32):X}/{(uint)Value:X}");
}

/// <summary>Writes an LSN the way Postgres prints one.</summary>
public sealed class LsnJsonConverter : JsonConverter<Lsn>
{
    /// <inheritdoc />
    public override Lsn Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        Lsn.Parse(reader.GetString() ?? throw new JsonException("A log sequence number cannot be null."));

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, Lsn value, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(writer);

        writer.WriteStringValue(value.ToString());
    }
}
