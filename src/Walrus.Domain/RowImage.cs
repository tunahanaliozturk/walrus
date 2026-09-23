using System.Buffers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Walrus.Domain;

/// <summary>One column of a captured row, as the text Postgres sent for it.</summary>
/// <param name="Name">The column name.</param>
/// <param name="Value">The value in Postgres's text representation, or null for SQL NULL.</param>
public readonly record struct RowColumn(string Name, string? Value);

/// <summary>
/// A row as captured: column names and their text values, in column order.
/// </summary>
/// <remarks>
/// <para>
/// Values stay as the text Postgres sent rather than being parsed into .NET types. Every sink either writes
/// them back into Postgres, where the column's own input function parses them exactly as the source printed
/// them, or indexes them as text. Parsing in between would add a place for a timestamp to lose a time zone
/// or a numeric to lose a digit, and gain nothing.
/// </para>
/// <para>
/// A column can be absent, which is different from being null. Postgres does not resend a large value that an
/// update left untouched, and writing null in its place would destroy the value in every sink. An absent
/// column means "this change says nothing about it", and <see cref="KeyState"/> keeps whatever it already knew.
/// </para>
/// <para>
/// Lookup is a linear scan. Rows have tens of columns, not thousands, and a dictionary would cost more to
/// build than it saves.
/// </para>
/// </remarks>
[JsonConverter(typeof(RowImageJsonConverter))]
public sealed class RowImage : IEquatable<RowImage>
{
    private readonly RowColumn[] _columns;

    /// <summary>Creates a row image, rejecting duplicate column names.</summary>
    /// <param name="columns">The columns, in order.</param>
    public RowImage(IEnumerable<RowColumn> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        _columns = [.. columns];

        for (int index = 0; index < _columns.Length; index++)
        {
            for (int other = index + 1; other < _columns.Length; other++)
            {
                if (string.Equals(_columns[index].Name, _columns[other].Name, StringComparison.Ordinal))
                {
                    throw new ArgumentException($"Column '{_columns[index].Name}' appears twice.", nameof(columns));
                }
            }
        }
    }

    /// <summary>An image with no columns.</summary>
    public static RowImage Empty { get; } = new([]);

    /// <summary>The columns, in the order the source table declares them.</summary>
    public IReadOnlyList<RowColumn> Columns => _columns;

    /// <summary>Looks up a column.</summary>
    /// <param name="name">The column name.</param>
    /// <param name="value">Its value, which may be null.</param>
    /// <returns>Whether the image contains the column at all.</returns>
    public bool TryGetValue(string name, out string? value)
    {
        foreach (RowColumn column in _columns)
        {
            if (string.Equals(column.Name, name, StringComparison.Ordinal))
            {
                value = column.Value;

                return true;
            }
        }

        value = null;

        return false;
    }

    /// <summary>Whether the image contains a column, null or not.</summary>
    /// <param name="name">The column name.</param>
    public bool Contains(string name) => TryGetValue(name, out _);

    /// <summary>The image as a JSON object with string or null values, in column order.</summary>
    public string ToJson()
    {
        var buffer = new ArrayBufferWriter<byte>(64 + (_columns.Length * 24));

        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteTo(writer);
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Reads an image written by <see cref="ToJson"/>.</summary>
    /// <param name="json">The JSON object.</param>
    public static RowImage FromJson(string json)
    {
        ArgumentNullException.ThrowIfNull(json);

        var reader = new Utf8JsonReader(Encoding.UTF8.GetBytes(json));
        reader.Read();

        return ReadFrom(ref reader);
    }

    /// <inheritdoc />
    public bool Equals(RowImage? other) =>
        other is not null && _columns.AsSpan().SequenceEqual(other._columns);

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as RowImage);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();

        foreach (RowColumn column in _columns)
        {
            hash.Add(column);
        }

        return hash.ToHashCode();
    }

    /// <inheritdoc />
    public override string ToString() => ToJson();

    internal void WriteTo(Utf8JsonWriter writer)
    {
        writer.WriteStartObject();

        foreach (RowColumn column in _columns)
        {
            if (column.Value is null)
            {
                writer.WriteNull(column.Name);
            }
            else
            {
                writer.WriteString(column.Name, column.Value);
            }
        }

        writer.WriteEndObject();
    }

    internal static RowImage ReadFrom(ref Utf8JsonReader reader)
    {
        if (reader.TokenType is not JsonTokenType.StartObject)
        {
            throw new JsonException("A row image is a JSON object.");
        }

        List<RowColumn> columns = [];

        while (reader.Read() && reader.TokenType is not JsonTokenType.EndObject)
        {
            string name = reader.GetString() ?? throw new JsonException("A column name cannot be null.");
            reader.Read();

            string? value = reader.TokenType switch
            {
                JsonTokenType.Null => null,
                JsonTokenType.String => reader.GetString(),

                // Captured values are always text. A number here means the JSON was written by something
                // other than this service, and guessing how to print it back would be a guess about data.
                _ => throw new JsonException($"Column '{name}' must be a string or null."),
            };

            columns.Add(new RowColumn(name, value));
        }

        return new RowImage(columns);
    }
}

/// <summary>Reads and writes a <see cref="RowImage"/> as an ordered JSON object.</summary>
public sealed class RowImageJsonConverter : JsonConverter<RowImage>
{
    /// <inheritdoc />
    public override RowImage Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        RowImage.ReadFrom(ref reader);

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, RowImage value, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(value);

        value.WriteTo(writer);
    }
}
