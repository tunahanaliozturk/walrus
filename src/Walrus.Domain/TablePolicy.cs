using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Serialization;

namespace Walrus.Domain;

/// <summary>How changes from different sources to the same row are combined.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<ConflictMode>))]
public enum ConflictMode
{
    /// <summary>The newest write replaces the whole row.</summary>
    LastWriterWins = 0,

    /// <summary>Each column takes its newest write, so two sources changing different columns both survive.</summary>
    MergeColumns = 1,
}

/// <summary>What happens to a sensitive column before a change is stored anywhere.</summary>
[JsonConverter(typeof(JsonStringEnumConverter<ColumnMask>))]
public enum ColumnMask
{
    /// <summary>The value becomes SQL NULL.</summary>
    Null = 0,

    /// <summary>The value becomes a fixed marker, so a sink can tell a masked value from a missing one.</summary>
    Redact = 1,

    /// <summary>
    /// The value becomes a keyed hash of itself. Equal inputs give equal outputs, so a masked column can still
    /// be joined on or counted distinct, and nobody without the key can reverse it by hashing guesses.
    /// </summary>
    Hash = 2,
}

/// <summary>Everything configured about one captured table.</summary>
/// <param name="Table">The qualified name, <c>schema.table</c>.</param>
/// <param name="Conflict">How changes from different sources combine.</param>
/// <param name="Masks">Masked columns and how each is masked.</param>
public sealed record TablePolicy(string Table, ConflictMode Conflict, IReadOnlyDictionary<string, ColumnMask> Masks)
{
    /// <summary>The policy for a table nothing was configured for: whole-row last writer wins, nothing masked.</summary>
    /// <param name="table">The qualified table name.</param>
    public static TablePolicy Default(string table) =>
        new(table, ConflictMode.LastWriterWins, new Dictionary<string, ColumnMask>(StringComparer.Ordinal));
}

/// <summary>Applies column masks to row images, with the key that makes hashed values unguessable.</summary>
/// <remarks>
/// Masking happens at capture, before the outbox. A value that reaches the outbox reaches every sink, every
/// replay and every backup of the store, so masking any later would only hide it from some of them.
/// </remarks>
public sealed class ColumnMasker
{
    /// <summary>What a redacted value is replaced with.</summary>
    public const string RedactedMarker = "[redacted]";

    private readonly byte[] _key;

    /// <summary>Creates a masker.</summary>
    /// <param name="key">The HMAC key for <see cref="ColumnMask.Hash"/>, at least 32 bytes.</param>
    public ColumnMasker(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);

        if (key.Length < 32)
        {
            throw new ArgumentException("The masking key must be at least 32 bytes.", nameof(key));
        }

        _key = [.. key];
    }

    /// <summary>Masks the configured columns of an image, leaving every other column untouched.</summary>
    /// <param name="image">The image, or null.</param>
    /// <param name="masks">The masked columns.</param>
    public RowImage? Mask(RowImage? image, IReadOnlyDictionary<string, ColumnMask> masks)
    {
        ArgumentNullException.ThrowIfNull(masks);

        if (image is null || masks.Count == 0)
        {
            return image;
        }

        bool touched = false;
        var columns = new RowColumn[image.Columns.Count];

        for (int index = 0; index < columns.Length; index++)
        {
            RowColumn column = image.Columns[index];

            if (masks.TryGetValue(column.Name, out ColumnMask mask) && column.Value is not null)
            {
                columns[index] = column with { Value = Apply(mask, column.Value) };
                touched = true;
            }
            else
            {
                columns[index] = column;
            }
        }

        return touched ? new RowImage(columns) : image;
    }

    private string? Apply(ColumnMask mask, string value) => mask switch
    {
        ColumnMask.Null => null,
        ColumnMask.Redact => RedactedMarker,
        ColumnMask.Hash => Convert.ToHexStringLower(HMACSHA256.HashData(_key, Encoding.UTF8.GetBytes(value))),
        _ => throw new ArgumentOutOfRangeException(nameof(mask), mask, "Unknown column mask."),
    };
}
