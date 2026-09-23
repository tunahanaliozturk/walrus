using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Walrus.Domain;

/// <summary>The newest value one column has been given, and by which change.</summary>
/// <param name="Version">The change that wrote it.</param>
/// <param name="Value">The value, which may be null.</param>
public readonly record struct ColumnRegister(ChangeVersion Version, string? Value);

/// <summary>The result of applying one change to a row's state.</summary>
/// <param name="State">The state afterwards.</param>
/// <param name="Changed">
/// Whether anything moved. False for a change the sink has already seen, and for one that arrived after a
/// newer change had already superseded it. Either way the sink has nothing to write.
/// </param>
public readonly record struct KeyApplication(KeyState State, bool Changed);

/// <summary>
/// Everything a sink needs to remember about one row so that changes can arrive twice, late, or from two
/// databases at once, and every sink still ends up holding the same thing.
/// </summary>
/// <remarks>
/// <para>
/// The state is built entirely from maximums: the newest delete, the newest write, and for each column the
/// newest write that touched it. A maximum does not care what order its inputs arrive in, gives the same answer
/// when an input arrives twice, and gives the same answer however the inputs are grouped. Those three
/// properties are the whole of the convergence argument, and they are tested as properties rather than
/// illustrated with examples.
/// </para>
/// <para>
/// What a row materialises to is defined against the set of changes, not against the order they were applied
/// in. A row exists when its newest write is newer than its newest delete. Each column takes its value from the
/// newest live write that actually changed that column, and falls back to the newest write's full image when
/// none did. For an ordinary table every write changes every column it carries, so this collapses to plain
/// last-writer-wins on the whole row. For a table configured to merge columns, two sources updating different
/// columns of the same row both keep their change.
/// </para>
/// <para>
/// Anything older than the newest delete is discarded as soon as the delete is known. That looks like a space
/// optimisation and is not one. <see cref="Materialize"/> treats every register it holds as live, so a register
/// left behind by a write the delete superseded would leak that write's value back into a resurrected row, and
/// only when the write happened to arrive before the delete. Deliberately removing the pruning makes both the
/// convergence property and the reference comparison fail, which is how this paragraph came to be corrected.
/// </para>
/// </remarks>
[JsonConverter(typeof(KeyStateJsonConverter))]
public sealed class KeyState : IEquatable<KeyState>
{
    private KeyState(
        ChangeVersion? deleteVersion,
        ChangeVersion? newestVersion,
        RowImage? newestImage,
        ImmutableSortedDictionary<string, ColumnRegister> registers)
    {
        DeleteVersion = deleteVersion;
        NewestVersion = newestVersion;
        NewestImage = newestImage;
        Registers = registers;
    }

    /// <summary>A row nothing has been heard about.</summary>
    public static KeyState Empty { get; } =
        new(null, null, null, ImmutableSortedDictionary.Create<string, ColumnRegister>(StringComparer.Ordinal));

    /// <summary>The newest delete, if the row has ever been deleted.</summary>
    public ChangeVersion? DeleteVersion { get; }

    /// <summary>The newest write that is still live.</summary>
    public ChangeVersion? NewestVersion { get; }

    /// <summary>The full row image carried by <see cref="NewestVersion"/>.</summary>
    public RowImage? NewestImage { get; }

    /// <summary>For each column, the newest live write that changed it.</summary>
    public ImmutableSortedDictionary<string, ColumnRegister> Registers { get; }

    /// <summary>Whether the row currently exists.</summary>
    public bool Exists => NewestVersion is not null;

    /// <summary>The newest change of any kind this state has absorbed.</summary>
    public ChangeVersion? LatestVersion => ChangeVersion.Max(DeleteVersion, NewestVersion);

    /// <summary>Applies one change.</summary>
    /// <param name="change">The change.</param>
    public KeyApplication Apply(ChangeEvent change)
    {
        ArgumentNullException.ThrowIfNull(change);

        return change.Operation is ChangeOperation.Delete
            ? ApplyDelete(change.Version)
            : ApplyWrite(change);
    }

    /// <summary>The row as it should exist in a sink, or null when it should not exist.</summary>
    public RowImage? Materialize()
    {
        if (NewestImage is null)
        {
            return null;
        }

        List<RowColumn> columns = new(NewestImage.Columns.Count + 2);

        foreach (RowColumn column in NewestImage.Columns)
        {
            // Every register left after pruning belongs to a live write, so a register always beats the
            // newest image. When the newest write did change the column the two agree anyway.
            columns.Add(Registers.TryGetValue(column.Name, out ColumnRegister register)
                ? new RowColumn(column.Name, register.Value)
                : column);
        }

        // A column an older live write set, which the newest write's image does not carry. Postgres leaves a
        // large unchanged value out of an update, and this is where the value it left out comes back from.
        foreach ((string name, ColumnRegister register) in Registers)
        {
            if (!NewestImage.Contains(name))
            {
                columns.Add(new RowColumn(name, register.Value));
            }
        }

        return new RowImage(columns);
    }

    /// <summary>The state as JSON, stable enough to compare two states byte for byte.</summary>
    public string ToJson() => JsonSerializer.Serialize(this, DomainJson.Default.KeyState);

    /// <summary>Reads a state written by <see cref="ToJson"/>.</summary>
    /// <param name="json">The JSON.</param>
    public static KeyState FromJson(string json) =>
        JsonSerializer.Deserialize(json, DomainJson.Default.KeyState)
        ?? throw new JsonException("A key state cannot be null.");

    /// <inheritdoc />
    public bool Equals(KeyState? other) =>
        other is not null && string.Equals(ToJson(), other.ToJson(), StringComparison.Ordinal);

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as KeyState);

    /// <inheritdoc />
    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(ToJson());

    internal static KeyState Restore(
        ChangeVersion? deleteVersion,
        ChangeVersion? newestVersion,
        RowImage? newestImage,
        IEnumerable<KeyValuePair<string, ColumnRegister>> registers) =>
        new(deleteVersion,
            newestVersion,
            newestImage,
            ImmutableSortedDictionary.CreateRange(StringComparer.Ordinal, registers));

    private KeyApplication ApplyDelete(ChangeVersion version)
    {
        if (DeleteVersion is { } existing && existing >= version)
        {
            return new KeyApplication(this, Changed: false);
        }

        bool newestSurvives = NewestVersion is { } newest && newest > version;

        ImmutableSortedDictionary<string, ColumnRegister> registers = newestSurvives
            ? Registers.RemoveRange(Registers.Where(pair => pair.Value.Version <= version).Select(pair => pair.Key))
            : Registers.Clear();

        return new KeyApplication(
            new KeyState(
                version,
                newestSurvives ? NewestVersion : null,
                newestSurvives ? NewestImage : null,
                registers),
            Changed: true);
    }

    private KeyApplication ApplyWrite(ChangeEvent change)
    {
        ChangeVersion version = change.Version;
        RowImage after = change.After
            ?? throw new ArgumentException($"A {change.Operation} carries no row image.", nameof(change));

        // Older than a delete already known. It can never become live, so it changes nothing.
        if (DeleteVersion is { } deleted && version <= deleted)
        {
            return new KeyApplication(this, Changed: false);
        }

        bool changed = false;
        ImmutableSortedDictionary<string, ColumnRegister>.Builder registers = Registers.ToBuilder();

        foreach (string column in change.ChangedColumns ?? after.Columns.Select(static column => column.Name))
        {
            if (!after.TryGetValue(column, out string? value))
            {
                // Named as changed but not carried, which happens for a large value Postgres did not resend.
                // Recording null here would be recording a value nobody wrote.
                continue;
            }

            if (!registers.TryGetValue(column, out ColumnRegister existing) || version > existing.Version)
            {
                registers[column] = new ColumnRegister(version, value);
                changed = true;
            }
        }

        bool newest = NewestVersion is not { } current || version > current;

        if (!changed && !newest)
        {
            return new KeyApplication(this, Changed: false);
        }

        return new KeyApplication(
            new KeyState(
                DeleteVersion,
                newest ? version : NewestVersion,
                newest ? after : NewestImage,
                registers.ToImmutable()),
            Changed: true);
    }
}

/// <summary>Reads and writes <see cref="KeyState"/> through a plain document shape.</summary>
public sealed class KeyStateJsonConverter : JsonConverter<KeyState>
{
    /// <inheritdoc />
    public override KeyState Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        KeyStateDocument document = JsonSerializer.Deserialize(ref reader, DomainJson.Default.KeyStateDocument)
            ?? throw new JsonException("A key state cannot be null.");

        return KeyState.Restore(
            document.DeleteVersion,
            document.NewestVersion,
            document.NewestImage,
            document.Registers.Select(static register =>
                KeyValuePair.Create(register.Column, new ColumnRegister(register.Version, register.Value))));
    }

    /// <inheritdoc />
    public override void Write(Utf8JsonWriter writer, KeyState value, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(value);

        JsonSerializer.Serialize(
            writer,
            new KeyStateDocument(
                value.DeleteVersion,
                value.NewestVersion,
                value.NewestImage,
                [.. value.Registers.Select(static pair =>
                    new ColumnRegisterDocument(pair.Key, pair.Value.Version, pair.Value.Value))]),
            DomainJson.Default.KeyStateDocument);
    }
}

/// <summary>The stored shape of a <see cref="KeyState"/>.</summary>
/// <param name="DeleteVersion">The newest delete.</param>
/// <param name="NewestVersion">The newest live write.</param>
/// <param name="NewestImage">Its full image.</param>
/// <param name="Registers">Per-column registers, ordered by column name.</param>
public sealed record KeyStateDocument(
    ChangeVersion? DeleteVersion,
    ChangeVersion? NewestVersion,
    RowImage? NewestImage,
    IReadOnlyList<ColumnRegisterDocument> Registers);

/// <summary>The stored shape of one column register.</summary>
/// <param name="Column">The column.</param>
/// <param name="Version">The change that wrote it.</param>
/// <param name="Value">The value.</param>
public sealed record ColumnRegisterDocument(string Column, ChangeVersion Version, string? Value);

/// <summary>Source-generated serialisation for everything the domain puts on the wire or on disk.</summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.Never)]
[JsonSerializable(typeof(KeyState))]
[JsonSerializable(typeof(KeyStateDocument))]
[JsonSerializable(typeof(ChangeEvent))]
[JsonSerializable(typeof(RowImage))]
public sealed partial class DomainJson : JsonSerializerContext;
