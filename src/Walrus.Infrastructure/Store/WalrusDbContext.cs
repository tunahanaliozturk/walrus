using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using NpgsqlTypes;

namespace Walrus.Infrastructure.Store;

/// <summary>
/// The store: the outbox every captured change lands in, each source's capture checkpoint, and the sinks with
/// their cursors and dead letters.
/// </summary>
/// <remarks>
/// Used directly by the adapters that need it, with no repository layer in between. Two paths leave LINQ on
/// purpose and say why where they do: the outbox write, which is a binary COPY inside the context's own
/// transaction because it is the hot path, and the capture lease, which is an advisory lock that has to live on
/// one connection for as long as the session does.
/// </remarks>
/// <param name="options">The context options.</param>
public sealed class WalrusDbContext(DbContextOptions<WalrusDbContext> options) : DbContext(options)
{
    /// <summary>Every captured change, in capture order.</summary>
    public DbSet<OutboxRow> Outbox => Set<OutboxRow>();

    /// <summary>One row per source: how far capture has got and which session owns it.</summary>
    public DbSet<CaptureStateRow> CaptureStates => Set<CaptureStateRow>();

    /// <summary>Registered sinks.</summary>
    public DbSet<SinkRow> Sinks => Set<SinkRow>();

    /// <summary>Each sink's position in each source.</summary>
    public DbSet<SinkCursorRow> SinkCursors => Set<SinkCursorRow>();

    /// <summary>Changes a sink could not apply.</summary>
    public DbSet<DeadLetterRow> DeadLetters => Set<DeadLetterRow>();

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);

        ConfigureOutbox(modelBuilder.Entity<OutboxRow>());
        ConfigureCaptureState(modelBuilder.Entity<CaptureStateRow>());
        ConfigureSinks(modelBuilder.Entity<SinkRow>(), modelBuilder.Entity<SinkCursorRow>(), modelBuilder.Entity<DeadLetterRow>());
    }

    private static void ConfigureOutbox(EntityTypeBuilder<OutboxRow> outbox)
    {
        outbox.ToTable("walrus_outbox", table =>
            table.HasCheckConstraint("ck_walrus_outbox_op", "op in ('c', 'u', 'd')"));

        outbox.HasKey(row => row.Seq);
        outbox.Property(row => row.Seq).UseIdentityAlwaysColumn();
        outbox.Property(row => row.Op).HasColumnType("char(1)");

        // json rather than jsonb. jsonb sorts object keys, and a row image's column order is part of what a sink
        // materialises. It is also cheaper to insert, and nothing ever queries inside these columns.
        outbox.Property(row => row.Key).HasColumnType("json");
        outbox.Property(row => row.Before).HasColumnType("json");
        outbox.Property(row => row.After).HasColumnType("json");
        outbox.Property(row => row.CapturedAt).HasDefaultValueSql("now()");

        // Reading the same transaction from the log twice produces the same three values. Capture already skips
        // transactions it has by position; this is the backstop that turns a bug there into a loud failure
        // instead of a duplicate every sink would have to absorb.
        outbox.HasIndex(row => new { row.Source, row.CommitLsn, row.Ordinal }).IsUnique();

        // Dispatch reads one source at a time in capture order.
        outbox.HasIndex(row => new { row.Source, row.Seq });

        // Pruning goes by age.
        outbox.HasIndex(row => row.CapturedAt);
    }

    private static void ConfigureCaptureState(EntityTypeBuilder<CaptureStateRow> state)
    {
        state.ToTable("walrus_capture_state");
        state.HasKey(row => row.Source);
        state.Property(row => row.Epoch).HasDefaultValue(0L);
        state.Property(row => row.LastHlc).HasDefaultValue(0L);
        state.Property(row => row.ConfirmedLsn).HasDefaultValueSql("'0/0'::pg_lsn");
        state.Property(row => row.UpdatedAt).HasDefaultValueSql("now()");
    }

    private static void ConfigureSinks(
        EntityTypeBuilder<SinkRow> sinks,
        EntityTypeBuilder<SinkCursorRow> cursors,
        EntityTypeBuilder<DeadLetterRow> letters)
    {
        sinks.ToTable("walrus_sinks", table =>
        {
            table.HasCheckConstraint("ck_walrus_sinks_id", "id ~ '^[a-z0-9][a-z0-9-]{0,62}$'");
            table.HasCheckConstraint("ck_walrus_sinks_kind", "kind in ('postgres', 'index', 'webhook')");
        });

        sinks.HasKey(row => row.Id);
        sinks.Property(row => row.CreatedAt).HasDefaultValueSql("now()");

        cursors.ToTable("walrus_sink_cursors");
        cursors.HasKey(row => new { row.SinkId, row.Source });
        cursors.Property(row => row.UpdatedAt).HasDefaultValueSql("now()");
        cursors.HasOne<SinkRow>().WithMany().HasForeignKey(row => row.SinkId).OnDelete(DeleteBehavior.Cascade);

        letters.ToTable("walrus_dead_letters");
        letters.HasKey(row => row.Id);
        letters.Property(row => row.Id).UseIdentityAlwaysColumn();
        letters.Property(row => row.Event).HasColumnType("json");
        letters.Property(row => row.DeadAt).HasDefaultValueSql("now()");
        letters.HasOne<SinkRow>().WithMany().HasForeignKey(row => row.SinkId).OnDelete(DeleteBehavior.Cascade);

        // The question dispatch asks on start: which rows have an unresolved letter, so that everything after it
        // for that row has to queue behind it.
        letters.HasIndex(row => new { row.SinkId, row.EntityKey }).HasFilter("resolved_at is null");
    }
}

/// <summary>One captured change.</summary>
public sealed class OutboxRow
{
    /// <summary>Capture order within a source.</summary>
    public long Seq { get; set; }

    /// <summary>The source.</summary>
    public required string Source { get; set; }

    /// <summary>The commit position of the change's transaction.</summary>
    public NpgsqlLogSequenceNumber CommitLsn { get; set; }

    /// <summary>The change's position in its transaction.</summary>
    public int Ordinal { get; set; }

    /// <summary>The source transaction id.</summary>
    public long Xid { get; set; }

    /// <summary>When the transaction committed at the source.</summary>
    public DateTimeOffset CommitTs { get; set; }

    /// <summary>The transaction's clock stamp.</summary>
    public long Hlc { get; set; }

    /// <summary>The qualified table.</summary>
    public required string TableName { get; set; }

    /// <summary>c, u or d.</summary>
    public char Op { get; set; }

    /// <summary>The primary key, as a JSON object.</summary>
    public required string Key { get; set; }

    /// <summary>The row before, as a JSON object.</summary>
    public string? Before { get; set; }

    /// <summary>The row after, as a JSON object.</summary>
    public string? After { get; set; }

    /// <summary>For a merge table, the columns the change wrote.</summary>
    public string[]? ChangedColumns { get; set; }

    /// <summary>When capture wrote it.</summary>
    public DateTimeOffset CapturedAt { get; set; }
}

/// <summary>A source's capture checkpoint.</summary>
public sealed class CaptureStateRow
{
    /// <summary>The source.</summary>
    public required string Source { get; set; }

    /// <summary>
    /// Incremented by every capture session as it starts. A session writes only while the epoch is still its
    /// own, so one that lost its lease without noticing cannot finish a write after its replacement started.
    /// </summary>
    public long Epoch { get; set; }

    /// <summary>The end of the last transaction in the outbox.</summary>
    public NpgsqlLogSequenceNumber ConfirmedLsn { get; set; }

    /// <summary>The highest clock stamp issued.</summary>
    public long LastHlc { get; set; }

    /// <summary>When the checkpoint last moved.</summary>
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>A registered sink.</summary>
public sealed class SinkRow
{
    /// <summary>The id.</summary>
    public required string Id { get; set; }

    /// <summary>postgres, index or webhook.</summary>
    public required string Kind { get; set; }

    /// <summary>The tables it receives.</summary>
    public required string[] Tables { get; set; }

    /// <summary>A connection name or a URL. Never a credential.</summary>
    public string? Target { get; set; }

    /// <summary>When it was registered.</summary>
    public DateTimeOffset CreatedAt { get; set; }
}

/// <summary>A sink's position in one source.</summary>
public sealed class SinkCursorRow
{
    /// <summary>The sink.</summary>
    public required string SinkId { get; set; }

    /// <summary>The source.</summary>
    public required string Source { get; set; }

    /// <summary>Everything up to here is applied or parked.</summary>
    public long LastSeq { get; set; }

    /// <summary>Changes committed before this are not delivered.</summary>
    public NpgsqlLogSequenceNumber StartLsn { get; set; }

    /// <summary>When it last moved.</summary>
    public DateTimeOffset UpdatedAt { get; set; }
}

/// <summary>A parked change.</summary>
public sealed class DeadLetterRow
{
    /// <summary>The id, which orders letters for one row.</summary>
    public long Id { get; set; }

    /// <summary>The sink.</summary>
    public required string SinkId { get; set; }

    /// <summary>The source.</summary>
    public required string Source { get; set; }

    /// <summary>The change's outbox position.</summary>
    public long Seq { get; set; }

    /// <summary>The row, table and key.</summary>
    public required string EntityKey { get; set; }

    /// <summary>The change as JSON.</summary>
    public required string Event { get; set; }

    /// <summary>Why it was parked, or why the last retry failed.</summary>
    public required string Error { get; set; }

    /// <summary>How many times it has been tried.</summary>
    public int Attempts { get; set; }

    /// <summary>When it was parked.</summary>
    public DateTimeOffset DeadAt { get; set; }

    /// <summary>When it was finally applied, or null while it is still parked.</summary>
    public DateTimeOffset? ResolvedAt { get; set; }
}
