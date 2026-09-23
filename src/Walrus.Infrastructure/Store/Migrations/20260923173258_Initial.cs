using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;
using NpgsqlTypes;

#nullable disable

namespace Walrus.Infrastructure.Store.Migrations
{
    /// <inheritdoc />
    public partial class Initial : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "walrus_capture_state",
                columns: table => new
                {
                    source = table.Column<string>(type: "text", nullable: false),
                    epoch = table.Column<long>(type: "bigint", nullable: false, defaultValue: 0L),
                    confirmed_lsn = table.Column<NpgsqlLogSequenceNumber>(type: "pg_lsn", nullable: false, defaultValueSql: "'0/0'::pg_lsn"),
                    last_hlc = table.Column<long>(type: "bigint", nullable: false, defaultValue: 0L),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_walrus_capture_state", x => x.source);
                });

            migrationBuilder.CreateTable(
                name: "walrus_outbox",
                columns: table => new
                {
                    seq = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityAlwaysColumn),
                    source = table.Column<string>(type: "text", nullable: false),
                    commit_lsn = table.Column<NpgsqlLogSequenceNumber>(type: "pg_lsn", nullable: false),
                    ordinal = table.Column<int>(type: "integer", nullable: false),
                    xid = table.Column<long>(type: "bigint", nullable: false),
                    commit_ts = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false),
                    hlc = table.Column<long>(type: "bigint", nullable: false),
                    table_name = table.Column<string>(type: "text", nullable: false),
                    op = table.Column<char>(type: "char(1)", nullable: false),
                    key = table.Column<string>(type: "json", nullable: false),
                    before = table.Column<string>(type: "json", nullable: true),
                    after = table.Column<string>(type: "json", nullable: true),
                    changed_columns = table.Column<string[]>(type: "text[]", nullable: true),
                    captured_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_walrus_outbox", x => x.seq);
                    table.CheckConstraint("ck_walrus_outbox_op", "op in ('c', 'u', 'd')");
                });

            migrationBuilder.CreateTable(
                name: "walrus_sinks",
                columns: table => new
                {
                    id = table.Column<string>(type: "text", nullable: false),
                    kind = table.Column<string>(type: "text", nullable: false),
                    tables = table.Column<string[]>(type: "text[]", nullable: false),
                    target = table.Column<string>(type: "text", nullable: true),
                    created_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_walrus_sinks", x => x.id);
                    table.CheckConstraint("ck_walrus_sinks_id", "id ~ '^[a-z0-9][a-z0-9-]{0,62}$'");
                    table.CheckConstraint("ck_walrus_sinks_kind", "kind in ('postgres', 'index', 'webhook')");
                });

            migrationBuilder.CreateTable(
                name: "walrus_dead_letters",
                columns: table => new
                {
                    id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityAlwaysColumn),
                    sink_id = table.Column<string>(type: "text", nullable: false),
                    source = table.Column<string>(type: "text", nullable: false),
                    seq = table.Column<long>(type: "bigint", nullable: false),
                    entity_key = table.Column<string>(type: "text", nullable: false),
                    @event = table.Column<string>(name: "event", type: "json", nullable: false),
                    error = table.Column<string>(type: "text", nullable: false),
                    attempts = table.Column<int>(type: "integer", nullable: false),
                    dead_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()"),
                    resolved_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_walrus_dead_letters", x => x.id);
                    table.ForeignKey(
                        name: "fk_walrus_dead_letters_walrus_sinks_sink_id",
                        column: x => x.sink_id,
                        principalTable: "walrus_sinks",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "walrus_sink_cursors",
                columns: table => new
                {
                    sink_id = table.Column<string>(type: "text", nullable: false),
                    source = table.Column<string>(type: "text", nullable: false),
                    last_seq = table.Column<long>(type: "bigint", nullable: false),
                    start_lsn = table.Column<NpgsqlLogSequenceNumber>(type: "pg_lsn", nullable: false),
                    updated_at = table.Column<DateTimeOffset>(type: "timestamp with time zone", nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("pk_walrus_sink_cursors", x => new { x.sink_id, x.source });
                    table.ForeignKey(
                        name: "fk_walrus_sink_cursors_walrus_sinks_sink_id",
                        column: x => x.sink_id,
                        principalTable: "walrus_sinks",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "ix_walrus_dead_letters_sink_id_entity_key",
                table: "walrus_dead_letters",
                columns: new[] { "sink_id", "entity_key" },
                filter: "resolved_at is null");

            migrationBuilder.CreateIndex(
                name: "ix_walrus_outbox_captured_at",
                table: "walrus_outbox",
                column: "captured_at");

            migrationBuilder.CreateIndex(
                name: "ix_walrus_outbox_source_commit_lsn_ordinal",
                table: "walrus_outbox",
                columns: new[] { "source", "commit_lsn", "ordinal" },
                unique: true);

            migrationBuilder.CreateIndex(
                name: "ix_walrus_outbox_source_seq",
                table: "walrus_outbox",
                columns: new[] { "source", "seq" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "walrus_capture_state");

            migrationBuilder.DropTable(
                name: "walrus_dead_letters");

            migrationBuilder.DropTable(
                name: "walrus_outbox");

            migrationBuilder.DropTable(
                name: "walrus_sink_cursors");

            migrationBuilder.DropTable(
                name: "walrus_sinks");
        }
    }
}
