using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Walrus.Application.Capture;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.UnitTests;

public sealed class WatermarkTests
{
    [Property(MaxTest = 500)]
    public Property The_mark_never_passes_a_position_that_has_not_finished() =>
        Prop.ForAll(
            Gen.Choose(1, 60).SelectMany(count => Gen.Shuffle(Enumerable.Range(0, count).ToArray()).Select(order => (count, order))).ToArbitrary(),
            scenario =>
            {
                // Positions with gaps, as a rolled-back capture transaction leaves them, finishing in any order.
                long[] positions = [.. Enumerable.Range(1, scenario.count).Select(static index => (long)index * 3)];
                var watermark = new ContiguousWatermark(0);

                foreach (long position in positions)
                {
                    watermark.Dispatched(position);
                }

                HashSet<long> finished = [];

                foreach (int index in scenario.order)
                {
                    watermark.Finished(positions[index]);
                    finished.Add(positions[index]);

                    long mark = watermark.Mark;

                    if (positions.Where(position => position <= mark).Any(position => !finished.Contains(position)))
                    {
                        return false.Label($"mark {mark} passed an unfinished position");
                    }
                }

                return (watermark.Mark == positions[^1]).Label("everything finished, so the mark is the last position");
            });

    [Fact]
    public void Positions_must_be_dispatched_in_order()
    {
        var watermark = new ContiguousWatermark(10);
        watermark.Dispatched(11);

        Should.Throw<InvalidOperationException>(() => watermark.Dispatched(11));
    }
}

public sealed class StampingTests
{
    private static readonly DateTimeOffset Commit = new(2026, 9, 23, 12, 0, 0, TimeSpan.Zero);

    private static readonly ColumnMasker Masker = new([.. Enumerable.Range(0, 32).Select(static value => (byte)value)]);

    private static RowImage Row(params (string Name, string? Value)[] columns) =>
        new(columns.Select(static column => new RowColumn(column.Name, column.Value)));

    [Fact]
    public void Every_change_in_a_transaction_shares_its_stamp_and_counts_up_from_zero()
    {
        var stamper = new TransactionStamper("eu", new Dictionary<string, TablePolicy>(), Masker);
        var clock = new HybridLogicalClock();

        IReadOnlyList<ChangeEvent> changes = stamper.Stamp(
            new DecodedTransaction(new Lsn(100), new Lsn(120), 7, Commit,
            [
                new DecodedChange("public.accounts", ChangeOperation.Insert, Row(("id", "1")), null, Row(("id", "1"), ("owner", "ada"))),
                new DecodedChange("public.accounts", ChangeOperation.Insert, Row(("id", "2")), null, Row(("id", "2"), ("owner", "grace"))),
            ]),
            clock);

        changes.Select(static change => change.Ordinal).ShouldBe([0, 1]);
        changes.Select(static change => change.Hlc).Distinct().ShouldHaveSingleItem().ShouldBe(clock.Last);
        changes.ShouldAllBe(change => change.CommitLsn == new Lsn(100) && change.Source == "eu");
    }

    [Fact]
    public void A_heartbeat_with_no_changes_does_not_advance_the_clock()
    {
        var stamper = new TransactionStamper("eu", new Dictionary<string, TablePolicy>(), Masker);
        var clock = new HybridLogicalClock();

        stamper.Stamp(new DecodedTransaction(new Lsn(1), new Lsn(2), 1, Commit, []), clock).ShouldBeEmpty();
        clock.Last.ShouldBe(0);
    }

    [Fact]
    public void An_update_that_changes_the_key_becomes_a_delete_and_an_insert()
    {
        var stamper = new TransactionStamper("eu", new Dictionary<string, TablePolicy>(), Masker);

        IReadOnlyList<ChangeEvent> changes = stamper.Stamp(
            new DecodedTransaction(new Lsn(1), new Lsn(2), 1, Commit,
            [
                new DecodedChange("public.accounts", ChangeOperation.Update, Row(("id", "3")),
                    Row(("id", "2"), ("owner", "grace")), Row(("id", "3"), ("owner", "grace")), Row(("id", "2"))),
            ]),
            new HybridLogicalClock());

        changes.Select(static change => (change.Operation, change.Key.ToJson(), change.Ordinal)).ShouldBe(
        [
            (ChangeOperation.Delete, """{"id":"2"}""", 0),
            (ChangeOperation.Insert, """{"id":"3"}""", 1),
        ]);
    }

    [Fact]
    public void A_merge_table_records_which_columns_an_update_actually_wrote()
    {
        var stamper = new TransactionStamper(
            "eu",
            new Dictionary<string, TablePolicy> { ["public.profiles"] = new("public.profiles", ConflictMode.MergeColumns, new Dictionary<string, ColumnMask>()) },
            Masker);

        ChangeEvent change = stamper.Stamp(
            new DecodedTransaction(new Lsn(1), new Lsn(2), 1, Commit,
            [
                new DecodedChange("public.profiles", ChangeOperation.Update, Row(("id", "1")),
                    Row(("id", "1"), ("city", "London"), ("plan", "free")),
                    Row(("id", "1"), ("city", "Paris"), ("plan", "free"))),
            ]),
            new HybridLogicalClock()).ShouldHaveSingleItem();

        change.ChangedColumns.ShouldBe(["city"]);
    }

    [Fact]
    public void A_key_column_can_only_be_masked_by_hashing()
    {
        var stamper = new TransactionStamper(
            "eu",
            new Dictionary<string, TablePolicy>
            {
                ["public.people"] = new("public.people", ConflictMode.LastWriterWins, new Dictionary<string, ColumnMask> { ["email"] = ColumnMask.Redact }),
            },
            Masker);

        Should.Throw<InvalidOperationException>(() => stamper.Stamp(
            new DecodedTransaction(new Lsn(1), new Lsn(2), 1, Commit,
            [
                new DecodedChange("public.people", ChangeOperation.Insert, Row(("email", "ada@example.com")), null, Row(("email", "ada@example.com"))),
            ]),
            new HybridLogicalClock()));
    }

    [Fact]
    public void Snapshot_rows_take_the_lowest_version_so_any_captured_change_replaces_them()
    {
        var stamper = new TransactionStamper("eu", new Dictionary<string, TablePolicy>(), Masker);

        ChangeEvent row = stamper.StampSnapshot(
            [new DecodedChange("public.accounts", ChangeOperation.Insert, Row(("id", "1")), null, Row(("id", "1"), ("owner", "ada")))])
            .ShouldHaveSingleItem();

        ChangeEvent later = stamper.Stamp(
            new DecodedTransaction(new Lsn(5), new Lsn(6), 1, Commit,
            [new DecodedChange("public.accounts", ChangeOperation.Update, Row(("id", "1")), null, Row(("id", "1"), ("owner", "ada lovelace")))]),
            new HybridLogicalClock()).ShouldHaveSingleItem();

        (later.Version > row.Version).ShouldBeTrue();
        KeyState.Empty.Apply(later).State.Apply(row).Changed.ShouldBeFalse();
    }

    [Fact]
    public void Masking_leaves_unmasked_and_missing_columns_alone()
    {
        RowImage masked = Masker.Mask(
            Row(("id", "1"), ("email", "ada@example.com"), ("phone", null)),
            new Dictionary<string, ColumnMask> { ["email"] = ColumnMask.Hash, ["phone"] = ColumnMask.Redact, ["absent"] = ColumnMask.Null })!;

        masked.TryGetValue("id", out string? id);
        masked.TryGetValue("email", out string? email);
        masked.TryGetValue("phone", out string? phone);

        id.ShouldBe("1");
        email!.Length.ShouldBe(64);
        email.ShouldNotContain("ada");
        phone.ShouldBeNull();
        masked.Contains("absent").ShouldBeFalse();
    }
}

public sealed class SinkDefinitionTests
{
    [Theory]
    [InlineData("orders", SinkKind.Postgres, "public.orders", "replica", true)]
    [InlineData("Orders", SinkKind.Postgres, "public.orders", "replica", false)]
    [InlineData("orders", SinkKind.Postgres, "orders", "replica", false)]
    [InlineData("orders", SinkKind.Postgres, "public.orders", null, false)]
    [InlineData("search", SinkKind.Index, "public.orders", null, true)]
    [InlineData("search", SinkKind.Index, "public.orders", "something", false)]
    [InlineData("hook", SinkKind.Webhook, "public.orders", "https://example.com/hooks/walrus", true)]
    [InlineData("hook", SinkKind.Webhook, "public.orders", "ftp://example.com/", false)]
    [InlineData("hook", SinkKind.Webhook, "public.orders", "/relative", false)]
    public void A_definition_is_checked_on_its_own(string id, SinkKind kind, string table, string? target, bool valid) =>
        new SinkDefinition(id, kind, [table], target).Problems().Count.ShouldBe(valid ? 0 : 1);

    [Fact]
    public void A_table_listed_twice_is_refused() =>
        new SinkDefinition("dup", SinkKind.Index, ["public.a", "public.a"], null).Problems().ShouldHaveSingleItem();
}

public sealed class OutboxSignalTests
{
    [Fact]
    public async Task A_notification_between_reading_and_waiting_is_not_lost()
    {
        var signal = new OutboxSignal();
        long seen = signal.Version("eu");

        // The reader found nothing; before it starts waiting, a change lands.
        signal.Notify("eu");

        Task wait = signal.WaitAsync("eu", seen, TimeSpan.FromSeconds(30), CancellationToken.None);
        (await Task.WhenAny(wait, Task.Delay(TimeSpan.FromSeconds(5)))).ShouldBe(wait);
    }

    [Fact]
    public async Task A_wait_ends_when_the_source_moves_on()
    {
        var signal = new OutboxSignal();
        Task wait = signal.WaitAsync("eu", signal.Version("eu"), TimeSpan.FromSeconds(30), CancellationToken.None);

        wait.IsCompleted.ShouldBeFalse();
        signal.Notify("eu");

        (await Task.WhenAny(wait, Task.Delay(TimeSpan.FromSeconds(5)))).ShouldBe(wait);
    }
}
