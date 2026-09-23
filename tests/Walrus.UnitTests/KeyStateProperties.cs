using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Walrus.Core;

namespace Walrus.UnitTests;

/// <summary>
/// Sinks converge, and they converge to the right answer.
/// </summary>
/// <remarks>
/// <para>
/// Two different properties, and the second is the one that is easy to forget. A state that ignores every
/// change converges perfectly: every order of delivery gives the same empty row. So the permutation tests are
/// paired with a reference implementation written straight from the definition, over the whole set of changes
/// at once, with no incremental state at all. The incremental <see cref="KeyState"/> has to agree with it for
/// every generated history.
/// </para>
/// <para>
/// The histories are deliberately small and crowded: three sources, three columns, a handful of clock values,
/// so versions collide on the clock and fall back to the source and ordinal tie-breaks, and deletes land among
/// writes often enough to exercise pruning. A generator spread over a large space would mostly produce
/// histories where nothing interesting happens.
/// </para>
/// </remarks>
public sealed class KeyStateProperties
{
    private static readonly string[] Sources = ["alpha", "beta", "gamma"];
    private static readonly string[] Columns = ["a", "b", "c"];
    private static readonly string?[] Values = ["x", "y", "z", null];

    [Property(MaxTest = 2000)]
    public Property Every_delivery_order_produces_the_same_state() =>
        Prop.ForAll(HistoryWithTwoOrders(), scenario =>
        {
            KeyState first = Fold(scenario.First);
            KeyState second = Fold(scenario.Second);

            return first.Equals(second)
                .Label($"order one: {first.ToJson()}")
                .And(true.Label($"order two: {second.ToJson()}"));
        });

    [Property(MaxTest = 2000)]
    public Property The_state_agrees_with_the_definition_computed_from_the_whole_history() =>
        Prop.ForAll(HistoryWithTwoOrders(), scenario =>
        {
            RowImage? incremental = Fold(scenario.First).Materialize();
            RowImage? reference = Reference(scenario.First);

            return Equals(incremental, reference)
                .Label($"incremental: {incremental?.ToJson() ?? "absent"}")
                .And(true.Label($"reference:   {reference?.ToJson() ?? "absent"}"));
        });

    [Property(MaxTest = 1000)]
    public Property Applying_a_change_twice_is_the_same_as_applying_it_once() =>
        Prop.ForAll(History().ToArbitrary(), history =>
        {
            KeyState state = KeyState.Empty;

            foreach (ChangeEvent change in history)
            {
                KeyState once = state.Apply(change).State;
                KeyApplication twice = once.Apply(change);

                if (twice.Changed || !twice.State.Equals(once))
                {
                    return false.Label($"re-applying {change.Version} moved the state");
                }

                state = once;
            }

            return true.ToProperty();
        });

    [Property(MaxTest = 1000)]
    public Property A_single_source_in_commit_order_is_plain_last_writer_wins() =>
        Prop.ForAll(SingleSourceHistory().ToArbitrary(), history =>
        {
            RowImage? expected = history[^1].Operation is ChangeOperation.Delete ? null : history[^1].After;

            return Equals(Fold(history).Materialize(), expected);
        });

    [Fact]
    public void An_insert_followed_by_an_update_in_the_same_transaction_keeps_the_update()
    {
        // The case a commit-position watermark gets wrong. Both changes share the transaction's clock stamp,
        // and only the ordinal says the update came second.
        ChangeEvent insert = Write("alpha", hlc: 10, ordinal: 0, ("a", "inserted"));
        ChangeEvent update = Write("alpha", hlc: 10, ordinal: 1, ("a", "updated")) with { Operation = ChangeOperation.Update };

        Fold([insert, update]).Materialize()!.TryGetValue("a", out string? value).ShouldBeTrue();
        value.ShouldBe("updated");
    }

    [Fact]
    public void Two_sources_changing_different_columns_both_survive_when_columns_merge()
    {
        ChangeEvent initial = Write("alpha", hlc: 1, ordinal: 0, ("a", "a0"), ("b", "b0"));
        ChangeEvent fromAlpha = Write("alpha", hlc: 5, ordinal: 0, ("a", "a1"), ("b", "b0")) with { ChangedColumns = ["a"] };
        ChangeEvent fromBeta = Write("beta", hlc: 6, ordinal: 0, ("a", "a0"), ("b", "b1")) with { ChangedColumns = ["b"] };

        RowImage merged = Fold([initial, fromBeta, fromAlpha]).Materialize()!;

        merged.TryGetValue("a", out string? a).ShouldBeTrue();
        merged.TryGetValue("b", out string? b).ShouldBeTrue();

        // Row-level last writer wins would have kept beta's whole row, including its stale "a0", and quietly
        // thrown alpha's change away.
        a.ShouldBe("a1");
        b.ShouldBe("b1");
    }

    [Fact]
    public void A_write_older_than_a_delete_does_not_bring_the_row_back()
    {
        ChangeEvent late = Write("alpha", hlc: 4, ordinal: 0, ("a", "stale"));
        ChangeEvent delete = Delete("beta", hlc: 9, ordinal: 0);

        KeyState state = Fold([delete, late]);

        state.Exists.ShouldBeFalse();
        state.Materialize().ShouldBeNull();
    }

    [Fact]
    public void A_value_postgres_did_not_resend_is_kept_rather_than_nulled()
    {
        ChangeEvent insert = Write("alpha", hlc: 1, ordinal: 0, ("id", "1"), ("body", "a very large document"));

        // An update that left the large column untouched. Postgres omits it, and so does the image.
        ChangeEvent update = Write("alpha", hlc: 2, ordinal: 0, ("id", "1")) with { Operation = ChangeOperation.Update };

        Fold([insert, update]).Materialize()!.TryGetValue("body", out string? body).ShouldBeTrue();
        body.ShouldBe("a very large document");
    }

    [Fact]
    public void A_state_survives_a_round_trip_through_json()
    {
        KeyState state = Fold(
        [
            Write("alpha", hlc: 1, ordinal: 0, ("a", "1"), ("b", null)),
            Delete("beta", hlc: 2, ordinal: 0),
            Write("gamma", hlc: 3, ordinal: 1, ("a", "2")) with { ChangedColumns = ["a"] },
        ]);

        KeyState.FromJson(state.ToJson()).ShouldBe(state);
    }

    private static KeyState Fold(IEnumerable<ChangeEvent> changes)
    {
        KeyState state = KeyState.Empty;

        foreach (ChangeEvent change in changes)
        {
            state = state.Apply(change).State;
        }

        return state;
    }

    /// <summary>
    /// What a row should be, computed from the definition over the whole history at once.
    /// </summary>
    /// <remarks>
    /// Written to be obviously correct rather than efficient, and sharing no code with <see cref="KeyState"/>.
    /// If the two ever agree because they share a bug, the bug would have to be written twice.
    /// </remarks>
    private static RowImage? Reference(IEnumerable<ChangeEvent> history)
    {
        ChangeEvent[] distinct = [.. history.DistinctBy(change => change.Version)];

        ChangeVersion? newestDelete = distinct
            .Where(change => change.Operation is ChangeOperation.Delete)
            .Select(change => (ChangeVersion?)change.Version)
            .Aggregate((ChangeVersion?)null, ChangeVersion.Max);

        ChangeEvent[] live = [.. distinct
            .Where(change => change.Operation is not ChangeOperation.Delete)
            .Where(change => newestDelete is null || change.Version > newestDelete.Value)];

        if (live.Length is 0)
        {
            return null;
        }

        ChangeEvent newest = live.MaxBy(change => change.Version)!;

        IEnumerable<string> Changed(ChangeEvent change) =>
            (change.ChangedColumns ?? change.After!.Columns.Select(column => column.Name))
                .Where(change.After!.Contains);

        string? ValueFor(string column)
        {
            ChangeEvent? writer = live
                .Where(change => Changed(change).Contains(column))
                .MaxBy(change => change.Version);

            if (writer is not null)
            {
                writer.After!.TryGetValue(column, out string? written);

                return written;
            }

            newest.After!.TryGetValue(column, out string? fallback);

            return fallback;
        }

        List<RowColumn> columns = [.. newest.After!.Columns.Select(column => new RowColumn(column.Name, ValueFor(column.Name)))];

        foreach (string extra in live
            .SelectMany(Changed)
            .Where(column => !newest.After!.Contains(column))
            .Distinct()
            .Order(StringComparer.Ordinal))
        {
            columns.Add(new RowColumn(extra, ValueFor(extra)));
        }

        return new RowImage(columns);
    }

    private static ChangeEvent Write(string source, long hlc, int ordinal, params (string Name, string? Value)[] columns) =>
        new(source, new Lsn((ulong)hlc), ordinal, 1, DateTimeOffset.UnixEpoch, hlc, "public.item", ChangeOperation.Insert,
            new RowImage([new RowColumn("id", "1")]), null,
            new RowImage(columns.Select(column => new RowColumn(column.Name, column.Value))), null);

    private static ChangeEvent Delete(string source, long hlc, int ordinal) =>
        new(source, new Lsn((ulong)hlc), ordinal, 1, DateTimeOffset.UnixEpoch, hlc, "public.item", ChangeOperation.Delete,
            new RowImage([new RowColumn("id", "1")]), null, null, null);

    private static Gen<ChangeEvent> Change() =>
        from source in Gen.Elements(Sources)
        from hlc in Gen.Choose(1, 12)
        from ordinal in Gen.Choose(0, 2)
        from isDelete in Gen.Frequency((3, Gen.Constant(false)), (1, Gen.Constant(true)))
        from image in Image()
        from merge in Gen.Elements(false, true)
        from changedMask in Gen.Choose(1, 7)
        select isDelete
            ? Delete(source, hlc, ordinal)
            : Write(source, hlc, ordinal, image) with
            {
                Operation = hlc % 2 == 0 ? ChangeOperation.Update : ChangeOperation.Insert,
                ChangedColumns = merge ? [.. Columns.Where((_, index) => (changedMask & (1 << index)) != 0)] : null,
            };

    private static Gen<(string Name, string? Value)[]> Image() =>
        from present in Gen.Choose(1, 7)
        from values in Gen.Elements(Values).ArrayOf(Columns.Length)
        select Columns
            .Select((column, index) => (Present: (present & (1 << index)) != 0, Column: (Name: column, Value: values[index])))
            .Where(entry => entry.Present)
            .Select(entry => entry.Column)
            .ToArray();

    private static Gen<(string Name, string? Value)[]> FullImage() =>
        Gen.Elements(Values).ArrayOf(Columns.Length)
            .Select(values => Columns.Select((column, index) => (column, values[index])).ToArray());

    private static Gen<ChangeEvent[]> History() =>
        Change().ListOf().Select(changes => changes.DistinctBy(change => change.Version).ToArray());

    /// <remarks>
    /// Full images only. A partial image means Postgres left a large value out, and then the row correctly keeps
    /// the value from an earlier change rather than taking the last image verbatim, which is a different
    /// property and is covered by the reference comparison instead.
    /// </remarks>
    private static Gen<ChangeEvent[]> SingleSourceHistory() =>
        from count in Gen.Choose(1, 12)
        from kinds in Gen.Frequency((3, Gen.Constant(false)), (1, Gen.Constant(true))).ArrayOf(count)
        from images in FullImage().ArrayOf(count)
        select kinds
            .Select((isDelete, index) => isDelete
                ? Delete("alpha", index + 1, 0)
                : Write("alpha", index + 1, 0, images[index]))
            .ToArray();

    private static Arbitrary<Scenario> HistoryWithTwoOrders() =>
        (from history in History()
         from first in Reorder(history)
         from second in Reorder(history)
         select new Scenario(first, second)).ToArbitrary();

    /// <summary>A shuffled copy with some changes delivered twice, as a crash and redelivery would.</summary>
    private static Gen<ChangeEvent[]> Reorder(ChangeEvent[] history) =>
        from duplicated in Gen.Choose(0, Math.Min(3, history.Length))
        from shuffled in Gen.Shuffle([.. history, .. history.Take(duplicated)])
        select shuffled;

    /// <summary>One history, delivered in two different orders.</summary>
    /// <param name="First">The first delivery order.</param>
    /// <param name="Second">The second delivery order.</param>
    public sealed record Scenario(ChangeEvent[] First, ChangeEvent[] Second)
    {
        /// <inheritdoc />
        public override string ToString() =>
            string.Join(" ", First.Select(change => $"{change.Operation}@{change.Version.Hlc}/{change.Source}/{change.Ordinal}"));
    }
}
