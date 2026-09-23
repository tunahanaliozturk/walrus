using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using Walrus.Domain;

namespace Walrus.UnitTests;

/// <summary>
/// The clock never runs backwards, versions form a total order, and a log position prints the way Postgres
/// prints it.
/// </summary>
public sealed class ClockAndOrderingTests
{
    [Property(MaxTest = 1000)]
    public Property The_clock_rises_strictly_whatever_the_timestamps_do() =>
        Prop.ForAll(
            Gen.Choose(-5_000, 5_000).ListOf().ToArbitrary(),
            offsets =>
            {
                // Timestamps that jump forward, stall, and run backwards, which is what commit timestamps from
                // concurrent transactions actually look like.
                var clock = new HybridLogicalClock();
                DateTimeOffset start = new(2026, 9, 15, 12, 0, 0, TimeSpan.Zero);
                long previous = clock.Last;

                foreach (int offset in offsets)
                {
                    long next = clock.Next(start.AddMilliseconds(offset));

                    if (next <= previous)
                    {
                        return false.Label($"{next} did not rise above {previous}");
                    }

                    previous = next;
                }

                return true.ToProperty();
            });

    [Fact]
    public void The_physical_half_follows_the_wall_clock_when_it_moves_forward()
    {
        var clock = new HybridLogicalClock();
        DateTimeOffset now = new(2026, 9, 15, 12, 0, 0, TimeSpan.Zero);

        long stamp = clock.Next(now);

        HybridLogicalClock.PhysicalOf(stamp).ShouldBe(now.ToUnixTimeMilliseconds());
        HybridLogicalClock.LogicalOf(stamp).ShouldBe(0);
    }

    [Fact]
    public void A_full_counter_borrows_the_next_millisecond_instead_of_wrapping()
    {
        DateTimeOffset now = new(2026, 9, 15, 12, 0, 0, TimeSpan.Zero);
        long full = HybridLogicalClock.Pack(now.ToUnixTimeMilliseconds(), (1 << HybridLogicalClock.LogicalBits) - 1);
        var clock = new HybridLogicalClock(full);

        long next = clock.Next(now);

        next.ShouldBeGreaterThan(full);
        HybridLogicalClock.PhysicalOf(next).ShouldBe(now.ToUnixTimeMilliseconds() + 1);
        HybridLogicalClock.LogicalOf(next).ShouldBe(0);
    }

    [Fact]
    public void A_restarted_clock_never_issues_a_stamp_at_or_below_its_floor()
    {
        // A capture that restarts reads the highest stamp it already persisted and resumes above it, even when
        // the source's clock has since been wound back.
        long floor = HybridLogicalClock.Pack(DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeMilliseconds(), 7);
        var clock = new HybridLogicalClock(floor);

        clock.Next(DateTimeOffset.UtcNow).ShouldBeGreaterThan(floor);
    }

    [Property(MaxTest = 1000)]
    public Property Versions_form_a_total_order() =>
        Prop.ForAll(
            (from hlc in Gen.Choose(0, 3)
             from source in Gen.Elements("a", "b")
             from ordinal in Gen.Choose(0, 2)
             select new ChangeVersion(hlc, source, ordinal)).Three().ToArbitrary(),
            triple =>
            {
                (ChangeVersion x, ChangeVersion y, ChangeVersion z) = triple;

                bool antisymmetric = !(x < y && y < x);
                bool transitive = !(x <= y && y <= z) || x <= z;
                bool total = x <= y || y <= x;
                bool consistentWithEquality = (x.CompareTo(y) == 0) == x.Equals(y);

                return antisymmetric && transitive && total && consistentWithEquality;
            });

    [Fact]
    public void The_clock_decides_before_the_source_and_the_source_before_the_ordinal()
    {
        new ChangeVersion(2, "a", 0).ShouldBeGreaterThan(new ChangeVersion(1, "z", 9));
        new ChangeVersion(1, "b", 0).ShouldBeGreaterThan(new ChangeVersion(1, "a", 9));
        new ChangeVersion(1, "a", 1).ShouldBeGreaterThan(new ChangeVersion(1, "a", 0));
    }

    [Property(MaxTest = 1000)]
    public Property A_log_position_survives_printing_and_parsing(ulong value) =>
        (Lsn.Parse(new Lsn(value).ToString()) == new Lsn(value)).ToProperty();

    [Theory]
    [InlineData("16/B374D848", 0x16_B374D848UL)]
    [InlineData("0/0", 0UL)]
    [InlineData("FFFFFFFF/FFFFFFFF", ulong.MaxValue)]
    public void A_log_position_reads_the_way_postgres_prints_it(string text, ulong expected)
    {
        Lsn.Parse(text).Value.ShouldBe(expected);
        new Lsn(expected).ToString().ShouldBe(text);
    }

    [Theory]
    [InlineData("")]
    [InlineData("16")]
    [InlineData("/B374D848")]
    [InlineData("16/")]
    [InlineData("G/0")]
    [InlineData("100000000/0")]
    public void Anything_else_is_refused_rather_than_guessed(string text)
    {
        Lsn.TryParse(text, out _).ShouldBeFalse();
    }

    [Fact]
    public void A_row_always_lands_in_the_same_lane_and_the_lane_is_in_range()
    {
        const string Row = "public.orders {\"id\":\"42\"}";

        int lane = LaneRouter.LaneFor(Row, 16);

        LaneRouter.LaneFor(Row, 16).ShouldBe(lane);
        lane.ShouldBeInRange(0, 15);
    }

    [Fact]
    public void A_row_image_keeps_its_column_order_and_tells_null_apart_from_absent()
    {
        var image = new RowImage([new RowColumn("z", "1"), new RowColumn("a", null)]);

        RowImage read = RowImage.FromJson(image.ToJson());

        read.ToJson().ShouldBe("""{"z":"1","a":null}""");
        read.TryGetValue("a", out string? a).ShouldBeTrue();
        a.ShouldBeNull();
        read.Contains("missing").ShouldBeFalse();
    }

    [Fact]
    public void A_row_image_refuses_a_column_named_twice()
    {
        Should.Throw<ArgumentException>(() => new RowImage([new RowColumn("a", "1"), new RowColumn("a", "2")]));
    }
}
