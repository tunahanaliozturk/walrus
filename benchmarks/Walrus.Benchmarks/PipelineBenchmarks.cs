using BenchmarkDotNet.Attributes;
using Walrus.Application.Capture;
using Walrus.Application.Dispatch;
using Walrus.Domain;

namespace Walrus.Benchmarks;

/// <summary>
/// The CPU cost of each step a change takes between the log and a sink, with no database in the way, so the
/// end-to-end numbers can be split into what Walrus spends and what Postgres spends.
/// </summary>
[MemoryDiagnoser]
public class PipelineBenchmarks
{
    private static readonly DateTimeOffset Commit = new(2026, 9, 23, 12, 0, 0, TimeSpan.Zero);

    private TransactionStamper _stamper = null!;
    private DecodedTransaction _transaction = null!;
    private ChangeEvent _insert = null!;
    private ChangeEvent _update = null!;
    private ChangeEvent _mergeUpdate = null!;
    private KeyState _existing = null!;
    private string _json = "";

    [GlobalSetup]
    public void Setup()
    {
        _stamper = new TransactionStamper(
            "eu",
            new Dictionary<string, TablePolicy>
            {
                ["public.accounts"] = new("public.accounts", ConflictMode.LastWriterWins, new Dictionary<string, ColumnMask> { ["email"] = ColumnMask.Hash }),
            },
            new ColumnMasker([.. Enumerable.Range(0, 32).Select(static value => (byte)value)]));

        _transaction = new DecodedTransaction(new Lsn(1_000), new Lsn(1_100), 42, Commit,
            [.. Enumerable.Range(1, 5).Select(static id => new DecodedChange(
                "public.accounts", ChangeOperation.Update, Row(id, 1), Row(id, 1), Row(id, 2)))]);

        _insert = Change(ChangeOperation.Insert, hlc: 10, Row(7, 1), null);
        _update = Change(ChangeOperation.Update, hlc: 20, Row(7, 2), null);
        _mergeUpdate = _update with { ChangedColumns = ["balance"] };
        _existing = KeyState.Empty.Apply(_insert).State;
        _json = Row(7, 2).ToJson();
    }

    [Benchmark(Description = "stamp and mask a 5-change transaction")]
    public int Stamp() => _stamper.Stamp(_transaction, new HybridLogicalClock()).Count;

    [Benchmark(Description = "apply an update to a row's state (last writer wins)")]
    public bool ApplyUpdate() => _existing.Apply(_update).Changed;

    [Benchmark(Description = "apply an update to a row's state (merge columns)")]
    public bool ApplyMergeUpdate() => _existing.Apply(_mergeUpdate).Changed;

    [Benchmark(Description = "recognise a change already applied")]
    public bool ApplyDuplicate() => _existing.Apply(_insert).Changed;

    [Benchmark(Description = "materialise a row from its state")]
    public int Materialize() => _existing.Materialize()!.Columns.Count;

    [Benchmark(Description = "row image to JSON")]
    public int RowToJson() => _update.After!.ToJson().Length;

    [Benchmark(Description = "row image from JSON")]
    public int RowFromJson() => RowImage.FromJson(_json).Columns.Count;

    [Benchmark(Description = "key state round trip through JSON")]
    public bool StateRoundTrip() => KeyState.FromJson(_existing.ToJson()).Exists;

    [Benchmark(Description = "choose a lane")]
    public int Lane() => LaneRouter.LaneFor(_update.EntityKey, 8);

    [Benchmark(Description = "track 1,000 positions finishing out of order")]
    public long Watermark()
    {
        var watermark = new ContiguousWatermark(0);

        for (long seq = 1; seq <= 1_000; seq++)
        {
            watermark.Dispatched(seq);
        }

        for (long seq = 1_000; seq >= 1; seq--)
        {
            watermark.Finished(seq);
        }

        return watermark.Mark;
    }

    private static ChangeEvent Change(ChangeOperation operation, long hlc, RowImage after, IReadOnlyList<string>? changed) =>
        new("eu", new Lsn((ulong)hlc), 0, 1, Commit, hlc, "public.accounts", operation, new RowImage([new RowColumn("id", "7")]), null, after, changed);

    private static RowImage Row(int id, int version) => new(
    [
        new RowColumn("id", id.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new RowColumn("owner", $"owner-{version}"),
        new RowColumn("email", "ada@example.com"),
        new RowColumn("balance", $"{version * 10}.00"),
        new RowColumn("seq", version.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new RowColumn("written_at", "2026-09-23 12:00:00.123456+00"),
    ]);
}
