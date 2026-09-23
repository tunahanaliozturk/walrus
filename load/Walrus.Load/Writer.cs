using System.Diagnostics;
using System.Globalization;
using System.Text;
using Npgsql;

namespace Walrus.Load;

/// <summary>
/// Writes account upserts to a source at a fixed rate, from several connections, and remembers what the source
/// acknowledged.
/// </summary>
/// <remarks>
/// <para>
/// Each key belongs to one connection, so a key's writes commit in the order they were issued and its
/// <c>seq</c> column rises by one per write. A sink that ever applies a key out of order shows it as a falling
/// <c>seq</c> in its apply log.
/// </para>
/// <para>
/// A write whose commit returned is acknowledged, and must survive anything. A write that failed because the
/// connection died may or may not have committed; its sequence number is used up either way, so the check at
/// the end is that the source holds at least the acknowledged sequence for every key, never less.
/// </para>
/// </remarks>
internal sealed class Writer(Npgsql.NpgsqlDataSource source, int keys, int connections, int rowsPerTransaction)
{
    private readonly long[] _issued = new long[keys + 1];
    private readonly long[] _acknowledged = new long[keys + 1];
    private long _rows;
    private long _failures;

    public long Rows => Interlocked.Read(ref _rows);

    public long Failures => Interlocked.Read(ref _failures);

    /// <summary>The highest acknowledged sequence number for each key.</summary>
    public IReadOnlyList<long> Acknowledged => _acknowledged;

    /// <summary>Writes <paramref name="rate"/> rows a second until cancelled.</summary>
    public Task RunAsync(double rate, CancellationToken cancellationToken) =>
        Task.WhenAll(Enumerable.Range(0, connections).Select(lane => Task.Run(() => LaneAsync(lane, rate / connections, cancellationToken))));

    private async Task LaneAsync(int lane, double rowsPerSecond, CancellationToken cancellationToken)
    {
        var random = new Random(lane);
        int[] owned = [.. Enumerable.Range(1, keys).Where(key => key % connections == lane)];
        double interval = rowsPerTransaction / rowsPerSecond;
        var clock = Stopwatch.StartNew();
        long transaction = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            double due = transaction++ * interval;
            double wait = due - clock.Elapsed.TotalSeconds;

            if (wait > 0)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(wait), cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }

            var chosen = new int[rowsPerTransaction];
            var sequences = new long[rowsPerTransaction];
            var sql = new StringBuilder("insert into accounts (id, owner, balance, seq, written_at) values ");

            for (int row = 0; row < rowsPerTransaction; row++)
            {
                // Distinct keys within one statement, since one statement cannot upsert a row twice.
                int key;

                do
                {
                    key = owned[random.Next(owned.Length)];
                }
                while (chosen.AsSpan(0, row).Contains(key));

                chosen[row] = key;
                sequences[row] = ++_issued[key];

                sql.Append(CultureInfo.InvariantCulture, $"{(row == 0 ? "" : ",")}({key}, 'owner-{random.Next(1_000)}', {random.Next(-100_000, 100_000) / 100m}, {sequences[row]}, clock_timestamp())");
            }

            sql.Append(" on conflict (id) do update set owner = excluded.owner, balance = excluded.balance, seq = excluded.seq, written_at = excluded.written_at");

            try
            {
                await using NpgsqlCommand command = source.CreateCommand(sql.ToString());
                await command.ExecuteNonQueryAsync(cancellationToken);

                for (int row = 0; row < rowsPerTransaction; row++)
                {
                    Volatile.Write(ref _acknowledged[chosen[row]], sequences[row]);
                }

                Interlocked.Add(ref _rows, rowsPerTransaction);
            }
            catch (Exception exception) when (exception is NpgsqlException or TimeoutException or InvalidOperationException)
            {
                // The primary went away mid-commit. Keep issuing on the schedule; the pool finds the new primary.
                Interlocked.Increment(ref _failures);
                await Task.Delay(50, CancellationToken.None);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
