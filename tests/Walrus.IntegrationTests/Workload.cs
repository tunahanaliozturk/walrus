using System.Globalization;
using System.Text;
using Npgsql;

namespace Walrus.IntegrationTests;

/// <summary>
/// Random inserts, updates and deletes over a small set of keys, in transactions of one to five statements.
/// </summary>
/// <remarks>
/// Every write to a row sets its <c>seq</c> column to the next number for that row, so a sink that ever applies
/// a row's changes out of order is caught by the sequence it records going down. Few keys and many writes, so
/// most changes land on a row that has changed recently, which is where ordering goes wrong if it can.
/// </remarks>
internal sealed class Workload(TestRig rig, string table, int keys, int seed)
{
    public const string Columns = "id bigint primary key, owner text not null, balance numeric(18,2) not null, seq bigint not null, note text";

    private readonly Random _random = new(seed);
    private readonly Dictionary<long, long> _sequence = [];

    public async Task RunAsync(string source, int transactions, CancellationToken cancellationToken = default)
    {
        await using NpgsqlConnection connection = await rig.OpenAsync(source);

        for (int transaction = 0; transaction < transactions && !cancellationToken.IsCancellationRequested; transaction++)
        {
            var sql = new StringBuilder("begin;");
            int statements = _random.Next(1, 6);

            for (int statement = 0; statement < statements; statement++)
            {
                long id = _random.Next(1, keys + 1);
                long seq = _sequence[id] = _sequence.GetValueOrDefault(id) + 1;
                string owner = $"owner-{_random.Next(1_000)}";
                string balance = (_random.Next(-100_000, 100_000) / 100m).ToString(CultureInfo.InvariantCulture);

                sql.Append(_random.Next(10) switch
                {
                    0 => $"delete from {table} where id = {id};",
                    _ => $"insert into {table} (id, owner, balance, seq) values ({id}, '{owner}', {balance}, {seq}) " +
                         $"on conflict (id) do update set owner = excluded.owner, balance = excluded.balance, seq = excluded.seq;",
                });
            }

            sql.Append("commit;");
            await TestRig.ExecuteAsync(connection, sql.ToString());
        }
    }

    /// <summary>
    /// Adds a trigger to the sink's copy of the table that records every applied version of every row, so a
    /// test can check the order the sink saw them in.
    /// </summary>
    public static async Task RecordAppliesAsync(TestRig rig, string table)
    {
        await using NpgsqlConnection sink = await rig.OpenAsync("sink");
        await TestRig.ExecuteAsync(sink, $"""
            create table public.{table}_applied (n bigserial primary key, id bigint not null, seq bigint not null);
            create function public.{table}_record() returns trigger language plpgsql as $$
            begin
                insert into public.{table}_applied (id, seq) values (new.id, new.seq);
                return new;
            end $$;
            create trigger {table}_record after insert or update on public.{table}
                for each row execute function public.{table}_record();
            """);
    }

    /// <summary>Rows whose recorded sequence ever went down: each one is an ordering violation.</summary>
    public static async Task<long> InversionsAsync(TestRig rig, string table)
    {
        await using NpgsqlConnection sink = await rig.OpenAsync("sink");
        await using var count = new NpgsqlCommand($"""
            select count(*) from (
                select seq < lag(seq) over (partition by id order by n) as inverted
                from public.{table}_applied) applies
            where inverted
            """, sink);

        return (long)(await count.ExecuteScalarAsync())!;
    }
}
