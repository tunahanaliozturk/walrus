using Walrus.Load;

// Load and chaos harness for the compose stack. Every number in docs/benchmark-results comes from here.
//
//   soak       --rate 3000 --seconds 600     commit-to-apply lag, ordering and equality at a steady rate
//   failover   --cycles 50 --rate 500        kill the eu primary under load, promote, rejoin, repeat
//   replay     --changes 10000000            capture a large table, then replay it into a new sink
//   traffic                                  a gentle stream for the console

if (args.Length == 0)
{
    Console.Error.WriteLine("Usage: Walrus.Load <soak|failover|replay|traffic> [--option value ...]");
    return 2;
}

Dictionary<string, string> options = new(StringComparer.Ordinal);

for (int index = 1; index + 1 < args.Length; index += 2)
{
    options[args[index].TrimStart('-')] = args[index + 1];
}

await using var stack = new Stack(options);

return args[0] switch
{
    "soak" => await Scenarios.SoakAsync(stack),
    "failover" => await Scenarios.FailoverAsync(stack),
    "replay" => await Scenarios.ReplayAsync(stack),
    "traffic" => await Scenarios.TrafficAsync(stack),
    _ => 2,
};
