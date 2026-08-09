using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;

namespace TychoDB.Benchmarks;

public static class Program
{
    public static async Task Main(string[] args)
    {
        // `dotnet run -- diagnose` runs the indexing evidence harness instead of
        // BenchmarkDotNet (see docs/indexing-analysis.md).
        if (args.Length > 0 && string.Equals(args[0], "diagnose", StringComparison.OrdinalIgnoreCase))
        {
            await Diagnostics.RunAsync();
            return;
        }

        // Discover all benchmark classes so runs can be selected with --filter
        // (e.g. --filter '*Insertion*' or --filter '*Querying*').
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
