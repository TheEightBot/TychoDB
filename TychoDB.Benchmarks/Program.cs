using System;
using BenchmarkDotNet.Running;

namespace TychoDB.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        // Discover all benchmark classes so runs can be selected with --filter
        // (e.g. --filter '*Insertion*' or --filter '*Querying*').
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
