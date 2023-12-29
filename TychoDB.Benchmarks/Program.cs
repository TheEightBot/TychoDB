using System;
using BenchmarkDotNet.Running;

namespace TychoDB.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        var summary = BenchmarkRunner.Run<Benchmarks.Insertion>();
    }
}
