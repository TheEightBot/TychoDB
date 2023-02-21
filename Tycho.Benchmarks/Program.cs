using System;
using BenchmarkDotNet.Running;

namespace Tycho.Benchmarks
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<Benchmarks.Insertion>();
        }
    }
}