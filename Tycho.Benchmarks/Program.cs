using System;
using BenchmarkDotNet.Running;

namespace Tycho.Benchmarks
{
    class Program
    {
        static void Main (string[] args)
        {
            var summary = BenchmarkRunner.Run<Benchmarks.Insertion> ();
        }
    }
}
