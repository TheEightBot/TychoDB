using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Tycho.Benchmarks.Benchmarks
{
    public class Insertion
    {
        [Benchmark]
        public async Task<int> InsertManyAsync ()
        {
            using var db =
                new TychoDb (Path.GetTempPath (), rebuildCache: true)
                    .Connect ();

            var successWrites = 0;

            for (int i = 100; i < 1100; i++)
            {
                var testObj =
                    new TestClassA
                    {
                        StringProperty = $"Test String {i}",
                        IntProperty = i,
                        TimestampMillis = 123451234,
                    };


                var resultWrite = await db.WriteObjectAsync (testObj, x => x.StringProperty).ConfigureAwait (false);

                if (resultWrite)
                {
                    Interlocked.Increment (ref successWrites);
                }
            }

            return successWrites;
        }

        [Benchmark]
        public async Task<int> InsertManyConcurrentAsync ()
        {
            using var db =
                new TychoDb (Path.GetTempPath (), rebuildCache: true)
                    .Connect ();

            var successWrites = 0;

            var tasks =
                Enumerable
                    .Range (100, 1000)
                    .Select (
                        async i =>
                        {
                            var testObj =
                                new TestClassA
                                {
                                    StringProperty = $"Test String {i}",
                                    IntProperty = i,
                                    TimestampMillis = 123451234,
                                };


                            var resultWrite = await db.WriteObjectAsync (testObj, x => x.StringProperty).ConfigureAwait (false);

                            if (resultWrite)
                            {
                                Interlocked.Increment (ref successWrites);
                            }
                        })
                    .ToList ();

            await Task.WhenAll (tasks).ConfigureAwait (false);

            return successWrites;
        }
    }
}
