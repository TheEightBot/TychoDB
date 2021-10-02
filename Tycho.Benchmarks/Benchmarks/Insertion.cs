using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Diagnostics.Tracing.Parsers.ApplicationServer;

namespace Tycho.Benchmarks.Benchmarks
{
    [MemoryDiagnoser]
    [SimpleJob (launchCount: 1, warmupCount: 1, targetCount: 10)]
    public class Insertion
    {
        [ParamsSource (nameof (JsonSerializers))]
        public IJsonSerializer JsonSerializer { get; set; }

        public static IEnumerable<IJsonSerializer> JsonSerializers ()
            => new IJsonSerializer[] { new SystemTextJsonSerializer (), new NewtonsoftJsonSerializer() };

        private static TestClassE _largeTestObject =
            new TestClassE()
            {
                TestClassId = Guid.NewGuid(),
                Values = 
                    new []
                    {
                        new TestClassD()
                        {
                            DoubleProperty = 12d,
                            FloatProperty = 15f,
                            ValueC = 
                                new TestClassC()
                                {
                                    DoubleProperty = 14d,
                                    IntProperty = 15,
                                }
                        },
                        new TestClassD()
                        {
                            DoubleProperty = 12d,
                            FloatProperty = 15f,
                            ValueC = 
                                new TestClassC()
                                {
                                    DoubleProperty = 14d,
                                    IntProperty = 15,
                                }
                        },
                        new TestClassD()
                        {
                            DoubleProperty = 12d,
                            FloatProperty = 15f,
                            ValueC = 
                                new TestClassC()
                                {
                                    DoubleProperty = 14d,
                                    IntProperty = 15,
                                }
                        },
                        new TestClassD()
                        {
                            DoubleProperty = 12d,
                            FloatProperty = 15f,
                            ValueC = 
                                new TestClassC()
                                {
                                    DoubleProperty = 14d,
                                    IntProperty = 15,
                                }
                        }
                    }
                
                
            };
        
        internal static TestClassE LargeTestObject => _largeTestObject;
        
        [Benchmark]
        public async Task InsertSingularAsync()
        {
            using var db =
                BuildDatabaseConnection()
                    .Connect();

            var testObj =
                new TestClassA
                {
                    StringProperty = $"Test String",
                    IntProperty = 100,
                    TimestampMillis = 123451234,
                };


            await db.WriteObjectAsync(testObj, x => x.StringProperty).ConfigureAwait(false);
        }
        
        [Benchmark]
        public async Task InsertSingularLargeObjectAsync ()
        {
            using var db =
                BuildDatabaseConnection()
                    .Connect();

            await db.WriteObjectAsync (LargeTestObject, x => x.TestClassId).ConfigureAwait (false);
        }

        [Benchmark]
        public async Task InsertManyAsync ()
        {
            using var db =
                BuildDatabaseConnection()
                    .Connect ();

            for (int i = 100; i < 1100; i++)
            {
                var testObj =
                    new TestClassA
                    {
                        StringProperty = $"Test String {i}",
                        IntProperty = i,
                        TimestampMillis = 123451234,
                    };


                await db.WriteObjectAsync (testObj, x => x.StringProperty).ConfigureAwait (false);
            }
        }

        [Benchmark]
        public async Task InsertManyConcurrentAsync ()
        {
            using var db =
                BuildDatabaseConnection()
                    .Connect ();
            
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


                            await db.WriteObjectAsync (testObj, x => x.StringProperty).ConfigureAwait (false);
                        })
                    .ToList ();

            await Task.WhenAll (tasks).ConfigureAwait (false);
        }

        public TychoDb BuildDatabaseConnection()
        {
#if ENCRYPTED
            return new TychoDb(Path.GetTempPath(), JsonSerializer, "tycho_cache_enc.db", "Password", rebuildCache: true);
#else
            return new TychoDb(Path.GetTempPath(), JsonSerializer, rebuildCache: true);
#endif
        }
    }
}
