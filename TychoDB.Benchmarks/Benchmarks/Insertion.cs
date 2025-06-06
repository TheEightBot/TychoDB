using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using SQLite;

namespace TychoDB.Benchmarks.Benchmarks;

[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, invocationCount: 100)]
public class Insertion
{
    [ParamsSource(nameof(JsonSerializers))]
    public IJsonSerializer JsonSerializer { get; set; }

    public static IEnumerable<IJsonSerializer> JsonSerializers()
        => new IJsonSerializer[]
            {
                new SystemTextJsonSerializer(
                    jsonTypeSerializers:
                        new Dictionary<Type, JsonTypeInfo>
                        {
                            [typeof(TestClassA)] = TestJsonContext.Default.TestClassA,
                            [typeof(List<TestClassA>)] = TestJsonContext.Default.ListTestClassA,
                            [typeof(TestClassB)] = TestJsonContext.Default.TestClassB,
                            [typeof(TestClassC)] = TestJsonContext.Default.TestClassC,
                            [typeof(TestClassD)] = TestJsonContext.Default.TestClassD,
                            [typeof(TestClassE)] = TestJsonContext.Default.TestClassE,
                            [typeof(TestClassF)] = TestJsonContext.Default.TestClassF,
                        }),
                new NewtonsoftJsonSerializer(),
            };

    private static TestClassE _largeTestObject =
        new TestClassE()
        {
            TestClassId = Guid.NewGuid(),
            Values =
                new[]
                {
                    new TestClassD()
                    {
                        DoubleProperty = 1d,
                        FloatProperty = 2f,
                        ValueC =
                            new TestClassC()
                            {
                                DoubleProperty = 3d,
                                IntProperty = 4,
                            },
                    },
                    new TestClassD()
                    {
                        DoubleProperty = 5d,
                        FloatProperty = 6f,
                        ValueC =
                            new TestClassC()
                            {
                                DoubleProperty = 7d,
                                IntProperty = 8,
                            },
                    },
                    new TestClassD()
                    {
                        DoubleProperty = 9d,
                        FloatProperty = 10f,
                        ValueC =
                            new TestClassC()
                            {
                                DoubleProperty = 11d,
                                IntProperty = 12,
                            },
                    },
                    new TestClassD()
                    {
                        DoubleProperty = 13d,
                        FloatProperty = 14f,
                        ValueC =
                            new TestClassC()
                            {
                                DoubleProperty = 15d,
                                IntProperty = 16,
                            },
                    },
                },
        };

    internal static TestClassE LargeTestObject => _largeTestObject;

    internal string TempPath { get; } = Path.GetTempPath();

    [IterationSetup]
    public void IterationSetup()
    {
        var sqliteFile = Path.Combine(TempPath, "sqlitenet.db");
        var encryptedFile = Path.Combine(TempPath, "tycho_cache_enc.db");
        var standardFile = Path.Combine(TempPath, "tycho_cache.db");

        if (File.Exists(sqliteFile))
        {
            File.Delete(sqliteFile);
        }

        if (File.Exists(encryptedFile))
        {
            File.Delete(encryptedFile);
        }

        if (File.Exists(standardFile))
        {
            File.Delete(standardFile);
        }
    }

    [Benchmark]
    public async Task InsertSingularAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        var testObj =
            new TestClassA
            {
                StringProperty = $"Test String",
                LongProperty = 100,
                TimestampMillis = 123451234,
            };

        await db.WriteObjectAsync(testObj, x => x.StringProperty).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertSingularWithoutTransactionAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        var testObj =
            new TestClassA
            {
                StringProperty = $"Test String",
                LongProperty = 100,
                TimestampMillis = 123451234,
            };

        await db.WriteObjectAsync(testObj, x => x.StringProperty, withTransaction: false).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertSingularSqliteNetAsync()
    {
        var db = new SQLiteAsyncConnection(Path.Combine(TempPath, "sqlitenet.db"));
        await db.CreateTableAsync<TestClassA>().ConfigureAwait(false);

        var testObj =
            new TestClassA
            {
                StringProperty = $"Test String",
                LongProperty = 100,
                TimestampMillis = 123451234,
            };

        await db.InsertOrReplaceAsync(testObj).ConfigureAwait(false);

        await db.CloseAsync().ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertSingularLargeObjectAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        await db.WriteObjectAsync(LargeTestObject, x => x.TestClassId).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertManyAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        for (int i = 100; i < 1100; i++)
        {
            var testObj =
                new TestClassA
                {
                    StringProperty = $"Test String {i}",
                    LongProperty = i,
                    TimestampMillis = 123451234,
                };

            await db.WriteObjectAsync(testObj, x => x.StringProperty).ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async Task InsertManyWithoutTranactionAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        for (int i = 100; i < 1100; i++)
        {
            var testObj =
                new TestClassA
                {
                    StringProperty = $"Test String {i}",
                    LongProperty = i,
                    TimestampMillis = 123451234,
                };

            await db.WriteObjectAsync(testObj, x => x.StringProperty, withTransaction: false).ConfigureAwait(false);
        }
    }

    [Benchmark]
    public async Task InsertManySqliteNetAsync()
    {
        var db = new SQLiteAsyncConnection(Path.Combine(TempPath, "sqlitenet.db"));
        await db.CreateTableAsync<TestClassA>().ConfigureAwait(false);

        await db.RunInTransactionAsync(
            conn =>
            {
                for (int i = 100; i < 1100; i++)
                {
                    var testObj =
                        new TestClassA
                        {
                            StringProperty = $"Test String {i}",
                            LongProperty = i,
                            TimestampMillis = 123451234,
                        };

                    conn.InsertOrReplace(testObj);
                }
            });

        await db.CloseAsync().ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertManyConcurrentAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        var tasks =
            Enumerable
                .Range(100, 1000)
                .Select(
                    async i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                LongProperty = i,
                                TimestampMillis = 123451234,
                            };

                        await db.WriteObjectAsync(testObj, x => x.StringProperty).ConfigureAwait(false);
                    })
                .ToList();

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertManyConcurrentWithoutTransactionAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        var tasks =
            Enumerable
                .Range(100, 1000)
                .Select(
                    async i =>
                    {
                        var testObj =
                            new TestClassA
                            {
                                StringProperty = $"Test String {i}",
                                LongProperty = i,
                                TimestampMillis = 123451234,
                            };

                        await db.WriteObjectAsync(testObj, x => x.StringProperty, withTransaction: false).ConfigureAwait(false);
                    })
                .ToList();

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertManyBulkAsync()
    {
        using var db =
            BuildDatabaseConnection()
                .Connect();

        var list = new List<TestClassA>();

        var timestampStart = DateTimeOffset.Now.ToUnixTimeMilliseconds();

        for (long i = timestampStart; i < 1100; i++)
        {
            var testObj =
                new TestClassA
                {
                    StringProperty = $"Test String {i}",
                    LongProperty = i,
                    TimestampMillis = 123451234,
                };

            list.Add(testObj);
        }

        await db.WriteObjectsAsync(list, x => x.StringProperty).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertManyBulkWithoutTransactionAsync()
    {
        using var db = await this.BuildDatabaseConnection().ConnectAsync();

        var list = new List<TestClassA>();

        var timestampStart = DateTimeOffset.Now.ToUnixTimeMilliseconds();

        for (long i = timestampStart; i < 1100; i++)
        {
            var testObj =
                new TestClassA
                {
                    StringProperty = $"Test String {i}",
                    LongProperty = i,
                    TimestampMillis = 123451234,
                };

            list.Add(testObj);
        }

        await db.WriteObjectsAsync(list, x => x.StringProperty, withTransaction: false).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task InsertManyBulkSqliteAsync()
    {
        var db = new SQLiteAsyncConnection(Path.Combine(TempPath, "sqlitenet.db"));
        await db.CreateTableAsync<TestClassA>().ConfigureAwait(false);

        var list = new List<TestClassA>();

        var timestampStart = DateTimeOffset.Now.ToUnixTimeMilliseconds();

        for (long i = timestampStart; i < 1100; i++)
        {
            var testObj =
                new TestClassA
                {
                    StringProperty = $"Test String {i}",
                    LongProperty = i,
                    TimestampMillis = 123451234,
                };

            list.Add(testObj);
        }

        await db.InsertAllAsync(list).ConfigureAwait(false);
    }

    public Tycho BuildDatabaseConnection()
    {
#if ENCRYPTED
        return new Tycho(TempPath, JsonSerializer, "tycho_cache_enc.db", "Password", rebuildCache: true);
#else
        return new Tycho(TempPath, JsonSerializer, rebuildCache: true);
#endif
    }
}

[JsonSourceGenerationOptions(
    IgnoreReadOnlyFields = true,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    WriteIndented = true)]
[JsonSerializable(typeof(TestClassA))]
[JsonSerializable(typeof(List<TestClassA>))]
[JsonSerializable(typeof(TestClassA))]
[JsonSerializable(typeof(TestClassB))]
[JsonSerializable(typeof(TestClassC))]
[JsonSerializable(typeof(TestClassD))]
[JsonSerializable(typeof(TestClassE))]
[JsonSerializable(typeof(TestClassF))]
internal partial class TestJsonContext : JsonSerializerContext
{
}
