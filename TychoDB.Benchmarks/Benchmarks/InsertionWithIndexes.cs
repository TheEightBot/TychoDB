using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace TychoDB.Benchmarks.Benchmarks;

/// <summary>
/// Write-amplification benchmarks: the cost of maintaining user indexes on the
/// write path. IndexCount 0 is the floor (only the built-in schema indexes);
/// 1 adds a working string index; 3 adds the value-type and composite indexes
/// that currently degrade to whole-document JSON_EXTRACT(Data, '$') entries.
/// Writes replace existing keys, so each write pays full index maintenance
/// without growing the database across iterations.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, invocationCount: 16)]
public class InsertionWithIndexes
{
    private const string DbName = "tycho_indexed_insert_bench.db";
    private const int PreSeedCount = 5_000;
    private const int BatchSize = 1_000;

    [Params(0, 1, 3)]
    public int IndexCount { get; set; }

    private string TempPath { get; } = Path.GetTempPath();

    private Tycho _db;
    private TestClassA _singleObject;
    private List<TestClassA> _batch;

    [GlobalSetup]
    public async Task Setup()
    {
        var path = Path.Combine(TempPath, DbName);
        if (File.Exists(path))
        {
            File.Delete(path);
        }

        var serializer = new SystemTextJsonSerializer(
            jsonTypeSerializers: new Dictionary<Type, JsonTypeInfo>
            {
                [typeof(TestClassA)] = TestJsonContext.Default.TestClassA,
                [typeof(List<TestClassA>)] = TestJsonContext.Default.ListTestClassA,
            });

        _db = await new Tycho(TempPath, serializer, DbName, rebuildCache: true, requireTypeRegistration: false)
            .ConnectAsync();

        if (IndexCount >= 1)
        {
            await _db.CreateIndexAsync<TestClassA>(x => x.StringProperty, "str_prop").ConfigureAwait(false);
        }

        if (IndexCount >= 3)
        {
            await _db.CreateIndexAsync<TestClassA>(x => x.LongProperty, "long_prop").ConfigureAwait(false);
            await _db.CreateIndexAsync<TestClassA>(
                new Expression<Func<TestClassA, object>>[] { x => x.StringProperty, x => x.TimestampMillis },
                "str_ts").ConfigureAwait(false);
        }

        var seed = new List<TestClassA>(PreSeedCount);
        for (int i = 0; i < PreSeedCount; i++)
        {
            seed.Add(new TestClassA
            {
                StringProperty = $"Seed String {i}",
                LongProperty = i,
                TimestampMillis = 123451234 + i,
            });
        }

        await _db.WriteObjectsAsync(seed, x => x.StringProperty).ConfigureAwait(false);

        _singleObject = new TestClassA
        {
            StringProperty = "Seed String 2500",
            LongProperty = 2500,
            TimestampMillis = 123453734,
        };

        _batch = new List<TestClassA>(BatchSize);
        for (int i = 0; i < BatchSize; i++)
        {
            _batch.Add(new TestClassA
            {
                StringProperty = $"Seed String {i}",
                LongProperty = i,
                TimestampMillis = 123451234 + i,
            });
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _db?.Dispose();

    [Benchmark]
    public async Task WriteSingleAsync()
    {
        await _db.WriteObjectAsync(_singleObject, x => x.StringProperty).ConfigureAwait(false);
    }

    [Benchmark]
    public async Task WriteBatch1000Async()
    {
        await _db.WriteObjectsAsync(_batch, x => x.StringProperty).ConfigureAwait(false);
    }
}
