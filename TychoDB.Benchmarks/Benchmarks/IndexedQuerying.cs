using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace TychoDB.Benchmarks.Benchmarks;

/// <summary>
/// Indexed-vs-unindexed query benchmarks. Indexes are created through the real
/// public expression API — including the value-type properties that exercise the
/// boxed-Convert path — so these measure what a consumer actually gets, not an
/// idealized index. Paired with the `diagnose` harness output in
/// docs/indexing-analysis.md.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, invocationCount: 16)]
public class IndexedQuerying
{
    private const string DbName = "tycho_indexed_query_bench.db";
    private const string HotPartition = "hot";

    [Params(false, true)]
    public bool Indexed { get; set; }

    [Params(1_000, 25_000)]
    public int SeedCount { get; set; }

    private string TempPath { get; } = Path.GetTempPath();

    private Tycho _db;

    private string _equalsStringTarget;
    private string _equalsPartitionTarget;
    private long _equalsNumericTarget;
    private long _rangeNumericThreshold;

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

        var list = new List<TestClassA>(SeedCount);
        for (int i = 0; i < SeedCount; i++)
        {
            list.Add(new TestClassA
            {
                StringProperty = $"Test String {i}",
                LongProperty = i,
                TimestampMillis = 123451234 + i,
            });
        }

        await _db.WriteObjectsAsync(list, x => x.StringProperty).ConfigureAwait(false);

        var partitioned = new List<TestClassA>(SeedCount);
        for (int i = 0; i < SeedCount; i++)
        {
            partitioned.Add(new TestClassA
            {
                StringProperty = $"Partitioned String {i}",
                LongProperty = i,
                TimestampMillis = 223451234 + i,
            });
        }

        await _db.WriteObjectsAsync(partitioned, x => x.StringProperty, HotPartition).ConfigureAwait(false);

        if (Indexed)
        {
            // String property: reference type, the expression API produces a real path.
            await _db.CreateIndexAsync<TestClassA>(x => x.StringProperty, "str_prop").ConfigureAwait(false);

            // Long property: value type, exercises the boxed-Convert path.
            await _db.CreateIndexAsync<TestClassA>(x => x.LongProperty, "long_prop").ConfigureAwait(false);

            // Composite with a value-type member.
            await _db.CreateIndexAsync<TestClassA>(
                new Expression<Func<TestClassA, object>>[] { x => x.StringProperty, x => x.TimestampMillis },
                "str_ts").ConfigureAwait(false);
        }

        // Selective targets: equality hits 1 row, the range hits the last 100 rows.
        _equalsStringTarget = $"Test String {SeedCount / 2}";
        _equalsPartitionTarget = $"Partitioned String {SeedCount / 2}";
        _equalsNumericTarget = SeedCount / 2;
        _rangeNumericThreshold = SeedCount - 100;
    }

    [GlobalCleanup]
    public void Cleanup() => _db?.Dispose();

    [Benchmark]
    public async Task EqualsStringAsync()
    {
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Equals, x => x.StringProperty, _equalsStringTarget);
        var results = await _db.ReadObjectsAsync<TestClassA>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task EqualsNumericAsync()
    {
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Equals, x => x.LongProperty, _equalsNumericTarget);
        var results = await _db.ReadObjectsAsync<TestClassA>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task RangeNumericAsync()
    {
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.GreaterThan, x => x.LongProperty, _rangeNumericThreshold);
        var results = await _db.ReadObjectsAsync<TestClassA>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task SortByStringTop50Async()
    {
        var sort = SortBuilder<TestClassA>.Create()
            .OrderBy(SortDirection.Ascending, x => x.StringProperty);
        var results = await _db.ReadObjectsAsync<TestClassA>(sort: sort, top: 50).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task SortByNumericTop50Async()
    {
        var sort = SortBuilder<TestClassA>.Create()
            .OrderBy(SortDirection.Ascending, x => x.LongProperty);
        var results = await _db.ReadObjectsAsync<TestClassA>(sort: sort, top: 50).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task EqualsStringWithPartitionAsync()
    {
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Equals, x => x.StringProperty, _equalsPartitionTarget);
        var results = await _db.ReadObjectsAsync<TestClassA>(partition: HotPartition, filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }
}
