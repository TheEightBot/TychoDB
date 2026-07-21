using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace TychoDB.Benchmarks.Benchmarks;

/// <summary>
/// Read, query and startup benchmarks. The original suite only covered inserts;
/// these establish a baseline for the read/query-latency and startup-overhead
/// optimization work so changes there can be proven with before/after numbers.
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, invocationCount: 16)]
public class Querying
{
    private const int SeedCount = 1000;
    private const string DbName = "tycho_query_bench.db";

    [ParamsSource(nameof(JsonSerializers))]
    public IJsonSerializer JsonSerializer { get; set; }

    public static IEnumerable<IJsonSerializer> JsonSerializers()
        => new IJsonSerializer[]
        {
            new SystemTextJsonSerializer(
                jsonTypeSerializers: new Dictionary<Type, JsonTypeInfo>
                {
                    [typeof(TestClassA)] = TestJsonContext.Default.TestClassA,
                    [typeof(List<TestClassA>)] = TestJsonContext.Default.ListTestClassA,
                }),
            new NewtonsoftJsonSerializer(),
        };

    private string TempPath { get; } = Path.GetTempPath();

    private Tycho _db;

    [GlobalSetup]
    public async Task Setup()
    {
        var path = Path.Combine(TempPath, DbName);
        if (File.Exists(path))
        {
            File.Delete(path);
        }

        _db = await BuildDatabaseConnection(rebuild: true).ConnectAsync();

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
    }

    [GlobalCleanup]
    public void Cleanup() => _db?.Dispose();

    [Benchmark]
    public async Task ReadAllAsync()
    {
        var results = await _db.ReadObjectsAsync<TestClassA>().ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task ReadFilteredEqualsAsync()
    {
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Equals, x => x.StringProperty, "Test String 500");
        var results = await _db.ReadObjectsAsync<TestClassA>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task ReadFilteredGreaterThanAsync()
    {
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.GreaterThan, x => x.LongProperty, 500L);
        var results = await _db.ReadObjectsAsync<TestClassA>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task ReadFilteredContainsAsync()
    {
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Contains, x => x.StringProperty, "String 5");
        var results = await _db.ReadObjectsAsync<TestClassA>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    [Benchmark]
    public async Task CountAsync()
    {
        _ = await _db.CountObjectsAsync<TestClassA>().ConfigureAwait(false);
    }

    // Startup / connection overhead against an already-populated database
    // (schema-exists path + PRAGMA configuration + first query readiness).
    [Benchmark]
    public async Task ConnectStartupAsync()
    {
        using var db = await BuildDatabaseConnection(rebuild: false).ConnectAsync();
    }

    private Tycho BuildDatabaseConnection(bool rebuild)
    {
#if ENCRYPTED
        return new Tycho(TempPath, JsonSerializer, DbName, "Password", rebuildCache: rebuild, requireTypeRegistration: false);
#else
        return new Tycho(TempPath, JsonSerializer, DbName, rebuildCache: rebuild, requireTypeRegistration: false);
#endif
    }
}
