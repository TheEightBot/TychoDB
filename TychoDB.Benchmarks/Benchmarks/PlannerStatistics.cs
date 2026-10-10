using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Microsoft.Data.Sqlite;

namespace TychoDB.Benchmarks.Benchmarks;

/// <summary>
/// The query shapes whose speed depends on planner statistics, measured through the
/// public API on the layout that exposed the 5.3.1 regression: forty tiny types whose
/// names sort first, then one wide type with 40,000 rows, indexed on a unique property
/// (<c>Seq</c>) and a 200-value property (<c>GroupId</c>). The store is built, closed and
/// reopened like an app across launches, so whatever each release leaves in
/// <c>sqlite_stat1</c> at <c>CreateIndex</c>, connect and dispose is what the planner sees.
/// <para>
/// <see cref="SelectiveIndexFirst"/> controls which partial index was created last: with
/// no statistics the planner cannot rank two partial indexes and takes the most recent,
/// so the two-property filter is fast in one order and slow in the other.
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(launchCount: 1, warmupCount: 1, invocationCount: 16)]
public class PlannerStatistics
{
    private const string DbName = "tycho_planner_statistics_bench.db";
    private const int SmallTypeCount = 40;
    private const int RowsPerSmallType = 10;
    private const int GroupCount = 200;
    private const int RowsPerGroup = 200;
    private const int WideRowCount = GroupCount * RowsPerGroup;

    [Params(false, true)]
    public bool SelectiveIndexFirst { get; set; }

    private Tycho _db;

    public class WideModel
    {
        public string Key { get; set; }

        public string GroupId { get; set; }

        public int Seq { get; set; }

        public string Label { get; set; }
    }

    public class SmallModel
    {
        public string Key { get; set; }

        public int Id { get; set; }
    }

    [GlobalSetup]
    public async Task Setup()
    {
        var path = Path.GetTempPath();
        var dbFile = Path.Combine(path, DbName);
        var serializer = new NewtonsoftJsonSerializer();

        // Launch 1: the wide type is written.
        using (var db = await new Tycho(path, serializer, DbName, rebuildCache: true, requireTypeRegistration: false).ConnectAsync())
        {
            var rows = new List<WideModel>(WideRowCount);
            for (int i = 0; i < WideRowCount; i++)
            {
                rows.Add(new WideModel
                {
                    Key = $"wide-{i}",
                    GroupId = "G" + (i % GroupCount).ToString("D4", System.Globalization.CultureInfo.InvariantCulture),
                    Seq = i,
                    Label = $"row {i}",
                });
            }

            await db.WriteObjectsAsync(rows, x => x.Key).ConfigureAwait(false);
        }

        SeedSmallTypes(dbFile);

        // Launch 2: the app declares its indexes.
        SqliteConnection.ClearAllPools();
        using (var db = await new Tycho(path, serializer, DbName, rebuildCache: false, requireTypeRegistration: false).ConnectAsync())
        {
            if (SelectiveIndexFirst)
            {
                await db.CreateIndexAsync<WideModel>(x => x.Seq, "seq_idx").ConfigureAwait(false);
                await db.CreateIndexAsync<WideModel>(x => x.GroupId, "group_idx").ConfigureAwait(false);
            }
            else
            {
                await db.CreateIndexAsync<WideModel>(x => x.GroupId, "group_idx").ConfigureAwait(false);
                await db.CreateIndexAsync<WideModel>(x => x.Seq, "seq_idx").ConfigureAwait(false);
            }
        }

        // Launch 3: the one measured.
        SqliteConnection.ClearAllPools();
        _db = await new Tycho(path, serializer, DbName, rebuildCache: false, requireTypeRegistration: false).ConnectAsync();
    }

    [GlobalCleanup]
    public void Cleanup() => _db?.Dispose();

    /// <summary>Equality on an indexed 200-value property: 200 rows of 40,000.</summary>
    [Benchmark]
    public async Task IndexedEqualityAsync()
    {
        var filter = FilterBuilder<WideModel>.Create().Filter(FilterType.Equals, x => x.GroupId, "G0042");
        var results = await _db.ReadObjectsAsync<WideModel>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    /// <summary>Two indexed equalities ANDed, one unique and one 200-value: 1 row.</summary>
    [Benchmark]
    public async Task TwoIndexedEqualitiesAsync()
    {
        var filter = FilterBuilder<WideModel>.Create()
            .Filter(FilterType.Equals, x => x.GroupId, "G0042")
            .And()
            .Filter(FilterType.Equals, x => x.Seq, 20042);
        var results = await _db.ReadObjectsAsync<WideModel>(filter: filter).ConfigureAwait(false);
        _ = results.Count();
    }

    /// <summary>Every row of a 10-row type in a store dominated by the wide type.</summary>
    [Benchmark]
    public async Task ReadSmallTypeAsync()
    {
        var results = await _db.ReadObjectsAsync<SmallModel>().ConfigureAwait(false);
        _ = results.Count();
    }

    /// <summary>
    /// Forty tiny types whose names sort before the wide type's, written straight to the
    /// table so the general index starts with them, as a real store's does. One of them
    /// is <see cref="SmallModel"/>, read back through the public API.
    /// </summary>
    private static void SeedSmallTypes(string dbFile)
    {
        SqliteConnection.ClearAllPools();

        using var conn = new SqliteConnection($"Filename={dbFile}");
        conn.Open();
        using var transaction = conn.BeginTransaction();
        using var insert = conn.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            "INSERT INTO JsonValue (Key, FullTypeName, Partition, Data) " +
            "VALUES ($key, $fullTypeName, '', json_object('Key', $key, 'Id', $id))";

        var key = insert.Parameters.Add("$key", SqliteType.Text);
        var id = insert.Parameters.Add("$id", SqliteType.Integer);
        var fullTypeName = insert.Parameters.Add("$fullTypeName", SqliteType.Text);

        for (int t = 0; t < SmallTypeCount; t++)
        {
            fullTypeName.Value = t == 0
                ? typeof(SmallModel).FullName
                : "Aaa.Small" + t.ToString("D2", System.Globalization.CultureInfo.InvariantCulture);

            for (int i = 0; i < RowsPerSmallType; i++)
            {
                key.Value = "small-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
                id.Value = i;
                insert.ExecuteNonQuery();
            }
        }

        transaction.Commit();
    }
}
