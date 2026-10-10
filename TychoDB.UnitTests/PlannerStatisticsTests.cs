using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;

namespace TychoDB.UnitTests;

/// <summary>
/// Proves the planner chooses the right index for every query shape TychoDB emits with no
/// planner statistics at all, and that TychoDB gathers none.
/// <para>
/// Without a <c>sqlite_stat1</c> row the planner credits a partial index with half the
/// table and every index with a fixed selectivity per equality column, so a per-type
/// partial index always beats the general <c>(FullTypeName, Partition)</c> index for the
/// property it covers, whatever the data looks like. Statistics can only make that choice
/// worse: a sample of the type-ordered general index sees only the smallest types and
/// credits every type with a handful of rows, while a sample of the partial index sits
/// inside one key and credits every key with hundreds, and the planner then reads every
/// row of the type on each lookup (5.3.1).
/// </para>
/// </summary>
[TestClass]
public class PlannerStatisticsTests
{
#if ENCRYPTED
    private const string DbPassword = "Password";
#endif

    // Forty tiny types whose names sort before the test namespace, then one type with
    // 20,000 rows over 200 keys: the layout on which sampled statistics mislead.
    private const int SmallTypeCount = 40;
    private const int RowsPerSmallType = 10;
    private const int GroupCount = 200;
    private const int RowsPerGroup = 100;
    private const int WideRowCount = GroupCount * RowsPerGroup;

    private const string GroupIndexName = "group_idx";
    private const string SeqIndexName = "seq_idx";
    private const string GeneralIndexName = "idx_jsonvalue_fulltypename_partition";
    private const string LookupGroup = "G0042";

    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    public class WideModel
    {
        public string Key { get; set; }

        public string GroupId { get; set; }

        public int Seq { get; set; }

        public string Label { get; set; }
    }

    [TestMethod]
    public async Task CreateIndex_OnStoreWithManySmallTypesFirst_PartialIndexChosen()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
        }

        SeedSmallTypes(dbFile);

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);

            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        HasStatTable(dbFile).ShouldBeFalse();
        AssertGroupLookupUsesPartialIndex(dbFile);
    }

    [TestMethod]
    public async Task Reconnect_AfterBulkLoadAndDispose_PartialIndexChosen()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        // Launch 1: the app declares its index before any data exists.
        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);
        }

        SeedSmallTypes(dbFile);

        // Launch 2: a sync loads the large type, then the app closes.
        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
        }

        // Launch 3: the index is re-declared (a no-op) and queried.
        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);

            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        HasStatTable(dbFile).ShouldBeFalse();
        AssertGroupLookupUsesPartialIndex(dbFile);
    }

    [TestMethod]
    public async Task WithoutStatistics_EqualityUsesThePartialIndex()
    {
        var dbFile = await SeedIndexedStore();

        var plan = Explain(dbFile, filter: GroupFilter());

        plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, GroupIndexName)}", Case.Sensitive);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    [TestMethod]
    public async Task WithoutStatistics_RangeUsesThePartialIndex()
    {
        var dbFile = await SeedIndexedStore();

        var plan = Explain(
            dbFile,
            filter: FilterBuilder<WideModel>.Create().Filter(FilterType.GreaterThan, x => x.Seq, WideRowCount - 1000));

        plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, SeqIndexName)}", Case.Sensitive);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    [TestMethod]
    public async Task WithoutStatistics_SortWithLimitUsesThePartialIndex()
    {
        var dbFile = await SeedIndexedStore();

        var plan = Explain(
            dbFile,
            sort: SortBuilder<WideModel>.Create().OrderBy(SortDirection.Ascending, x => x.Seq),
            top: 20);

        plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, SeqIndexName)}", Case.Sensitive);
        plan.ShouldNotContain("TEMP B-TREE", Case.Sensitive);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    [TestMethod]
    public async Task WithoutStatistics_TwoIndexedEqualitiesUseOneOfThePartialIndexes()
    {
        var dbFile = await SeedIndexedStore();

        var plan = Explain(
            dbFile,
            filter: GroupFilter().And().Filter(FilterType.Equals, x => x.Seq, 42));

        var partialIndexes = new[]
        {
            $"USING INDEX {ReadPhysicalIndexName(dbFile, GroupIndexName)}",
            $"USING INDEX {ReadPhysicalIndexName(dbFile, SeqIndexName)}",
        };

        partialIndexes.Count(name => plan.Contains(name, StringComparison.Ordinal)).ShouldBe(1);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    [TestMethod]
    public async Task WithoutStatistics_UnindexedPropertyUsesTheGeneralIndex()
    {
        var dbFile = await SeedIndexedStore();

        var plan = Explain(
            dbFile,
            filter: FilterBuilder<WideModel>.Create().Filter(FilterType.Equals, x => x.Label, "row 7"));

        plan.ShouldContain($"USING INDEX {GeneralIndexName}", Case.Sensitive);
    }

    [TestMethod]
    public async Task WithoutStatistics_WholeTypeUsesTheGeneralIndex()
    {
        var dbFile = await SeedIndexedStore();

        Explain(dbFile).ShouldContain($"USING INDEX {GeneralIndexName}", Case.Sensitive);
    }

    [TestMethod]
    public async Task WithoutStatistics_CountUsesTheGeneralIndexWithoutReadingRows()
    {
        var dbFile = await SeedIndexedStore();

        Explain(dbFile, count: true).ShouldContain($"USING COVERING INDEX {GeneralIndexName}", Case.Sensitive);
    }

    // ---------- assertions ----------
    private static void AssertGroupLookupUsesPartialIndex(string dbFile)
    {
        var plan = Explain(dbFile, filter: GroupFilter());

        plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, GroupIndexName)}", Case.Sensitive);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    private static FilterBuilder<WideModel> GroupFilter()
        => FilterBuilder<WideModel>.Create().Filter(FilterType.Equals, x => x.GroupId, LookupGroup);

    /// <summary>The plan for the SQL the library composes for a read with these options.</summary>
    private static string Explain(
        string dbFile,
        FilterBuilder<WideModel> filter = null,
        SortBuilder<WideModel> sort = null,
        int? top = null,
        bool count = false)
    {
        var sb = new StringBuilder(count ? Queries.SelectCountFromJsonValueWithFullTypeName : Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();

        filter?.Build(sb, Serializer, parameters);
        sort?.Build(sb, Serializer);

        if (top is not null)
        {
            sb.AppendLine().AppendLine(Queries.Limit(top.Value));
        }

        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // SQL is produced by the library's own builders.
        command.CommandText = "EXPLAIN QUERY PLAN " + sb;
#pragma warning restore CA2100

        command.Parameters.AddWithValue("$fullTypeName", typeof(WideModel).FullName);
        command.Parameters.AddWithValue("$partition", string.Empty);
        for (int i = 0; i < parameters.Count; i++)
        {
            command.Parameters.AddWithValue(
                FilterParameters.ParameterPrefix + i.ToString(System.Globalization.CultureInfo.InvariantCulture),
                parameters.Values[i] ?? (object)DBNull.Value);
        }

        var plan = new StringBuilder();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            plan.AppendLine(reader.GetString(reader.FieldCount - 1));
        }

        return plan.ToString();
    }

    private static string ReadPhysicalIndexName(string dbFile, string indexName)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT PhysicalName FROM TychoIndex WHERE IndexName = $indexName";
        command.Parameters.AddWithValue("$indexName", indexName);
        return (string)command.ExecuteScalar();
    }

    private static bool HasStatTable(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name LIKE 'sqlite_stat%'";
        return command.ExecuteScalar() is not null;
    }

    // ---------- seeding ----------

    /// <summary>A store with the wide type indexed on GroupId and Seq, and the small types ahead of it.</summary>
    private static async Task<string> SeedIndexedStore()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);
            await db.CreateIndexAsync<WideModel>(x => x.Seq, SeqIndexName);
        }

        var dbFile = Path.Combine(path, dbName);
        SeedSmallTypes(dbFile);
        return dbFile;
    }

    private static List<WideModel> WideRows()
    {
        var rows = new List<WideModel>(WideRowCount);
        for (int i = 0; i < WideRowCount; i++)
        {
            rows.Add(new WideModel { Key = $"wide-{i}", GroupId = GroupId(i), Seq = i, Label = $"row {i}" });
        }

        return rows;
    }

    private static string GroupId(int i)
        => "G" + (i % GroupCount).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);

    private static void SeedSmallTypes(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var transaction = conn.BeginTransaction();
        using var insert = conn.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            "INSERT INTO JsonValue (Key, FullTypeName, Partition, Data) " +
            "VALUES ($key, $fullTypeName, '', json_object('Id', $key))";

        var key = insert.Parameters.Add("$key", SqliteType.Text);
        var fullTypeName = insert.Parameters.Add("$fullTypeName", SqliteType.Text);

        for (int t = 0; t < SmallTypeCount; t++)
        {
            fullTypeName.Value = "Aaa.Small" + t.ToString("D2", System.Globalization.CultureInfo.InvariantCulture);
            for (int i = 0; i < RowsPerSmallType; i++)
            {
                key.Value = "small-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
                insert.ExecuteNonQuery();
            }
        }

        transaction.Commit();
    }

    // ---------- plumbing ----------
    private static (string Path, string DbName) NewDbPath()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tycho_planner_statistics_tests");
        Directory.CreateDirectory(dir);
        return (dir, $"{Guid.NewGuid()}.db");
    }

    private static Tycho BuildDb(string path, string dbName)
    {
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, DbPassword, rebuildCache: true, requireTypeRegistration: false);
#else
        return new Tycho(path, Serializer, dbName, rebuildCache: true, requireTypeRegistration: false);
#endif
    }

    /// <summary>Reopens an existing database without rebuilding it.</summary>
    private static Tycho BuildDb2(string path, string dbName, bool autoOptimize = true)
    {
        SqliteConnection.ClearAllPools();
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, DbPassword, rebuildCache: false, requireTypeRegistration: false, autoOptimize: autoOptimize);
#else
        return new Tycho(path, Serializer, dbName, rebuildCache: false, requireTypeRegistration: false, autoOptimize: autoOptimize);
#endif
    }

    /// <summary>
    /// Opens an inspection connection carrying the same key the database was created with.
    /// Tycho holds locking_mode = EXCLUSIVE and pooling keeps a disposed connection's lock
    /// alive, so the pool must be cleared after the Tycho instance is disposed and before
    /// the file is reopened.
    /// </summary>
    private static SqliteConnection OpenInspection(string dbFile)
    {
        SqliteConnection.ClearAllPools();

        var connectionStringBuilder =
            new SqliteConnectionStringBuilder
            {
                ConnectionString = $"Filename={dbFile}",
                Cache = SqliteCacheMode.Private,
                Mode = SqliteOpenMode.ReadWriteCreate,
            };

#if ENCRYPTED
        connectionStringBuilder.Password = DbPassword;
#endif

        var conn = new SqliteConnection(connectionStringBuilder.ToString());
        conn.Open();
        return conn;
    }
}
