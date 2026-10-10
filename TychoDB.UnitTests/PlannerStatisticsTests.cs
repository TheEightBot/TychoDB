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
/// Pins how TychoDB keeps the planner able to choose the right index: full statistics for
/// <c>JsonValue</c> gathered when an index is built (or re-declared after being built on an
/// empty type), a one-time repair of the sampled rows 5.0.1–5.3.1 left behind, and nothing
/// at all at connect once a store is current.
/// <para>
/// Sampled statistics (<c>analysis_limit = 400</c>) mislead the planner: a sample of the
/// type-ordered general index credits every type with a handful of rows, while a sample of
/// a per-type partial index sits inside one key and credits every key with hundreds, so the
/// planner read every row of the type on each indexed lookup. No statistics are not enough
/// either: with two partial indexes on one type and a filter on both properties, the planner
/// cannot tell which is more selective and takes whichever was created last.
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

    private const int BucketCount = 8;

    private const string GroupIndexName = "group_idx";
    private const string SeqIndexName = "seq_idx";
    private const string BucketIndexName = "bucket_idx";
    private const string GeneralIndexName = "idx_jsonvalue_fulltypename_partition";
    private const string LookupGroup = "G0042";

    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    public class WideModel
    {
        public string Key { get; set; }

        public string GroupId { get; set; }

        public int Seq { get; set; }

        public int Bucket { get; set; }

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

        AssertFullStatistics(dbFile, GroupIndexName);
        AssertGroupLookupUsesPartialIndex(dbFile);
    }

    [TestMethod]
    public async Task Reconnect_AfterBulkLoadAndDispose_PartialIndexChosen()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        // Launch 1: the app declares its index before any data exists; statistics
        // gathered on an empty type are not kept.
        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);
        }

        ReadStatRows(dbFile).ShouldBeEmpty();

        SeedSmallTypes(dbFile);

        // Launch 2: a sync loads the large type, then the app closes.
        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
        }

        // Launch 3: the index is re-declared; it has rows now but no statistics, so
        // they are gathered before the query runs.
        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);

            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        AssertFullStatistics(dbFile, GroupIndexName);
        AssertGroupLookupUsesPartialIndex(dbFile);
    }

    [TestMethod]
    public async Task Connect_RepairsSampledStatisticsLeftByAnEarlierRelease()
    {
        var dbFile = await SeedIndexedStore();
        var (path, dbName) = (Path.GetDirectoryName(dbFile), Path.GetFileName(dbFile));

        LeaveSampledStatistics(dbFile);
        Explain(dbFile, filter: GroupFilter()).ShouldContain(GeneralIndexName, Case.Sensitive);

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        ReadUserVersion(dbFile).ShouldBe(1);
        AssertFullStatistics(dbFile, GroupIndexName);
        AssertGroupLookupUsesPartialIndex(dbFile);
    }

    [TestMethod]
    public async Task Connect_WithAutoOptimizeOff_LeavesStatisticsUntilOptimize()
    {
        var dbFile = await SeedIndexedStore();
        var (path, dbName) = (Path.GetDirectoryName(dbFile), Path.GetFileName(dbFile));

        LeaveSampledStatistics(dbFile);
        var sampled = ReadStatRows(dbFile);

        // Connect leaves the store exactly as the previous release did.
        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        ReadUserVersion(dbFile).ShouldBe(0);
        ReadStatRows(dbFile).ShouldBe(sampled);
        Explain(dbFile, filter: GroupFilter()).ShouldContain(GeneralIndexName, Case.Sensitive);

        // Optimize does the repair instead, once.
        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            (await db.OptimizeAsync()).ShouldBeTrue();
        }

        ReadUserVersion(dbFile).ShouldBe(1);
        AssertFullStatistics(dbFile, GroupIndexName);
        AssertGroupLookupUsesPartialIndex(dbFile);
    }

    [TestMethod]
    public async Task Connect_OnStoreWithoutIndexes_DiscardsStatisticsAndStamps()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
        }

        LeaveSampledStatistics(dbFile);
        ReadStatRows(dbFile).ShouldNotBeEmpty();

        using (await BuildDb2(path, dbName).ConnectAsync())
        {
        }

        // Nothing to rank, so nothing to keep: the rows are discarded rather than analyzed.
        ReadStatRows(dbFile).ShouldBeEmpty();
        ReadUserVersion(dbFile).ShouldBe(1);
    }

    [TestMethod]
    public async Task FreshStore_IsStampedAndHasNoStatisticsTable()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (await BuildDb(path, dbName).ConnectAsync())
        {
        }

        ReadUserVersion(dbFile).ShouldBe(1);
        StatisticsTableExists(dbFile).ShouldBeFalse();
    }

    [TestMethod]
    public async Task Connect_OnCurrentStore_WritesNothing()
    {
        var dbFile = await SeedIndexedStore();
        var (path, dbName) = (Path.GetDirectoryName(dbFile), Path.GetFileName(dbFile));

        var schemaVersion = ReadSchemaVersion(dbFile);
        var statRows = ReadStatRows(dbFile);

        for (int i = 0; i < 3; i++)
        {
            using var db = await BuildDb2(path, dbName).ConnectAsync();
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);
            await db.CreateIndexAsync<WideModel>(x => x.Seq, SeqIndexName);
            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        ReadSchemaVersion(dbFile).ShouldBe(schemaVersion);
        ReadStatRows(dbFile).ShouldBe(statRows);
        ReadUserVersion(dbFile).ShouldBe(1);
    }

    [TestMethod]
    public async Task TwoIndexedEqualities_UseTheMoreSelectiveIndex_InEitherCreationOrder()
    {
        foreach (var selectiveFirst in new[] { true, false })
        {
            var dbFile = await SeedIndexedStore(seqIndexFirst: selectiveFirst);

            // Seq is unique; GroupId has 100 rows per value. Without statistics the planner
            // takes whichever partial index was created last.
            var plan = Explain(dbFile, filter: GroupFilter().And().Filter(FilterType.Equals, x => x.Seq, 42));

            plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, SeqIndexName)}", Case.Sensitive, $"seqIndexFirst: {selectiveFirst}");
            plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
        }
    }

    [TestMethod]
    public async Task WithStatistics_EqualityUsesThePartialIndex()
    {
        var dbFile = await SeedIndexedStore();

        var plan = Explain(dbFile, filter: GroupFilter());

        plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, GroupIndexName)}", Case.Sensitive);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    [TestMethod]
    public async Task WithStatistics_RangeUsesThePartialIndex()
    {
        var dbFile = await SeedIndexedStore();

        var plan = Explain(
            dbFile,
            filter: FilterBuilder<WideModel>.Create().Filter(FilterType.GreaterThan, x => x.Seq, WideRowCount - 1000));

        plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, SeqIndexName)}", Case.Sensitive);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    [TestMethod]
    public async Task WithStatistics_SortWithLimitUsesThePartialIndex()
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
    public async Task WithStatistics_NonSelectiveIndexedEqualityUsesThePartialIndex()
    {
        var dbFile = await SeedIndexedStore();

        // One of eight values: 2,500 rows, an eighth of the table. The general index's
        // pinned row says half, so the partial index still wins.
        var plan = Explain(dbFile, filter: FilterBuilder<WideModel>.Create().Filter(FilterType.Equals, x => x.Bucket, 3));

        plan.ShouldContain($"USING INDEX {ReadPhysicalIndexName(dbFile, BucketIndexName)}", Case.Sensitive);
        plan.ShouldNotContain(GeneralIndexName, Case.Sensitive);
    }

    [TestMethod]
    public async Task WithStatistics_UnindexedPropertyUsesTheGeneralIndex()
    {
        var dbFile = await SeedIndexedStore();

        var filter = FilterBuilder<WideModel>.Create().Filter(FilterType.Equals, x => x.Label, "row 7");

        Explain(dbFile, filter: filter).ShouldContain($"USING INDEX {GeneralIndexName}", Case.Sensitive);
        Explain(dbFile, filter: filter, fullTypeName: SmallTypeName(0)).ShouldContain($"USING INDEX {GeneralIndexName}", Case.Sensitive);
    }

    [TestMethod]
    public async Task WithStatistics_WholeTypeUsesTheGeneralIndex()
    {
        var dbFile = await SeedIndexedStore();

        // The wide type is 98% of the table and the small type 0.05%; both are reached
        // through the general index, never by a scan, whatever ANALYZE averaged.
        Explain(dbFile).ShouldContain($"USING INDEX {GeneralIndexName}", Case.Sensitive);
        Explain(dbFile, fullTypeName: SmallTypeName(0)).ShouldContain($"USING INDEX {GeneralIndexName}", Case.Sensitive);
    }

    [TestMethod]
    public async Task Statistics_GeneralIndexRowIsPinnedToHalfTheTable()
    {
        var dbFile = await SeedIndexedStore();

        // Statistics were gathered when the wide type was the whole table.
        ReadStatRows(dbFile).ShouldContain($"JsonValue {GeneralIndexName} {WideRowCount} {WideRowCount / 2} {WideRowCount / 2}");
    }

    [TestMethod]
    public async Task WithStatistics_CountUsesTheGeneralIndexWithoutReadingRows()
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

    /// <summary>The index has a full statistics row: the type's true row count, not a 400-row sample.</summary>
    private static void AssertFullStatistics(string dbFile, string indexName)
    {
        var physicalName = ReadPhysicalIndexName(dbFile, indexName);
        var row = ReadStatRows(dbFile).SingleOrDefault(r => r.StartsWith($"JsonValue {physicalName} ", StringComparison.Ordinal));

        row.ShouldNotBeNull($"no statistics row for {physicalName}");
        row.ShouldStartWith($"JsonValue {physicalName} {WideRowCount} ");
    }

    private static long ReadUserVersion(string dbFile) => ReadPragma(dbFile, "user_version");

    private static long ReadSchemaVersion(string dbFile) => ReadPragma(dbFile, "schema_version");

    private static long ReadPragma(string dbFile, string pragma)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
#pragma warning disable CA2100 // Pragma name is a test constant.
        command.CommandText = "PRAGMA " + pragma;
#pragma warning restore CA2100
        return (long)command.ExecuteScalar();
    }

    private static bool StatisticsTableExists(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_stat1'";
        return command.ExecuteScalar() is not null;
    }

    private static FilterBuilder<WideModel> GroupFilter()
        => FilterBuilder<WideModel>.Create().Filter(FilterType.Equals, x => x.GroupId, LookupGroup);

    /// <summary>The plan for the SQL the library composes for a read with these options.</summary>
    private static string Explain(
        string dbFile,
        FilterBuilder<WideModel> filter = null,
        SortBuilder<WideModel> sort = null,
        int? top = null,
        bool count = false,
        string fullTypeName = null)
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

        command.Parameters.AddWithValue("$fullTypeName", fullTypeName ?? typeof(WideModel).FullName);
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

    /// <summary>Every statistics row in the store, as "tbl idx stat".</summary>
    private static List<string> ReadStatRows(string dbFile)
    {
        var rows = new List<string>();

        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_stat1'";
        if (command.ExecuteScalar() is null)
        {
            return rows;
        }

        command.CommandText = "SELECT tbl || ' ' || ifnull(idx, '-') || ' ' || stat FROM sqlite_stat1";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    // ---------- seeding ----------

    /// <summary>A store with the wide type indexed on GroupId and Seq, and the small types ahead of it.</summary>
    private static async Task<string> SeedIndexedStore(bool seqIndexFirst = false)
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);

            if (seqIndexFirst)
            {
                await db.CreateIndexAsync<WideModel>(x => x.Seq, SeqIndexName);
                await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);
            }
            else
            {
                await db.CreateIndexAsync<WideModel>(x => x.GroupId, GroupIndexName);
                await db.CreateIndexAsync<WideModel>(x => x.Seq, SeqIndexName);
            }

            await db.CreateIndexAsync<WideModel>(x => x.Bucket, BucketIndexName);
        }

        var dbFile = Path.Combine(path, dbName);
        SeedSmallTypes(dbFile);
        return dbFile;
    }

    /// <summary>
    /// Leaves what 5.3.1 left: statistics sampled at 400 rows per index, and no
    /// user_version stamp.
    /// </summary>
    private static void LeaveSampledStatistics(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "PRAGMA analysis_limit = 400; ANALYZE; PRAGMA user_version = 0;";
        command.ExecuteNonQuery();
    }

    private static List<WideModel> WideRows()
    {
        var rows = new List<WideModel>(WideRowCount);
        for (int i = 0; i < WideRowCount; i++)
        {
            rows.Add(new WideModel { Key = $"wide-{i}", GroupId = GroupId(i), Seq = i, Bucket = i % BucketCount, Label = $"row {i}" });
        }

        return rows;
    }

    private static string GroupId(int i)
        => "G" + (i % GroupCount).ToString("D4", System.Globalization.CultureInfo.InvariantCulture);

    private static string SmallTypeName(int t)
        => "Aaa.Small" + t.ToString("D2", System.Globalization.CultureInfo.InvariantCulture);

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
            fullTypeName.Value = SmallTypeName(t);
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
