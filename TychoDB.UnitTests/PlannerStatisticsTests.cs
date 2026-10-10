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
/// Proves the planner statistics TychoDB gathers keep a per-type partial index chosen
/// on a store where many small types sort ahead of one large one.
/// <para>
/// Statistics gathered with an <c>analysis_limit</c> read only the first few hundred
/// entries of each index. The general <c>(FullTypeName, Partition)</c> index is ordered
/// by type name, so its sample sees only the small types and reports a handful of rows
/// per type, while the partial index's sample sits inside one key and reports hundreds
/// of rows per key. The planner then prefers the general index and reads and parses
/// every row of the large type on each lookup. Full statistics, or none at all, keep the
/// partial index chosen.
/// </para>
/// </summary>
[TestClass]
public class PlannerStatisticsTests
{
#if ENCRYPTED
    private const string DbPassword = "Password";
#endif

    // Forty tiny types whose names sort before the test namespace, then one type with
    // 20,000 rows over 200 keys. The sample of the general index spans every small type;
    // the sample of the partial index spans four keys.
    private const int SmallTypeCount = 40;
    private const int RowsPerSmallType = 10;
    private const int GroupCount = 200;
    private const int RowsPerGroup = 100;
    private const int WideRowCount = GroupCount * RowsPerGroup;

    private const string IndexName = "group_idx";
    private const string LookupGroup = "G0042";

    // Documents large enough that each fills a leaf page of its own. Stored first, they
    // occupy the lowest rowids and the leftmost leaf of the table's b-tree.
    private const int LargeDocumentCount = 8;
    private const int LargeDocumentPayloadLength = 3000;

    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    private static readonly long StampedUserVersion =
        long.Parse(Queries.FullStatisticsUserVersion, System.Globalization.CultureInfo.InvariantCulture);

    public class WideModel
    {
        public string Key { get; set; }

        public string GroupId { get; set; }

        public int Seq { get; set; }
    }

    public class LargeModel
    {
        public string Key { get; set; }

        public string Payload { get; set; }
    }

    [TestMethod]
    public async Task CreateIndex_OnStoreWithManySmallTypesFirst_PartialIndexStaysChosen()
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
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, IndexName);

            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Reconnect_AfterBulkLoadAndDispose_PartialIndexStaysChosen()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        // Launch 1: the app declares its index before any data exists.
        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, IndexName);
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
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, IndexName);

            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Connect_AfterRowsArrivedUnanalyzed_GathersFullStatistics()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, IndexName);
        }

        // Rows that arrived without any connection gathering statistics for them: what
        // the empty store left behind is nothing, or zeros, depending on the SQLite build.
        SeedSmallTypes(dbFile);
        SeedWideRowsDirectly(dbFile);
        ReadStatRows(dbFile).ShouldAllBe(row => row.EndsWith(" 0 0 0", StringComparison.Ordinal));

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Connect_RepairsSampledStatisticsLeftByAnEarlierRelease()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        await SeedIndexedStore(path, dbName);
        LeaveSampledStatistics(dbFile);
        ExplainGroupLookup(dbFile).ShouldContain("idx_jsonvalue_fulltypename_partition", Case.Sensitive);

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        ReadUserVersion(dbFile).ShouldBe(StampedUserVersion);
        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Connect_DoesNotLowerAHigherUserVersion()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        await SeedIndexedStore(path, dbName);
        SetUserVersion(dbFile, 7);

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<WideModel>(x => x.Seq, "seq_idx");
        }

        ReadUserVersion(dbFile).ShouldBe(7L);
    }

    [TestMethod]
    public async Task Connect_WithAutoOptimizeOff_LeavesStatisticsUntilOptimizeIsCalled()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        await SeedIndexedStore(path, dbName);
        LeaveSampledStatistics(dbFile);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
        }

        // Neither connecting nor disposing touched the sampled rows.
        ReadUserVersion(dbFile).ShouldBe(0L);
        ExplainGroupLookup(dbFile).ShouldContain("idx_jsonvalue_fulltypename_partition", Case.Sensitive);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            (await db.OptimizeAsync()).ShouldBeTrue();
        }

        ReadUserVersion(dbFile).ShouldBe(StampedUserVersion);
        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Optimize_OnStoreFromAnEarlierRelease_GathersFullStatistics()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        await SeedIndexedStore(path, dbName);
        LeaveSampledStatistics(dbFile);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            db.Optimize();
        }

        ReadUserVersion(dbFile).ShouldBe(StampedUserVersion);
        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Optimize_WithCurrentStatistics_LeavesThemAlone()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        await SeedIndexedStore(path, dbName);
        var before = ReadStatRows(dbFile);
        before.ShouldNotBeEmpty();

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            db.Optimize();
        }

        ReadStatRows(dbFile).ShouldBe(before, ignoreOrder: true);
        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task CreateIndex_WithAutoOptimizeOff_LeavesStatisticsToOptimize()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
        }

        SeedSmallTypes(dbFile);

        // The connection closes between operations, so the file can be inspected while
        // the instance is alive.
        using var detached = BuildDetachedDb(path, dbName).Connect();

        await detached.CreateIndexAsync<WideModel>(x => x.GroupId, IndexName);

        var physicalName = ReadPhysicalIndexName(dbFile);
        ReadStat(dbFile, physicalName).ShouldBeNull();
        ExplainGroupLookup(dbFile).ShouldContain($"USING INDEX {physicalName}", Case.Sensitive);
        (await detached.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);

        detached.Optimize();

        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Connect_WithLargeDocumentsFirst_DoesNotGatherStatisticsEveryLaunch()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(LargeDocuments(), x => x.Key);
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, IndexName);
        }

#if !ENCRYPTED
        // SQLite 3.46+ estimates the row count from the cell counts down the leftmost
        // path of the b-tree; the large documents make that estimate an order of
        // magnitude low, so its PRAGMA optimize would re-analyze on every connect.
        SqliteWouldReanalyze(dbFile).ShouldBeTrue();
#endif

        PlantSentinelStatRow(dbFile);

        for (int launch = 0; launch < 2; launch++)
        {
            using (var db = await BuildDb2(path, dbName).ConnectAsync())
            {
                (await db.ReadObjectsAsync<WideModel>(filter: GroupFilter())).Count().ShouldBe(RowsPerGroup);
            }
        }

        HasSentinelStatRow(dbFile).ShouldBeTrue();
        AssertPartialIndexChosen(dbFile);
    }

    [TestMethod]
    public async Task Connect_GathersStatisticsAgain_OnlyAfterTenfoldGrowth()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        await SeedIndexedStore(path, dbName);
        PlantSentinelStatRow(dbFile);

        // Six times the rows: not enough.
        SeedWideRowsDirectly(dbFile, WideRowCount, 5 * WideRowCount);

        using (await BuildDb2(path, dbName).ConnectAsync())
        {
        }

        HasSentinelStatRow(dbFile).ShouldBeTrue();

        // Eleven times the rows: statistics are gathered again, over everything.
        SeedWideRowsDirectly(dbFile, 6 * WideRowCount, 5 * WideRowCount);

        using (await BuildDb2(path, dbName).ConnectAsync())
        {
        }

        HasSentinelStatRow(dbFile).ShouldBeFalse();
        ReadStat(dbFile, ReadPhysicalIndexName(dbFile))
            .ShouldBe($"{11 * WideRowCount} {11 * WideRowCount} {11 * RowsPerGroup}");
    }

    [TestMethod]
    public async Task Connect_GathersStatisticsAgain_AfterTenfoldShrink()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        await SeedIndexedStore(path, dbName);
        PlantSentinelStatRow(dbFile);
        DeleteWideRowsFrom(dbFile, WideRowCount / 20);

        using (await BuildDb2(path, dbName).ConnectAsync())
        {
        }

        HasSentinelStatRow(dbFile).ShouldBeFalse();
        ReadStat(dbFile, ReadPhysicalIndexName(dbFile))
            .ShouldBe($"{WideRowCount / 20} {WideRowCount / 20} {RowsPerGroup / 20}");
    }

    // ---------- assertions ----------
    private static void AssertPartialIndexChosen(string dbFile)
    {
        var physicalName = ReadPhysicalIndexName(dbFile);
        var plan = ExplainGroupLookup(dbFile);

        plan.ShouldContain($"USING INDEX {physicalName}", Case.Sensitive);
        plan.ShouldNotContain("idx_jsonvalue_fulltypename_partition", Case.Sensitive);

        // (Partition, JSON_EXTRACT(...)) over every row of the type, with RowsPerGroup
        // rows per key: statistics read from the whole index, not a sample of it.
        ReadStat(dbFile, physicalName).ShouldBe($"{WideRowCount} {WideRowCount} {RowsPerGroup}");
    }

    private static FilterBuilder<WideModel> GroupFilter()
        => FilterBuilder<WideModel>.Create().Filter(FilterType.Equals, x => x.GroupId, LookupGroup);

    private static string ExplainGroupLookup(string dbFile)
    {
        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        GroupFilter().Build(sb, Serializer, parameters);

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

    private static string ReadPhysicalIndexName(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT PhysicalName FROM TychoIndex WHERE IndexName = $indexName";
        command.Parameters.AddWithValue("$indexName", IndexName);
        return (string)command.ExecuteScalar();
    }

    private static string ReadStat(string dbFile, string indexName)
    {
        using var conn = OpenInspection(dbFile);
        if (!HasStatTable(conn))
        {
            return null;
        }

        using var command = conn.CreateCommand();
        command.CommandText = "SELECT stat FROM sqlite_stat1 WHERE idx = $idx";
        command.Parameters.AddWithValue("$idx", indexName);
        return command.ExecuteScalar() as string;
    }

    private static List<string> ReadStatRows(string dbFile)
    {
        var rows = new List<string>();

        using var conn = OpenInspection(dbFile);
        if (!HasStatTable(conn))
        {
            return rows;
        }

        using var command = conn.CreateCommand();
        command.CommandText = "SELECT idx || ' ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue'";

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(reader.GetString(0));
        }

        return rows;
    }

    private static bool HasStatTable(SqliteConnection conn)
    {
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_stat1'";
        return command.ExecuteScalar() is not null;
    }

    private static bool SqliteWouldReanalyze(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

        command.CommandText = "SELECT 1 FROM JsonValue WHERE FullTypeName = '' AND Partition = '' LIMIT 1";
        command.ExecuteScalar();

        command.CommandText = "PRAGMA optimize(0x10003)";
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            if (reader.GetString(0).Contains("ANALYZE", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// A whole-database ANALYZE empties sqlite_stat1 before rewriting it, so this row
    /// survives only if none ran. It names StreamValue because SQLite reads a row whose
    /// index it does not know as that table's row count, which must not touch JsonValue.
    /// </summary>
    private static void PlantSentinelStatRow(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES ('StreamValue', 'idx_sentinel', '1 1')";
        command.ExecuteNonQuery();
    }

    private static bool HasSentinelStatRow(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT count(*) FROM sqlite_stat1 WHERE idx = 'idx_sentinel'";
        return (long)command.ExecuteScalar() == 1;
    }

    private static long ReadUserVersion(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "PRAGMA user_version";
        return (long)command.ExecuteScalar();
    }

    private static void SetUserVersion(string dbFile, int value)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // A test-chosen integer literal.
        command.CommandText = "PRAGMA user_version = " + value.ToString(System.Globalization.CultureInfo.InvariantCulture);
#pragma warning restore CA2100

        command.ExecuteNonQuery();
    }

    // ---------- seeding ----------
    private static async Task SeedIndexedStore(string path, string dbName)
    {
        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(WideRows(), x => x.Key);
            await db.CreateIndexAsync<WideModel>(x => x.GroupId, IndexName);
        }

        SeedSmallTypes(Path.Combine(path, dbName));
    }

    /// <summary>Leaves what 5.3.1 left: statistics sampled at 400 rows, and no version stamp.</summary>
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
            rows.Add(new WideModel { Key = $"wide-{i}", GroupId = GroupId(i), Seq = i });
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

    private static List<LargeModel> LargeDocuments()
    {
        var payload = new string('x', LargeDocumentPayloadLength);
        var rows = new List<LargeModel>(LargeDocumentCount);
        for (int i = 0; i < LargeDocumentCount; i++)
        {
            rows.Add(new LargeModel { Key = $"large-{i}", Payload = payload });
        }

        return rows;
    }

    /// <summary>Writes wide rows with the same shape as <see cref="WideRows"/>, for Seq values from..from + count - 1.</summary>
    private static void SeedWideRowsDirectly(string dbFile, int from = 0, int count = WideRowCount)
    {
        using var conn = OpenInspection(dbFile);
        using var insert = conn.CreateCommand();
        insert.CommandText =
            """
            WITH RECURSIVE s(i) AS (SELECT $from UNION ALL SELECT i + 1 FROM s WHERE i < $from + $count - 1)
            INSERT INTO JsonValue (Key, FullTypeName, Partition, Data)
            SELECT 'wide-' || i, $fullTypeName, '',
                   json_object('Key', 'wide-' || i, 'GroupId', 'G' || printf('%04d', i % $groups), 'Seq', i)
            FROM s
            """;
        insert.Parameters.AddWithValue("$from", from);
        insert.Parameters.AddWithValue("$count", count);
        insert.Parameters.AddWithValue("$groups", GroupCount);
        insert.Parameters.AddWithValue("$fullTypeName", typeof(WideModel).FullName);
        insert.ExecuteNonQuery();
    }

    private static void DeleteWideRowsFrom(string dbFile, int seq)
    {
        using var conn = OpenInspection(dbFile);
        using var delete = conn.CreateCommand();
        delete.CommandText =
            "DELETE FROM JsonValue WHERE FullTypeName = $fullTypeName AND JSON_EXTRACT(Data, '$.Seq') >= $seq";
        delete.Parameters.AddWithValue("$fullTypeName", typeof(WideModel).FullName);
        delete.Parameters.AddWithValue("$seq", seq);
        delete.ExecuteNonQuery();
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
    /// Reopens an existing database, opted out of automatic maintenance, with a
    /// connection that closes between operations and no pooling, so an inspection
    /// connection can read the file while the instance is alive.
    /// </summary>
    private static Tycho BuildDetachedDb(string path, string dbName)
    {
        SqliteConnection.ClearAllPools();
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, DbPassword, persistConnection: false, rebuildCache: false, requireTypeRegistration: false, useConnectionPooling: false, autoOptimize: false);
#else
        return new Tycho(path, Serializer, dbName, persistConnection: false, rebuildCache: false, requireTypeRegistration: false, useConnectionPooling: false, autoOptimize: false);
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
