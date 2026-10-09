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

    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    public class WideModel
    {
        public string Key { get; set; }

        public string GroupId { get; set; }

        public int Seq { get; set; }
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

        ReadUserVersion(dbFile).ShouldBe((long)Queries.FullStatisticsUserVersion);
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

    private static void SeedWideRowsDirectly(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var transaction = conn.BeginTransaction();
        using var insert = conn.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText =
            "INSERT INTO JsonValue (Key, FullTypeName, Partition, Data) " +
            "VALUES ($key, $fullTypeName, '', json_object('Key', $key, 'GroupId', $groupId, 'Seq', $seq))";

        var key = insert.Parameters.Add("$key", SqliteType.Text);
        var fullTypeName = insert.Parameters.Add("$fullTypeName", SqliteType.Text);
        var groupId = insert.Parameters.Add("$groupId", SqliteType.Text);
        var seq = insert.Parameters.Add("$seq", SqliteType.Integer);

        fullTypeName.Value = typeof(WideModel).FullName;

        foreach (var row in WideRows())
        {
            key.Value = row.Key;
            groupId.Value = row.GroupId;
            seq.Value = row.Seq;
            insert.ExecuteNonQuery();
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
    private static Tycho BuildDb2(string path, string dbName)
    {
        SqliteConnection.ClearAllPools();
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, DbPassword, rebuildCache: false, requireTypeRegistration: false);
#else
        return new Tycho(path, Serializer, dbName, rebuildCache: false, requireTypeRegistration: false);
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
