using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;

namespace TychoDB.UnitTests;

/// <summary>
/// Proves the upgrade path a store takes from 4.x and the work <c>Optimize</c> does for it:
/// under <c>autoOptimize: false</c> a declaration whose index 4.x built is recorded as pending
/// and served by that index until <c>Optimize</c> rebuilds it; the string overload's
/// same-named 4.x index is recognised as stale instead of kept; the index a 4.x rollback
/// leaves next to a current one is dropped, while one another declaration records as its
/// own is kept; a current store is never written to by connect, by a declaration, or by
/// <c>Optimize</c>; and a pending definition that is not the statement Tycho built is
/// refused rather than run.
/// </summary>
[TestClass]
public class OptimizeTests
{
#if ENCRYPTED
    private const string DbPassword = "Password";
#endif

    private const string IndexName = "seq_idx";
    private const string LegacyIndexName = "idx_seq_idx_Item";
    private const string ManualIndexName = "manual_idx";
    private const string ManualPhysicalName = "idx_manual_idx_Item";
    private const string GeneralIndexName = "idx_jsonvalue_fulltypename_partition";

    private const long LookupSeq = 42;

    private static readonly string[] BuiltInLegacyIndexNames =
    {
        "idx_jsonvalue_fulltypename",
        "idx_jsonvalue_key_fulltypename",
        "idx_jsonvalue_key_fulltypename_partition",
        "idx_streamvalue_key_partition",
    };

    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    public class Item
    {
        public string Key { get; set; }

        public string Group { get; set; }

        public long Seq { get; set; }
    }

    private readonly record struct MetadataRow(
        string IndexName,
        string FullTypeName,
        string PhysicalName,
        string Definition,
        long ShapeVersion);

    [TestMethod]
    public async Task LegacyStore_WithAutoOptimizeOff_DefersTheRebuildToOptimize()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);
        long schemaVersion = ReadPragma(dbFile, "schema_version");

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        // Recorded as pending: no DDL ran, the 4.x index still serves the lookup.
        var ddl = ReadIndexDdl(dbFile);
        ddl[LegacyIndexName].ShouldContain("ON JsonValue(FullTypeName, CAST", Case.Sensitive);
        ddl.Keys.ShouldNotContain(k => k.StartsWith(LegacyIndexName + "_", StringComparison.Ordinal));
        ddl.Keys.ShouldContain("idx_jsonvalue_fulltypename");

        var pending = ReadMetadata(dbFile).ShouldHaveSingleItem();
        pending.IndexName.ShouldBe(IndexName);
        pending.FullTypeName.ShouldBe(typeof(Item).FullName);
        pending.PhysicalName.ShouldStartWith(LegacyIndexName + "_");
        pending.Definition.ShouldContain("WHERE FullTypeName", Case.Sensitive);
        pending.ShapeVersion.ShouldBe(0);

        var plan = ExplainLookup(dbFile);
        plan.ShouldContain(LegacyIndexName, Case.Sensitive);
        plan.ShouldNotContain(LegacyIndexName + "_", Case.Sensitive);
        ReadPragma(dbFile, "schema_version").ShouldBe(schemaVersion);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(0);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.Optimize();
            await AssertLookup(db);
        }

        ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[pending.PhysicalName].ShouldContain("WHERE FullTypeName", Case.Sensitive);
        ddl.ShouldContainKey(GeneralIndexName);

        foreach (var name in BuiltInLegacyIndexNames)
        {
            ddl.ShouldNotContainKey(name);
        }

        ReadMetadata(dbFile).ShouldHaveSingleItem().ShapeVersion.ShouldBe(2);
        ExplainLookup(dbFile).ShouldContain(pending.PhysicalName, Case.Sensitive);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadPragma(dbFile, "freelist_count").ShouldBe(0);

        // Done once: declaring and optimizing again touch nothing.
        var hash = FileHash(dbFile);
        schemaVersion = ReadPragma(dbFile, "schema_version");

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.Optimize();
        }

        FileHash(dbFile).ShouldBe(hash);
        ReadPragma(dbFile, "schema_version").ShouldBe(schemaVersion);
    }

    [TestMethod]
    public async Task LegacyStore_WithAutoOptimizeOn_RebuildsOnDeclaration()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
        row.ShapeVersion.ShouldBe(2);

        var ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[row.PhysicalName].ShouldContain("WHERE FullTypeName", Case.Sensitive);

        foreach (var name in BuiltInLegacyIndexNames)
        {
            ddl.ShouldNotContainKey(name);
        }

        ExplainLookup(dbFile).ShouldContain(row.PhysicalName, Case.Sensitive);
    }

    [TestMethod]
    public async Task NewIndex_WithAutoOptimizeOff_IsBuiltAtOnce()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
        }

        // Nothing in the schema serves the declaration, so deferring it would leave the
        // lookup without an index.
        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
        row.ShapeVersion.ShouldBe(2);
        ReadIndexDdl(dbFile).ShouldContainKey(row.PhysicalName);
        ExplainLookup(dbFile).ShouldContain(row.PhysicalName, Case.Sensitive);
    }

    [TestMethod]
    public async Task RolledBackStore_OrphanLegacyIndex_IsDropped()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
        }

        var current = ReadMetadata(dbFile).ShouldHaveSingleItem();
        var currentSql = ReadIndexDdl(dbFile)[current.PhysicalName];

        // 4.x reopened the store and built its own index for the same declaration.
        Execute(dbFile, LegacyIndexDdl(LegacyIndexName));
        ReadIndexDdl(dbFile).ShouldContainKey(LegacyIndexName);

        // Declared again with autoOptimize off: the orphan is left for Optimize.
        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
        }

        ReadIndexDdl(dbFile).ShouldContainKey(LegacyIndexName);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            db.Optimize();
            await AssertLookup(db);
        }

        var ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[current.PhysicalName].ShouldBe(currentSql);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShouldBe(current);

        // Declared again with autoOptimize on: dropped on the spot.
        Execute(dbFile, LegacyIndexDdl(LegacyIndexName));

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[current.PhysicalName].ShouldBe(currentSql);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShouldBe(current);
    }

    [TestMethod]
    public async Task StringOverload_SameNamedLegacyIndex_IsRecognisedAsStale()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
        }

        // 4.x built its index under the very name the string overload uses.
        Execute(dbFile, LegacyIndexDdl(ManualPhysicalName));

        // autoOptimize off: recorded as pending, the 4.x index left to serve queries.
        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", ManualIndexName);
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, CAST", Case.Sensitive);

        var pending = ReadMetadata(dbFile).ShouldHaveSingleItem();
        pending.IndexName.ShouldBe(ManualIndexName);
        pending.FullTypeName.ShouldBe("Item");
        pending.PhysicalName.ShouldBe(ManualPhysicalName);
        pending.ShapeVersion.ShouldBe(0);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            db.Optimize();
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, Partition, CAST", Case.Sensitive);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShapeVersion.ShouldBe(2);

        // Earlier 5.x releases recorded the declaration as current over the 4.x index,
        // which CREATE INDEX IF NOT EXISTS had silently kept. autoOptimize on rebuilds it.
        Execute(dbFile, $"DROP INDEX {ManualPhysicalName}; " + LegacyIndexDdl(ManualPhysicalName));
        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, CAST", Case.Sensitive);

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", ManualIndexName);
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, Partition, CAST", Case.Sensitive);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShapeVersion.ShouldBe(2);
    }

    [TestMethod]
    public async Task CurrentStore_ConnectDeclareAndOptimize_WriteNothing()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await db.CreateIndexAsync("$.Group", isNumeric: false, "Item", ManualIndexName);
        }

        var hash = FileHash(dbFile);
        long schemaVersion = ReadPragma(dbFile, "schema_version");

        foreach (var autoOptimize in new[] { true, false })
        {
            using (var db = await BuildDb2(path, dbName, autoOptimize).ConnectAsync())
            {
                await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
                await db.CreateIndexAsync("$.Group", isNumeric: false, "Item", ManualIndexName);
                db.Optimize();
                await AssertLookup(db);
            }

            FileHash(dbFile).ShouldBe(hash);
            ReadPragma(dbFile, "schema_version").ShouldBe(schemaVersion);
        }
    }

    [TestMethod]
    public async Task Connect_DeletesLeftoverStatisticsOnce_ThenLeavesTheEmptyTableAlone()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
        }

        ReadScalar(dbFile, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'sqlite_stat1'").ShouldBe(0);

        Execute(dbFile, "ANALYZE;");
        ReadScalar(dbFile, "SELECT COUNT(*) FROM sqlite_stat1").ShouldBeGreaterThan(0);

        using (await BuildDb2(path, dbName).ConnectAsync())
        {
        }

        ReadScalar(dbFile, "SELECT COUNT(*) FROM sqlite_stat1").ShouldBe(0);
        ReadScalar(dbFile, "SELECT COUNT(*) FROM sqlite_master WHERE name = 'sqlite_stat1'").ShouldBe(1);

        var hash = FileHash(dbFile);

        using (await BuildDb2(path, dbName).ConnectAsync())
        {
        }

        FileHash(dbFile).ShouldBe(hash);
    }

    [TestMethod]
    public async Task BothOverloads_UnderOneName_LeaveEachOthersIndexAlone()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        // The string overload names its index exactly as 4.x did: the generic overload's
        // legacy name. Declared second, the generic index must not take it for an orphan.
        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", IndexName);
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
        }

        var rows = ReadMetadata(dbFile);
        rows.Count.ShouldBe(2);
        rows.ShouldAllBe(r => r.ShapeVersion == 2);

        var manual = rows.Single(r => r.PhysicalName == LegacyIndexName);
        var generic = rows.Single(r => r.PhysicalName != LegacyIndexName);
        manual.FullTypeName.ShouldBe("Item");
        generic.FullTypeName.ShouldBe(typeof(Item).FullName);
        generic.PhysicalName.ShouldStartWith(LegacyIndexName + "_");

        var ddl = ReadIndexDdl(dbFile);
        ddl[LegacyIndexName].ShouldContain("ON JsonValue(FullTypeName, Partition, CAST", Case.Sensitive);
        ddl[generic.PhysicalName].ShouldContain("WHERE FullTypeName", Case.Sensitive);

        // Every later launch finds both current and touches nothing, in either order.
        var hash = FileHash(dbFile);
        long schemaVersion = ReadPragma(dbFile, "schema_version");

        foreach (var autoOptimize in new[] { true, false })
        {
            using (var db = await BuildDb2(path, dbName, autoOptimize).ConnectAsync())
            {
                await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
                await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", IndexName);
                db.Optimize();
                await AssertLookup(db);
            }

            FileHash(dbFile).ShouldBe(hash);
            ReadPragma(dbFile, "schema_version").ShouldBe(schemaVersion);
        }

        // Dropping the generic declaration leaves the string overload's index and row.
        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            db.DropIndex<Item>(IndexName).ShouldBeTrue();
            await AssertLookup(db);
        }

        ddl = ReadIndexDdl(dbFile);
        ddl.ShouldContainKey(LegacyIndexName);
        ddl.ShouldNotContainKey(generic.PhysicalName);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShouldBe(manual);
    }

    [TestMethod]
    public async Task Optimize_RefusesATamperedPendingDefinition()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
        }

        var pending = ReadMetadata(dbFile).ShouldHaveSingleItem();
        pending.ShapeVersion.ShouldBe(0);

        // Each keeps the prefix and the name Tycho wrote and carries a second statement:
        // after the column list, and inside the type literal of the WHERE clause.
        var tampered = new[]
        {
            $"CREATE INDEX IF NOT EXISTS {pending.PhysicalName}\nON JsonValue(Partition); DROP TABLE StreamValue; --",
            pending.Definition.Replace("WHERE FullTypeName = '", "WHERE FullTypeName = ''; DROP TABLE StreamValue; --", StringComparison.Ordinal),
        };

        foreach (var definition in tampered)
        {
            definition.ShouldContain("DROP TABLE", Case.Sensitive);
            WriteDefinition(dbFile, definition);

            using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
            {
                Should.Throw<TychoException>(() => db.Optimize()).Message.ShouldContain(IndexName, Case.Sensitive);
                await AssertLookup(db);
            }

            ReadScalar(dbFile, "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'StreamValue'").ShouldBe(1);
            ReadIndexDdl(dbFile).ShouldContainKey(LegacyIndexName);
            ReadIndexDdl(dbFile).ShouldNotContainKey(pending.PhysicalName);

            var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
            row.ShapeVersion.ShouldBe(0);
            row.Definition.ShouldBe(definition);
        }

        // The definition as Tycho wrote it is built.
        WriteDefinition(dbFile, pending.Definition);

        using (var db = await BuildDb2(path, dbName, autoOptimize: false).ConnectAsync())
        {
            db.Optimize();
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile).ShouldContainKey(pending.PhysicalName);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShapeVersion.ShouldBe(2);
    }

    // ---------- seeding ----------

    /// <summary>
    /// A store as 4.4.2 leaves it: its per-type index for Seq under the legacy name and
    /// shape, its four built-in indexes, no index metadata, and no incremental auto-vacuum.
    /// </summary>
    private static async Task<string> SeedLegacyStore()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
        }

        var legacySchema =
            LegacyIndexDdl(LegacyIndexName) +
            "CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename ON JsonValue (FullTypeName);" +
            "CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename ON JsonValue (Key, FullTypeName);" +
            "CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename_partition ON JsonValue (Key, FullTypeName, Partition);" +
            "CREATE INDEX IF NOT EXISTS idx_streamvalue_key_partition ON StreamValue (Key, Partition);" +
            "DELETE FROM TychoIndex;" +
            "PRAGMA auto_vacuum = NONE; VACUUM;";

        Execute(dbFile, legacySchema);

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(0);
        ReadMetadata(dbFile).ShouldBeEmpty();

        return dbFile;
    }

    /// <summary>The DDL 4.4.2 ran for CreateIndex&lt;Item&gt;(x => x.Seq, …): whole-table, led by FullTypeName.</summary>
    private static string LegacyIndexDdl(string name)
        => $"CREATE INDEX IF NOT EXISTS {name} ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC));";

    private static List<Item> Items()
    {
        var items = new List<Item>(100);
        for (int i = 0; i < 100; i++)
        {
            items.Add(new Item
            {
                Key = $"item-{i}",
                Group = "G" + (i % 10).ToString(CultureInfo.InvariantCulture),
                Seq = i,
            });
        }

        return items;
    }

    // ---------- assertions ----------
    private static async Task AssertLookup(Tycho db)
    {
        var rows = await db.ReadObjectsAsync<Item>(
            filter: FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Seq, LookupSeq));

        rows.ShouldHaveSingleItem().Seq.ShouldBe(LookupSeq);
    }

    /// <summary>The plan for the SQL the library composes for the lookup <see cref="AssertLookup"/> runs.</summary>
    private static string ExplainLookup(string dbFile)
    {
        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Seq, LookupSeq).Build(sb, Serializer, parameters);

        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // SQL is produced by the library's own builders.
        command.CommandText = "EXPLAIN QUERY PLAN " + sb;
#pragma warning restore CA2100

        command.Parameters.AddWithValue("$fullTypeName", typeof(Item).FullName);
        command.Parameters.AddWithValue("$partition", string.Empty);
        for (int i = 0; i < parameters.Count; i++)
        {
            command.Parameters.AddWithValue(
                FilterParameters.ParameterPrefix + i.ToString(CultureInfo.InvariantCulture),
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

    // ---------- inspection ----------
    private static Dictionary<string, string> ReadIndexDdl(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL";

        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = reader.GetString(1);
        }

        return result;
    }

    private static List<MetadataRow> ReadMetadata(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT IndexName, FullTypeName, PhysicalName, Definition, ShapeVersion FROM TychoIndex";

        var rows = new List<MetadataRow>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new MetadataRow(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt64(4)));
        }

        return rows;
    }

    private static long ReadPragma(string dbFile, string pragma)
        => ReadScalar(dbFile, "PRAGMA " + pragma);

    private static long ReadScalar(string dbFile, string sql)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // Test-local SQL.
        command.CommandText = sql;
#pragma warning restore CA2100

        return command.ExecuteScalar() is long value ? value : -1;
    }

    private static void Execute(string dbFile, string sql)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // Test-local SQL.
        command.CommandText = sql;
#pragma warning restore CA2100

        command.ExecuteNonQuery();
    }

    /// <summary>Overwrites the one recorded index's definition, as a tampered file would carry it.</summary>
    private static void WriteDefinition(string dbFile, string definition)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "UPDATE TychoIndex SET Definition = $definition";
        command.Parameters.AddWithValue("$definition", definition);
        command.ExecuteNonQuery().ShouldBe(1);
    }

    /// <summary>The main file's bytes, once every pooled connection has closed and checkpointed.</summary>
    private static string FileHash(string dbFile)
    {
        SqliteConnection.ClearAllPools();

        using var stream = File.OpenRead(dbFile);
        return Convert.ToHexString(SHA256.HashData(stream));
    }

    // ---------- plumbing ----------
    private static (string Path, string DbName) NewDbPath()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tycho_optimize_tests");
        Directory.CreateDirectory(dir);
        return (dir, $"{Guid.NewGuid()}.db");
    }

    private static (string Path, string DbName) Split(string dbFile)
        => (Path.GetDirectoryName(dbFile), Path.GetFileName(dbFile));

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
