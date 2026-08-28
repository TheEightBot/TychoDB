using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;

namespace TychoDB.UnitTests;

/// <summary>
/// Locks in the SQL that index creation actually generates, and the query plans
/// the generated SQL produces. The original defect — value-type properties being
/// indexed as JSON_EXTRACT(Data, '$'), i.e. the whole document — was invisible to
/// the existing tests because they only asserted that CreateIndex returned true.
/// See docs/indexing-analysis.md.
/// </summary>
[TestClass]
public class IndexDdlTests
{
    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    public class IndexTestModel
    {
        public string StringProperty { get; set; }

        public int IntProperty { get; set; }

        public long LongProperty { get; set; }

        public double DoubleProperty { get; set; }

        public bool BoolProperty { get; set; }

        public Guid GuidProperty { get; set; }

        public DateTime DateTimeProperty { get; set; }

        public int? NullableIntProperty { get; set; }

        public NestedModel Nested { get; set; }
    }

    public class NestedModel
    {
        public int NestedInt { get; set; }

        public string NestedString { get; set; }
    }

    // ---------- generated DDL ----------
    [TestMethod]
    public async Task CreateIndex_ValueTypeProperties_ProduceRealPaths_NotWholeDocument()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<IndexTestModel>(x => x.IntProperty, "int_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.DoubleProperty, "double_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.BoolProperty, "bool_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.GuidProperty, "guid_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.DateTimeProperty, "datetime_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.NullableIntProperty, "nullable_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.Nested.NestedInt, "nested_idx");
        }

        var ddl = ReadIndexDdl(Path.Combine(path, dbName));

        // The regression itself: no index may target the whole document.
        foreach (var (name, sql) in ddl)
        {
            if (name.StartsWith("idx_", StringComparison.Ordinal))
            {
                sql.ShouldNotContain(
                    "JSON_EXTRACT(Data, '$')",
                    Case.Sensitive,
                    $"Index {name} indexes the entire document instead of a property.");
            }
        }

        Find(ddl, "int_idx").ShouldContain("'$.IntProperty'");
        Find(ddl, "long_idx").ShouldContain("'$.LongProperty'");
        Find(ddl, "double_idx").ShouldContain("'$.DoubleProperty'");
        Find(ddl, "bool_idx").ShouldContain("'$.BoolProperty'");
        Find(ddl, "guid_idx").ShouldContain("'$.GuidProperty'");
        Find(ddl, "datetime_idx").ShouldContain("'$.DateTimeProperty'");
        Find(ddl, "nullable_idx").ShouldContain("'$.NullableIntProperty'");
        Find(ddl, "nested_idx").ShouldContain("'$.Nested.NestedInt'");
    }

    [TestMethod]
    public async Task CreateIndex_NumericProperties_UseCastForm_MatchingFilterSql()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<IndexTestModel>(x => x.IntProperty, "int_idx");
            await db.CreateIndexAsync<IndexTestModel>(x => x.StringProperty, "str_idx");
        }

        var ddl = ReadIndexDdl(Path.Combine(path, dbName));

        // Numeric properties must use the CAST(... as NUMERIC) form, because that
        // is exactly what FilterBuilder emits for numeric comparisons. SQLite only
        // matches expression indexes structurally.
        Find(ddl, "int_idx").ShouldContain("CAST(JSON_EXTRACT(Data, '$.IntProperty') as NUMERIC)");

        // Non-numeric properties must use the plain form, matching FilterBuilder's
        // equality SQL for strings.
        Find(ddl, "str_idx").ShouldContain("JSON_EXTRACT(Data, '$.StringProperty')");
        Find(ddl, "str_idx").ShouldNotContain("as NUMERIC");
    }

    [TestMethod]
    public async Task CreateIndex_UsesPartialShape_ScopedToTypeAndLedByPartition()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");
        }

        var sql = Find(ReadIndexDdl(Path.Combine(path, dbName)), "long_idx");

        // Partition leads the index: every generated read constrains Partition.
        sql.ShouldContain("ON JsonValue(Partition,");

        // The index carries no entries for rows of other stored types.
        sql.ShouldContain($"WHERE FullTypeName = '{typeof(IndexTestModel).FullName}'");
    }

    [TestMethod]
    public async Task CreateIndex_SameNameDifferentNamespaces_DoNotCollide()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "shared_name");
            await db.CreateIndexAsync<Collision.IndexTestModel>(x => x.OtherProperty, "shared_name");
        }

        var ddl = ReadIndexDdl(Path.Combine(path, dbName));
        var matching = ddl.Where(kvp => kvp.Key.StartsWith("idx_shared_name_", StringComparison.Ordinal)).ToList();

        // Before the hash suffix, the second CREATE INDEX IF NOT EXISTS silently
        // no-opped and the second type got no index at all.
        matching.Count.ShouldBe(2);
        matching.ShouldContain(kvp => kvp.Value.Contains("'$.LongProperty'", StringComparison.Ordinal));
        matching.ShouldContain(kvp => kvp.Value.Contains("'$.OtherProperty'", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task CreateIndex_CalledRepeatedly_IsIdempotent_AndRecordsMetadataOnce()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            // Mobile apps re-declare their indexes on every launch.
            for (int i = 0; i < 3; i++)
            {
                await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");
            }
        }

        var dbFile = Path.Combine(path, dbName);

        ReadIndexDdl(dbFile)
            .Count(kvp => kvp.Key.StartsWith("idx_long_idx_", StringComparison.Ordinal))
            .ShouldBe(1);

        ReadMetadata(dbFile).Count.ShouldBe(1);
    }

    [TestMethod]
    public async Task CreateIndex_ChangedDefinition_RebuildsAndLeavesNoStaleIndex()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "movable");
        }

        using (var db = await BuildDb2(path, dbName).ConnectAsync())
        {
            // Same logical index name, different property — the old b-tree must go.
            await db.CreateIndexAsync<IndexTestModel>(x => x.StringProperty, "movable");
        }

        var ddl = ReadIndexDdl(Path.Combine(path, dbName));
        var matching = ddl.Where(kvp => kvp.Key.StartsWith("idx_movable_", StringComparison.Ordinal)).ToList();

        matching.Count.ShouldBe(1);
        matching[0].Value.ShouldContain("'$.StringProperty'");
        matching[0].Value.ShouldNotContain("'$.LongProperty'");
    }

    [TestMethod]
    public async Task CreateIndex_RefreshesPlannerStatistics()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");
        }

        // Before this bucket sqlite_stat1 did not exist at all, so the planner ran
        // on default heuristics for the life of the process.
        using var conn = OpenInspection(Path.Combine(path, dbName));
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_stat1'";
        Convert.ToInt32(command.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture).ShouldBe(1);
    }

    [TestMethod]
    public async Task CreateIndex_Composite_ResolvesEveryMember()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<IndexTestModel>(
                new Expression<Func<IndexTestModel, object>>[] { x => x.StringProperty, x => x.LongProperty },
                "composite_idx");
        }

        var sql = Find(ReadIndexDdl(Path.Combine(path, dbName)), "composite_idx");

        sql.ShouldContain("JSON_EXTRACT(Data, '$.StringProperty')");
        sql.ShouldContain("CAST(JSON_EXTRACT(Data, '$.LongProperty') as NUMERIC)");
        sql.ShouldNotContain("JSON_EXTRACT(Data, '$')");
    }

    /// <summary>
    /// The manual overload only receives a short type name, so it cannot scope a
    /// partial index to a stored type. It falls back to a non-partial shape led by
    /// FullTypeName and Partition — still matched by every generated query, just
    /// larger. This test pins that contract.
    /// </summary>
    [TestMethod]
    public async Task CreateIndex_ManualOverload_UsesNonPartialFallbackShape()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
            await db.CreateIndexAsync("$.LongProperty", isNumeric: true, "IndexTestModel", "manual_idx");
        }

        var dbFile = Path.Combine(path, dbName);
        var sql = ReadIndexDdl(dbFile)["idx_manual_idx_IndexTestModel"];

        sql.ShouldContain("ON JsonValue(FullTypeName, Partition,");
        sql.ShouldContain("CAST(JSON_EXTRACT(Data, '$.LongProperty') as NUMERIC)");
        sql.ShouldNotContain("WHERE FullTypeName");

        // The fallback shape must still be chosen by the planner.
        ExplainFilter(dbFile, f => f.Filter(FilterType.Equals, x => x.LongProperty, 42L))
            .ShouldContain("idx_manual_idx_IndexTestModel");
    }

    // ---------- query plans over the real generated SQL ----------
    [TestMethod]
    public async Task NumericFilters_UseTheCreatedIndex()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");
        }

        var dbFile = Path.Combine(path, dbName);

        ExplainFilter(dbFile, f => f.Filter(FilterType.Equals, x => x.LongProperty, 42L))
            .ShouldContain("idx_long_idx_IndexTestModel");

        ExplainFilter(dbFile, f => f.Filter(FilterType.GreaterThan, x => x.LongProperty, 90L))
            .ShouldContain("idx_long_idx_IndexTestModel");

        ExplainFilter(dbFile, f => f.Filter(FilterType.LessThanOrEqualTo, x => x.LongProperty, 10L))
            .ShouldContain("idx_long_idx_IndexTestModel");
    }

    [TestMethod]
    public async Task StringEqualityFilter_UsesTheCreatedIndex()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
            await db.CreateIndexAsync<IndexTestModel>(x => x.StringProperty, "str_idx");
        }

        ExplainFilter(Path.Combine(path, dbName), f => f.Filter(FilterType.Equals, x => x.StringProperty, "Item 42"))
            .ShouldContain("idx_str_idx_IndexTestModel");
    }

    [TestMethod]
    public async Task GuidEqualityFilter_UsesTheCreatedIndex()
    {
        var (path, dbName) = NewDbPath();
        var target = Guid.Parse("11111111-2222-3333-4444-555555555555");

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            var seed = SeedData();
            seed[7].GuidProperty = target;
            await db.WriteObjectsAsync(seed, x => x.StringProperty);
            await db.CreateIndexAsync<IndexTestModel>(x => x.GuidProperty, "guid_idx");
        }

        ExplainFilter(Path.Combine(path, dbName), f => f.Filter(FilterType.Equals, x => x.GuidProperty, target))
            .ShouldContain("idx_guid_idx_IndexTestModel");
    }

    /// <summary>
    /// SortBuilder must emit the same JSON_EXTRACT form as the index, so an ordered
    /// read streams out of the index instead of sorting the whole result set into a
    /// temporary b-tree.
    /// </summary>
    [TestMethod]
    public async Task Sort_UsesTheIndex_AndAvoidsATemporaryBTree()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
            await db.CreateIndexAsync<IndexTestModel>(x => x.StringProperty, "str_idx");
        }

        var sort = SortBuilder<IndexTestModel>.Create().OrderBy(SortDirection.Ascending, x => x.StringProperty);
        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        sort.Build(sb, Serializer);

        var plan = Explain(Path.Combine(path, dbName), sb.ToString(), new FilterParameters());

        plan.ShouldNotContain("USE TEMP B-TREE FOR ORDER BY");
        plan.ShouldContain("idx_str_idx_");
    }

    [TestMethod]
    public async Task Sort_OrdersResultsCorrectly_AcrossTypesAndNulls()
    {
        var (path, dbName) = NewDbPath();

        using var db = await BuildDb(path, dbName).ConnectAsync();
        await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
        await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");

        var ascending = (await db.ReadObjectsAsync<IndexTestModel>(
                sort: SortBuilder<IndexTestModel>.Create().OrderBy(SortDirection.Ascending, x => x.LongProperty)))
            .Select(x => x.LongProperty)
            .ToList();

        ascending.ShouldBe(Enumerable.Range(0, 100).Select(i => (long)i).ToList());

        var descending = (await db.ReadObjectsAsync<IndexTestModel>(
                sort: SortBuilder<IndexTestModel>.Create().OrderBy(SortDirection.Descending, x => x.LongProperty)))
            .Select(x => x.LongProperty)
            .ToList();

        descending.ShouldBe(Enumerable.Range(0, 100).Select(i => (long)i).Reverse().ToList());

        // NullableIntProperty is null on every third row; nulls must still sort
        // together at one end rather than being dropped.
        var withNulls = (await db.ReadObjectsAsync<IndexTestModel>(
                sort: SortBuilder<IndexTestModel>.Create().OrderBy(SortDirection.Ascending, x => x.NullableIntProperty)))
            .ToList();

        withNulls.Count.ShouldBe(100);
        withNulls.Take(34).ShouldAllBe(x => x.NullableIntProperty == null);
    }

    /// <summary>
    /// The redundant built-in indexes were removed; this proves the queries they
    /// nominally covered still resolve through an index rather than a table scan.
    /// </summary>
    [TestMethod]
    public async Task BuiltInIndexes_AreSlimmed_WithoutRegressingCoreQueries()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
        }

        var dbFile = Path.Combine(path, dbName);
        var ddl = ReadIndexDdl(dbFile);

        ddl.ShouldNotContainKey("idx_jsonvalue_fulltypename");
        ddl.ShouldNotContainKey("idx_jsonvalue_key_fulltypename");
        ddl.ShouldNotContainKey("idx_jsonvalue_key_fulltypename_partition");
        ddl.ShouldNotContainKey("idx_streamvalue_key_partition");
        ddl.ShouldContainKey("idx_jsonvalue_fulltypename_partition");

        // The dropped indexes were duplicates or prefixes of what remains, so the
        // planner's choices must be byte-identical with and without them. Comparing
        // plans directly proves "no regression" independently of table size, which a
        // bare SCAN/SEARCH assertion cannot (on a small single-type table a scan is
        // genuinely the cheaper plan).
        var planBefore = CorePlans(dbFile);

        using (var conn = OpenInspection(dbFile))
        {
            using var command = conn.CreateCommand();
            command.CommandText =
                "CREATE INDEX idx_jsonvalue_fulltypename ON JsonValue (FullTypeName);" +
                "CREATE INDEX idx_jsonvalue_key_fulltypename ON JsonValue (Key, FullTypeName);" +
                "CREATE INDEX idx_jsonvalue_key_fulltypename_partition ON JsonValue (Key, FullTypeName, Partition);" +

                // Statistics must be refreshed for a like-for-like comparison:
                // CREATE INDEX leaves sqlite_stat1 without rows for the new indexes,
                // and the planner behaves differently with incomplete statistics.
                "ANALYZE;";
            command.ExecuteNonQuery();
        }

        var planWithRedundantIndexes = CorePlans(dbFile);

        planWithRedundantIndexes.ShouldBe(planBefore);
    }

    /// <summary>
    /// Query plans for the core read shapes, used to prove index changes do not
    /// alter how the planner resolves them.
    /// </summary>
    private static string CorePlans(string dbFile)
    {
        var plans = new StringBuilder();

        plans.AppendLine(Explain(dbFile, Queries.SelectDataFromJsonValueWithFullTypeName, new FilterParameters()));

        var equals = FilterBuilder<IndexTestModel>.Create()
            .Filter(FilterType.Equals, x => x.StringProperty, "Item 42");
        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        equals.Build(sb, Serializer, parameters);
        plans.AppendLine(Explain(dbFile, sb.ToString(), parameters));

        return plans.ToString();
    }

    [TestMethod]
    public async Task LegacyDatabase_ShedsRedundantIndexesOnConnect()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
        }

        // Recreate the pre-slimming index set, as an upgraded database would have.
        using (var conn = OpenInspection(dbFile))
        {
            using var command = conn.CreateCommand();
            command.CommandText =
                "CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename ON JsonValue (FullTypeName);" +
                "CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename ON JsonValue (Key, FullTypeName);" +
                "CREATE INDEX IF NOT EXISTS idx_streamvalue_key_partition ON StreamValue (Key, Partition);";
            command.ExecuteNonQuery();
        }

        ReadIndexDdl(dbFile).ShouldContainKey("idx_jsonvalue_fulltypename");

        using (await BuildDb2(path, dbName).ConnectAsync())
        {
        }

        var after = ReadIndexDdl(dbFile);
        after.ShouldNotContainKey("idx_jsonvalue_fulltypename");
        after.ShouldNotContainKey("idx_jsonvalue_key_fulltypename");
        after.ShouldNotContainKey("idx_streamvalue_key_partition");
    }

    [TestMethod]
    public async Task StringPathSort_CanRequestNumericForm_AndUsesTheNumericIndex()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");
        }

        var dbFile = Path.Combine(path, dbName);

        // Without the numeric flag the string-path overload emits the plain
        // JSON_EXTRACT form, which cannot match a numeric index.
        var plain = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        SortBuilder<IndexTestModel>.Create()
            .OrderBy(SortDirection.Ascending, "$.LongProperty")
            .Build(plain, Serializer);
        Explain(dbFile, plain.ToString(), new FilterParameters())
            .ShouldContain("USE TEMP B-TREE FOR ORDER BY");

        // With it, the ordering matches the index expression.
        var numeric = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        SortBuilder<IndexTestModel>.Create()
            .OrderBy(SortDirection.Ascending, "$.LongProperty", isPropertyPathNumeric: true)
            .Build(numeric, Serializer);

        var plan = Explain(dbFile, numeric.ToString(), new FilterParameters());
        plan.ShouldNotContain("USE TEMP B-TREE FOR ORDER BY");
        plan.ShouldContain("idx_long_idx_");
    }

    [TestMethod]
    public async Task CreateIndex_InvalidPath_NamesTheOffendingParameter()
    {
        var (path, dbName) = NewDbPath();

        using var db = await BuildDb(path, dbName).ConnectAsync();

        // The manual overload reports the path parameter by its own name rather than
        // the name of the internal collection it is passed through.
        var single = await Assert.ThrowsExactlyAsync<ArgumentException>(
            async () => await db.CreateIndexAsync("$.Bad;DROP", isNumeric: false, "IndexTestModel", "bad_idx"));

        single.ParamName.ShouldBe("propertyPathString");
    }

    [TestMethod]
    public async Task CreateIndex_OnGenericTypeWithMultipleArguments_IsUsableAndInjectionSafe()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<Dictionary<string, int>>(
                new Expression<Func<Dictionary<string, int>, object>>[] { x => x.Count },
                "generic_idx");
        }

        // The derived name for a closed generic contains characters that are not valid
        // in an identifier; they are normalized rather than rejected.
        var ddl = ReadIndexDdl(Path.Combine(path, dbName));
        var name = ddl.Keys.Single(k => k.StartsWith("idx_generic_idx_", StringComparison.Ordinal));

        name.ShouldNotContain(",");
        name.ShouldAllBe(c => char.IsLetterOrDigit(c) || c == '_');
    }

    // ---------- index management API ----------
    [TestMethod]
    public async Task DropIndex_RemovesIndexAndMetadata()
    {
        var (path, dbName) = NewDbPath();

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "droppable");
            db.ListIndexes().Count.ShouldBe(1);

            (await db.DropIndexAsync<IndexTestModel>("droppable")).ShouldBeTrue();
            db.ListIndexes().ShouldBeEmpty();

            // Dropping something that was never created reports false rather than throwing.
            (await db.DropIndexAsync<IndexTestModel>("never_existed")).ShouldBeFalse();
        }

        ReadIndexDdl(Path.Combine(path, dbName))
            .Keys.ShouldNotContain(k => k.StartsWith("idx_droppable_", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task ListIndexes_ReportsWhatWasCreated()
    {
        var (path, dbName) = NewDbPath();

        using var db = await BuildDb(path, dbName).ConnectAsync();

        await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "long_idx");
        await db.CreateIndexAsync<IndexTestModel>(x => x.StringProperty, "str_idx");

        var indexes = db.ListIndexes();

        indexes.Count.ShouldBe(2);
        indexes.ShouldContain(i => i.IndexName == "long_idx" && i.FullTypeName == typeof(IndexTestModel).FullName);
        indexes.ShouldContain(i => i.IndexName == "str_idx");
        indexes.ShouldAllBe(i => i.PhysicalName.StartsWith("idx_", StringComparison.Ordinal));
        indexes.ShouldAllBe(i => i.Definition.Contains("CREATE INDEX", StringComparison.Ordinal));
    }

    [TestMethod]
    public async Task DroppedIndex_CanBeRecreated()
    {
        var (path, dbName) = NewDbPath();

        using var db = await BuildDb(path, dbName).ConnectAsync();
        await db.WriteObjectsAsync(SeedData(), x => x.StringProperty);

        await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "cycle");
        (await db.DropIndexAsync<IndexTestModel>("cycle")).ShouldBeTrue();
        await db.CreateIndexAsync<IndexTestModel>(x => x.LongProperty, "cycle");

        db.ListIndexes().Count.ShouldBe(1);

        var results = await db.ReadObjectsAsync<IndexTestModel>(
            filter: FilterBuilder<IndexTestModel>.Create().Filter(FilterType.Equals, x => x.LongProperty, 42L));
        results.Count().ShouldBe(1);
    }

    // ---------- helpers ----------

    /// <summary>
    /// Physical index names carry a hash suffix of the full type name, so tests
    /// locate an index by its logical name prefix rather than an exact key.
    /// </summary>
    private static string Find(Dictionary<string, string> ddl, string logicalIndexName)
    {
        var prefix = $"idx_{logicalIndexName}_";
        foreach (var kvp in ddl)
        {
            if (kvp.Key.StartsWith(prefix, StringComparison.Ordinal))
            {
                return kvp.Value;
            }
        }

        throw new KeyNotFoundException($"No index found with logical name '{logicalIndexName}'. Present: {string.Join(", ", ddl.Keys)}");
    }

    private static List<(string IndexName, string FullTypeName, string PhysicalName)> ReadMetadata(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT IndexName, FullTypeName, PhysicalName FROM TychoIndex";

        var rows = new List<(string, string, string)>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
        }

        return rows;
    }

    private static List<IndexTestModel> SeedData()
    {
        var list = new List<IndexTestModel>(100);
        for (int i = 0; i < 100; i++)
        {
            list.Add(new IndexTestModel
            {
                StringProperty = $"Item {i}",
                IntProperty = i,
                LongProperty = i,
                DoubleProperty = i * 1.5,
                BoolProperty = i % 2 == 0,
                GuidProperty = Guid.NewGuid(),
                DateTimeProperty = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(i),
                NullableIntProperty = i % 3 == 0 ? null : i,
                Nested = new NestedModel { NestedInt = i, NestedString = $"Nested {i}" },
            });
        }

        return list;
    }

    private static (string Path, string DbName) NewDbPath()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tycho_index_ddl_tests");
        Directory.CreateDirectory(dir);
        return (dir, $"{Guid.NewGuid()}.db");
    }

    private static Tycho BuildDb(string path, string dbName)
    {
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, "Password", rebuildCache: true, requireTypeRegistration: false);
#else
        return new Tycho(path, Serializer, dbName, rebuildCache: true, requireTypeRegistration: false);
#endif
    }

    /// <summary>Reopens an existing database without rebuilding it.</summary>
    private static Tycho BuildDb2(string path, string dbName)
    {
        SqliteConnection.ClearAllPools();
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, "Password", rebuildCache: false, requireTypeRegistration: false);
#else
        return new Tycho(path, Serializer, dbName, rebuildCache: false, requireTypeRegistration: false);
#endif
    }

    /// <summary>
    /// Opens a plain inspection connection. Tycho holds locking_mode = EXCLUSIVE and
    /// pooling keeps a disposed connection's lock alive, so the pool must be cleared
    /// after the Tycho instance is disposed and before the file is reopened.
    /// </summary>
    private static SqliteConnection OpenInspection(string dbFile)
    {
        SqliteConnection.ClearAllPools();
        var conn = new SqliteConnection($"Data Source={dbFile}");
        conn.Open();
        return conn;
    }

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

    private static string ExplainFilter(string dbFile, Action<FilterBuilder<IndexTestModel>> configure)
    {
        var filter = FilterBuilder<IndexTestModel>.Create();
        configure(filter);

        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        filter.Build(sb, Serializer, parameters);

        return Explain(dbFile, sb.ToString(), parameters);
    }

    private static string Explain(string dbFile, string sql, FilterParameters parameters)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // SQL is produced by the library's own builders.
        command.CommandText = "EXPLAIN QUERY PLAN " + sql;
#pragma warning restore CA2100

        command.Parameters.AddWithValue("$fullTypeName", typeof(IndexTestModel).FullName);
        command.Parameters.AddWithValue("$partition", string.Empty);
        for (int i = 0; i < parameters.Count; i++)
        {
            command.Parameters.AddWithValue(
                FilterParameters.ParameterPrefix + i.ToString(System.Globalization.CultureInfo.InvariantCulture),
                parameters.Values[i] ?? (object)DBNull.Value);
        }

        var sb = new StringBuilder();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            sb.AppendLine(reader.GetString(reader.FieldCount - 1));
        }

        return sb.ToString();
    }
}
