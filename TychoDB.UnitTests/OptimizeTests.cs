using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;
using static TychoDB.UnitTests.StoreFixture;

namespace TychoDB.UnitTests;

/// <summary>
/// Proves the deferred upgrade path and the work <c>Optimize</c> does: under
/// <c>autoOptimize: false</c> a declaration the index 4.x built can serve is left to
/// <c>Optimize</c>, with nothing written, and one no index serves is built at once; a current
/// store is never written to by connect, by a declaration, or by <c>Optimize</c>; and every
/// operation keeps working during <c>Optimize</c> and runs between its steps.
/// </summary>
[TestClass]
public class OptimizeTests
{
    [TestMethod]
    public async Task LegacyStore_WithAutoOptimizeOff_DefersTheRebuildToOptimize()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);
        long schemaVersion = ReadPragma(dbFile, "schema_version");

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        // Nothing written: no DDL ran, no row was recorded, the 4.x index serves the lookup.
        var ddl = ReadIndexDdl(dbFile);
        ddl[LegacyIndexName].ShouldContain("ON JsonValue(FullTypeName, CAST", Case.Sensitive);
        ddl.Keys.ShouldNotContain(k => k.StartsWith(LegacyIndexName + "_", StringComparison.Ordinal));
        ddl.Keys.ShouldContain("idx_jsonvalue_fulltypename");
        ReadMetadata(dbFile).ShouldBeEmpty();

        var plan = ExplainLookup(dbFile);
        plan.ShouldContain(LegacyIndexName, Case.Sensitive);
        plan.ShouldNotContain(LegacyIndexName + "_", Case.Sensitive);
        ReadPragma(dbFile, "schema_version").ShouldBe(schemaVersion);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(0);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.ListIndexes().ShouldBeEmpty();
            db.Optimize().ShouldBeTrue();
            db.ListIndexes().ShouldHaveSingleItem().IndexName.ShouldBe(IndexName);
            await AssertLookup(db);
        }

        var built = ReadMetadata(dbFile).ShouldHaveSingleItem();
        built.IndexName.ShouldBe(IndexName);
        built.FullTypeName.ShouldBe(typeof(Item).FullName);
        built.PhysicalName.ShouldStartWith(LegacyIndexName + "_");
        built.ShapeVersion.ShouldBe(2);

        ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[built.PhysicalName].ShouldContain("WHERE FullTypeName", Case.Sensitive);
        ddl.ShouldContainKey(GeneralIndexName);

        foreach (var name in BuiltInLegacyIndexNames)
        {
            ddl.ShouldNotContainKey(name);
        }

        ExplainLookup(dbFile).ShouldContain(built.PhysicalName, Case.Sensitive);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadPragma(dbFile, "freelist_count").ShouldBe(0);

        // Done once: declaring and optimizing again touch nothing.
        var hash = FileHash(dbFile);
        schemaVersion = ReadPragma(dbFile, "schema_version");

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
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

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
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
        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
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
    public async Task RolledBackStore_WithAutoOptimizeOff_LeavesTheOrphanToOptimize()
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

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
        }

        ReadIndexDdl(dbFile).ShouldContainKey(LegacyIndexName);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.Optimize().ShouldBeTrue();
            await AssertLookup(db);
        }

        var ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[current.PhysicalName].ShouldBe(currentSql);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShouldBe(current);
    }

    [TestMethod]
    public async Task StringOverload_WithAutoOptimizeOff_LeavesTheRebuildToOptimize()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
        }

        // 4.x built its index under the very name the string overload uses.
        Execute(dbFile, LegacyIndexDdl(ManualPhysicalName));

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", ManualIndexName);
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, CAST", Case.Sensitive);
        ReadMetadata(dbFile).ShouldBeEmpty();

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", ManualIndexName);
            db.Optimize().ShouldBeTrue();
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
            using (var db = await ReopenDb(path, dbName, autoOptimize: autoOptimize).ConnectAsync())
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
    public async Task LegacyStore_Optimize_CompactsAndKeepsServing()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.Optimize().ShouldBeTrue();

            await AssertLookup(db);
            await db.WriteObjectAsync(new Item { Key = "item-new", Group = "G0", Seq = 1000 }, x => x.Key);
            (await db.CountObjectsAsync<Item>()).ShouldBe(101);
        }

        File.Exists(dbFile + Queries.CompactedFileSuffix).ShouldBeFalse();
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadPragma(dbFile, "freelist_count").ShouldBe(0);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShapeVersion.ShouldBe(2);
    }

    [TestMethod]
    public async Task LegacyStore_Optimize_WithoutAPersistentConnection_Compacts()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, persistConnection: false, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.Optimize().ShouldBeTrue();
            await AssertLookup(db);
            (await db.CountObjectsAsync<Item>()).ShouldBe(100);
        }

        File.Exists(dbFile + Queries.CompactedFileSuffix).ShouldBeFalse();
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShapeVersion.ShouldBe(2);
    }

    [TestMethod]
    public async Task Compaction_WhileAnotherConnectionHasTheFile_ReturnsFalse()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using var db = await ReopenDb(path, dbName, persistConnection: false, useConnectionPooling: false, autoOptimize: false).ConnectAsync();

        using (var other = OpenInspection(dbFile))
        {
            using var command = other.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM JsonValue";
            command.ExecuteScalar().ShouldBe(100L);

            db.Optimize().ShouldBeFalse();
        }

        SqliteConnection.ClearAllPools();

        db.Optimize().ShouldBeTrue();
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [TestMethod]
    public async Task LegacyStore_Cleanup_DoesTheDeferredWorkBeforeCompacting()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.Cleanup(shrinkMemory: false, vacuum: true);
            await AssertLookup(db);
        }

        var ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl.ShouldContainKey(ReadMetadata(dbFile).ShouldHaveSingleItem().PhysicalName);

        foreach (var name in BuiltInLegacyIndexNames)
        {
            ddl.ShouldNotContainKey(name);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadPragma(dbFile, "freelist_count").ShouldBe(0);
    }

    [TestMethod]
    public async Task Declaration_TheOldIndexDoesNotServe_IsBuiltAtOnce()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        // The 4.x index under this name covers Seq; the declaration has moved to Group.
        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Group, IndexName);
        }

        var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
        var ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[row.PhysicalName].ShouldContain("$.Group", Case.Sensitive);
    }

    [TestMethod]
    public async Task StringOverload_ChangedProperty_IsBuiltAtOnce()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", ManualIndexName);
        }

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Group", isNumeric: false, "Item", ManualIndexName);
        }

        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("$.Group", Case.Sensitive);
        ReadMetadata(dbFile).ShouldHaveSingleItem().Definition.ShouldContain("$.Group", Case.Sensitive);
    }

    [TestMethod]
    public async Task BothOverloads_OnDifferentProperties_AreBothBuiltAtOnce()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
        }

        // The string overload's index sits under the generic overload's legacy name, but
        // it is not an index an earlier release built for the generic declaration.
        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Group", isNumeric: false, "Item", IndexName);
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
        }

        var rows = ReadMetadata(dbFile);
        rows.Count.ShouldBe(2);

        var ddl = ReadIndexDdl(dbFile);
        ddl[LegacyIndexName].ShouldContain("$.Group", Case.Sensitive);
        ddl[rows.Single(r => r.PhysicalName != LegacyIndexName).PhysicalName].ShouldContain("$.Seq", Case.Sensitive);
    }

    [TestMethod]
    public async Task CompositeDeclaration_IsDeferredOnlyWhenTheOldIndexLeadsWithItsFirstProperty()
    {
        const string composite = "CREATE INDEX idx_pair_Item ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC), JSON_EXTRACT(Data, '$.Group'));";

        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);
        Execute(dbFile, composite);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(new Expression<Func<Item, object>>[] { x => x.Seq, x => x.Group }, "pair");
        }

        ReadMetadata(dbFile).ShouldBeEmpty();
        ReadIndexDdl(dbFile).ShouldContainKey("idx_pair_Item");

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(new Expression<Func<Item, object>>[] { x => x.Group, x => x.Seq }, "pair");
        }

        ReadMetadata(dbFile).ShouldHaveSingleItem().Definition.ShouldContain("$.Group", Case.Sensitive);
        ReadIndexDdl(dbFile).ShouldNotContainKey("idx_pair_Item");
    }

    [TestMethod]
    public async Task Declaration_OverAWholeDocumentIndex_IsLeftToOptimize()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        // What 4.x built for a value-type property: the boxed path indexed the document.
        Execute(dbFile, $"DROP INDEX {LegacyIndexName}; CREATE INDEX {LegacyIndexName} ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '$'));");

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        ReadMetadata(dbFile).ShouldBeEmpty();
        ReadIndexDdl(dbFile).ShouldContainKey(LegacyIndexName);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.Optimize().ShouldBeTrue();
            await AssertLookup(db);
        }

        var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
        ReadIndexDdl(dbFile).ShouldNotContainKey(LegacyIndexName);
        ExplainLookup(dbFile).ShouldContain(row.PhysicalName, Case.Sensitive);
    }

    [TestMethod]
    public async Task Declaration_WithNoProperties_DoesNotStopOptimize()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);
        Execute(dbFile, "CREATE INDEX idx_empty_Item ON JsonValue(FullTypeName);");

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(Array.Empty<Expression<Func<Item, object>>>(), "empty");
            db.Optimize().ShouldBeTrue();
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [TestMethod]
    public async Task Redeclared_BeforeOptimize_TheLatestDeclarationIsTheOneBuilt()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.ListIndexes().ShouldBeEmpty();

            await db.CreateIndexAsync<Item>(x => x.Group, IndexName);
            db.Optimize().ShouldBeTrue();
        }

        var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
        ReadIndexDdl(dbFile)[row.PhysicalName].ShouldContain("$.Group", Case.Sensitive);
    }

    [TestMethod]
    public async Task DropIndex_OfADeferredDeclaration_LeavesNothingForOptimize()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            db.DropIndex<Item>(IndexName).ShouldBeTrue();
            db.Optimize().ShouldBeTrue();
            await AssertLookup(db);
        }

        ReadMetadata(dbFile).ShouldBeEmpty();

        var ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl.Keys.ShouldNotContain(k => k.StartsWith(LegacyIndexName + "_", StringComparison.Ordinal));
    }

    [DataTestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task OperationsDuringOptimize_AllSucceed_AndNothingIsLost(bool persistConnection)
    {
        const int rows = 20_000;

        var dbFile = await SeedLegacyStore(rows);
        var (path, dbName) = Split(dbFile);
        int writes = 0;
        int reads = 0;

        using (var db = await ReopenDb(path, dbName, autoOptimize: false, persistConnection: persistConnection).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);

            var optimize = Task.Run(db.Optimize);

            var readers = Enumerable.Range(0, 3).Select(_ => Task.Run(async () =>
            {
                do
                {
                    await AssertLookup(db);
                    (await db.CountObjectsAsync<Item>()).ShouldBeGreaterThanOrEqualTo(rows);
                    Interlocked.Increment(ref reads);
                }
                while (!optimize.IsCompleted);
            }));

            var writer = Task.Run(async () =>
            {
                do
                {
                    await db.WriteObjectAsync(new Item { Key = $"written-{writes}", Group = "G0", Seq = rows + writes }, x => x.Key);
                    writes++;
                }
                while (!optimize.IsCompleted);
            });

            (await optimize).ShouldBeTrue();
            await Task.WhenAll(readers.Append(writer));

            reads.ShouldBeGreaterThan(0);
            (await db.CountObjectsAsync<Item>()).ShouldBe(rows + writes);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadScalar(dbFile, "SELECT COUNT(*) FROM JsonValue").ShouldBe(rows + writes);
        ReadText(dbFile, "PRAGMA integrity_check").ShouldBe("ok");
    }

    [TestMethod]
    public async Task Optimize_LetsWaitingOperationsRunBetweenItsSteps()
    {
        const int declarations = 12;

        var dbFile = await SeedLegacyStore(20_000);
        var (path, dbName) = Split(dbFile);

        Execute(
            dbFile,
            string.Concat(Enumerable.Range(0, declarations).Select(i =>
                $"CREATE INDEX idx_i{i}_Item ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC));")));

        using var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync();

        for (int i = 0; i < declarations; i++)
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, $"i{i}");
        }

        Task<bool> optimize = null;
        int asynchronous = 0;
        int synchronous = 0;
        using var started = new ManualResetEventSlim();

        var asyncCaller = Task.Run(async () =>
        {
            started.Wait();

            while (!optimize!.IsCompleted)
            {
                await db.CountObjectsAsync<Item>();

                if (!optimize.IsCompleted)
                {
                    asynchronous++;
                }
            }
        });

        var syncCaller = Task.Run(() =>
        {
            started.Wait();

            while (!optimize!.IsCompleted)
            {
                db.ListIndexes();

                if (!optimize.IsCompleted)
                {
                    synchronous++;
                }
            }
        });

        optimize = Task.Run(db.Optimize);
        started.Set();

        (await optimize).ShouldBeTrue();
        await Task.WhenAll(asyncCaller, syncCaller);

        asynchronous.ShouldBeGreaterThanOrEqualTo(2);
        synchronous.ShouldBeGreaterThanOrEqualTo(2);
    }

    [TestMethod]
    public async Task Optimize_WithABlobStreamOpen_LeavesTheWorkForALaterCallWithoutHoldingTheGate()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);

            using (var blob = new MemoryStream(new byte[4096]))
            {
                await db.WriteBlobAsync(blob, "blob-1");
            }

            var stream = await db.ReadBlobAsync("blob-1");
            var timer = System.Diagnostics.Stopwatch.StartNew();

            db.Optimize().ShouldBeFalse();
            timer.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(20));

            await AssertLookup(db);
            await stream.DisposeAsync();

            db.Optimize().ShouldBeTrue();
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile).ShouldNotContainKey(LegacyIndexName);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [TestMethod]
    public async Task Optimize_WithABlobStreamOpenAtTheCompaction_LeavesItForALaterCall()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            using (var blob = new MemoryStream(new byte[4096]))
            {
                await db.WriteBlobAsync(blob, "blob-1");
            }

            var stream = await db.ReadBlobAsync("blob-1");

            db.Optimize().ShouldBeFalse();
            stream.ReadByte().ShouldBe(0);

            await stream.DisposeAsync();

            db.Optimize().ShouldBeTrue();
            await AssertLookup(db);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [TestMethod]
    public async Task ConcurrentOptimizeCalls_BothFinish()
    {
        var dbFile = await SeedLegacyStore(5_000);
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);

            var results = await Task.WhenAll(Task.Run(db.Optimize), Task.Run(db.Optimize));

            results.ShouldAllBe(finished => finished);
            await AssertLookup(db);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadMetadata(dbFile).ShouldHaveSingleItem();
    }

    [TestMethod]
    public async Task OptimizeAsync_DoesTheSameWork_AndStopsWhenCancelled()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);

            using var cancelled = new CancellationTokenSource();
            await cancelled.CancelAsync();

            await Should.ThrowAsync<OperationCanceledException>(async () => await db.OptimizeAsync(cancelled.Token));
            await AssertLookup(db);
            db.ListIndexes().ShouldBeEmpty();

            (await db.OptimizeAsync()).ShouldBeTrue();
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile).ShouldNotContainKey(LegacyIndexName);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [DataTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task DisconnectOrDispose_DuringOptimize_LeavesAStoreThatOpens(bool dispose)
    {
        foreach (int delay in new[] { 0, 1, 2, 5, 10, 20, 40 })
        {
            var dbFile = await SeedLegacyStore(5_000);
            var (path, dbName) = Split(dbFile);

            var db = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync();
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);

            var optimize = Task.Run(db.Optimize);
            await Task.Delay(delay);

            if (dispose)
            {
                db.Dispose();
            }
            else
            {
                await db.DisconnectAsync();
            }

            try
            {
                await optimize;
            }
            catch (Exception ex) when (ex is ArgumentNullException or ObjectDisposedException)
            {
                // Already disconnected when it started: the check every operation makes.
            }

            db.Dispose();

            using var reopened = await ReopenDb(path, dbName, autoOptimize: false).ConnectAsync();
            await reopened.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            reopened.Optimize().ShouldBeTrue();
            await AssertLookup(reopened);
            (await reopened.CountObjectsAsync<Item>()).ShouldBe(5_000);
        }
    }

    [TestMethod]
    public async Task StoreWithIncrementalAutoVacuum_ReclaimsInPlace_OnlyWhenATenthIsFree()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(5_000), x => x.Key);
            await db.WriteObjectsAsync(Items(500).Select(x => new Item { Key = "other-" + x.Key, Group = x.Group, Seq = x.Seq }), x => x.Key, partition: "kept");

            db.Optimize().ShouldBeTrue();
        }

        ReadPragma(dbFile, "freelist_count").ShouldBe(0);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.DeleteObjectsAsync<Item>(filter: FilterBuilder<Item>.Create().Filter(FilterType.LessThan, x => x.Seq, 10L));
        }

        long freeAfterAFewDeletes = ReadPragma(dbFile, "freelist_count");
        var hash = FileHash(dbFile);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            db.Optimize().ShouldBeTrue();
        }

        // Under a tenth free: left alone.
        ReadPragma(dbFile, "freelist_count").ShouldBe(freeAfterAFewDeletes);
        FileHash(dbFile).ShouldBe(hash);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.DeleteObjectsAsync<Item>();
        }

        ReadPragma(dbFile, "freelist_count").ShouldBeGreaterThan(0);
        long size = new FileInfo(dbFile).Length;

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            db.Optimize().ShouldBeTrue();
            (await db.CountObjectsAsync<Item>(partition: "kept")).ShouldBe(500);
        }

        ReadPragma(dbFile, "freelist_count").ShouldBe(0);
        new FileInfo(dbFile).Length.ShouldBeLessThan(size);
    }

    [TestMethod]
    public void ConstructorOfEarlierReleases_IsKept()
    {
        var (path, dbName) = NewDbPath();

        var constructor = typeof(Tycho).GetConstructor(
            new[]
            {
                typeof(string), typeof(IJsonSerializer), typeof(string), typeof(string),
                typeof(bool), typeof(bool), typeof(bool), typeof(bool), typeof(int),
                typeof(TychoPerformanceProfile), typeof(int?), typeof(long?),
            });

        constructor.ShouldNotBeNull();

#if ENCRYPTED
        const string password = DbPassword;
#else
        const string password = null;
#endif

        using var db = (Tycho)constructor.Invoke(
            new object[] { path, Serializer, dbName, password, true, false, false, true, 30, TychoPerformanceProfile.Mobile, null, null });

        db.Connect();
    }
}
