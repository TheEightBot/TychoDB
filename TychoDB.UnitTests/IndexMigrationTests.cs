using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;
using static TychoDB.UnitTests.StoreFixture;

namespace TychoDB.UnitTests;

/// <summary>
/// Proves a declaration replaces the index an earlier release left in its place: the string
/// overload's same-named 4.x index is recognised as stale instead of kept, and the index a
/// 4.x rollback leaves next to a current one is dropped, while one another declaration
/// records as its own is kept.
/// </summary>
[TestClass]
public class IndexMigrationTests
{
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

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        var ddl = ReadIndexDdl(dbFile);
        ddl.ShouldNotContainKey(LegacyIndexName);
        ddl[current.PhysicalName].ShouldBe(currentSql);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShouldBe(current);
    }

    [TestMethod]
    public async Task StringOverload_SameNamedLegacyIndex_IsRebuilt()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
        }

        // 4.x built its index under the very name the string overload uses.
        Execute(dbFile, LegacyIndexDdl(ManualPhysicalName));

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", ManualIndexName);
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, Partition, CAST", Case.Sensitive);

        var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
        row.IndexName.ShouldBe(ManualIndexName);
        row.FullTypeName.ShouldBe("Item");
        row.PhysicalName.ShouldBe(ManualPhysicalName);
        row.ShapeVersion.ShouldBe(2);

        // A row that says current over an index of another shape is rebuilt all the same.
        Execute(dbFile, $"DROP INDEX {ManualPhysicalName}; " + LegacyIndexDdl(ManualPhysicalName));
        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, CAST", Case.Sensitive);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", ManualIndexName);
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile)[ManualPhysicalName].ShouldContain("ON JsonValue(FullTypeName, Partition, CAST", Case.Sensitive);
        ReadMetadata(dbFile).ShouldHaveSingleItem().ShapeVersion.ShouldBe(2);
    }

    [TestMethod]
    public async Task IndexNameChangedOnlyInCase_IsTheSameIndex_AndSettles()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);
        }

        // SQLite compares index names without regard to case, so CREATE INDEX IF NOT EXISTS
        // would keep this one, shape and all, under the name declared below.
        Execute(dbFile, LegacyIndexDdl(ManualPhysicalName));

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", "MANUAL_IDX");
            await AssertLookup(db);
        }

        ReadIndexDdl(dbFile)
            .Single(index => index.Key.Equals(ManualPhysicalName, StringComparison.OrdinalIgnoreCase))
            .Value
            .ShouldContain("ON JsonValue(FullTypeName, Partition, CAST", Case.Sensitive);

        var hash = FileHash(dbFile);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", "MANUAL_IDX");
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

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await db.CreateIndexAsync("$.Seq", isNumeric: true, "Item", IndexName);
            await AssertLookup(db);
        }

        FileHash(dbFile).ShouldBe(hash);
        ReadPragma(dbFile, "schema_version").ShouldBe(schemaVersion);

        // Dropping the generic declaration leaves the string overload's index and row.
        using (var db = await ReopenDb(path, dbName).ConnectAsync())
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
    public async Task Declaration_OverAWholeDocumentIndex_IsBuilt()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        // What 4.x built for a value-type property: the boxed path indexed the document.
        Execute(dbFile, $"DROP INDEX {LegacyIndexName}; CREATE INDEX {LegacyIndexName} ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '$'));");

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.CreateIndexAsync<Item>(x => x.Seq, IndexName);
            await AssertLookup(db);
        }

        var row = ReadMetadata(dbFile).ShouldHaveSingleItem();
        ReadIndexDdl(dbFile).ShouldNotContainKey(LegacyIndexName);
        ExplainLookup(dbFile).ShouldContain(row.PhysicalName, Case.Sensitive);
    }
}
