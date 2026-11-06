using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;
using static TychoDB.UnitTests.StoreFixture;

namespace TychoDB.UnitTests;

/// <summary>
/// Proves the compaction of a store without incremental auto-vacuum: the instance keeps
/// serving on the swapped-in file, operations waiting on the gate survive it, a copy that
/// cannot be written leaves the store in use, an interrupted run's leftover goes at connect,
/// the file's permissions and a symbolic link survive, and it is skipped while another
/// connection has the file.
/// </summary>
[TestClass]
public class CompactionTests
{
    [TestMethod]
    public async Task Cleanup_SwapsInTheCompactedCopy_AndKeepsServingOnIt()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            db.Cleanup(shrinkMemory: true, vacuum: true);

            // The instance works on the swapped-in file without reconnecting: the
            // lookup, a write, and a read of that write all go through it.
            await AssertLookup(db);
            await db.WriteObjectAsync(new Item { Key = "item-new", Group = "G0", Seq = 1000 }, x => x.Key);
            (await db.ReadObjectAsync<Item>("item-new")).ShouldNotBeNull().Seq.ShouldBe(1000);
            (await db.CountObjectsAsync<Item>()).ShouldBe(101);
        }

        File.Exists(dbFile + Queries.CompactedFileSuffix).ShouldBeFalse();
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadPragma(dbFile, "freelist_count").ShouldBe(0);
        ReadText(dbFile, "PRAGMA journal_mode").ShouldBe("wal");
        ReadScalar(dbFile, "SELECT COUNT(*) FROM JsonValue").ShouldBe(101);

        // Reopened from scratch, the store is the compacted one.
        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await AssertLookup(db);
            (await db.CountObjectsAsync<Item>()).ShouldBe(101);
        }
    }

    [DataTestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task Cleanup_WithoutAPersistentConnection_Compacts(bool useConnectionPooling)
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using (var db = await ReopenDb(path, dbName, persistConnection: false, useConnectionPooling: useConnectionPooling).ConnectAsync())
        {
            db.Cleanup(shrinkMemory: false, vacuum: true);
            await AssertLookup(db);
            (await db.CountObjectsAsync<Item>()).ShouldBe(100);
        }

        File.Exists(dbFile + Queries.CompactedFileSuffix).ShouldBeFalse();
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [DataTestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task OperationsDuringACompaction_AllSucceed_AndNothingIsLost(bool persistConnection)
    {
        const int rows = 20_000;

        var dbFile = await SeedLegacyStore(rows);
        var (path, dbName) = Split(dbFile);
        int writes = 0;
        int reads = 0;

        using (var db = await ReopenDb(path, dbName, persistConnection: persistConnection).ConnectAsync())
        {
            var compaction = Task.Run(() => db.Cleanup(shrinkMemory: false, vacuum: true));

            var readers = Enumerable.Range(0, 3).Select(_ => Task.Run(async () =>
            {
                do
                {
                    await AssertLookup(db);
                    (await db.CountObjectsAsync<Item>()).ShouldBeGreaterThanOrEqualTo(rows);
                    Interlocked.Increment(ref reads);
                }
                while (!compaction.IsCompleted);
            }));

            var writer = Task.Run(async () =>
            {
                do
                {
                    await db.WriteObjectAsync(new Item { Key = $"written-{writes}", Group = "G0", Seq = rows + writes }, x => x.Key);
                    writes++;
                }
                while (!compaction.IsCompleted);
            });

            await compaction;
            await Task.WhenAll(readers.Append(writer));

            reads.ShouldBeGreaterThan(0);
            (await db.CountObjectsAsync<Item>()).ShouldBe(rows + writes);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        ReadScalar(dbFile, "SELECT COUNT(*) FROM JsonValue").ShouldBe(rows + writes);
        ReadText(dbFile, "PRAGMA integrity_check").ShouldBe("ok");
    }

    [TestMethod]
    public async Task Cleanup_WhenTheCopyCannotBeWritten_ThrowsAndKeepsServing()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);
        var compactedFile = dbFile + Queries.CompactedFileSuffix;

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            // A directory in the way can be neither deleted as a file nor written over.
            Directory.CreateDirectory(compactedFile);

            Should.Throw<TychoException>(() => db.Cleanup(shrinkMemory: false, vacuum: true));
            await AssertLookup(db);
            await db.WriteObjectAsync(new Item { Key = "item-new", Group = "G0", Seq = 1000 }, x => x.Key);

            Directory.Delete(compactedFile);

            db.Cleanup(shrinkMemory: false, vacuum: true);
            await AssertLookup(db);
            (await db.CountObjectsAsync<Item>()).ShouldBe(101);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [TestMethod]
    public async Task Cleanup_WhileABlobStreamIsOpen_LeavesTheStoreAsItWas()
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

            // SQLite refuses to run VACUUM INTO under an open blob, before anything changes.
            db.Cleanup(shrinkMemory: false, vacuum: true);
            await AssertLookup(db);
            stream.ReadByte().ShouldBe(0);

            await stream.DisposeAsync();

            db.Cleanup(shrinkMemory: false, vacuum: true);
            await AssertLookup(db);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [TestMethod]
    public async Task Cleanup_OfAStoreInFullAutoVacuum_ConvertsIt()
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        Execute(dbFile, "PRAGMA auto_vacuum = FULL; CREATE TABLE Bootstrap(Id INTEGER); DROP TABLE Bootstrap;");

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(1);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(), x => x.Key);

            db.Cleanup(shrinkMemory: false, vacuum: true);

            (await db.CountObjectsAsync<Item>()).ShouldBe(100);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }

    [DataTestMethod]
    [DataRow(false)]
    [DataRow(true)]
    public async Task Connect_DeletesTheCopyAnInterruptedCompactionLeft(bool rebuildCache)
    {
        // A store with incremental auto-vacuum never compacts again, so nothing else would.
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);
        var compactedFile = dbFile + Queries.CompactedFileSuffix;

        using (await BuildDb(path, dbName).ConnectAsync())
        {
        }

        File.WriteAllText(compactedFile, "not a database");
        File.WriteAllText(compactedFile + "-journal", "not a journal");

        SqliteConnection.ClearAllPools();

        using (await (rebuildCache ? BuildDb(path, dbName) : ReopenDb(path, dbName)).ConnectAsync())
        {
        }

        File.Exists(compactedFile).ShouldBeFalse();
        File.Exists(compactedFile + "-journal").ShouldBeFalse();
    }

    [TestMethod]
    public async Task Compaction_KeepsTheFilePermissions()
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Inconclusive("Unix file modes do not exist on Windows.");
        }

        const UnixFileMode ownerOnly = UnixFileMode.UserRead | UnixFileMode.UserWrite;

        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        SqliteConnection.ClearAllPools();
        File.SetUnixFileMode(dbFile, ownerOnly);

        using (var db = await ReopenDb(path, dbName).ConnectAsync())
        {
            db.Cleanup(shrinkMemory: false, vacuum: true);
        }

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        File.GetUnixFileMode(dbFile).ShouldBe(ownerOnly);
    }

    [TestMethod]
    public async Task Compaction_OfALinkedStore_ReplacesTheFileAndKeepsTheLink()
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.Inconclusive("Creating a symbolic link needs a privilege on Windows.");
        }

        var dbFile = await SeedLegacyStore();
        var (path, _) = Split(dbFile);
        var linkName = $"{Guid.NewGuid()}.db";
        var linkFile = Path.Combine(path, linkName);

        File.CreateSymbolicLink(linkFile, dbFile);

        using (var db = await ReopenDb(path, linkName).ConnectAsync())
        {
            db.Cleanup(shrinkMemory: false, vacuum: true);
            await AssertLookup(db);
        }

        new FileInfo(linkFile).LinkTarget.ShouldBe(dbFile);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
        File.Exists(dbFile + Queries.CompactedFileSuffix).ShouldBeFalse();
        File.Exists(linkFile + Queries.CompactedFileSuffix).ShouldBeFalse();
    }

    [TestMethod]
    public async Task Compaction_WhileAnotherConnectionHasTheFile_IsSkipped()
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using var db = await ReopenDb(path, dbName, persistConnection: false, useConnectionPooling: false).ConnectAsync();

        using (var other = OpenInspection(dbFile))
        {
            using var command = other.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM JsonValue";
            command.ExecuteScalar().ShouldBe(100L);

            db.Cleanup(shrinkMemory: false, vacuum: true);

            await db.WriteObjectAsync(new Item { Key = "item-new", Group = "G0", Seq = 1000 }, x => x.Key);
            command.ExecuteScalar().ShouldBe(101L);

            command.CommandText = "PRAGMA auto_vacuum";
            command.ExecuteScalar().ShouldBe(0L);
        }

        SqliteConnection.ClearAllPools();

        db.Cleanup(shrinkMemory: false, vacuum: true);
        (await db.CountObjectsAsync<Item>()).ShouldBe(101);
        ReadPragma(dbFile, "auto_vacuum").ShouldBe(2);
    }
}
