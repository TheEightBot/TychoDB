using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;

namespace TychoDB.UnitTests;

/// <summary>
/// Tests for the device-profile SQLite PRAGMA configuration.
/// </summary>
[TestClass]
public class ConfigurationTests
{
    [TestMethod]
    public void MobileProfile_UsesConservativePragmas()
    {
        var script = Queries.BuildConnectionScript(TychoPerformanceProfile.Mobile);

        script.ShouldContain("PRAGMA cache_size = -8000;");
        script.ShouldContain("PRAGMA mmap_size = 33554432;");
        script.ShouldContain("PRAGMA wal_autocheckpoint = 512;");
        script.ShouldContain("PRAGMA journal_size_limit = 8388608;");

        // Shared PRAGMAs still present.
        script.ShouldContain("PRAGMA journal_mode = WAL;");
        script.ShouldContain("PRAGMA locking_mode = EXCLUSIVE;");
        script.ShouldContain("PRAGMA synchronous = NORMAL;");

        // Schema DDL still appended.
        script.ShouldContain("CREATE TABLE IF NOT EXISTS JsonValue");
    }

    [TestMethod]
    public void DesktopProfile_UsesThroughputPragmas()
    {
        var script = Queries.BuildConnectionScript(TychoPerformanceProfile.Desktop);

        script.ShouldContain("PRAGMA cache_size = -65536;");
        script.ShouldContain("PRAGMA mmap_size = 268435456;");
        script.ShouldContain("PRAGMA wal_autocheckpoint = 2000;");
        script.ShouldContain("PRAGMA journal_size_limit = -1;");
    }

    [TestMethod]
    public void AutoVacuum_IsSetBeforeJournalMode()
    {
        // Regression guard: switching to WAL writes the DB header and locks the
        // auto_vacuum mode, so auto_vacuum must precede journal_mode = WAL or new
        // databases are silently created with auto_vacuum = NONE.
        var script = Queries.BuildConnectionScript(TychoPerformanceProfile.Mobile);

        var autoVacuumIndex = script.IndexOf("PRAGMA auto_vacuum", StringComparison.Ordinal);
        var journalModeIndex = script.IndexOf("PRAGMA journal_mode", StringComparison.Ordinal);

        autoVacuumIndex.ShouldBeGreaterThanOrEqualTo(0);
        journalModeIndex.ShouldBeGreaterThanOrEqualTo(0);
        autoVacuumIndex.ShouldBeLessThan(journalModeIndex, "auto_vacuum must be set before journal_mode = WAL");
    }

    [TestMethod]
    public async Task FreshDatabase_IsCreatedWithIncrementalAutoVacuum()
    {
        var path = System.IO.Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";

        using (var db = new Tycho(path, GetSerializer(), dbName: name, rebuildCache: true, requireTypeRegistration: false, useConnectionPooling: false))
        {
            await db.ConnectAsync();
            await db.WriteObjectAsync(new TestClassA { StringProperty = "k", IntProperty = 1 }, x => x.StringProperty);
        }

        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();

        // 2 == incremental. Verified on a separate connection after the exclusive one closes.
        ReadAutoVacuumMode(System.IO.Path.Combine(path, name)).ShouldBe(2);
    }

    [TestMethod]
    public async Task Cleanup_MigratesLegacyNoneDatabase_AndReclaimsSpace()
    {
        var path = System.IO.Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";
        var fullPath = System.IO.Path.Combine(path, name);

        // Pre-create a legacy NONE database (journal_mode set before auto_vacuum).
        using (var raw = new Microsoft.Data.Sqlite.SqliteConnection($"Filename={fullPath};Pooling=False"))
        {
            raw.Open();
            using var cmd = raw.CreateCommand();
            cmd.CommandText = "PRAGMA journal_mode = WAL; CREATE TABLE _legacy(x);";
            cmd.ExecuteNonQuery();
        }

        ReadAutoVacuumMode(fullPath).ShouldBe(0, "precondition: legacy DB is NONE");

        using (var db = new Tycho(path, GetSerializer(), dbName: name, rebuildCache: false, requireTypeRegistration: false, useConnectionPooling: false))
        {
            await db.ConnectAsync();

            // Create bloat: write many objects, then delete them (free pages).
            var many = Enumerable.Range(0, 500)
                .Select(i => new TestClassA { StringProperty = $"k{i}", IntProperty = i })
                .ToList();
            await db.WriteObjectsAsync(many, x => x.StringProperty);

            await db.DeleteObjectsAsync<TestClassA>();

            // incremental_vacuum would be a no-op here; Cleanup must detect NONE and
            // run a full VACUUM to reclaim space and convert to incremental.
            db.Cleanup(shrinkMemory: true, vacuum: true);
        }

        ReadAutoVacuumMode(fullPath).ShouldBe(2, "Cleanup should convert the DB to incremental auto-vacuum");
    }

    private static long ReadAutoVacuumMode(string dbFullPath)
    {
        using var conn = new Microsoft.Data.Sqlite.SqliteConnection($"Filename={dbFullPath};Pooling=False");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "PRAGMA auto_vacuum;";
        return cmd.ExecuteScalar() is long v ? v : -1;
    }

    [TestMethod]
    public void Overrides_TakePrecedenceOverProfile()
    {
        var script = Queries.BuildConnectionScript(
            TychoPerformanceProfile.Mobile,
            cacheSizeKbOverride: 12345,
            mmapSizeBytesOverride: 0);

        script.ShouldContain("PRAGMA cache_size = -12345;");
        script.ShouldContain("PRAGMA mmap_size = 0;");
    }

    [DataTestMethod]
    [DataRow(TychoPerformanceProfile.Mobile)]
    [DataRow(TychoPerformanceProfile.Desktop)]
    public async Task BothProfiles_OpenAndRoundTrip(TychoPerformanceProfile profile)
    {
        using var db = new Tycho(
            System.IO.Path.GetTempPath(),
            GetSerializer(),
            dbName: $"{Guid.NewGuid()}.db",
            rebuildCache: true,
            requireTypeRegistration: false,
            performanceProfile: profile);

        db.Connect();

        await db.WriteObjectAsync(new TestClassA { StringProperty = "k", IntProperty = 7 }, x => x.StringProperty);
        var results = (await db.ReadObjectsAsync<TestClassA>()).ToList();

        results.Count.ShouldBe(1);
        results[0].IntProperty.ShouldBe(7);
    }

    private static IJsonSerializer GetSerializer()
        => new SystemTextJsonSerializer(
            jsonTypeSerializers: new System.Collections.Generic.Dictionary<Type, System.Text.Json.Serialization.Metadata.JsonTypeInfo>
            {
                [typeof(TestClassA)] = TestJsonContext.Default.TestClassA,
            });
}
