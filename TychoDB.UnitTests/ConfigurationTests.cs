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
