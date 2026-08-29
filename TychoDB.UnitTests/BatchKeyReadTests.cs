#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// Reading a batch of keys in one round trip. The key set is bound as a JSON array rather than
/// one parameter per key, so the tests that matter most are the ones that would break that
/// encoding: keys carrying JSON metacharacters, and key sets larger than SQLite's parameter
/// ceiling.
/// </summary>
[TestClass]
public class BatchKeyReadTests
{
    private const string PartitionA = "partitionA";
    private const string PartitionB = "partitionB";

    [TestMethod]
    public async Task ReadsTheObjectsForTheGivenKeys()
    {
        using var db = Connect();
        await SeedAsync(db);

        var results = await db.ReadObjectsByKeysAsync<Doc>(new object[] { "k1", "k3" }, PartitionA);

        results.Select(x => x.Key).OrderBy(x => x).ShouldBe(new[] { "k1", "k3" });
    }

    [TestMethod]
    public async Task KeysThatAreNotPresent_AreSimplyAbsent()
    {
        using var db = Connect();
        await SeedAsync(db);

        var results = await db.ReadObjectsByKeysAsync<Doc>(new object[] { "k1", "nope", "k2" }, PartitionA);

        results.Select(x => x.Key).OrderBy(x => x).ShouldBe(new[] { "k1", "k2" });
    }

    [TestMethod]
    public async Task EmptyKeySet_ReturnsNothing()
    {
        using var db = Connect();
        await SeedAsync(db);

        var results = await db.ReadObjectsByKeysAsync<Doc>(Array.Empty<object>(), PartitionA);

        results.ShouldBeEmpty();
    }

    [TestMethod]
    public async Task StaysWithinItsPartition()
    {
        using var db = Connect();
        await SeedAsync(db);

        // Both keys exist in both partitions, holding different values; only partition A's
        // versions may come back.
        var results = await db.ReadObjectsByKeysAsync<Doc>(new object[] { "k1", "shared" }, PartitionA);

        results.Select(x => x.Description).OrderBy(x => x, StringComparer.Ordinal)
            .ShouldBe(new[] { "a-shared", "a1" });
    }

    [TestMethod]
    public async Task StaysWithinItsType()
    {
        // Same key, same partition, different stored type.
        using var db = Connect();
        await SeedAsync(db);
        await db.WriteObjectsAsync(new[] { new Other { Key = "k1", Note = "wrong type" } }, x => x.Key, PartitionA);

        var results = await db.ReadObjectsByKeysAsync<Doc>(new object[] { "k1" }, PartitionA);

        results.Select(x => x.Description).ShouldBe(new[] { "a1" });
    }

    [TestMethod]
    public async Task DuplicateKeys_YieldOneObjectEach()
    {
        using var db = Connect();
        await SeedAsync(db);

        var results = await db.ReadObjectsByKeysAsync<Doc>(new object[] { "k1", "k1", "k1" }, PartitionA);

        results.Select(x => x.Key).ShouldBe(new[] { "k1" });
    }

    [TestMethod]
    public async Task KeysCarryingJsonMetacharacters_RoundTrip()
    {
        // The key set is rendered as JSON, so a quote, a backslash, a control character or a
        // non-BMP character in a key would corrupt the array if it were not properly encoded.
        var hostile = new[]
        {
            "he said \"hi\"",
            @"back\slash",
            "tab\tand\nnewline",
            "emoji \U0001F389",
            "unicode é中",
            "'; DROP TABLE JsonValue; --",
        };

        using var db = Connect();
        await db.WriteObjectsAsync(
            hostile.Select(k => new Doc { Key = k, Description = "held" }), x => x.Key, PartitionA);

        var results = await db.ReadObjectsByKeysAsync<Doc>(hostile.Cast<object>(), PartitionA);

        results.Select(x => x.Key).OrderBy(x => x, StringComparer.Ordinal)
            .ShouldBe(hostile.OrderBy(x => x, StringComparer.Ordinal));
    }

    [TestMethod]
    public async Task KeySetLargerThanTheParameterCeiling_Works()
    {
        // One bound parameter regardless of key count, so SQLITE_MAX_VARIABLE_NUMBER — 999 on
        // older builds — does not apply and no chunking is needed.
        const int count = 5_000;

        using var db = Connect();
        await db.WriteObjectsAsync(
            Enumerable.Range(0, count).Select(i => new Doc
            {
                Key = "b" + i.ToString(CultureInfo.InvariantCulture),
                Description = "bulk",
            }),
            x => x.Key,
            PartitionA);

        var keys = Enumerable.Range(0, count).Select(i => (object)("b" + i.ToString(CultureInfo.InvariantCulture)));

        var results = await db.ReadObjectsByKeysAsync<Doc>(keys, PartitionA);

        results.Count().ShouldBe(count);
    }

    [TestMethod]
    public async Task NonStringKeys_MatchTheSingleKeyOverload()
    {
        using var db = Connect();
        await db.WriteObjectsAsync(
            new[] { new Numbered { Id = 7, Note = "seven" }, new Numbered { Id = 8, Note = "eight" } },
            x => x.Id,
            PartitionA);

        var batch = await db.ReadObjectsByKeysAsync<Numbered>(new object[] { 7, 8 }, PartitionA);
        var single = await db.ReadObjectAsync<Numbered>(7, PartitionA);

        batch.Select(x => x.Note).OrderBy(x => x).ShouldBe(new[] { "eight", "seven" });
        single.Note.ShouldBe("seven");
    }

    [TestMethod]
    public async Task AppliesSorting()
    {
        using var db = Connect();
        await SeedAsync(db);

        var results =
            await db.ReadObjectsByKeysAsync<Doc>(
                new object[] { "k1", "k2", "k3" },
                PartitionA,
                SortBuilder<Doc>.Create().OrderBy(SortDirection.Descending, x => x.Description));

        results.Select(x => x.Description).ShouldBe(new[] { "a3", "a2", "a1" });
    }

    [TestMethod]
    public async Task ReportsProgress()
    {
        using var db = Connect();
        await SeedAsync(db);

        // A synchronous reporter, not Progress<double>, which dispatches its callbacks
        // asynchronously and would make this a race.
        var progress = new RecordingProgress();

        await db.ReadObjectsByKeysAsync<Doc>(new object[] { "k1", "k2", "k3" }, PartitionA, progress: progress);

        progress.Reports.ShouldNotBeEmpty();
        progress.Reports[^1].ShouldBe(1.0);
    }

    [TestMethod]
    public async Task NullKeyInTheSet_IsRejected()
    {
        using var db = Connect();
        await SeedAsync(db);

        await Should.ThrowAsync<ArgumentNullException>(
            async () => await db.ReadObjectsByKeysAsync<Doc>(new object?[] { "k1", null }!, PartitionA));
    }

    [TestMethod]
    public async Task NullKeySet_IsRejected()
    {
        using var db = Connect();
        await SeedAsync(db);

        await Should.ThrowAsync<ArgumentNullException>(
            async () => await db.ReadObjectsByKeysAsync<Doc>(null!, PartitionA));
    }

    private static Tycho Connect()
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";

        var db = new Tycho(dir, new NewtonsoftJsonSerializer(), dbName: name, rebuildCache: true, requireTypeRegistration: false);
        return db.Connect();
    }

    private static async Task SeedAsync(Tycho db)
    {
        await db.WriteObjectsAsync(
            new[]
            {
                new Doc { Key = "k1", Description = "a1" },
                new Doc { Key = "k2", Description = "a2" },
                new Doc { Key = "k3", Description = "a3" },
                new Doc { Key = "shared", Description = "a-shared" },
            },
            x => x.Key,
            PartitionA);

        await db.WriteObjectsAsync(
            new[]
            {
                new Doc { Key = "k1", Description = "b1" },
                new Doc { Key = "shared", Description = "b-shared" },
            },
            x => x.Key,
            PartitionB);
    }

    private sealed class RecordingProgress : IProgress<double>
    {
        public List<double> Reports { get; } = new();

        public void Report(double value) => Reports.Add(value);
    }

    public class Doc
    {
        public string Key { get; set; } = string.Empty;

        public string Description { get; set; } = string.Empty;
    }

    public class Other
    {
        public string Key { get; set; } = string.Empty;

        public string Note { get; set; } = string.Empty;
    }

    public class Numbered
    {
        public int Id { get; set; }

        public string Note { get; set; } = string.Empty;
    }
}
