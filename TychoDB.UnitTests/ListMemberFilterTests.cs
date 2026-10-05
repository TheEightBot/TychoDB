using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// The list-member overload of <c>Filter</c> walks every node under the list, string leaves
/// included, and hands each one to <c>JSON_EACH</c>, which accepts only JSON. The walk stops
/// at the first element that satisfies the predicate, so only a document whose elements never
/// match makes it reach a leaf. Every element seeded here carries a serialized
/// <see cref="DateTime"/> and a plain string, and every filter has a document it must reject.
/// </summary>
[TestClass]
public class ListMemberFilterTests
{
    private static readonly DateTime FirstDay = new(2026, 1, 1, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime SecondDay = new(2026, 1, 2, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime ThirdDay = new(2026, 1, 3, 8, 0, 0, DateTimeKind.Utc);

    public static IEnumerable<object[]> Serializers
    {
        get
        {
            yield return new object[] { new SystemTextJsonSerializer() };
            yield return new object[] { new NewtonsoftJsonSerializer() };
        }
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task Equals_OnDateTimeMember_MatchesDocumentsWithSuchAnElement(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectAsync(jsonSerializer);

        var ids = await ReadIdsAsync(db, FilterType.Equals, x => x.When, FirstDay);

        ids.ShouldBe(new[] { "mixed", "uniform" });
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_OnDateTimeMember_MatchesDocumentsWithSuchAnElement(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectAsync(jsonSerializer);

        var ids = await ReadIdsAsync(db, FilterType.NotEquals, x => x.When, FirstDay);

        ids.ShouldBe(new[] { "mixed", "other" });
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task Equals_OnNumericMember_MatchesDocumentsWithSuchAnElement(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectAsync(jsonSerializer);

        var ids = await ReadIdsAsync(db, FilterType.Equals, x => x.Amount, 1.5d);

        ids.ShouldBe(new[] { "mixed", "uniform" });
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_OnNumericMember_MatchesDocumentsWithSuchAnElement(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectAsync(jsonSerializer);

        var ids = await ReadIdsAsync(db, FilterType.NotEquals, x => x.Amount, 1.5d);

        ids.ShouldBe(new[] { "mixed", "other" });
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task Equals_OnStringMemberHoldingJsonText_DoesNotSearchInsideTheString(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectAsync(jsonSerializer);

        // No element has Amount 99; only this Note, a string, spells one out.
        var embedded = new Ledger
        {
            Id = "embedded",
            Entries = new List<Entry> { new() { When = ThirdDay, Amount = 3d, Note = "{\"Amount\":99}" } },
        };
        await db.WriteObjectAsync(embedded, x => x.Id);

        var ids = await ReadIdsAsync(db, FilterType.Equals, x => x.Amount, 99d);

        ids.ShouldBeEmpty();
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task Equals_OnNumericListItself_MatchesDocumentsHoldingTheValue(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectTalliesAsync(jsonSerializer);

        var ids = await ReadTallyIdsAsync(db, FilterBuilder<Tally>.Create().Filter(FilterType.Equals, x => x.Numbers, x => x, 5));

        ids.ShouldBe(new[] { "a" });
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task GreaterThan_OnNumericListItself_MatchesDocumentsHoldingSuchAValue(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectTalliesAsync(jsonSerializer);

        var ids = await ReadTallyIdsAsync(db, FilterBuilder<Tally>.Create().Filter(FilterType.GreaterThan, x => x.Numbers, x => x, 2));

        ids.ShouldBe(new[] { "a", "b" });
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task Equals_OnStringListItself_MatchesDocumentsHoldingTheValue(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectTalliesAsync(jsonSerializer);

        var ids = await ReadTallyIdsAsync(db, FilterBuilder<Tally>.Create().Filter(FilterType.Equals, x => x.Tags, x => x, "red"));

        ids.ShouldBe(new[] { "a" });
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task Contains_OnStringListItself_MatchesDocumentsHoldingSuchAValue(IJsonSerializer jsonSerializer)
    {
        using var db = await ConnectTalliesAsync(jsonSerializer);

        var ids = await ReadTallyIdsAsync(db, FilterBuilder<Tally>.Create().Filter(FilterType.Contains, x => x.Tags, x => x, "re"));

        ids.ShouldBe(new[] { "a", "b" });
    }

    private static async Task<Tycho> ConnectAsync(IJsonSerializer jsonSerializer)
    {
        var db = new Tycho(Path.GetTempPath(), jsonSerializer, dbName: $"{Guid.NewGuid()}.db", rebuildCache: true, requireTypeRegistration: false);
        await db.ConnectAsync();
        await db.WriteObjectsAsync(Seed(), x => x.Id);

        return db;
    }

    private static async Task<string[]> ReadIdsAsync<TProp>(Tycho db, FilterType filterType, Expression<Func<Entry, TProp>> member, object value)
    {
        var results =
            await db.ReadObjectsAsync<Ledger>(
                filter: FilterBuilder<Ledger>.Create().Filter(filterType, x => x.Entries, member, value));

        return results.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToArray();
    }

    private static async Task<Tycho> ConnectTalliesAsync(IJsonSerializer jsonSerializer)
    {
        var db = new Tycho(Path.GetTempPath(), jsonSerializer, dbName: $"{Guid.NewGuid()}.db", rebuildCache: true, requireTypeRegistration: false);
        await db.ConnectAsync();
        await db.WriteObjectsAsync(SeedTallies(), x => x.Id);

        return db;
    }

    private static async Task<string[]> ReadTallyIdsAsync(Tycho db, FilterBuilder<Tally> filter)
    {
        var results = await db.ReadObjectsAsync<Tally>(filter: filter);

        return results.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ToArray();
    }

    private static Tally[] SeedTallies() =>
        new[]
        {
            new Tally { Id = "a", Numbers = new List<int> { 1, 5 }, Tags = new List<string> { "red", "blue" } },
            new Tally { Id = "b", Numbers = new List<int> { 2, 3 }, Tags = new List<string> { "green" } },
            new Tally { Id = "c", Numbers = new List<int> { 1 }, Tags = new List<string> { "blue" } },
        };

    private static Ledger[] Seed() =>
        new[]
        {
            new Ledger
            {
                Id = "mixed",
                Entries = new List<Entry>
                {
                    new() { When = FirstDay, Amount = 1.5d, Note = "opening" },
                    new() { When = SecondDay, Amount = 2d, Note = "deposit" },
                },
            },
            new Ledger
            {
                Id = "other",
                Entries = new List<Entry>
                {
                    new() { When = ThirdDay, Amount = 3d, Note = "closing" },
                },
            },
            new Ledger
            {
                Id = "uniform",
                Entries = new List<Entry>
                {
                    new() { When = FirstDay, Amount = 1.5d, Note = "opening" },
                    new() { When = FirstDay, Amount = 1.5d, Note = "repeat" },
                },
            },
        };

    public class Ledger
    {
        public string Id { get; set; }

        public List<Entry> Entries { get; set; }
    }

    public class Tally
    {
        public string Id { get; set; }

        public List<int> Numbers { get; set; }

        public List<string> Tags { get; set; }
    }

    public class Entry
    {
        public DateTime When { get; set; }

        public double Amount { get; set; }

        public string Note { get; set; }
    }
}
