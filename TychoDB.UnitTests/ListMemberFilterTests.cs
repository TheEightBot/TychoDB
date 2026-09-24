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

    public class Entry
    {
        public DateTime When { get; set; }

        public double Amount { get; set; }

        public string Note { get; set; }
    }
}
