using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// <c>NotEquals</c> has to bind a date-time value in the form the serializer wrote it, exactly
/// as <c>Equals</c> does. Bound as <c>DateTime.ToString()</c> instead, the value never equals
/// any stored text, so the negation admits every row, including the one it exists to exclude.
/// </summary>
[TestClass]
public class NotEqualsDateTimeTests
{
    private static readonly DateTime FirstDay = new(2026, 1, 1, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime SecondDay = new(2026, 1, 2, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime ThirdDay = new(2026, 1, 3, 8, 0, 0, DateTimeKind.Utc);

    public static IEnumerable<object[]> Serializers
    {
        get
        {
            yield return new object[] { new SystemTextJsonSerializer(), "stj" };
            yield return new object[] { new NewtonsoftJsonSerializer(), "nsj" };
        }
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_OnDateTimeProperty_ExcludesTheEqualRow(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);

        var ids = await ReadIdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.When, FirstDay));

        ids.ShouldBe(new[] { 2, 3 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_OnDateTimeOffsetProperty_ExcludesTheEqualRow(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);

        var ids = await ReadIdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.WhenOffset, new DateTimeOffset(FirstDay)));

        ids.ShouldBe(new[] { 2, 3 }, label);
    }

    private static async Task<Tycho> SeedAsync(IJsonSerializer jsonSerializer)
    {
        var db = new Tycho(Path.GetTempPath(), jsonSerializer, dbName: $"{Guid.NewGuid()}.db", rebuildCache: true, requireTypeRegistration: false);
        await db.ConnectAsync();

        var docs = new[]
        {
            new Doc { Id = 1, When = FirstDay, WhenOffset = new DateTimeOffset(FirstDay) },
            new Doc { Id = 2, When = SecondDay, WhenOffset = new DateTimeOffset(SecondDay) },
            new Doc { Id = 3, When = ThirdDay, WhenOffset = new DateTimeOffset(ThirdDay) },
        };

        await db.WriteObjectsAsync(docs, x => x.Id.ToString());

        return db;
    }

    private static async Task<int[]> ReadIdsAsync(Tycho db, FilterBuilder<Doc> filter)
        => (await db.ReadObjectsAsync<Doc>(filter: filter)).Select(x => x.Id).OrderBy(x => x).ToArray();

    public class Doc
    {
        public int Id { get; set; }

        public DateTime When { get; set; }

        public DateTimeOffset WhenOffset { get; set; }
    }
}
