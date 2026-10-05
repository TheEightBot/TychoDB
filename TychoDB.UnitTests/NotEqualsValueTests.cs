using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// <c>NotEquals</c> has to compare against exactly the form <c>Equals</c> does, or its negation
/// admits the very row it should exclude: a date-time bound as culture text never equals the
/// stored ISO form, and a numeric member compared without its <c>CAST</c> never equals a bound
/// text value. Both the scalar and the list-member paths are pinned here.
/// </summary>
[TestClass]
public class NotEqualsValueTests
{
    private static readonly DateTime First = new(2026, 1, 1, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Second = new(2026, 1, 3, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTimeOffset FirstOffset = new(First);
    private static readonly DateTimeOffset SecondOffset = new(Second);

    public class Item
    {
        public DateTime When { get; set; }

        public decimal Amount { get; set; }
    }

    public class Doc
    {
        public int Id { get; set; }

        public DateTime When { get; set; }

        public DateTimeOffset WhenOffset { get; set; }

        public DateTime? MaybeWhen { get; set; }

        public int Count { get; set; }

        public decimal Amount { get; set; }

        public List<Item> Items { get; set; }
    }

    public static IEnumerable<object[]> Serializers
    {
        get
        {
            yield return new object[] { new NewtonsoftJsonSerializer(), "newtonsoft" };
            yield return new object[] { new SystemTextJsonSerializer(), "stj" };
        }
    }

    private static async Task<Tycho> SeedAsync(IJsonSerializer jsonSerializer)
    {
        var db = new Tycho(Path.GetTempPath(), jsonSerializer, dbName: $"{Guid.NewGuid()}.db", rebuildCache: true, requireTypeRegistration: false);
        await db.ConnectAsync();

        await db.WriteObjectsAsync(
            new[]
            {
                new Doc
                {
                    Id = 1,
                    When = First,
                    WhenOffset = FirstOffset,
                    MaybeWhen = First,
                    Count = 1,
                    Amount = 1.5m,
                    Items = new List<Item> { new() { When = First, Amount = 1.5m } },
                },
                new Doc
                {
                    Id = 2,
                    When = Second,
                    WhenOffset = SecondOffset,
                    MaybeWhen = null,
                    Count = 2,
                    Amount = 2m,
                    Items = new List<Item> { new() { When = Second, Amount = 2m } },
                },
            },
            x => x.Id.ToString());

        return db;
    }

    private static async Task<int[]> IdsAsync(Tycho db, FilterBuilder<Doc> filter)
        => (await db.ReadObjectsAsync<Doc>(filter: filter)).Select(x => x.Id).OrderBy(x => x).ToArray();

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_DateTime_ExcludesTheEqualRow(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);

        var equal = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.When, First));
        var notEqual = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.When, First));

        equal.ShouldBe(new[] { 1 }, label);
        notEqual.ShouldBe(new[] { 2 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_DateTimeOffset_ExcludesTheEqualRow(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);

        var equal = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.WhenOffset, FirstOffset));
        var notEqual = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.WhenOffset, FirstOffset));

        equal.ShouldBe(new[] { 1 }, label);
        notEqual.ShouldBe(new[] { 2 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_NullableDateTime_ExcludesTheEqualRowAndAbsentMembers(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);

        var equal = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.MaybeWhen, First));
        var notEqual = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.MaybeWhen, First));
        var notSecond = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.MaybeWhen, Second));

        equal.ShouldBe(new[] { 1 }, label);
        notEqual.ShouldBeEmpty(label);
        notSecond.ShouldBe(new[] { 1 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_DateTimePath_WithTextValue_ComparesAgainstTheTextAsGiven(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);
        var first = First.ToString(jsonSerializer.DateTimeSerializationFormat);

        var equal = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.When, first));
        var notEqual = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.When, first));

        equal.ShouldBe(new[] { 1 }, label);
        notEqual.ShouldBe(new[] { 2 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_NumericPath_WithTextValue_MirrorsEquals(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);

        var equalAmount = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Amount, "1.5"));
        var notEqualAmount = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Amount, "1.5"));
        var notEqualCount = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Count, "1"));

        equalAmount.ShouldBe(new[] { 1 }, label);
        notEqualAmount.ShouldBe(new[] { 2 }, label);
        notEqualCount.ShouldBe(new[] { 2 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_NumericPath_WithNumericValue_ExcludesTheEqualRow(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);

        var notEqualAmount = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Amount, 1.5m));
        var notEqualCount = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Count, 1));

        notEqualAmount.ShouldBe(new[] { 2 }, label);
        notEqualCount.ShouldBe(new[] { 2 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_ListMember_ExcludesDocumentsWhoseOnlyElementIsEqual(IJsonSerializer jsonSerializer, string label)
    {
        using var db = await SeedAsync(jsonSerializer);
        var first = First.ToString(jsonSerializer.DateTimeSerializationFormat);

        var equalWhen = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Items, x => x.When, First));
        var notEqualWhen = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Items, x => x.When, First));
        var notEqualWhenText = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Items, x => x.When, first));
        var equalAmountText = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Items, x => x.Amount, "1.5"));
        var notEqualAmountText = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Items, x => x.Amount, "1.5"));

        equalWhen.ShouldBe(new[] { 1 }, label);
        notEqualWhen.ShouldBe(new[] { 2 }, label);
        notEqualWhenText.ShouldBe(new[] { 2 }, label);
        equalAmountText.ShouldBe(new[] { 1 }, label);
        notEqualAmountText.ShouldBe(new[] { 2 }, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public void NotEquals_ListMemberDateTime_BindsTheSerializerDateForm(IJsonSerializer jsonSerializer, string label)
    {
        var equal = Render(jsonSerializer, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Items, x => x.When, First), out var equalParameters);
        var notEqual = Render(jsonSerializer, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Items, x => x.When, First), out var notEqualParameters);

        equal.ShouldContain("VAL.value = " + FilterParameters.ParameterPrefix);
        notEqual.ShouldContain("VAL.value <> " + FilterParameters.ParameterPrefix);
        equalParameters.Values.ShouldBe(new object[] { First.ToString(jsonSerializer.DateTimeSerializationFormat) }, label);
        notEqualParameters.Values.ShouldBe(equalParameters.Values, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public void NotEquals_ListMemberDateTime_WithTextValue_BindsTheTextAsGiven(IJsonSerializer jsonSerializer, string label)
    {
        var first = First.ToString(jsonSerializer.DateTimeSerializationFormat);

        var equal = Render(jsonSerializer, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Items, x => x.When, first), out var equalParameters);
        var notEqual = Render(jsonSerializer, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Items, x => x.When, first), out var notEqualParameters);

        equal.ShouldContain("VAL.value = " + FilterParameters.ParameterPrefix);
        notEqual.ShouldContain("VAL.value <> " + FilterParameters.ParameterPrefix);
        equalParameters.Values.ShouldBe(new object[] { first }, label);
        notEqualParameters.Values.ShouldBe(equalParameters.Values, label);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public void NotEquals_ListMemberNumeric_RendersTheNumericCast(IJsonSerializer jsonSerializer, string label)
    {
        var equal = Render(jsonSerializer, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Items, x => x.Amount, "1.5"), out var equalParameters);
        var notEqual = Render(jsonSerializer, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Items, x => x.Amount, "1.5"), out var notEqualParameters);

        equal.ShouldContain("CAST(VAL.value as NUMERIC) = " + FilterParameters.ParameterPrefix);
        notEqual.ShouldContain("CAST(VAL.value as NUMERIC) <> " + FilterParameters.ParameterPrefix);
        equalParameters.Values.ShouldBe(new object[] { "1.5" }, label);
        notEqualParameters.Values.ShouldBe(equalParameters.Values, label);
    }

    private static string Render(IJsonSerializer jsonSerializer, FilterBuilder<Doc> filter, out FilterParameters parameters)
    {
        var sb = new StringBuilder();
        parameters = new FilterParameters();
        filter.Build(sb, jsonSerializer, parameters);

        return sb.ToString();
    }
}
