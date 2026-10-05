using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// <c>Filter(NotEquals, x => x.Member, null)</c> is the "member is present" test, mirroring
/// <c>Equals</c> with null as the "member is absent" test. Binding the null as a parameter renders
/// <c>&lt;&gt; NULL</c>, which is never true in SQL, so the filter silently matched nothing; the
/// scalar path has to emit <c>IS NOT NULL</c> exactly as the list-member path already does.
/// </summary>
[TestClass]
public class NotEqualsNullTests
{
    public class Inner
    {
        public string Label { get; set; }
    }

    public class Doc
    {
        public long Id { get; set; }

        public DateTime? When { get; set; }

        public string Text { get; set; }

        public bool? Flag { get; set; }

        public Inner Nested { get; set; }
    }

    public static IEnumerable<object[]> Serializers
    {
        get
        {
            yield return new object[] { new NewtonsoftJsonSerializer(), "newtonsoft" };
            yield return new object[] { new SystemTextJsonSerializer(), "stj" };
        }
    }

    private static async Task<(Tycho Db, string Path)> SeedAsync(IJsonSerializer jsonSerializer)
    {
        var path = Path.Combine(Path.GetTempPath(), "tycho-notequals-null-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);

        var db =
            new Tycho(path, jsonSerializer, requireTypeRegistration: true)
                .AddTypeRegistrationWithCustomKeySelector<Doc>(x => x.Id)
                .Connect();

        await db.WriteObjectsAsync(
            new[]
            {
                new Doc { Id = 1, When = new DateTime(2026, 1, 1), Text = "a", Flag = true, Nested = new Inner { Label = "x" } },
                new Doc { Id = 2, When = null, Text = null, Flag = null, Nested = null },
                new Doc { Id = 3, When = new DateTime(2026, 1, 3), Text = string.Empty, Flag = false, Nested = new Inner { Label = null } },
                new Doc { Id = 4, When = null, Text = "d", Flag = null, Nested = null },
            });

        return (db, path);
    }

    private static async Task<long[]> IdsAsync(Tycho db, FilterBuilder<Doc> filter)
        => (await db.ReadObjectsAsync<Doc>(filter: filter)).Select(x => x.Id).OrderBy(x => x).ToArray();

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEqualsNull_OnNullableDateTime_ReturnsRowsWithAValue(IJsonSerializer jsonSerializer, string label)
    {
        var (db, path) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var present = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.When, null));
        var absent = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.When, null));

        present.ShouldBe(new long[] { 1, 3 }, label);
        absent.ShouldBe(new long[] { 2, 4 }, label);

        db.Disconnect();
        Directory.Delete(path, true);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEqualsNull_OnString_KeepsEmptyStrings(IJsonSerializer jsonSerializer, string label)
    {
        var (db, path) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var present = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Text, null));
        var presentAndNonEmpty =
            await IdsAsync(
                db,
                FilterBuilder<Doc>.Create()
                    .Filter(FilterType.NotEquals, x => x.Text, null)
                    .And()
                    .Filter(FilterType.NotEquals, x => x.Text, string.Empty));

        present.ShouldBe(new long[] { 1, 3, 4 }, label);
        presentAndNonEmpty.ShouldBe(new long[] { 1, 4 }, label);

        db.Disconnect();
        Directory.Delete(path, true);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEqualsNull_OnNullableBool_ReturnsRowsWithAValue(IJsonSerializer jsonSerializer, string label)
    {
        var (db, path) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var present = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Flag, null));

        present.ShouldBe(new long[] { 1, 3 }, label);

        db.Disconnect();
        Directory.Delete(path, true);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEqualsNull_OnNestedObject_ReturnsRowsWhereObjectIsPresent(IJsonSerializer jsonSerializer, string label)
    {
        var (db, path) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var present = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Nested, null));

        present.ShouldBe(new long[] { 1, 3 }, label);

        db.Disconnect();
        Directory.Delete(path, true);
    }

    [TestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NotEquals_WithValue_StillExcludesNulls(IJsonSerializer jsonSerializer, string label)
    {
        var (db, path) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var notA = await IdsAsync(db, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Text, "a"));

        notA.ShouldBe(new long[] { 3, 4 }, label);

        db.Disconnect();
        Directory.Delete(path, true);
    }
}
