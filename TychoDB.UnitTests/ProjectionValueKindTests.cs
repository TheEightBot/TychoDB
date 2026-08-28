using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// The projection overloads select a single member out of the stored document, so the value
/// reaches the deserializer on its own rather than inside an object. Every JSON kind has to
/// survive that round trip — including the two that carry no value at all, a JSON null and a
/// member that was never written.
/// </summary>
[TestClass]
public class ProjectionValueKindTests
{
    public static IEnumerable<object[]> JsonSerializers
    {
        get
        {
            yield return new object[] { new SystemTextJsonSerializer() };
            yield return new object[] { new NewtonsoftJsonSerializer() };
        }
    }

    [TestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task Projects_BooleanProperty(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer);
        await SeedAsync(db);

        var flags = await db.ReadObjectsAsync<Doc, bool>(x => x.Flag);

        flags.Length.ShouldBe(3);
        flags.Count(x => x).ShouldBe(2);
        flags.Count(x => !x).ShouldBe(1);
    }

    [TestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task Projects_NullableBooleanProperty(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer);
        await SeedAsync(db);

        var flags = await db.ReadObjectsAsync<Doc, bool?>(x => x.OptionalFlag);

        flags.Length.ShouldBe(3);
        flags.Count(x => x == true).ShouldBe(1);
        flags.Count(x => x is null).ShouldBe(2);
    }

    [TestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task Projects_StringProperty_ThatIsSometimesAbsent(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer);
        await SeedAsync(db);

        var names = await db.ReadObjectsAsync<Doc, string>(x => x.OptionalName);

        names.Length.ShouldBe(3);
        names.Count(x => x is null).ShouldBe(2);
        names.ShouldContain("present");
    }

    [TestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task Projects_PropertyMissingFromEveryDocument(IJsonSerializer jsonSerializer)
    {
        // Nothing ever wrote this member, so the path matches no row. That is an absence, not
        // a failure: the projection yields the default for each row rather than throwing.
        using var db = Connect(jsonSerializer);
        await SeedAsync(db);

        var values = await db.ReadObjectsAsync<Doc, string>(x => x.NeverWritten);

        values.Length.ShouldBe(3);
        values.ShouldAllBe(x => x == null);
    }

    [TestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task Projects_ValueTypeProperty_WhenMemberIsAbsent_YieldsDefault(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer);
        await SeedAsync(db);

        var values = await db.ReadObjectsAsync<Doc, int>(x => x.NeverWrittenNumber);

        values.Length.ShouldBe(3);
        values.ShouldAllBe(x => x == 0);
    }

    [TestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task Projects_NumberStringObjectAndArray(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer);
        await SeedAsync(db);

        (await db.ReadObjectsAsync<Doc, int>(x => x.Count)).OrderBy(x => x).ShouldBe(new[] { 0, 1, 2 });
        (await db.ReadObjectsAsync<Doc, double>(x => x.Ratio)).Length.ShouldBe(3);
        (await db.ReadObjectsAsync<Doc, string>(x => x.Name)).ShouldContain("doc-1");
        (await db.ReadObjectsAsync<Doc, Nested>(x => x.Child)).ShouldAllBe(x => x.Label != null);
        (await db.ReadObjectsAsync<Doc, List<int>>(x => x.Numbers)).ShouldAllBe(x => x.Count == 2);
    }

    [TestMethod]
    [DynamicData(nameof(JsonSerializers))]
    public async Task ProjectsWithKeys_BooleanAndAbsentMembers(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer);
        await SeedAsync(db);

        var flagged = await db.ReadObjectsWithKeysAsync<Doc, bool>(x => x.Flag);
        var absent = await db.ReadObjectsWithKeysAsync<Doc, string>(x => x.NeverWritten);

        flagged.Count().ShouldBe(3);
        flagged.ShouldAllBe(x => x.Key != null);
        absent.ShouldAllBe(x => x.InnerObject == null);
    }

    private static Tycho Connect(IJsonSerializer jsonSerializer)
    {
        return new Tycho(
                Path.GetTempPath(),
                jsonSerializer,
                dbName: $"{Guid.NewGuid()}.db",
                rebuildCache: true,
                requireTypeRegistration: false)
            .Connect();
    }

    private static async Task SeedAsync(Tycho db)
    {
        var docs =
            new[]
            {
                new Doc { Id = 0, Name = "doc-0", Count = 0, Ratio = 0.5, Flag = true, OptionalFlag = true, OptionalName = "present" },
                new Doc { Id = 1, Name = "doc-1", Count = 1, Ratio = 1.5, Flag = true },
                new Doc { Id = 2, Name = "doc-2", Count = 2, Ratio = 2.5, Flag = false },
            };

        foreach (var doc in docs)
        {
            doc.Child = new Nested { Label = $"child-{doc.Id}" };
            doc.Numbers = new List<int> { doc.Id, doc.Id + 1 };
        }

        await db.WriteObjectsAsync(docs, x => x.Id.ToString());
    }

    public class Doc
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public int Count { get; set; }

        public double Ratio { get; set; }

        public bool Flag { get; set; }

        public bool? OptionalFlag { get; set; }

        public string OptionalName { get; set; }

        public Nested Child { get; set; }

        public List<int> Numbers { get; set; }

        [JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        public string NeverWritten { get; set; }

        [JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        public int NeverWrittenNumber { get; set; }
    }

    public class Nested
    {
        public string Label { get; set; }
    }
}
