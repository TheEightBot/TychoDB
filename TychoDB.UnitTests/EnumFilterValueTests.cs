using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Converters;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// A filter value has to be compared against the JSON the serializer actually wrote. An enum
/// is the case where CLR and JSON disagree most sharply: by default both serializers write it
/// as a number, but with a string-enum converter they write its name — and with a naming
/// policy, a transformed name. Comparing the wrong form matches nothing, silently.
/// </summary>
[TestClass]
public class EnumFilterValueTests
{
    private const int ProduceRows = 3;

    private static IJsonSerializer StjNumeric() => new SystemTextJsonSerializer();

    private static IJsonSerializer StjStringEnum() =>
        new SystemTextJsonSerializer(
            new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } });

    private static IJsonSerializer StjStringEnumCamelCase() =>
        new SystemTextJsonSerializer(
            new JsonSerializerOptions
            {
                Converters = { new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) },
            });

    private static IJsonSerializer NewtonsoftNumeric() => new NewtonsoftJsonSerializer();

    private static IJsonSerializer NewtonsoftStringEnum()
    {
        var serializer = new Newtonsoft.Json.JsonSerializer();
        serializer.Converters.Add(new StringEnumConverter());
        return new NewtonsoftJsonSerializer(serializer);
    }

    public static IEnumerable<object[]> AllSerializers
    {
        get
        {
            yield return new object[] { StjNumeric(), "stj-numeric" };
            yield return new object[] { StjStringEnum(), "stj-string" };
            yield return new object[] { StjStringEnumCamelCase(), "stj-string-camel" };
            yield return new object[] { NewtonsoftNumeric(), "nsj-numeric" };
            yield return new object[] { NewtonsoftStringEnum(), "nsj-string" };
        }
    }

    [TestMethod]
    [DynamicData(nameof(AllSerializers))]
    public async Task Equals_WithEnumValue_MatchesRows(IJsonSerializer jsonSerializer, string label)
    {
        var (db, path) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var results =
            await db.ReadObjectsAsync<Doc>(
                filter: FilterBuilder<Doc>.Create()
                    .Filter(FilterType.Equals, x => x.StoreAllocation, StoreAllocationType.Produce));

        Console.WriteLine($"[{label}] stored: {StoredJson(db, path)}");
        results.Count().ShouldBe(ProduceRows, label);
    }

    [TestMethod]
    [DynamicData(nameof(AllSerializers))]
    public async Task NotEquals_WithEnumValue_MatchesRows(IJsonSerializer jsonSerializer, string label)
    {
        var (db, _) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var results =
            await db.ReadObjectsAsync<Doc>(
                filter: FilterBuilder<Doc>.Create()
                    .Filter(FilterType.NotEquals, x => x.StoreAllocation, StoreAllocationType.Produce));

        results.Count().ShouldBe(2, label);
    }

    [TestMethod]
    [DynamicData(nameof(AllSerializers))]
    public async Task Equals_WithNullableEnumValue_MatchesRows(IJsonSerializer jsonSerializer, string label)
    {
        var (db, _) = await SeedAsync(jsonSerializer);
        using var scoped = db;

        var results =
            await db.ReadObjectsAsync<Doc>(
                filter: FilterBuilder<Doc>.Create()
                    .Filter(FilterType.Equals, x => x.OptionalAllocation, StoreAllocationType.Dairy));

        results.Count().ShouldBe(1, label);
    }

    [TestMethod]
    public async Task Equals_WithExplicitIntCast_StillMatches_OnNumericSerializer()
    {
        // The caller's workaround must keep working on a numeric-enum serializer.
        var (db, _) = await SeedAsync(StjNumeric());
        using var scoped = db;

        var results =
            await db.ReadObjectsAsync<Doc>(
                filter: FilterBuilder<Doc>.Create()
                    .Filter(FilterType.Equals, x => x.StoreAllocation, (int)StoreAllocationType.Produce));

        results.Count().ShouldBe(ProduceRows);
    }

    private static async Task<(Tycho Db, string Path)> SeedAsync(IJsonSerializer jsonSerializer)
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";
        var db = new Tycho(dir, jsonSerializer, dbName: name, rebuildCache: true, requireTypeRegistration: false);
        await db.ConnectAsync();

        var docs =
            new[]
            {
                new Doc { Id = 0, StoreAllocation = StoreAllocationType.Produce },
                new Doc { Id = 1, StoreAllocation = StoreAllocationType.Produce },
                new Doc { Id = 2, StoreAllocation = StoreAllocationType.Produce, OptionalAllocation = StoreAllocationType.Dairy },
                new Doc { Id = 3, StoreAllocation = StoreAllocationType.Dairy },
                new Doc { Id = 4, StoreAllocation = StoreAllocationType.Bakery },
            };

        await db.WriteObjectsAsync(docs, x => x.Id.ToString());
        return (db, Path.Combine(dir, name));
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Test-only inspection with constant SQL.")]
    private static string StoredJson(Tycho db, string dbPath)
    {
        db.Disconnect();
        SqliteConnection.ClearAllPools();
        using var conn = new SqliteConnection($"Filename={dbPath};Pooling=False");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT json(Data) FROM JsonValue LIMIT 1";
        return cmd.ExecuteScalar() as string;
    }

    public enum StoreAllocationType
    {
        Produce = 0,
        Dairy = 1,
        Bakery = 2,
    }

    public class Doc
    {
        public int Id { get; set; }

        public StoreAllocationType StoreAllocation { get; set; }

        public StoreAllocationType? OptionalAllocation { get; set; }
    }
}
