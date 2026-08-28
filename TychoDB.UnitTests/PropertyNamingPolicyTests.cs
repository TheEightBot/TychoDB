using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Serialization;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// Property expressions must be translated into JSON paths that match how the serializer
/// actually named the members. When they are not, SQLite finds nothing at the path and the
/// failure is silent — reads return zero rows, sorts do not sort, and indexes never match —
/// so each test here asserts on the value, never merely that the call did not throw.
/// </summary>
[TestClass]
public class PropertyNamingPolicyTests
{
    private const string Target = "target";

    private static IJsonSerializer CamelCase() =>
        new SystemTextJsonSerializer(new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

    private static IJsonSerializer NewtonsoftCamelCase() =>
        new NewtonsoftJsonSerializer(
            new Newtonsoft.Json.JsonSerializer { ContractResolver = new CamelCasePropertyNamesContractResolver() });

    public static IEnumerable<object[]> RenamingSerializers
    {
        get
        {
            yield return new object[] { CamelCase() };
            yield return new object[] { NewtonsoftCamelCase() };
        }
    }

    [TestMethod]
    [DynamicData(nameof(RenamingSerializers))]
    public async Task Filter_WithRenamedMembers_MatchesRows(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer, out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                filter: FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Description, Target));

        results.Count().ShouldBe(3);
        results.ShouldAllBe(x => x.Description == Target);
    }

    [TestMethod]
    [DynamicData(nameof(RenamingSerializers))]
    public async Task Sort_WithRenamedMembers_ActuallySorts(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer, out _);
        await SeedAsync(db);

        var results =
            (await db.ReadObjectsAsync<Doc>(
                sort: SortBuilder<Doc>.Create().OrderBy(SortDirection.Descending, x => x.Count)))
            .ToList();

        results.Select(x => x.Count).ShouldBe(new[] { 4, 3, 2, 1, 0 });
    }

    [TestMethod]
    [DynamicData(nameof(RenamingSerializers))]
    public async Task Projection_WithRenamedMembers_ReturnsValues(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer, out _);
        await SeedAsync(db);

        var descriptions = await db.ReadObjectsAsync<Doc, string>(x => x.Description);

        descriptions.Length.ShouldBe(5);
        descriptions.Count(x => x == Target).ShouldBe(3);
    }

    [TestMethod]
    [DynamicData(nameof(RenamingSerializers))]
    public async Task NestedPath_ResolvesEverySegment(IJsonSerializer jsonSerializer)
    {
        using var db = Connect(jsonSerializer, out _);
        await db.WriteObjectsAsync(
            new[]
            {
                new Outer { Id = 1, InnerValue = new Inner { DeepName = "match" } },
                new Outer { Id = 2, InnerValue = new Inner { DeepName = "miss" } },
            },
            x => x.Id.ToString());

        var results =
            await db.ReadObjectsAsync<Outer>(
                filter: FilterBuilder<Outer>.Create()
                    .Filter(FilterType.Equals, x => x.InnerValue.DeepName, "match"));

        results.Single().Id.ShouldBe(1);
    }

    [TestMethod]
    public async Task JsonPropertyNameAttribute_IsHonoured_WithoutAnyNamingPolicy()
    {
        // The attribute renames the member regardless of naming policy, so this breaks even on
        // a default-configured serializer.
        using var db = Connect(new SystemTextJsonSerializer(), out _);
        await db.WriteObjectsAsync(
            new[] { new AttributedDoc { Id = 1, Description = Target } }, x => x.Id.ToString());

        var results =
            await db.ReadObjectsAsync<AttributedDoc>(
                filter: FilterBuilder<AttributedDoc>.Create().Filter(FilterType.Equals, x => x.Description, Target));

        results.Single().Description.ShouldBe(Target);
    }

    [TestMethod]
    public async Task CreateIndex_UsesResolvedJsonPath()
    {
        var db = Connect(CamelCase(), out var path);
        await SeedAsync(db);
        await db.CreateIndexAsync<Doc>(x => x.Description, "ix_naming_description");

        db.Dispose();
        SqliteConnection.ClearAllPools();

        IndexDdl(path, "%naming_description%").ShouldContain("$.description", Case.Sensitive);
    }

    [TestMethod]
    public async Task CreateIndex_OnValueTypeProperty_TargetsThatProperty_NotWholeDocument()
    {
        // Expression<Func<T, object>> boxes a value-type property behind a Convert node. Left
        // unwrapped, the path collapses to "$" and the index covers the entire document.
        var db = Connect(CamelCase(), out var path);
        await SeedAsync(db);
        await db.CreateIndexAsync<Doc>(x => x.Count, "ix_naming_count");

        db.Dispose();
        SqliteConnection.ClearAllPools();

        var ddl = IndexDdl(path, "%naming_count%");
        ddl.ShouldContain("$.count", Case.Sensitive);
        ddl.ShouldNotContain("Data, '$')", Case.Sensitive);
    }

    [TestMethod]
    public async Task SerializerWithoutResolver_FallsBackToClrNames()
    {
        // Third-party serializers that do not implement IJsonPropertyNameResolver must keep
        // working exactly as before, using CLR property names.
        using var db = Connect(new ClrNameSerializer(), out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                filter: FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Description, Target));

        results.Count().ShouldBe(3);
    }

    [TestMethod]
    public async Task ResolvedNameThatCannotBeExpressedInAPath_IsRejected()
    {
        // A resolved name is arbitrary text from an attribute or policy, not a CLR identifier.
        // It is concatenated into the single-quoted SQL literal holding the JSON path, so a
        // quote would escape that literal.
        using var db = Connect(new SystemTextJsonSerializer(), out _);
        await db.WriteObjectsAsync(new[] { new HostileDoc { Id = 1, Description = Target } }, x => x.Id.ToString());

        var build = () =>
            FilterBuilder<HostileDoc>.Create().Filter(FilterType.Equals, x => x.Description, Target);

        // The path is rendered during the read, where the serializer is known.
        var ex = await Should.ThrowAsync<ArgumentException>(
            async () => await db.ReadObjectsAsync<HostileDoc>(filter: build()));

        ex.Message.ShouldContain("HostileDoc.Description");
    }

    private static Tycho Connect(IJsonSerializer jsonSerializer, out string path)
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";
        path = Path.Combine(dir, name);

        var db = new Tycho(dir, jsonSerializer, dbName: name, rebuildCache: true, requireTypeRegistration: false);
        return db.Connect();
    }

    private static async Task SeedAsync(Tycho db)
    {
        var docs =
            Enumerable
                .Range(0, 5)
                .Select(i => new Doc { Id = i, Description = i < 3 ? Target : $"other-{i}", Count = i })
                .ToList();

        await db.WriteObjectsAsync(docs, x => x.Id.ToString());
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Test-only inspection of sqlite_master with a constant query.")]
    private static string IndexDdl(string dbPath, string namePattern)
    {
        using var conn = new SqliteConnection($"Filename={dbPath};Pooling=False");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sql FROM sqlite_master WHERE type = 'index' AND name LIKE $pattern";
        cmd.Parameters.AddWithValue("$pattern", namePattern);
        return cmd.ExecuteScalar() as string;
    }

    /// <summary>A serializer that deliberately does not implement the name-resolution capability.</summary>
    private sealed class ClrNameSerializer : IJsonSerializer
    {
        private readonly SystemTextJsonSerializer _inner = new();

        public string DateTimeSerializationFormat => _inner.DateTimeSerializationFormat;

        public object Serialize<T>(T obj) => _inner.Serialize(obj);

        public void Serialize<T>(T obj, System.Buffers.IBufferWriter<byte> bufferWriter) => _inner.Serialize(obj, bufferWriter);

        public ValueTask<T> DeserializeAsync<T>(Stream stream, System.Threading.CancellationToken cancellationToken)
            => _inner.DeserializeAsync<T>(stream, cancellationToken);
    }

    public class Doc
    {
        public int Id { get; set; }

        public string Description { get; set; }

        public int Count { get; set; }
    }

    public class AttributedDoc
    {
        public int Id { get; set; }

        [JsonPropertyName("desc")]
        public string Description { get; set; }
    }

    public class HostileDoc
    {
        public int Id { get; set; }

        [JsonPropertyName("desc') = 'x' OR '1'='1")]
        public string Description { get; set; }
    }

    public class Outer
    {
        public int Id { get; set; }

        public Inner InnerValue { get; set; }
    }

    public class Inner
    {
        public string DeepName { get; set; }
    }
}
