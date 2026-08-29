#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// Set membership as a single atomic term. The point of it over a chain of Or()s is that it
/// cannot be mis-grouped, so these tests assert the results themselves rather than the SQL —
/// except where the shape is the behaviour under test (the numeric CAST that keeps an
/// expression index usable, and the chunking that keeps a long list under SQLite's parameter
/// ceiling).
/// </summary>
[TestClass]
public class FilterInTests
{
    private const string PartitionA = "partitionA";
    private const string PartitionB = "partitionB";

    [TestMethod]
    public async Task In_MatchesOnlyTheListedValues()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47 }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public async Task NotIn_MatchesEverythingElse()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.NotIn, x => x.DepartmentId, new[] { 33, 47 }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 3 });
    }

    [TestMethod]
    public async Task In_StaysWithinItsPartition()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionB,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47 }));

        results.Select(x => x.Id).ShouldBe(new[] { 4 });
    }

    [TestMethod]
    public async Task In_OnStringProperty_MatchesRows()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.Description, new[] { "alpha", "gamma" }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 3 });
    }

    [TestMethod]
    public async Task In_OnEnumProperty_ComparesTheSerializedForm()
    {
        // The set has to be resolved element by element, the way a scalar comparison value is:
        // with a string-enum converter the stored form is the member name, not the number.
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions { Converters = { new JsonStringEnumConverter() } });

        using var db = Connect(out _, serializer);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create()
                    .Filter(FilterType.In, x => x.Allocation, new[] { Allocation.Produce, Allocation.Dairy }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public async Task In_OnDateTimeProperty_MatchesRows()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create()
                    .Filter(FilterType.In, x => x.Created, new[] { BaseDate, BaseDate.AddDays(2) }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 3 });
    }

    [TestMethod]
    public async Task In_OnBoolProperty_MatchesRows()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.IsActive, new[] { true }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 3 });
    }

    [TestMethod]
    public async Task In_WithEmptySet_MatchesNothing()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.DepartmentId, Array.Empty<int>()));

        results.ShouldBeEmpty();
    }

    [TestMethod]
    public async Task NotIn_WithEmptySet_MatchesEverything()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.NotIn, x => x.DepartmentId, Array.Empty<int>()));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2, 3 });
    }

    [TestMethod]
    public async Task In_WithNullInTheSet_MatchesMissingValues()
    {
        // SQL's own IN never matches NULL against a NULL in the list; the builder pulls the
        // null out into an IS NULL test so the caller's intent survives.
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.Description, new[] { "alpha", null }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public async Task In_WithDuplicates_MatchesTheSameRowsOnce()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create()
                    .Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 33, 47, 33 }));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public void In_WithDuplicates_RendersEachValueOnce()
    {
        var sql = Render(f => f.Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 33, 47, 33 }), out _);

        sql.ShouldContain("IN (33, 47)");
    }

    [TestMethod]
    public void In_OnNumericProperty_KeepsTheCastThatMakesAnIndexUsable()
    {
        var sql = Render(f => f.Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47 }), out _);

        sql.ShouldContain("CAST(JSON_EXTRACT(Data, '$.DepartmentId') as NUMERIC) IN (33, 47)");
    }

    [TestMethod]
    public async Task In_OnAnIndexedNumericProperty_UsesTheIndex()
    {
        // The whole point of rendering the numeric CAST is that SQLite matches expression
        // indexes structurally; get the form wrong and this silently degrades to a table scan.
        // Enough rows are seeded that the planner has a reason to prefer the index.
        var db = Connect(out var path);
        await db.WriteObjectsAsync(
            Enumerable.Range(1, 2_000)
                .Select(i => new Doc { Id = i, DepartmentId = i, Description = "d" + i.ToString(CultureInfo.InvariantCulture) }),
            x => x.Id,
            PartitionA);
        await db.CreateIndexAsync<Doc>(x => x.DepartmentId, "ix_in_department");

        db.Dispose();
        SqliteConnection.ClearAllPools();

        var plan = ExplainFilter(path, f => f.Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47 }));

        plan.ShouldContain("ix_in_department");
        plan.ShouldNotContain("SCAN JsonValue", Case.Sensitive);
    }

    [TestMethod]
    public async Task In_WithMoreValuesThanTheParameterCeiling_StillMatches()
    {
        // Long lists are split across several IN terms rather than exceeding
        // SQLITE_MAX_VARIABLE_NUMBER, which is 999 on older builds.
        using var db = Connect(out _);
        await SeedAsync(db);

        var manyStrings =
            Enumerable.Range(0, 2_500).Select(i => "filler" + i.ToString(CultureInfo.InvariantCulture))
                .Concat(new[] { "alpha", "gamma" })
                .ToArray();

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.Description, manyStrings));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 3 });
    }

    [TestMethod]
    public async Task NotIn_WithMoreValuesThanTheParameterCeiling_StillMatches()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var manyStrings =
            Enumerable.Range(0, 2_500).Select(i => "filler" + i.ToString(CultureInfo.InvariantCulture))
                .Concat(new[] { "gamma" })
                .ToArray();

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.NotIn, x => x.Description, manyStrings));

        // Id 2 has no description: NULL fails NOT IN, exactly as it fails NotEquals.
        results.Select(x => x.Id).ShouldBe(new[] { 1 });
    }

    [TestMethod]
    public async Task NotIn_ExcludesNullValues_LikeNotEquals()
    {
        // Pinning the SQL NULL semantics deliberately rather than by accident: a row whose
        // member is absent or null is not returned by NotIn, which is how the scalar
        // NotEquals already behaves. Callers who want those rows add an explicit
        // Or(Equals(path, null)) term.
        using var db = Connect(out _);
        await SeedAsync(db);

        var notIn =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.NotIn, x => x.Description, new[] { "alpha" }));

        var notEquals =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Description, "alpha"));

        notIn.Select(x => x.Id).ShouldBe(new[] { 3 });
        notIn.Select(x => x.Id).ShouldBe(notEquals.Select(x => x.Id));
    }

    [TestMethod]
    public async Task In_ComposesWithOtherTerms()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create()
                    .Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47 }).And()
                    .Filter(FilterType.Equals, x => x.IsActive, true));

        results.Select(x => x.Id).ShouldBe(new[] { 1 });
    }

    [TestMethod]
    public async Task In_UsedForDeletion_RemovesOnlyItsOwnPartition()
    {
        using var db = Connect(out _);
        await SeedAsync(db);

        var deleted =
            await db.DeleteObjectsAsync(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47 }));

        deleted.ShouldBe(2);
        (await db.ReadObjectsAsync<Doc>(PartitionB)).Select(x => x.Id).ShouldBe(new[] { 4 });
    }

    [TestMethod]
    public void SetFilterType_OnTheScalarOverload_IsRejected()
    {
        var build = () => FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.DepartmentId, (object)33);

        Should.Throw<ArgumentException>(build).Message.ShouldContain("IEnumerable");
    }

    [TestMethod]
    public async Task NullValue_StillMeansTheScalarNullComparison()
    {
        // Adding an IEnumerable<TProp> overload changes where a literal null binds: it is the
        // more specific parameter type, so Filter(Equals, x => x.Description, null) now lands on
        // the collection overload. It has to keep meaning "compare against null".
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Description, null));

        results.Select(x => x.Id).ShouldBe(new[] { 2 });
    }

    [TestMethod]
    public void NullValue_WithASetFilterType_IsRejected()
    {
        // The empty set is a meaningful request; a missing set is not.
        var build = () => FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.Description, null);

        Should.Throw<ArgumentNullException>(build).Message.ShouldContain("empty one");
    }

    [TestMethod]
    public async Task RawPathOverload_TakesAValueTypeCollectionViaCast()
    {
        // IEnumerable<object> is deliberate: a generic overload here would capture an ordinary
        // string comparison value, because string is IEnumerable<char>. The cost is that a
        // value-type collection needs Cast<object>(), and the error when it is omitted has to
        // say so.
        using var db = Connect(out _);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                PartitionA,
                FilterBuilder<Doc>.Create().Filter(
                    FilterType.In, "$.DepartmentId", true, false, false, new[] { 33, 47 }.Cast<object>()));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public void RawPathOverload_WithAnUncastValueTypeCollection_SaysWhatToDo()
    {
        // int[] binds to the scalar object overload, which rejects a set filter type loudly
        // rather than rendering "System.Int32[]" and matching nothing.
        var build = () =>
            FilterBuilder<Doc>.Create().Filter(FilterType.In, "$.DepartmentId", true, false, false, new[] { 33, 47 });

        Should.Throw<ArgumentException>(build).Message.ShouldContain("Cast<object>()");
    }

    [TestMethod]
    public void ScalarFilterType_OnTheCollectionOverload_IsRejected()
    {
        var build = () => FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.DepartmentId, new[] { 33, 47 });

        Should.Throw<ArgumentException>(build).Message.ShouldContain("collection");
    }

    private static readonly DateTime BaseDate = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static readonly IJsonSerializer DefaultSerializer = new NewtonsoftJsonSerializer();

    private static string Render(Action<FilterBuilder<Doc>> configure, out FilterParameters parameters)
    {
        var filter = FilterBuilder<Doc>.Create();
        configure(filter);

        var sb = new StringBuilder();
        parameters = new FilterParameters();
        filter.Build(sb, DefaultSerializer, parameters);

        return sb.ToString();
    }

    private static string ExplainFilter(string dbFile, Action<FilterBuilder<Doc>> configure)
    {
        var filter = FilterBuilder<Doc>.Create();
        configure(filter);

        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        filter.Build(sb, DefaultSerializer, parameters);

        using var conn = new SqliteConnection($"Data Source={dbFile}");
        conn.Open();
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // SQL is produced by the library's own builders.
        command.CommandText = "EXPLAIN QUERY PLAN " + sb.ToString();
#pragma warning restore CA2100

        command.Parameters.AddWithValue("$fullTypeName", typeof(Doc).FullName);
        command.Parameters.AddWithValue("$partition", PartitionA);
        for (var i = 0; i < parameters.Count; i++)
        {
            command.Parameters.AddWithValue(
                FilterParameters.ParameterPrefix + i.ToString(CultureInfo.InvariantCulture),
                parameters.Values[i] ?? (object)DBNull.Value);
        }

        var plan = new StringBuilder();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            plan.AppendLine(reader.GetString(reader.FieldCount - 1));
        }

        return plan.ToString();
    }

    private static Tycho Connect(out string path, IJsonSerializer? jsonSerializer = null)
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";
        path = Path.Combine(dir, name);

        var db = new Tycho(
            dir, jsonSerializer ?? DefaultSerializer, dbName: name, rebuildCache: true, requireTypeRegistration: false);
        return db.Connect();
    }

    private static async Task SeedAsync(Tycho db)
    {
        await db.WriteObjectsAsync(
            new[]
            {
                new Doc { Id = 1, DepartmentId = 33, Description = "alpha", IsActive = true, Created = BaseDate, Allocation = Allocation.Produce },
                new Doc { Id = 2, DepartmentId = 47, Description = null, IsActive = false, Created = BaseDate.AddDays(1), Allocation = Allocation.Dairy },
                new Doc { Id = 3, DepartmentId = 51, Description = "gamma", IsActive = true, Created = BaseDate.AddDays(2), Allocation = Allocation.Bakery },
            },
            x => x.Id,
            PartitionA);

        await db.WriteObjectsAsync(
            new[]
            {
                new Doc { Id = 4, DepartmentId = 33, Description = "alpha", IsActive = true, Created = BaseDate, Allocation = Allocation.Produce },
            },
            x => x.Id,
            PartitionB);
    }

    public enum Allocation
    {
        Produce = 0,
        Dairy = 1,
        Bakery = 2,
    }

    public class Doc
    {
        public int Id { get; set; }

        public int DepartmentId { get; set; }

        public string? Description { get; set; }

        public bool IsActive { get; set; }

        public DateTime Created { get; set; }

        public Allocation Allocation { get; set; }
    }
}
