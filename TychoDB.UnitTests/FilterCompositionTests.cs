#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// The caller's filter is only one conjunct of the generated WHERE clause — the partition and
/// type predicates are the others. Because AND binds tighter than OR, an ungrouped OR in the
/// caller's filter splits the clause and its trailing terms escape both predicates, matching
/// rows of other partitions and other stored types. Every test here therefore seeds more than
/// one partition and more than one type: a single-partition, single-type fixture cannot observe
/// the difference.
/// </summary>
[TestClass]
public class FilterCompositionTests
{
    private const string PartitionA = "partitionA";
    private const string PartitionB = "partitionB";
    private const string Shared = "shared";

    [TestMethod]
    public async Task UngroupedOr_DoesNotMatchRowsInOtherPartitions()
    {
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[] { Item(1, 33), Item(2, 47) }, x => x.Id, PartitionA);
        await db.WriteObjectsAsync(
            new[] { Item(3, 33), Item(4, 47) }, x => x.Id, PartitionB);

        var results =
            await db.ReadObjectsAsync<ItemModel>(
                PartitionA,
                FilterBuilder<ItemModel>.Create()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 33).Or()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 47));

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public async Task UngroupedOr_DoesNotMatchRowsOfOtherTypes()
    {
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[] { Item(1, 33), Item(2, 47) }, x => x.Id, Shared);
        await db.WriteObjectsAsync(
            new[] { new VendorModel { Id = 900, Description = "VENDOR" } },
            x => x.Id.ToString(),
            Shared);

        var results =
            await db.ReadObjectsAsync<ItemModel>(
                Shared,
                FilterBuilder<ItemModel>.Create()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 33).Or()
                    .Filter(FilterType.Contains, x => x.Description, "VENDOR"));

        results.Select(x => x.Id).ShouldBe(new[] { 1 });
    }

    [TestMethod]
    public async Task UngroupedOr_DoesNotDeleteRowsInOtherPartitions()
    {
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[] { Item(1, 33), Item(2, 47) }, x => x.Id, PartitionA);
        await db.WriteObjectsAsync(
            new[] { Item(3, 33), Item(4, 47) }, x => x.Id, PartitionB);

        var deleted =
            await db.DeleteObjectsAsync(
                PartitionA,
                FilterBuilder<ItemModel>.Create()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 33).Or()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 47));

        deleted.ShouldBe(2);

        var survivors = await db.ReadObjectsAsync<ItemModel>(PartitionB);
        survivors.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 3, 4 });
    }

    [TestMethod]
    public async Task UngroupedOr_DoesNotCountRowsInOtherPartitions()
    {
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[] { Item(1, 33), Item(2, 47) }, x => x.Id, PartitionA);
        await db.WriteObjectsAsync(
            new[] { Item(3, 33), Item(4, 47) }, x => x.Id, PartitionB);

        var count =
            await db.CountObjectsAsync(
                PartitionA,
                FilterBuilder<ItemModel>.Create()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 33).Or()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 47));

        count.ShouldBe(2);
    }

    [TestMethod]
    public async Task ExplicitlyGroupedOr_StillMatchesOnlyItsOwnPartition()
    {
        // The grouped form was already correct; it must stay correct once the builder adds its
        // own enclosing parentheses.
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[] { Item(1, 33), Item(2, 47) }, x => x.Id, PartitionA);
        await db.WriteObjectsAsync(
            new[] { Item(3, 33), Item(4, 47) }, x => x.Id, PartitionB);

        var results =
            await db.ReadObjectsAsync<ItemModel>(
                PartitionA,
                FilterBuilder<ItemModel>.Create()
                    .StartGroup()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 33).Or()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 47)
                    .EndGroup());

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public async Task AndChain_IsUnaffected()
    {
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[] { Item(1, 33), Item(2, 47) }, x => x.Id, PartitionA);
        await db.WriteObjectsAsync(
            new[] { Item(3, 33) }, x => x.Id, PartitionB);

        var results =
            await db.ReadObjectsAsync<ItemModel>(
                PartitionA,
                FilterBuilder<ItemModel>.Create()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 33).And()
                    .Filter(FilterType.Equals, x => x.Description, "ITEM"));

        results.Select(x => x.Id).ShouldBe(new[] { 1 });
    }

    [TestMethod]
    public async Task Linq_PrecedenceWithinPredicate_IsPreserved()
    {
        // (a || b) && c must not be flattened into "a OR b AND c", which SQL reads as
        // "a OR (b AND c)" — an item matching only `a` then comes back despite failing `c`.
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[]
            {
                new ItemModel { Id = 1, DepartmentId = 33, Description = "OTHER" },
                new ItemModel { Id = 2, DepartmentId = 47, Description = "ITEM" },
            },
            x => x.Id,
            PartitionA);

        var results =
            await db.Query<ItemModel>(PartitionA)
                .Where(x => (x.DepartmentId == 33 || x.DepartmentId == 47) && x.Description == "ITEM")
                .ToListAsync();

        results.Select(x => x.Id).ShouldBe(new[] { 2 });
    }

    [TestMethod]
    public async Task Linq_OrPredicate_DoesNotMatchRowsInOtherPartitions()
    {
        using var db = Connect();

        await db.WriteObjectsAsync(
            new[] { Item(1, 33), Item(2, 47) }, x => x.Id, PartitionA);
        await db.WriteObjectsAsync(
            new[] { Item(3, 33), Item(4, 47) }, x => x.Id, PartitionB);

        var results =
            await db.Query<ItemModel>(PartitionA)
                .Where(x => x.DepartmentId == 33 || x.DepartmentId == 47)
                .ToListAsync();

        results.Select(x => x.Id).OrderBy(x => x).ShouldBe(new[] { 1, 2 });
    }

    [TestMethod]
    public async Task UngroupedOr_OnAnIndexedProperty_UsesTheIndex()
    {
        // The correctness bug had a performance face: the escaped terms lost the Partition
        // predicate too, so they could not use the partition-prefixed index and fell back to
        // scanning the whole table.
        var db = Connect(out var path);
        await db.WriteObjectsAsync(
            Enumerable.Range(1, 2_000).Select(i => Item(i, i)), x => x.Id, PartitionA);
        await db.CreateIndexAsync<ItemModel>(x => x.DepartmentId, "ix_or_department");

        db.Dispose();
        SqliteConnection.ClearAllPools();

        var plan =
            Explain(
                path,
                FilterBuilder<ItemModel>.Create()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 33).Or()
                    .Filter(FilterType.Equals, x => x.DepartmentId, 47));

        plan.ShouldContain("ix_or_department");
        plan.ShouldNotContain("SCAN JsonValue", Case.Sensitive);
    }

    [TestMethod]
    public void GeneratedSql_BindsTheCallerFilterAsASingleConjunct()
    {
        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);

        FilterBuilder<ItemModel>.Create()
            .Filter(FilterType.Equals, x => x.DepartmentId, 33).Or()
            .Filter(FilterType.Equals, x => x.DepartmentId, 47)
            .Build(sb, Serializer, new FilterParameters());

        var sql = string.Join(' ', sb.ToString().Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        System.Console.WriteLine(sql);
        sql.ShouldContain("Partition = $partition AND (");
        sql.ShouldEndWith(")");
    }

    private static string Explain(string dbFile, FilterBuilder<ItemModel> filter)
    {
        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        filter.Build(sb, Serializer, parameters);

        using var conn = new SqliteConnection($"Data Source={dbFile}");
        conn.Open();
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // SQL is produced by the library's own builders.
        command.CommandText = "EXPLAIN QUERY PLAN " + sb.ToString();
#pragma warning restore CA2100

        command.Parameters.AddWithValue("$fullTypeName", typeof(ItemModel).FullName);
        command.Parameters.AddWithValue("$partition", PartitionA);
        for (var i = 0; i < parameters.Count; i++)
        {
            command.Parameters.AddWithValue(
                FilterParameters.ParameterPrefix + i.ToString(System.Globalization.CultureInfo.InvariantCulture),
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

    private static ItemModel Item(int id, int departmentId) =>
        new() { Id = id, DepartmentId = departmentId, Description = "ITEM" };

    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    private static Tycho Connect() => Connect(out _);

    private static Tycho Connect(out string path)
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";
        path = Path.Combine(dir, name);

        var db = new Tycho(dir, Serializer, dbName: name, rebuildCache: true, requireTypeRegistration: false);
        return db.Connect();
    }

    public class ItemModel
    {
        public int Id { get; set; }

        public int DepartmentId { get; set; }

        public string? Description { get; set; }
    }

    public class VendorModel
    {
        public int Id { get; set; }

        public string? Description { get; set; }
    }
}
