#nullable enable

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// How a row's key is decided, and what follows from it: convention-based registration, the
/// strict-mode guard that stops a write from producing a key the by-object overloads cannot
/// reach, and the key-column rewrite those two together make sound.
/// </summary>
[TestClass]
public class KeyRegistrationTests
{
    private const string Partition = "p";

    // ---------- convention-based registration ----------
    [TestMethod]
    public async Task ConventionRegistration_FindsAnIdProperty()
    {
        using var db = Connect(t => t.AddTypeRegistration<Doc>());

        var doc = new Doc { Id = "id-1", Value = "v" };
        await db.WriteObjectAsync(doc, Partition);

        (await db.ReadObjectAsync(doc, Partition)).Value.ShouldBe("v");
        db.GetIdFor(doc).ShouldBe("id-1");
    }

    [TestMethod]
    public async Task ConventionRegistration_FindsTypeNameIdProperty()
    {
        using var db = Connect(t => t.AddTypeRegistration<Widget>());

        var widget = new Widget { WidgetId = 7, Value = "w" };
        await db.WriteObjectAsync(widget, Partition);

        (await db.ReadObjectAsync(widget, Partition)).Value.ShouldBe("w");
    }

    [TestMethod]
    public async Task ConventionRegistration_PrefersIdOverTypeNameId()
    {
        using var db = Connect(t => t.AddTypeRegistration<Both>());

        db.GetIdFor(new Both { Id = "plain", BothId = "prefixed" }).ShouldBe("plain");
    }

    [TestMethod]
    public async Task ConventionRegistration_WithNoIdProperty_StillAllowsExplicitKeys()
    {
        // The pre-existing behaviour has to survive: registering a key-less type is how a caller
        // satisfies requireTypeRegistration while supplying keys at the call site.
        using var db = Connect(t => t.AddTypeRegistration<Keyless>());

        await db.WriteObjectsAsync(new[] { new Keyless { Value = "v" } }, x => "explicit", Partition);

        (await db.ReadObjectAsync<Keyless>("explicit", Partition)).Value.ShouldBe("v");
        Should.Throw<TychoException>(() => db.GetIdFor(new Keyless())).Message.ShouldContain("id mapping");
    }

    [TestMethod]
    public void ConventionRegistration_IgnoresAPropertyWithoutAPublicGetter()
    {
        // A public setter is enough for the property to show up in a Public lookup, and the
        // expression selector would even compile against a private getter — but neither
        // serializer writes such a property, so its JSON path would match nothing.
        using var db = Connect(t => t.AddTypeRegistration<PrivateGetterId>());

        Should.Throw<TychoException>(() => db.GetIdFor(new PrivateGetterId())).Message.ShouldContain("id mapping");
    }

    [TestMethod]
    public async Task ConventionRegistration_IgnoresAnIndexer()
    {
        // An indexer cannot become a property path; the type must fall back to explicit keys.
        using var db = Connect(t => t.AddTypeRegistration<Indexed>());

        Should.Throw<TychoException>(() => db.GetIdFor(new Indexed())).Message.ShouldContain("id mapping");
        await Task.CompletedTask;
    }

    // ---------- strict-mode divergence guard ----------
    [TestMethod]
    public async Task StrictMode_RejectsAKeySelectorThatDisagreesWithTheRegistration()
    {
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);

        var ex = await Should.ThrowAsync<TychoException>(
            async () => await db.WriteObjectsAsync(
                new[] { new Doc { Id = "id-1", Value = "v" } }, x => "custom-" + x.Id, Partition));

        // The guard fires while the sequence is being enumerated inside the write, so it
        // arrives wrapped in the write path's usual TychoException, as every write failure does.
        var guard = ex.InnerException.ShouldBeOfType<TychoException>();
        guard.Message.ShouldContain("custom-id-1");
        guard.Message.ShouldContain("registered id property");

        // Nothing was written under either key.
        (await db.ReadObjectsAsync<Doc>(Partition)).ShouldBeEmpty();
    }

    [TestMethod]
    public async Task StrictMode_AllowsAKeySelectorThatAgrees()
    {
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);

        await db.WriteObjectsAsync(new[] { new Doc { Id = "id-1", Value = "v" } }, x => x.Id, Partition);

        (await db.ReadObjectAsync(new Doc { Id = "id-1" }, Partition)).Value.ShouldBe("v");
    }

    [TestMethod]
    public async Task StrictMode_WithADelegateRegistration_DoesNotGuard()
    {
        // A delegate registration has no id property to compare against, so the override stays
        // available exactly as before.
        using var db = Connect(t => t.AddTypeRegistrationWithCustomKeySelector<Doc>(x => x.Id), strict: true);

        await db.WriteObjectsAsync(new[] { new Doc { Id = "id-1", Value = "v" } }, x => "anything", Partition);

        (await db.ReadObjectAsync<Doc>("anything", Partition)).Value.ShouldBe("v");
    }

    [TestMethod]
    public async Task OutsideStrictMode_TheOverrideIsStillPermitted()
    {
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: false);

        await db.WriteObjectsAsync(new[] { new Doc { Id = "id-1", Value = "v" } }, x => "custom-" + x.Id, Partition);

        (await db.ReadObjectAsync<Doc>("custom-id-1", Partition)).Value.ShouldBe("v");
    }

    [TestMethod]
    public async Task Guard_EnumeratesTheSequenceOnlyOnce()
    {
        // The guard wraps the selector rather than pre-scanning, because callers pass lazy
        // sequences that must not be enumerated twice.
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);

        var enumerations = 0;

        IEnumerable<Doc> Lazy()
        {
            enumerations++;
            yield return new Doc { Id = "id-1", Value = "v" };
        }

        await db.WriteObjectsAsync(Lazy(), x => x.Id, Partition);

        enumerations.ShouldBe(1);
    }

    // ---------- key-column rewrite ----------
    [TestMethod]
    public async Task KeyPropertyFilter_UsesTheKeyColumn_InStrictMode()
    {
        var (db, path) = ConnectAt(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);
        using var scoped = db;
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                Partition, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Id, "id-2"));

        results.Select(x => x.Value).ShouldBe(new[] { "v2" });
        (await PlanFor(db, path)).ShouldNotContain("SCAN JsonValue", Case.Sensitive);
    }

    [TestMethod]
    public async Task KeyPropertyFilter_WithIn_UsesTheKeyColumn()
    {
        var (db, path) = ConnectAt(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);
        using var scoped = db;
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                Partition,
                FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.Id, new[] { "id-1", "id-3" }));

        results.Select(x => x.Value).OrderBy(x => x, StringComparer.Ordinal).ShouldBe(new[] { "v1", "v3" });
    }

    [TestMethod]
    public async Task KeyPropertyFilter_OutsideStrictMode_IsNotRewritten()
    {
        // Without the write guard the invariant is unenforced, so the ordinary predicate stands.
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: false);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                Partition, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Id, "id-2"));

        results.Select(x => x.Value).ShouldBe(new[] { "v2" });
    }

    [TestMethod]
    public async Task KeyPropertyFilter_WithLegacyDivergentRows_FallsBackAndStaysCorrect()
    {
        // Rows written before the guard existed can violate the invariant. The probe must catch
        // that and fall back to the JSON predicate rather than answer from the Key column.
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";

        using (var loose = new Tycho(dir, new NewtonsoftJsonSerializer(), dbName: name, rebuildCache: true, requireTypeRegistration: false)
                   .AddTypeRegistration<Doc, string>(x => x.Id)
                   .Connect())
        {
            await loose.WriteObjectsAsync(new[] { new Doc { Id = "id-1", Value = "v1" } }, x => x.Id, Partition);

            // Divergent: stored under a key its id property would never produce.
            await loose.WriteObjectsAsync(new[] { new Doc { Id = "id-2", Value = "v2" } }, x => "other", Partition);
        }

        SqliteConnection.ClearAllPools();

        using var strict = new Tycho(dir, new NewtonsoftJsonSerializer(), dbName: name, rebuildCache: false, requireTypeRegistration: true)
            .AddTypeRegistration<Doc, string>(x => x.Id)
            .Connect();

        var results =
            await strict.ReadObjectsAsync<Doc>(
                Partition, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Id, "id-2"));

        // Found by content, which the Key column could not have done.
        results.Select(x => x.Value).ShouldBe(new[] { "v2" });
    }

    [TestMethod]
    public async Task KeyPropertyFilter_CountAndDeleteAgreeWithTheRead()
    {
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);
        await SeedAsync(db);

        var filter = FilterBuilder<Doc>.Create().Filter(FilterType.In, x => x.Id, new[] { "id-1", "id-3" });

        (await db.CountObjectsAsync(Partition, filter)).ShouldBe(2);
        (await db.DeleteObjectsAsync(Partition, filter)).ShouldBe(2);
        (await db.ReadObjectsAsync<Doc>(Partition)).Select(x => x.Value).ShouldBe(new[] { "v2" });
    }

    [TestMethod]
    public async Task KeyPropertyFilter_UnderACamelCasePolicy_StillMatches()
    {
        // The registered id path and the filter path must both be resolved through the
        // serializer, or they would not compare equal and the rewrite would silently not apply.
        var serializer = new SystemTextJsonSerializer(
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true, serializer: serializer);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                Partition, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Id, "id-2"));

        results.Select(x => x.Value).ShouldBe(new[] { "v2" });
    }

    [TestMethod]
    public async Task NonKeyPropertyFilter_IsNeverRewritten()
    {
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                Partition, FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Value, "v2"));

        results.Select(x => x.Id).ShouldBe(new[] { "id-2" });
    }

    [TestMethod]
    public async Task NegatedKeyPropertyFilter_StaysOnTheJsonPath()
    {
        using var db = Connect(t => t.AddTypeRegistration<Doc, string>(x => x.Id), strict: true);
        await SeedAsync(db);

        var results =
            await db.ReadObjectsAsync<Doc>(
                Partition, FilterBuilder<Doc>.Create().Filter(FilterType.NotEquals, x => x.Id, "id-2"));

        results.Select(x => x.Id).OrderBy(x => x, StringComparer.Ordinal).ShouldBe(new[] { "id-1", "id-3" });
    }

    private static async Task<string> PlanFor(Tycho db, string path)
    {
        await Task.CompletedTask;
        db.Dispose();
        SqliteConnection.ClearAllPools();

        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        var filter = FilterBuilder<Doc>.Create().Filter(FilterType.Equals, x => x.Id, "id-2");
        filter.Build(sb, new NewtonsoftJsonSerializer(), parameters, new KeyColumnRewrite("$.Id"));

        using var conn = new SqliteConnection($"Data Source={path}");
        conn.Open();
        using var command = conn.CreateCommand();
#pragma warning disable CA2100
        command.CommandText = "EXPLAIN QUERY PLAN " + sb;
#pragma warning restore CA2100
        command.Parameters.AddWithValue("$fullTypeName", typeof(Doc).FullName);
        command.Parameters.AddWithValue("$partition", Partition);
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

    private static async Task SeedAsync(Tycho db)
    {
        await db.WriteObjectsAsync(
            new[]
            {
                new Doc { Id = "id-1", Value = "v1" },
                new Doc { Id = "id-2", Value = "v2" },
                new Doc { Id = "id-3", Value = "v3" },
            },
            x => x.Id,
            Partition);
    }

    private static Tycho Connect(Func<Tycho, Tycho> register, bool strict = false, IJsonSerializer? serializer = null)
        => ConnectAt(register, strict, serializer).Db;

    private static (Tycho Db, string Path) ConnectAt(Func<Tycho, Tycho> register, bool strict = false, IJsonSerializer? serializer = null)
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";

        var db = new Tycho(
            dir, serializer ?? new NewtonsoftJsonSerializer(), dbName: name, rebuildCache: true, requireTypeRegistration: strict);

        return (register(db).Connect(), Path.Combine(dir, name));
    }

    public class Doc
    {
        public string Id { get; set; } = string.Empty;

        public string Value { get; set; } = string.Empty;
    }

    public class Widget
    {
        public int WidgetId { get; set; }

        public string Value { get; set; } = string.Empty;
    }

    public class Both
    {
        public string Id { get; set; } = string.Empty;

        public string BothId { get; set; } = string.Empty;
    }

    public class Keyless
    {
        public string Value { get; set; } = string.Empty;
    }

    public class PrivateGetterId
    {
        public string Id { private get; set; } = "unreachable";

        public string Value { get; set; } = string.Empty;
    }

    public class Indexed
    {
        public string this[int i] => i.ToString(CultureInfo.InvariantCulture);

        public string Value { get; set; } = string.Empty;
    }
}
