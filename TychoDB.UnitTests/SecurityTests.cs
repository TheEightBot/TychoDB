using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;

namespace TychoDB.UnitTests;

/// <summary>
/// Regression tests for the SQL-injection hardening (Bucket 3 / findings S1–S4).
/// These lock in that untrusted filter values, paths, and index names cannot be
/// used to break out of the parameterized SQL.
/// </summary>
[TestClass]
public class SecurityTests
{
    private static IEnumerable<object[]> Serializers
    {
        get
        {
            yield return new object[]
            {
                new SystemTextJsonSerializer(
                    jsonTypeSerializers: new Dictionary<Type, JsonTypeInfo>
                    {
                        [typeof(TestClassA)] = TestJsonContext.Default.TestClassA,
                    }),
            };
        }
    }

    // S1 — boolean-based injection through an Equals filter value must not leak rows.
    [DataTestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task FilterValue_BooleanInjection_DoesNotLeakRows(IJsonSerializer serializer)
    {
        using var db = TychoDbTests.BuildDatabaseConnection(serializer).Connect();

        await db.WriteObjectAsync(new TestClassA { StringProperty = "alice", IntProperty = 1 }, x => x.StringProperty);
        await db.WriteObjectAsync(new TestClassA { StringProperty = "bob", IntProperty = 2 }, x => x.StringProperty);

        var malicious = "nomatch' OR '1'='1";
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Equals, x => x.StringProperty, malicious);

        var results = (await db.ReadObjectsAsync<TestClassA>(filter: filter)).ToList();

        results.Count.ShouldBe(0);
    }

    // S1 — stacked statements in a READ filter value must not execute (no data loss).
    [DataTestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task FilterValue_StackedStatement_DoesNotDeleteData(IJsonSerializer serializer)
    {
        using var db = TychoDbTests.BuildDatabaseConnection(serializer).Connect();

        await db.WriteObjectAsync(new TestClassA { StringProperty = "keep-me", IntProperty = 1 }, x => x.StringProperty);
        await db.WriteObjectAsync(new TestClassA { StringProperty = "keep-me-too", IntProperty = 2 }, x => x.StringProperty);

        var malicious = "x'; DELETE FROM JsonValue; --";
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Equals, x => x.StringProperty, malicious);

        var matches = (await db.ReadObjectsAsync<TestClassA>(filter: filter)).ToList();
        matches.Count.ShouldBe(0);

        // Data must be intact.
        (await db.ReadObjectsAsync<TestClassA>()).Count().ShouldBe(2);
    }

    // S2 — a string payload passed where a numeric comparison is used must be
    // parameterized (treated as data), never emitted as raw SQL tokens.
    [DataTestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task NumericComparison_WithStringPayload_IsSafe(IJsonSerializer serializer)
    {
        using var db = TychoDbTests.BuildDatabaseConnection(serializer).Connect();

        await db.WriteObjectAsync(new TestClassA { StringProperty = "a", IntProperty = 10 }, x => x.StringProperty);

        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.GreaterThan, "$.IntProperty", isPropertyPathNumeric: true, isPropertyPathBool: false, isPropertyPathDateTime: false, value: "5) OR 1=1 --");

        // Should not throw and should not leak via injection.
        var results = (await db.ReadObjectsAsync<TestClassA>(filter: filter)).ToList();
        results.Count.ShouldBe(0);

        (await db.ReadObjectsAsync<TestClassA>()).Count().ShouldBe(1);
    }

    // S1 — a legitimate value that contains a single quote must round-trip correctly.
    [DataTestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task FilterValue_WithApostrophe_MatchesLiterally(IJsonSerializer serializer)
    {
        using var db = TychoDbTests.BuildDatabaseConnection(serializer).Connect();

        await db.WriteObjectAsync(new TestClassA { StringProperty = "O'Brien", IntProperty = 1 }, x => x.StringProperty);
        await db.WriteObjectAsync(new TestClassA { StringProperty = "Smith", IntProperty = 2 }, x => x.StringProperty);

        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Equals, x => x.StringProperty, "O'Brien");

        var results = (await db.ReadObjectsAsync<TestClassA>(filter: filter)).ToList();
        results.Count.ShouldBe(1);
        results[0].StringProperty.ShouldBe("O'Brien");
    }

    // S4 — LIKE metacharacters in a Contains value are matched literally.
    [DataTestMethod]
    [DynamicData(nameof(Serializers))]
    public async Task Contains_WithWildcardChar_MatchesLiterally(IJsonSerializer serializer)
    {
        using var db = TychoDbTests.BuildDatabaseConnection(serializer).Connect();

        await db.WriteObjectAsync(new TestClassA { StringProperty = "100% cotton", IntProperty = 1 }, x => x.StringProperty);
        await db.WriteObjectAsync(new TestClassA { StringProperty = "polyester", IntProperty = 2 }, x => x.StringProperty);

        // "%" must be treated as a literal percent, not a wildcard.
        var filter = FilterBuilder<TestClassA>.Create()
            .Filter(FilterType.Contains, x => x.StringProperty, "100%");

        var results = (await db.ReadObjectsAsync<TestClassA>(filter: filter)).ToList();
        results.Count.ShouldBe(1);
        results[0].StringProperty.ShouldBe("100% cotton");
    }

    // S3 — a raw string property path with injection characters is rejected.
    [TestMethod]
    public void RawStringPath_WithInjectionChars_IsRejected()
    {
        Should.Throw<ArgumentException>(() =>
            FilterBuilder<TestClassA>.Create()
                .Filter(FilterType.Equals, "$.Name') OR 1=1 --", false, false, false, "x"));
    }

    // S3 — an index name with injection characters is rejected before any DDL runs.
    [DataTestMethod]
    [DynamicData(nameof(Serializers))]
    public void IndexName_WithInjectionChars_IsRejected(IJsonSerializer serializer)
    {
        using var db = TychoDbTests.BuildDatabaseConnection(serializer).Connect();

        Should.Throw<ArgumentException>(() =>
            db.CreateIndex<TestClassA>(x => x.StringProperty, "bad); DROP TABLE JsonValue; --"));
    }
}
