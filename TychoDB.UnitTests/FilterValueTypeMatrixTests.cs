using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// A filter compares a CLR value against the JSON the serializer wrote, so every CLR type
/// whose JSON form differs from its <c>ToString()</c> is a candidate for silently matching
/// nothing. This walks the type surface in one pass and reports every mismatch together,
/// rather than stopping at the first, so the blast radius is visible.
/// </summary>
[TestClass]
public class FilterValueTypeMatrixTests
{
    private static readonly Guid FixedGuid = new("11112222-3333-4444-5555-666677778888");
    private static readonly DateTime FixedDateTime = new(2026, 8, 28, 13, 45, 30, DateTimeKind.Utc);
    private static readonly DateTimeOffset FixedDateTimeOffset = new(2026, 8, 28, 13, 45, 30, TimeSpan.Zero);

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
    public async Task EveryScalarType_RoundTripsThroughAnEqualsFilter(IJsonSerializer jsonSerializer, string label)
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";
        using var db = new Tycho(dir, jsonSerializer, dbName: name, rebuildCache: true, requireTypeRegistration: false);
        await db.ConnectAsync();

        var doc = Sample();
        await db.WriteObjectsAsync(new[] { doc }, x => x.Id.ToString());

        var cases = new List<(string Name, FilterBuilder<Doc> Filter)>
        {
            ("string", Eq(x => x.StringValue, doc.StringValue)),
            ("int", Eq(x => x.IntValue, doc.IntValue)),
            ("long", Eq(x => x.LongValue, doc.LongValue)),
            ("short", Eq(x => x.ShortValue, doc.ShortValue)),
            ("byte", Eq(x => x.ByteValue, doc.ByteValue)),
            ("uint", Eq(x => x.UIntValue, doc.UIntValue)),
            ("ulong", Eq(x => x.ULongValue, doc.ULongValue)),
            ("double", Eq(x => x.DoubleValue, doc.DoubleValue)),
            ("float", Eq(x => x.FloatValue, doc.FloatValue)),
            ("decimal", Eq(x => x.DecimalValue, doc.DecimalValue)),
            ("bool", Eq(x => x.BoolValue, doc.BoolValue)),
            ("Guid", Eq(x => x.GuidValue, doc.GuidValue)),
            ("DateTime", Eq(x => x.DateTimeValue, doc.DateTimeValue)),
            ("DateTimeOffset", Eq(x => x.DateTimeOffsetValue, doc.DateTimeOffsetValue)),
            ("TimeSpan", Eq(x => x.TimeSpanValue, doc.TimeSpanValue)),
            ("DateOnly", Eq(x => x.DateOnlyValue, doc.DateOnlyValue)),
            ("TimeOnly", Eq(x => x.TimeOnlyValue, doc.TimeOnlyValue)),
            ("char", Eq(x => x.CharValue, doc.CharValue)),
            ("enum", Eq(x => x.EnumValue, doc.EnumValue)),
            ("enum(renamed)", Eq(x => x.RenamedEnumValue, doc.RenamedEnumValue)),
            ("Uri", Eq(x => x.UriValue, doc.UriValue)),
            ("nullable-int", Eq(x => x.NullableInt, doc.NullableInt)),
            ("nullable-enum", Eq(x => x.NullableEnum, doc.NullableEnum)),
            ("nullable-Guid", Eq(x => x.NullableGuid, doc.NullableGuid)),
        };

        var failures = new List<string>();
        foreach (var (caseName, filter) in cases)
        {
            int matched;
            try
            {
                matched = (await db.ReadObjectsAsync<Doc>(filter: filter)).Count();
            }
            catch (Exception ex)
            {
                failures.Add($"{caseName}: threw {ex.GetType().Name}");
                continue;
            }

            if (matched != 1)
            {
                failures.Add($"{caseName}: matched {matched}");
            }
        }

        Console.WriteLine($"[{label}] stored: {StoredJson(db, Path.Combine(dir, name))}");
        Console.WriteLine($"[{label}] failures ({failures.Count}/{cases.Count}): {string.Join(" | ", failures)}");

        failures.ShouldBeEmpty($"{label} filter value mismatches");
    }

    private static FilterBuilder<Doc> Eq<TProp>(
        System.Linq.Expressions.Expression<Func<Doc, TProp>> path, object value)
        => FilterBuilder<Doc>.Create().Filter(FilterType.Equals, path, value);

    private static Doc Sample() =>
        new()
        {
            Id = 1,
            StringValue = "hello",
            IntValue = 42,
            LongValue = 9_000_000_000L,
            ShortValue = 7,
            ByteValue = 3,
            UIntValue = 11u,
            ULongValue = 12ul,
            DoubleValue = 1.5d,
            FloatValue = 2.5f,
            DecimalValue = 3.25m,
            BoolValue = true,
            GuidValue = FixedGuid,
            DateTimeValue = FixedDateTime,
            DateTimeOffsetValue = FixedDateTimeOffset,
            TimeSpanValue = TimeSpan.FromMinutes(90),
            DateOnlyValue = new DateOnly(2026, 8, 28),
            TimeOnlyValue = new TimeOnly(13, 45, 30),
            CharValue = 'z',
            EnumValue = Colour.Green,
            RenamedEnumValue = Renamed.Second,
            UriValue = new Uri("https://example.com/a"),
            NullableInt = 5,
            NullableEnum = Colour.Blue,
            NullableGuid = FixedGuid,
        };

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

    public enum Colour
    {
        Red = 0,
        Green = 1,
        Blue = 2,
    }

    public enum Renamed
    {
        First = 0,

        [System.Runtime.Serialization.EnumMember(Value = "second-value")]
        Second = 1,
    }

    public class Doc
    {
        public int Id { get; set; }

        public string StringValue { get; set; }

        public int IntValue { get; set; }

        public long LongValue { get; set; }

        public short ShortValue { get; set; }

        public byte ByteValue { get; set; }

        public uint UIntValue { get; set; }

        public ulong ULongValue { get; set; }

        public double DoubleValue { get; set; }

        public float FloatValue { get; set; }

        public decimal DecimalValue { get; set; }

        public bool BoolValue { get; set; }

        public Guid GuidValue { get; set; }

        public DateTime DateTimeValue { get; set; }

        public DateTimeOffset DateTimeOffsetValue { get; set; }

        public TimeSpan TimeSpanValue { get; set; }

        public DateOnly DateOnlyValue { get; set; }

        public TimeOnly TimeOnlyValue { get; set; }

        public char CharValue { get; set; }

        public Colour EnumValue { get; set; }

        public Renamed RenamedEnumValue { get; set; }

        public Uri UriValue { get; set; }

        public int? NullableInt { get; set; }

        public Colour? NullableEnum { get; set; }

        public Guid? NullableGuid { get; set; }
    }
}
