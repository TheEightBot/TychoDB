using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Shouldly;
using TychoDB;

namespace TychoDB.UnitTests;

/// <summary>
/// A store as an earlier release leaves it, and the means of opening and inspecting one, for
/// the tests of the upgrade path.
/// </summary>
internal static class StoreFixture
{
#if ENCRYPTED
    public const string DbPassword = "Password";
#endif

    public const string IndexName = "seq_idx";
    public const string LegacyIndexName = "idx_seq_idx_Item";
    public const string ManualIndexName = "manual_idx";
    public const string ManualPhysicalName = "idx_manual_idx_Item";
    public const string GeneralIndexName = "idx_jsonvalue_fulltypename_partition";

    public const long LookupSeq = 42;

    public static readonly string[] BuiltInLegacyIndexNames =
    {
        "idx_jsonvalue_fulltypename",
        "idx_jsonvalue_key_fulltypename",
        "idx_jsonvalue_key_fulltypename_partition",
        "idx_streamvalue_key_partition",
    };

    public static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    public class Item
    {
        public string Key { get; set; }

        public string Group { get; set; }

        public long Seq { get; set; }
    }

    public readonly record struct MetadataRow(
        string IndexName,
        string FullTypeName,
        string PhysicalName,
        string Definition,
        long ShapeVersion);

    /// <summary>
    /// A store as 4.4.2 leaves it: its per-type index for Seq under the legacy name and
    /// shape, its four built-in indexes, no index metadata, and no incremental auto-vacuum.
    /// </summary>
    public static async Task<string> SeedLegacyStore(int rows = 100)
    {
        var (path, dbName) = NewDbPath();
        var dbFile = Path.Combine(path, dbName);

        using (var db = await BuildDb(path, dbName).ConnectAsync())
        {
            await db.WriteObjectsAsync(Items(rows), x => x.Key);
        }

        var legacySchema =
            LegacyIndexDdl(LegacyIndexName) +
            "CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename ON JsonValue (FullTypeName);" +
            "CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename ON JsonValue (Key, FullTypeName);" +
            "CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename_partition ON JsonValue (Key, FullTypeName, Partition);" +
            "CREATE INDEX IF NOT EXISTS idx_streamvalue_key_partition ON StreamValue (Key, Partition);" +
            "DELETE FROM TychoIndex;" +
            "PRAGMA auto_vacuum = NONE; VACUUM;";

        Execute(dbFile, legacySchema);

        ReadPragma(dbFile, "auto_vacuum").ShouldBe(0);
        ReadMetadata(dbFile).ShouldBeEmpty();

        return dbFile;
    }

    /// <summary>The DDL 4.4.2 ran for CreateIndex&lt;Item&gt;(x => x.Seq, …): whole-table, led by FullTypeName.</summary>
    public static string LegacyIndexDdl(string name)
        => $"CREATE INDEX IF NOT EXISTS {name} ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC));";

    public static List<Item> Items(int rows = 100)
    {
        var items = new List<Item>(rows);
        for (int i = 0; i < rows; i++)
        {
            items.Add(new Item
            {
                Key = $"item-{i}",
                Group = "G" + (i % 10).ToString(CultureInfo.InvariantCulture),
                Seq = i,
            });
        }

        return items;
    }

    public static async Task AssertLookup(Tycho db)
    {
        var rows = await db.ReadObjectsAsync<Item>(
            filter: FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Seq, LookupSeq));

        rows.ShouldHaveSingleItem().Seq.ShouldBe(LookupSeq);
    }

    /// <summary>The plan for the SQL the library composes for the lookup <see cref="AssertLookup"/> runs.</summary>
    public static string ExplainLookup(string dbFile)
    {
        var sb = new StringBuilder(Queries.SelectDataFromJsonValueWithFullTypeName);
        var parameters = new FilterParameters();
        FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Seq, LookupSeq).Build(sb, Serializer, parameters);

        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // SQL is produced by the library's own builders.
        command.CommandText = "EXPLAIN QUERY PLAN " + sb;
#pragma warning restore CA2100

        command.Parameters.AddWithValue("$fullTypeName", typeof(Item).FullName);
        command.Parameters.AddWithValue("$partition", string.Empty);
        for (int i = 0; i < parameters.Count; i++)
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

    public static Dictionary<string, string> ReadIndexDdl(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL";

        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            result[reader.GetString(0)] = reader.GetString(1);
        }

        return result;
    }

    public static List<MetadataRow> ReadMetadata(string dbFile)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();
        command.CommandText = "SELECT IndexName, FullTypeName, PhysicalName, Definition, ShapeVersion FROM TychoIndex";

        var rows = new List<MetadataRow>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            rows.Add(new MetadataRow(reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt64(4)));
        }

        return rows;
    }

    public static long ReadPragma(string dbFile, string pragma)
        => ReadScalar(dbFile, "PRAGMA " + pragma);

    public static long ReadScalar(string dbFile, string sql)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // Test-local SQL.
        command.CommandText = sql;
#pragma warning restore CA2100

        return command.ExecuteScalar() is long value ? value : -1;
    }

    public static string ReadText(string dbFile, string sql)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // Test-local SQL.
        command.CommandText = sql;
#pragma warning restore CA2100

        return command.ExecuteScalar() as string ?? string.Empty;
    }

    public static void Execute(string dbFile, string sql)
    {
        using var conn = OpenInspection(dbFile);
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // Test-local SQL.
        command.CommandText = sql;
#pragma warning restore CA2100

        command.ExecuteNonQuery();
    }

    public static bool CanOpenForWriting(string dbFile)
    {
        try
        {
            using var stream = new FileStream(dbFile, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
            return true;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }

    /// <summary>The main file's bytes, once every pooled connection has closed and checkpointed.</summary>
    public static string FileHash(string dbFile)
    {
        SqliteConnection.ClearAllPools();

        using var stream = File.OpenRead(dbFile);
        return Convert.ToHexString(SHA256.HashData(stream));
    }

    public static (string Path, string DbName) NewDbPath()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tycho_store_tests");
        Directory.CreateDirectory(dir);
        return (dir, $"{Guid.NewGuid()}.db");
    }

    public static (string Path, string DbName) Split(string dbFile)
        => (Path.GetDirectoryName(dbFile), Path.GetFileName(dbFile));

    public static Tycho BuildDb(string path, string dbName)
    {
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, DbPassword, rebuildCache: true, requireTypeRegistration: false);
#else
        return new Tycho(path, Serializer, dbName, rebuildCache: true, requireTypeRegistration: false);
#endif
    }

    /// <summary>Reopens an existing database without rebuilding it.</summary>
    public static Tycho ReopenDb(string path, string dbName, bool persistConnection = true, bool useConnectionPooling = true, bool autoOptimize = true)
    {
        SqliteConnection.ClearAllPools();
#if ENCRYPTED
        return new Tycho(path, Serializer, dbName, DbPassword, persistConnection: persistConnection, rebuildCache: false, requireTypeRegistration: false, useConnectionPooling: useConnectionPooling, autoOptimize: autoOptimize);
#else
        return new Tycho(path, Serializer, dbName, persistConnection: persistConnection, rebuildCache: false, requireTypeRegistration: false, useConnectionPooling: useConnectionPooling, autoOptimize: autoOptimize);
#endif
    }

    /// <summary>
    /// Opens an inspection connection carrying the same key the database was created with.
    /// Tycho holds locking_mode = EXCLUSIVE and pooling keeps a disposed connection's lock
    /// alive, so the pool must be cleared after the Tycho instance is disposed and before
    /// the file is reopened.
    /// </summary>
    public static SqliteConnection OpenInspection(string dbFile)
    {
        SqliteConnection.ClearAllPools();

        var connectionStringBuilder =
            new SqliteConnectionStringBuilder
            {
                ConnectionString = $"Filename={dbFile}",
                Cache = SqliteCacheMode.Private,
                Mode = SqliteOpenMode.ReadWriteCreate,
            };

#if ENCRYPTED
        connectionStringBuilder.Password = DbPassword;
#endif

        var conn = new SqliteConnection(connectionStringBuilder.ToString());
        conn.Open();
        return conn;
    }
}
