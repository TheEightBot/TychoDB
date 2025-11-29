using System;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace TychoDB;

internal static class Queries
{
    public const string KeyColumn = "Key";
    public const string DataColumn = "Data";

    // Performance pragmas optimized for single-user, multithreaded configuration
    // Mobile-friendly settings for iOS/Android apps
    public const string CreateDatabaseSchema =
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA busy_timeout = 5000;
        PRAGMA wal_autocheckpoint = 1000;
        PRAGMA auto_vacuum = INCREMENTAL;

        CREATE TABLE IF NOT EXISTS JsonValue
        (
            Key             TEXT NOT NULL,
            FullTypeName    TEXT NOT NULL,
            Partition       TEXT NOT NULL,
            Data            JSON NOT NULL,
            PRIMARY KEY (Key, FullTypeName, Partition)
        );

        CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename 
        ON JsonValue (FullTypeName);

        CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename_partition 
        ON JsonValue (FullTypeName, Partition);

        CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename 
        ON JsonValue (Key, FullTypeName);

        CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename_partition 
        ON JsonValue (Key, FullTypeName, Partition);

        CREATE TABLE IF NOT EXISTS StreamValue
        (
            Key             TEXT NOT NULL,
            Partition       TEXT NOT NULL,
            Data            BLOB NOT NULL,
            PRIMARY KEY (Key, Partition)
        );

        CREATE INDEX IF NOT EXISTS idx_streamvalue_key_partition 
        ON StreamValue (Key, Partition);
        """;

    public static string BuildPragmaCacheSize(uint cacheSizeBytes) =>
        $"PRAGMA cache_size = -{cacheSizeBytes};";

    public const string PragmaCompileOptions = "PRAGMA compile_options;";

    public const string SqliteVersion = "select sqlite_version();";

    public const string EnableJSON1Pragma = "ENABLE_JSON1";

    public const string InsertOrReplace =
        """
        INSERT OR REPLACE INTO JsonValue(Key, FullTypeName, Data, Partition)
        VALUES ($key, $fullTypeName, json($json), $partition);

        SELECT last_insert_rowid();
        """;

    public const string InsertOrReplaceBlob =
        """
        INSERT OR REPLACE INTO StreamValue(Key, Data, Partition)
        VALUES ($key, zeroblob($blobLength), $partition);

        SELECT last_insert_rowid();
        """;

    public const string SelectDataFromJsonValueWithKeyAndFullTypeName =
        """
        SELECT rowid, Data
        FROM JsonValue
        Where
        Key = $key
        AND
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        LIMIT 1
        """;

    public const string SelectExistsFromJsonValueWithKeyAndFullTypeName =
        """
        SELECT 1
        FROM JsonValue
        Where
        Key = $key
        AND
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        LIMIT 1
        """;

    public const string SelectDataFromStreamValueWithKey =
        """
        SELECT rowid, Data
        FROM StreamValue
        Where
        Key = $key
        AND
        Partition = $partition
        LIMIT 1
        """;

    public const string SelectExistsFromStreamValueWithKey =
        """
        SELECT 1
        FROM StreamValue
        Where
        Key = $key
        AND
        Partition = $partition
        LIMIT 1
        """;

    public const string SelectPartitions =
        """
        SELECT DISTINCT Partition
        From JsonValue
        """;

    public const string SelectDataFromJsonValueWithFullTypeName =
        """
        SELECT rowid, Data
        FROM JsonValue
        Where
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        """;

    public const string SelectCountFromJsonValueWithFullTypeName =
        """
        SELECT 1
        FROM JsonValue
        Where
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        """;

    public const string DeleteDataFromJsonValueWithKeyAndFullTypeName =
        """
        DELETE
        FROM JsonValue
        Where
        Key = $key
        AND
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        """;

    public const string DeleteDataFromJsonValueWithPartition =
        """
        DELETE
        FROM JsonValue
        Where
        Partition = $partition
        """;

    public const string DeleteDataFromJsonValue =
        """
        DELETE
        FROM JsonValue
        """;

    public const string DeleteDataFromStreamValueWithKey =
        """
        DELETE
        FROM StreamValue
        Where
        Key = $key
        AND
        Partition = $partition
        """;

    public const string DeleteDataFromStreamValueWithPartition =
        """
        DELETE
        FROM StreamValue
        Where
        Partition = $partition
        """;

    public const string DeleteDataFromJsonValueWithFullTypeName =
        """
        DELETE
        FROM JsonValue
        Where
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        """;

    // Pre-computed constant parts for dynamic query building
    private const string ExtractDataPrefix = "SELECT rowid, JSON_EXTRACT(Data, '";
    private const string ExtractDataSuffix =
        """
        ') AS Data
        FROM JsonValue
        Where
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        """;

    private const string ExtractDataAndKeyPrefix = "SELECT rowid, Key, JSON_EXTRACT(Data, '";
    private const string ExtractDataAndKeySuffix =
        """
        ') AS Data
        FROM JsonValue
        Where
        FullTypeName = $fullTypeName
        AND
        Partition = $partition
        """;

    private const string CreateIndexPrefix = "CREATE INDEX IF NOT EXISTS ";
    private const string CreateIndexJsonValueOn = "\nON JsonValue(FullTypeName, JSON_EXTRACT(Data, '";
    private const string CreateIndexJsonValueOnNumeric = "\nON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '";
    private const string CreateIndexSuffixNumeric = "') as NUMERIC));";
    private const string CreateIndexSuffix = "'));";

    public static string ExtractDataFromJsonValueWithFullTypeName(string selectionPath)
    {
        return string.Concat(ExtractDataPrefix, selectionPath, ExtractDataSuffix);
    }

    public static string ExtractDataAndKeyFromJsonValueWithFullTypeName(string selectionPath)
    {
        return string.Concat(ExtractDataAndKeyPrefix, selectionPath, ExtractDataAndKeySuffix);
    }

    public static string CreateIndexForJsonValueAsNumeric(string fullIndexName, string propertyPathString)
    {
        return string.Concat(
            CreateIndexPrefix,
            fullIndexName,
            CreateIndexJsonValueOnNumeric,
            propertyPathString,
            CreateIndexSuffixNumeric);
    }

    public static string CreateIndexForJsonValue(string fullIndexName, string propertyPathString)
    {
        return string.Concat(
            CreateIndexPrefix,
            fullIndexName,
            CreateIndexJsonValueOn,
            propertyPathString,
            CreateIndexSuffix);
    }

    private const string MultiIndexJsonExtractPrefix = ", JSON_EXTRACT(Data, '";
    private const string MultiIndexJsonExtractSuffix = "')";
    private const string MultiIndexCastPrefix = ", CAST(JSON_EXTRACT(Data, '";
    private const string MultiIndexCastSuffix = "') as NUMERIC)";
    private const string MultiIndexOnJsonValue = "\nON JsonValue(FullTypeName";
    private const string MultiIndexClose = ");";

    public static string CreateIndexForJsonValue(string fullIndexName, (string PropertyPathString, bool IsNumeric)[] propertyPaths)
    {
        // Pre-calculate capacity: base string + each property path entry
        int capacity = CreateIndexPrefix.Length + fullIndexName.Length + MultiIndexOnJsonValue.Length + MultiIndexClose.Length;
        foreach (var pp in propertyPaths)
        {
            capacity += pp.IsNumeric
                ? MultiIndexCastPrefix.Length + pp.PropertyPathString.Length + MultiIndexCastSuffix.Length
                : MultiIndexJsonExtractPrefix.Length + pp.PropertyPathString.Length + MultiIndexJsonExtractSuffix.Length;
        }

        var sb = new System.Text.StringBuilder(capacity);
        sb.Append(CreateIndexPrefix)
          .Append(fullIndexName)
          .Append(MultiIndexOnJsonValue);

        foreach (var pp in propertyPaths)
        {
            if (pp.IsNumeric)
            {
                sb.Append(MultiIndexCastPrefix)
                  .Append(pp.PropertyPathString)
                  .Append(MultiIndexCastSuffix);
            }
            else
            {
                sb.Append(MultiIndexJsonExtractPrefix)
                  .Append(pp.PropertyPathString)
                  .Append(MultiIndexJsonExtractSuffix);
            }
        }

        sb.Append(MultiIndexClose);
        return sb.ToString();
    }

    // Cache common LIMIT values using FrozenDictionary for O(1) lookup
    private static readonly FrozenDictionary<int, string> CachedLimits = new Dictionary<int, string>
    {
        [0] = "LIMIT 0", [1] = "LIMIT 1", [2] = "LIMIT 2", [3] = "LIMIT 3", [4] = "LIMIT 4",
        [5] = "LIMIT 5", [6] = "LIMIT 6", [7] = "LIMIT 7", [8] = "LIMIT 8", [9] = "LIMIT 9",
        [10] = "LIMIT 10", [20] = "LIMIT 20", [50] = "LIMIT 50", [100] = "LIMIT 100",
        [500] = "LIMIT 500", [1000] = "LIMIT 1000",
    }.ToFrozenDictionary();

    public static string Limit(int count)
    {
        // O(1) lookup in FrozenDictionary
        if (CachedLimits.TryGetValue(count, out var cached))
        {
            return cached;
        }

        return string.Concat("LIMIT ", count.ToString());
    }
}
