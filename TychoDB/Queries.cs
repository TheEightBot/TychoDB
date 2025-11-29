using System;
using System.Linq;

namespace TychoDB;

internal static class Queries
{
    public const string KeyColumn = "Key";
    public const string DataColumn = "Data";

    public const string CreateDatabaseSchema =
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA locking_mode = EXCLUSIVE;
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
        ON JsonValue (Key, Partition);
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

    public static string ExtractDataFromJsonValueWithFullTypeName(string selectionPath)
    {
        return
            $"""
            SELECT rowid, JSON_EXTRACT(Data, '{selectionPath}') AS Data
            FROM JsonValue
            Where
            FullTypeName = $fullTypeName
            AND
            Partition = $partition
            """;
    }

    public static string ExtractDataAndKeyFromJsonValueWithFullTypeName(string selectionPath)
    {
        return
            $"""
            SELECT rowid, Key, JSON_EXTRACT(Data, '{selectionPath}') AS Data
            FROM JsonValue
            Where
            FullTypeName = $fullTypeName
            AND
            Partition = $partition
            """;
    }

    public static string CreateIndexForJsonValueAsNumeric(string fullIndexName, string propertyPathString)
    {
        return
            $"""
            CREATE INDEX IF NOT EXISTS {fullIndexName}
            ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '{propertyPathString}') as NUMERIC));
            """;
    }

    public static string CreateIndexForJsonValue(string fullIndexName, string propertyPathString)
    {
        return
            $"""
            CREATE INDEX IF NOT EXISTS {fullIndexName}
            ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '{propertyPathString}'));
            """;
    }

    public static string CreateIndexForJsonValue(string fullIndexName, (string PropertyPathString, bool IsNumeric)[] propertyPaths)
    {
        var propertyPathStringsJoined =
            string.Join(
                string.Empty,
                propertyPaths
                    .Select(
                        pp =>
                            pp.IsNumeric
                            ? $", CAST(JSON_EXTRACT(Data, '{pp.PropertyPathString}') as NUMERIC)"
                            : $", JSON_EXTRACT(Data, '{pp.PropertyPathString}')"));

        return
            $"""
            CREATE INDEX IF NOT EXISTS {fullIndexName}
            ON JsonValue(FullTypeName{propertyPathStringsJoined});
            """;
    }

    public static string Limit(int count)
    {
        return $"LIMIT {count}";
    }
}
