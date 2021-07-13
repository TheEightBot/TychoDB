using System;
namespace Tycho
{
    public static class Queries
    {
        public const string CreateDatabaseSchema =
@"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = normal;

CREATE TABLE IF NOT EXISTS JsonValue
(
    Key             TEXT PRIMARY KEY,
    FullTypeName    TEXT NOT NULL,
    Data            JSON NOT NULL,
    Partition       TEXT
);

CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename 
ON JsonValue (FullTypeName);

CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename_partition 
ON JsonValue (FullTypeName, Partition);

CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename 
ON JsonValue (Key, FullTypeName);

CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename_partition 
ON JsonValue (Key, FullTypeName, Partition);";


        public const string PragmaCompileOptions = "PRAGMA compile_options;";

        public const string EnableJSON1Pragma = "ENABLE_JSON1";

        public const string InsertOrReplace =
@"
INSERT OR REPLACE INTO JsonValue(Key, FullTypeName, Data, Partition)
VALUES ($key, $fullTypeName, json($json), $partition);

SELECT last_insert_rowid();";

        public const string SelectDataFromJsonValueWithKeyAndFullTypeName =
@"
SELECT Data
FROM JsonValue
Where
    Key = $key
    AND
    FullTypeName = $fullTypeName";

        public const string SelectDataFromJsonValueWithFullTypeName =
@"
SELECT Data
FROM JsonValue
Where
    FullTypeName = $fullTypeName";

        public const string DeleteDataFromJsonValueWithKeyAndFullTypeName =
@"
DELETE
FROM JsonValue
Where
    Key = $key
    AND
    FullTypeName = $fullTypeName";

        public const string DeleteDataFromJsonValueWithFullTypeName =
@"
DELETE
FROM JsonValue
Where
    FullTypeName = $fullTypeName";

        public const string AndPartitionHasValue =
@"
AND
Partition = $partition
";

        public const string AndPartitionIsNull =
@"
AND
Partition is NULL
";

        public static string ExtractDataFromJsonValueWithFullTypeName(string selectionPath)
        {
            return
@$"
SELECT JSON_EXTRACT(Data, '{selectionPath}') AS Data
FROM JsonValue
Where
FullTypeName = $fullTypeName
";
        }

        public static string CreateIndexForJsonValueAsNumeric(string fullIndexName, string propertyPathString)
        {
            return
@$"
CREATE INDEX IF NOT EXISTS {fullIndexName}
ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '{propertyPathString}') as NUMERIC));";
        }

        public static string CreateIndexForJsonValue(string fullIndexName, string propertyPathString)
        {
            return
@$"
CREATE INDEX IF NOT EXISTS {fullIndexName}
ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '{propertyPathString}'));";
        }

    }
}
