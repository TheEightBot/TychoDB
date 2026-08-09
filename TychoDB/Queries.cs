using System;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace TychoDB;

internal static class Queries
{
    public const string KeyColumn = "Key";
    public const string DataColumn = "Data";

    // PRAGMAs shared by every profile. These are single-user / single-connection
    // choices: WAL journaling with EXCLUSIVE locking (one persistent connection),
    // NORMAL synchronous (safe under WAL), in-memory temp store, and incremental
    // auto-vacuum.
    //
    // auto_vacuum MUST be the first statement: it can only be applied when the
    // database is first created, and switching to WAL writes the database header —
    // so if auto_vacuum is set after journal_mode = WAL it silently stays at the
    // default (NONE) even on a brand-new file, leaving incremental_vacuum a no-op.
    private const string SharedPragmas =
        """
        PRAGMA auto_vacuum = INCREMENTAL;
        PRAGMA journal_mode = WAL;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA busy_timeout = 5000;
        """;

    // Table + index DDL. Idempotent (IF NOT EXISTS); runs on every connect.
    private const string SchemaDdl =
        """
        CREATE TABLE IF NOT EXISTS JsonValue
        (
            Key             TEXT NOT NULL,
            FullTypeName    TEXT NOT NULL,
            Partition       TEXT NOT NULL,
            Data            JSON NOT NULL,
            PRIMARY KEY (Key, FullTypeName, Partition)
        );

        -- The only secondary index JsonValue needs. Every read constrains
        -- FullTypeName and Partition by equality, and lookups by primary key are
        -- served by the PRIMARY KEY (Key, FullTypeName, Partition) autoindex.
        CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename_partition
        ON JsonValue (FullTypeName, Partition);

        -- Shed indexes that earlier versions created and that duplicate either the
        -- primary-key autoindex or a prefix of the index above. Each one cost a
        -- full b-tree of write maintenance on every insert, update and delete while
        -- serving no query the remaining indexes cannot. Idempotent and cheap.
        DROP INDEX IF EXISTS idx_jsonvalue_fulltypename;
        DROP INDEX IF EXISTS idx_jsonvalue_key_fulltypename;
        DROP INDEX IF EXISTS idx_jsonvalue_key_fulltypename_partition;
        DROP INDEX IF EXISTS idx_streamvalue_key_partition;

        CREATE TABLE IF NOT EXISTS StreamValue
        (
            Key             TEXT NOT NULL,
            Partition       TEXT NOT NULL,
            Data            BLOB NOT NULL,
            PRIMARY KEY (Key, Partition)
        );

        CREATE TABLE IF NOT EXISTS TychoIndex
        (
            IndexName       TEXT NOT NULL,
            FullTypeName    TEXT NOT NULL,
            PhysicalName    TEXT NOT NULL,
            Definition      TEXT NOT NULL,
            ShapeVersion    INTEGER NOT NULL,
            PRIMARY KEY (IndexName, FullTypeName)
        );
        """;

    // Profile defaults. cache_size is in KiB (negative = KiB, not pages);
    // mmap_size is in bytes; wal_autocheckpoint is in pages.
    private const int MobileCacheSizeKb = 8_000;         // ~8 MB page cache
    private const long MobileMmapSizeBytes = 33_554_432; // 32 MB memory-map
    private const int MobileWalAutocheckpoint = 512;     // small WAL

    // Cap the WAL file on mobile so it truncates back to this size after a
    // checkpoint instead of growing unbounded; -1 leaves it unlimited (desktop).
    private const long MobileJournalSizeLimitBytes = 8_388_608; // 8 MB
    private const long DesktopJournalSizeLimitBytes = -1;       // unlimited

    private const int DesktopCacheSizeKb = 65_536;        // 64 MB page cache
    private const long DesktopMmapSizeBytes = 268_435_456; // 256 MB memory-map
    private const int DesktopWalAutocheckpoint = 2_000;

    /// <summary>
    /// Builds the full per-connection setup script (PRAGMAs + schema DDL) for the
    /// given performance profile, honoring optional cache-size / mmap overrides.
    /// </summary>
    public static string BuildConnectionScript(
        TychoPerformanceProfile profile,
        int? cacheSizeKbOverride = null,
        long? mmapSizeBytesOverride = null)
    {
        bool desktop = profile == TychoPerformanceProfile.Desktop;

        int cacheSizeKb = cacheSizeKbOverride ?? (desktop ? DesktopCacheSizeKb : MobileCacheSizeKb);
        long mmapSizeBytes = mmapSizeBytesOverride ?? (desktop ? DesktopMmapSizeBytes : MobileMmapSizeBytes);
        int walAutocheckpoint = desktop ? DesktopWalAutocheckpoint : MobileWalAutocheckpoint;
        long journalSizeLimit = desktop ? DesktopJournalSizeLimitBytes : MobileJournalSizeLimitBytes;

        var sb = new System.Text.StringBuilder(SharedPragmas.Length + SchemaDdl.Length + 160);
        var ic = System.Globalization.CultureInfo.InvariantCulture;

        sb.Append(SharedPragmas).Append('\n')
          .Append("PRAGMA cache_size = -").Append(cacheSizeKb.ToString(ic)).Append(";\n")
          .Append("PRAGMA mmap_size = ").Append(mmapSizeBytes.ToString(ic)).Append(";\n")
          .Append("PRAGMA wal_autocheckpoint = ").Append(walAutocheckpoint.ToString(ic)).Append(";\n")
          .Append("PRAGMA journal_size_limit = ").Append(journalSizeLimit.ToString(ic)).Append(";\n\n")
          .Append(SchemaDdl);

        return sb.ToString();
    }

    public const string PragmaCompileOptions = "PRAGMA compile_options;";

    public const string SqliteVersion = "select sqlite_version();";

    public const string EnableJSON1Pragma = "ENABLE_JSON1";

    public const string InsertOrReplace =
        """
        INSERT OR REPLACE INTO JsonValue(Key, FullTypeName, Data, Partition)
        VALUES ($key, $fullTypeName, json($json), $partition);

        SELECT last_insert_rowid();
        """;

    private const string BatchInsertPrefix =
        "INSERT OR REPLACE INTO JsonValue(Key, FullTypeName, Data, Partition) VALUES ";

    /// <summary>
    /// Builds a multi-row INSERT OR REPLACE for <paramref name="rowCount"/> rows.
    /// FullTypeName and Partition are shared parameters ($fullTypeName, $partition);
    /// each row binds its own $key{n} and $json{n}. No trailing rowid SELECT — the
    /// caller uses the affected-row count. Collapses N executions into one.
    /// </summary>
    public static string BuildBatchInsertOrReplace(int rowCount)
    {
        // Per-row fragment is ~"($key99, $fullTypeName, json($json99), $partition),"
        var sb = new System.Text.StringBuilder(BatchInsertPrefix.Length + (rowCount * 48) + 1);
        sb.Append(BatchInsertPrefix);

        for (int i = 0; i < rowCount; i++)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            sb.Append("($key").Append(i)
              .Append(", $fullTypeName, json($json").Append(i)
              .Append("), $partition)");
        }

        sb.Append(';');
        return sb.ToString();
    }

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

    // Partial-index shape. Leads with Partition (every read constrains
    // Partition = $partition) and confines the index to a single stored type via
    // the WHERE clause, so it carries no entries for rows of other types. The
    // type name moves from per-entry storage into the index definition.
    private const string PartialIndexOnJsonValue = "\nON JsonValue(Partition";
    private const string PartialIndexWherePrefix = "\nWHERE FullTypeName = '";
    private const string PartialIndexWhereSuffix = "';";

    /// <summary>
    /// Builds a CREATE INDEX statement over JSON expressions.
    /// <para>
    /// When <paramref name="fullTypeNameLiteral"/> is supplied the index is
    /// <b>partial</b> — scoped to that stored type and led by Partition. When it is
    /// null (the manual string overload, which only receives a short type name) the
    /// index falls back to a non-partial shape led by FullTypeName and Partition,
    /// which still matches every generated query because both are equality-constrained.
    /// </para>
    /// <para>
    /// The indexed expressions are emitted in exactly the form
    /// <see cref="FilterBuilder{TObj}"/> emits for the same property, because SQLite
    /// matches expression indexes by structural comparison of the expression.
    /// </para>
    /// </summary>
    public static string CreateIndexForJsonValue(
        string fullIndexName,
        (string PropertyPathString, bool IsNumeric)[] propertyPaths,
        string? fullTypeNameLiteral = null)
    {
        bool partial = fullTypeNameLiteral is not null;

        int capacity = CreateIndexPrefix.Length + fullIndexName.Length + MultiIndexClose.Length +
                       (partial
                           ? PartialIndexOnJsonValue.Length + PartialIndexWherePrefix.Length +
                             fullTypeNameLiteral!.Length + PartialIndexWhereSuffix.Length
                           : MultiIndexOnJsonValue.Length + PartitionColumnSegment.Length);

        foreach (var pp in propertyPaths)
        {
            capacity += pp.IsNumeric
                ? MultiIndexCastPrefix.Length + pp.PropertyPathString.Length + MultiIndexCastSuffix.Length
                : MultiIndexJsonExtractPrefix.Length + pp.PropertyPathString.Length + MultiIndexJsonExtractSuffix.Length;
        }

        var sb = new System.Text.StringBuilder(capacity);
        sb.Append(CreateIndexPrefix)
          .Append(fullIndexName);

        if (partial)
        {
            sb.Append(PartialIndexOnJsonValue);
        }
        else
        {
            sb.Append(MultiIndexOnJsonValue).Append(PartitionColumnSegment);
        }

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

        if (partial)
        {
            // Replace the trailing ");" with ")\nWHERE ...;" so the WHERE clause
            // lands outside the column list.
            sb.Length -= MultiIndexClose.Length;
            sb.Append(')')
              .Append(PartialIndexWherePrefix)
              .Append(EscapeSqlLiteral(fullTypeNameLiteral!))
              .Append(PartialIndexWhereSuffix);
        }

        return sb.ToString();
    }

    private const string PartitionColumnSegment = ", Partition";

    /// <summary>
    /// Escapes a value for use inside a single-quoted SQL literal. CLR type names
    /// cannot contain quotes, but the index WHERE clause cannot be parameterized,
    /// so the value is escaped defensively rather than trusted.
    /// </summary>
    public static string EscapeSqlLiteral(string value)
        => value.Replace("'", "''", StringComparison.Ordinal);

    public const string DropIndexPrefix = "DROP INDEX IF EXISTS ";

    public static string DropIndex(string fullIndexName)
        => string.Concat(DropIndexPrefix, fullIndexName, ";");

    // Bounded ANALYZE: refreshes sqlite_stat1 so a newly created index is usable by
    // the very next query. analysis_limit caps the work so this stays cheap on mobile.
    public const string AnalyzeBounded = "PRAGMA analysis_limit = 400; ANALYZE;";

    public const string SelectIndexMetadata =
        """
        SELECT PhysicalName, Definition, ShapeVersion
        FROM TychoIndex
        WHERE IndexName = $indexName AND FullTypeName = $fullTypeName;
        """;

    public const string UpsertIndexMetadata =
        """
        INSERT OR REPLACE INTO TychoIndex(IndexName, FullTypeName, PhysicalName, Definition, ShapeVersion)
        VALUES ($indexName, $fullTypeName, $physicalName, $definition, $shapeVersion);
        """;

    public const string DeleteIndexMetadata =
        """
        DELETE FROM TychoIndex
        WHERE IndexName = $indexName AND FullTypeName = $fullTypeName;
        """;

    public const string SelectAllIndexMetadata =
        """
        SELECT IndexName, FullTypeName, PhysicalName, Definition, ShapeVersion
        FROM TychoIndex
        ORDER BY FullTypeName, IndexName;
        """;

    public const string SelectPhysicalIndexExists =
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = $physicalName;";

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
