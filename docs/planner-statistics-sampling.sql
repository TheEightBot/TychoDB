-- Sampled ANALYZE (PRAGMA analysis_limit = 400) makes SQLite prefer the general
-- (FullTypeName, Partition) index over a per-type partial expression index; with no
-- statistics at all, every query shape TychoDB emits picks the right index.
--
-- Usage:  rm -f repro.db repro.db-wal repro.db-shm; sqlite3 repro.db < planner-statistics-sampling.sql
-- Needs only the sqlite3 CLI (written against 3.51.0). Every row is generated below; no
-- application database is read. Runs in about 3 s.
--
-- Expected: [1] chooses idx_jsonvalue_fulltypename_partition and takes ~0.2 s per lookup;
-- [2] chooses a partial index for every indexed shape and takes ~0.001 s; [3] matches [2]
-- at the price of the ANALYZE; [4] ends up sampled again.

.bail on
.mode list
.headers off

PRAGMA journal_mode = WAL;
PRAGMA synchronous = OFF;

CREATE TABLE JsonValue
(
    Key             TEXT NOT NULL,
    FullTypeName    TEXT NOT NULL,
    Partition       TEXT NOT NULL,
    Data            JSON NOT NULL,
    PRIMARY KEY (Key, FullTypeName, Partition)
);
CREATE INDEX idx_jsonvalue_fulltypename_partition ON JsonValue (FullTypeName, Partition);

.print
.print '== Seed: 40 tiny types whose names sort first (10-199 rows each), then one wide'
.print '   type with 540,000 rows over 1,000 distinct GroupId values (540 rows per key).'

WITH RECURSIVE
    t(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM t WHERE n < 39),
    r(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM r WHERE i < 198)
INSERT INTO JsonValue (Key, FullTypeName, Partition, Data)
SELECT 'small-' || i, printf('Aaa.Small%02d', n), '', json_object('Id', i, 'Name', 'small ' || i)
FROM t JOIN r ON i < 10 + ((n * 37) % 190);

WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 539999)
INSERT INTO JsonValue (Key, FullTypeName, Partition, Data)
SELECT 'wide-' || i, 'Zzz.Models.WideModel', '',
       json_object('GroupId', printf('G%04d', i % 1000), 'Seq', i, 'Label', 'row ' || i)
FROM s;

SELECT 'first types in index order: ' || group_concat(FullTypeName || '=' || c, ', ')
FROM (SELECT FullTypeName, count(*) AS c FROM JsonValue GROUP BY 1 ORDER BY 1 LIMIT 6);
SELECT 'total rows: ' || count(*) FROM JsonValue;

-- The shapes TychoDB 5.x CreateIndex produces: plain JSON_EXTRACT for a string property,
-- CAST(... as NUMERIC) for a numeric one.
CREATE INDEX idx_GroupId_WideModel_0badcafe
ON JsonValue (Partition, JSON_EXTRACT(Data, '$.GroupId'))
WHERE FullTypeName = 'Zzz.Models.WideModel';

CREATE INDEX idx_Seq_WideModel_0badcafe
ON JsonValue (Partition, CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC))
WHERE FullTypeName = 'Zzz.Models.WideModel';

-- ---------------------------------------------------------------------------
.print
.print '== [1] Sampled statistics, as 5.3.1 writes them: PRAGMA analysis_limit = 400; ANALYZE;'
.timer on
PRAGMA analysis_limit = 400;
ANALYZE;
.timer off
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500';
.print '-- lookups (rows found | bytes read), then time:'
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0007');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'missing');
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [2] No statistics, as TychoDB connects: DROP TABLE sqlite_stat1; ANALYZE sqlite_master;'
DROP TABLE IF EXISTS sqlite_stat1;
DROP TABLE IF EXISTS sqlite_stat4;
ANALYZE sqlite_master;
SELECT 'stat1 rows: ' || count(*) FROM sqlite_stat1;
.print '-- equality on an indexed property:'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500';
.print '-- range on an indexed numeric property:'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC) > 539000;
.print '-- sort on an indexed numeric property with a limit (no temp b-tree):'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' ORDER BY CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC) ASC LIMIT 20;
.print '-- two indexed equalities (one partial index, chosen by rule):'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500' AND CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC) = 500;
.print '-- equality on an unindexed property (general index, then filter):'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.Label') = 'row 7';
.print '-- whole type, and count:'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '';
EXPLAIN QUERY PLAN SELECT COUNT(*) FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '';
.print '-- lookups:'
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0007');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'missing');
SELECT count(*) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC) > 539000);
SELECT count(*) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' ORDER BY CAST(JSON_EXTRACT(Data, '$.Seq') as NUMERIC) ASC LIMIT 20);
SELECT COUNT(*) FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '';
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [3] Full statistics, for comparison: PRAGMA analysis_limit = 0; ANALYZE;'
.timer on
PRAGMA analysis_limit = 0;
ANALYZE;
.timer off
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500';
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [4] The maintenance SQLite recommends, PRAGMA optimize, from no statistics:'
.print '       its default mask caps the ANALYZE it runs at 2,000 rows per index, so the'
.print '       statistics are sampled again, even after analysis_limit = 0.'
DROP TABLE IF EXISTS sqlite_stat1;
ANALYZE sqlite_master;
PRAGMA analysis_limit = 0;
PRAGMA optimize;
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500';
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.timer off
