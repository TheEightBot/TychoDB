-- Sampled ANALYZE (PRAGMA analysis_limit = 400) makes SQLite prefer the general
-- (FullTypeName, Partition) index over a per-type partial expression index.
--
-- Usage:  rm -f repro.db repro.db-wal repro.db-shm; sqlite3 repro.db < analyze-sampling-repro.sql
-- Needs only the sqlite3 CLI (PRAGMA optimize(0x10002) needs 3.46+; written against 3.51.0).
-- Every row is generated below; no application database is read. Runs in about 3 s.
--
-- Expected: [1] and [4] choose idx_jsonvalue_fulltypename_partition and take ~0.2 s per
-- lookup; [2], [3] and [5] choose the partial index and take ~0.001 s. [6] shows that
-- optimize(0x10002) leaves already-sampled statistics alone; [7] that a full row for the
-- partial index alone does not help while the general index's row is still sampled.

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
SELECT 'wide rows: ' || count(*) || ', distinct GroupId: ' || count(DISTINCT JSON_EXTRACT(Data, '$.GroupId'))
FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel';

-- The shape TychoDB 5.x CreateIndex produces.
CREATE INDEX idx_GroupId_WideModel_0badcafe
ON JsonValue (Partition, JSON_EXTRACT(Data, '$.GroupId'))
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
EXPLAIN QUERY PLAN SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.print '-- lookups (rows found | bytes read), then time:'
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0007');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0993');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'missing');
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [2] Full statistics: PRAGMA analysis_limit = 0; ANALYZE;'
.timer on
PRAGMA analysis_limit = 0;
ANALYZE;
.timer off
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.print '-- lookups:'
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0007');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0993');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'missing');
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [3] No statistics: DELETE FROM sqlite_stat1; ANALYZE sqlite_master;'
DELETE FROM sqlite_stat1;
ANALYZE sqlite_master;
SELECT 'stat1 rows: ' || count(*) FROM sqlite_stat1;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.print '-- lookups:'
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0007');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0993');
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'missing');
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [4] Plain PRAGMA optimize from no statistics, even with analysis_limit = 0:'
.print '       the default mask (bit 0x10) caps the ANALYZE it runs at 2,000 rows per index.'
PRAGMA analysis_limit = 0;
PRAGMA optimize;
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [5] PRAGMA optimize(0x10002) from no statistics: full ANALYZE of every table.'
DELETE FROM sqlite_stat1;
ANALYZE sqlite_master;
.print '-- what it would run (0x10003 = same mask plus the debug bit):'
PRAGMA optimize(0x10003);
.timer on
PRAGMA optimize(0x10002);
.timer off
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.timer on
SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
.timer off
.print '-- a second optimize(0x10002) with statistics already present does nothing:'
PRAGMA optimize(0x10003);
.timer on
PRAGMA optimize(0x10002);
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [6] A store that already holds the sampled statistics 5.3.1 writes, then optimize(0x10002):'
.print '       nothing is missing and nothing grew, so the sampled rows stay.'
PRAGMA analysis_limit = 400;
ANALYZE;
PRAGMA analysis_limit = 0;
.print '-- what it would run:'
PRAGMA optimize(0x10003);
PRAGMA optimize(0x10002);
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');

-- ---------------------------------------------------------------------------
.print
.print '== [7] Mixed: sampled row for the general index, full row for the partial index only.'
DELETE FROM sqlite_stat1;
ANALYZE sqlite_master;
PRAGMA analysis_limit = 400;
ANALYZE idx_jsonvalue_fulltypename_partition;
PRAGMA analysis_limit = 0;
.timer on
ANALYZE idx_GroupId_WideModel_0badcafe;
.timer off
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
.print '-- plan:'
EXPLAIN QUERY PLAN SELECT count(*), sum(length(Data)) FROM (SELECT rowid, Data FROM JsonValue WHERE FullTypeName = 'Zzz.Models.WideModel' AND Partition = '' AND JSON_EXTRACT(Data, '$.GroupId') = 'G0500');
