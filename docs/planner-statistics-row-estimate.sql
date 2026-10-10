-- PRAGMA optimize decides whether a table needs re-analysis from an estimate of its row
-- count: the product of the cell counts down the leftmost path of the table's b-tree.
-- A few large documents at the lowest rowids leave the leftmost leaf nearly empty, the
-- estimate lands an order of magnitude below the truth, and the pragma re-runs a full
-- ANALYZE on every call although every index has a full, current statistics row.
--
-- Usage:  rm -f estimate.db estimate.db-wal estimate.db-shm; sqlite3 estimate.db < planner-statistics-row-estimate.sql
-- Needs the sqlite3 CLI, 3.46 or later (older versions check growth only). Runs in about 3 s.
--
-- Expected: [1] prints ANALYZE "main"."JsonValue" right after the full ANALYZE and the
-- three timed optimize(0x10002) calls each take as long as the ANALYZE did; [2] shows the
-- exact count the library uses instead, and the index it reads.

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
.print '== Seed: three ~3 KB documents first (the lowest rowids, one per leaf page), then'
.print '   540,000 small rows of one type over 1,000 keys, and the partial index.'

WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 2)
INSERT INTO JsonValue (Key, FullTypeName, Partition, Data)
SELECT 'large-' || i, 'Aaa.LargeModel', '', json_object('Payload', replace(hex(zeroblob(1500)), '0', 'x'))
FROM s;

WITH RECURSIVE s(i) AS (SELECT 0 UNION ALL SELECT i + 1 FROM s WHERE i < 539999)
INSERT INTO JsonValue (Key, FullTypeName, Partition, Data)
SELECT 'wide-' || i, 'Zzz.Models.WideModel', '', json_object('GroupId', printf('G%04d', i % 1000), 'Seq', i)
FROM s;

CREATE INDEX idx_GroupId_WideModel_0badcafe
ON JsonValue (Partition, JSON_EXTRACT(Data, '$.GroupId'))
WHERE FullTypeName = 'Zzz.Models.WideModel';

-- ---------------------------------------------------------------------------
.print
.print '== [1] Full statistics, then what PRAGMA optimize thinks of them'
PRAGMA analysis_limit = 0;
.timer on
ANALYZE;
.timer off
SELECT 'stat1: ' || idx || ' -> ' || stat FROM sqlite_stat1 WHERE tbl = 'JsonValue' ORDER BY idx;
SELECT 'real rows: ' || count(*) FROM JsonValue;
.print '-- optimize(0x10003) (debug) right after the full ANALYZE; nothing should be listed:'
PRAGMA optimize(0x10003);
.print '-- optimize(0x10002), three calls in a row:'
.timer on
PRAGMA optimize(0x10002);
PRAGMA optimize(0x10002);
PRAGMA optimize(0x10002);
.timer off

-- ---------------------------------------------------------------------------
.print
.print '== [2] The exact count the library compares with the recorded row count instead'
EXPLAIN QUERY PLAN SELECT count(*) FROM JsonValue;
.timer on
SELECT count(*) FROM JsonValue;
SELECT count(*) FROM JsonValue;
.timer off
SELECT 'recorded by ANALYZE: ' || CAST(substr(stat, 1, instr(stat || ' ', ' ') - 1) AS INTEGER)
FROM sqlite_stat1 WHERE tbl = 'JsonValue' AND idx = 'idx_jsonvalue_fulltypename_partition';
