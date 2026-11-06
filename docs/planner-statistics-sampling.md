# Sampled planner statistics made SQLite skip the partial indexes (5.3.1)

## Symptom

On a 1.68M-row store whose largest type held 586,986 rows, an equality lookup on an indexed
property of that type took 82–366 ms, and a lookup for a missing key 310 ms. The same query
with correct statistics, or with none, takes 0–1 ms. Results were identical in every case;
only speed changed. Stores upgraded from 4.4.2, whose indexes are the legacy non-partial
shape, were not affected.

## Why

All objects share `JsonValue (Key, FullTypeName, Partition, Data)` with the general index
`idx_jsonvalue_fulltypename_partition (FullTypeName, Partition)`. 5.x `CreateIndex` adds a
per-type partial index:

```sql
CREATE INDEX idx_GroupId_WideModel_0badcafe
ON JsonValue (Partition, JSON_EXTRACT(Data, '$.GroupId'))
WHERE FullTypeName = 'Zzz.Models.WideModel';
```

5.3.0 started gathering planner statistics, and 5.3.1 gathered them with
`PRAGMA analysis_limit = 400` (`PRAGMA optimize` on connect, disconnect and dispose, and
`ANALYZE` after `CreateIndex`). With a limit, `ANALYZE` reads only the first ~400 entries of
each index:

- The general index is ordered by type name, and the types that sort first are tiny
  (22, 112, 10, 202 rows… in the real store). Its sample spans a handful of types, so
  `sqlite_stat1` credits every type with ~81 rows. The real type has 586,986.
- The partial index is ordered by `(Partition, GroupId)`, so its sample sits inside one key
  and credits every key with ~401 rows. The truth is ~542 across 1,083 keys.

The planner compares 81 against 401, chooses the general index, and reads and JSON-parses
every row of the type on each lookup.

## Fix

TychoDB gathers no statistics, and removes any it finds. Connect and `Optimize()` look for
`sqlite_stat1`; when it exists and has rows they run `DELETE FROM sqlite_stat1; ANALYZE
sqlite_master;` — the last statement is SQLite's documented way to reload statistics on an
open connection — and otherwise write nothing. The table is never dropped: `ANALYZE
sqlite_master` recreates it, so dropping it only guarantees a schema write on the next
connect. Nothing else runs `ANALYZE` or `PRAGMA optimize`. The one-time index drops and
rebuilds stay, behind `autoOptimize` and `Optimize`.

Without a `sqlite_stat1` row, SQLite's planner credits a partial index with half the table
and every index with a fixed selectivity per equality column, so the per-type partial index
beats the general index for the property it covers whatever the data looks like. Every
query shape TychoDB emits gets the right plan on the 544k-row synthetic store, on both
bundled engines (3.50.4 and SQLCipher's 3.39.2; the tests in
`TychoDB.UnitTests/PlannerStatisticsTests.cs` assert each one):

| Query | Plan with no statistics | Time |
|---|---|---|
| equality on an indexed property | partial index | 0–1 ms |
| range on an indexed numeric property | partial index | 1 ms |
| sort on an indexed numeric property, `LIMIT 20` | partial index, no temp b-tree | 0 ms |
| two indexed equalities | one of the two partial indexes, chosen by rule | 0 ms |
| equality on an unindexed property | general index, then filter | 190 ms (reads the type; the only plan there is) |
| whole type | general index | 20 ms |
| count | general index, covering | 20 ms |

The one thing statistics could add is the choice between two indexed properties filtered
together: without them the planner cannot tell which is more selective and picks one by
rule, so the lookup is bounded by the rows matching that property rather than the smaller
set. On the stores measured so far every indexed property is selective enough for this not
to matter, and a query that filters on two properties is what a composite index is for.

Two more cases rest on the planner's tie-break, which goes to the index declared last.
`Or`, `In` and null filters on an indexed property cost about the same through the
property's partial index as through any other index that leads with `FullTypeName,
Partition`, and reading through the latter reads the whole type:

- A store that mixes the two `CreateIndex` overloads. Declared after a generic index, the
  string overload's index takes those filters, for good. Equality and range filters are not
  affected. MoveScoutPro's stores each use one overload, and the README says to.
- A store part-way through the upgrade, with a partial index built on a type whose other
  indexes are still 4.x ones. Those filters then go through the general index until
  `Optimize` finishes.

## Why not gather them properly instead

The branch first did that: a full `ANALYZE` (`analysis_limit = 0`) after `CreateIndex` and
whenever an index had no statistics or a table had grown tenfold, a `user_version` stamp so
stores written by 5.3.1 were re-analyzed once, an opt-out so the connect-time cost could be
deferred, and finally TychoDB's own staleness check. Each layer existed to make gathering
statistics safe, and testing on real stores kept finding the next hole:

- A full `ANALYZE` took 3.2 s on the 1.68M-row store and 17.5 s on a 2.2 GB store, on the
  connecting thread.
- `PRAGMA optimize`, SQLite's recommended maintenance, cannot be used as is: its default
  mask includes bit `0x10`, a temporary 2,000-row limit regardless of `analysis_limit`, so
  it wrote `2001 501` against `101` and mis-ranked the indexes the same way (repro case 4).
- `PRAGMA optimize(0x10002)` lifts the limit but decides whether a table has grown from the
  cell counts down the leftmost path of its b-tree. A 2.2 GB production store (1.84M rows,
  estimated at 174,370) was re-analyzed on every call, 20–30 s each, because its lowest
  rowids held large documents. Any store can drift into that layout.
- Statistics gathered on a few hundred rows, then 540,000 rows arriving, are fine; a full
  partial-index row next to a stale general-index row is not; a 5.3.1 build writing to a
  repaired store brings the sampled rows back undetected. The states that mislead the
  planner are the inconsistent ones, and every refresh policy has a window in which the
  store is inconsistent.

With no statistics there is nothing to sample, go stale, half-refresh or roll back, and
plan choice stops depending on data layout or on which release last wrote the file.
`docs/indexing-analysis.md` (5.3.0) listed "planner statistics are never available" as a
defect to fix; the 5.3.0 speedups came from the partial-index redesign, not from the
statistics, and that finding is reversed.

## Repro

- `docs/planner-statistics-sampling.sql` — sqlite3 CLI only, synthetic data, ~3 s. Forty
  tiny types that sort first, one type with 540,000 rows over 1,000 keys, the two partial
  index shapes. Case 1 is 5.3.1's sampled statistics (general index, ~0.2 s per lookup);
  case 2 is what TychoDB now does at connect (the table above); case 3 is a full `ANALYZE`
  for comparison; case 4 is plain `PRAGMA optimize`.
- `TychoDB.UnitTests/PlannerStatisticsTests.cs` — through Tycho's own `CreateIndexAsync`,
  `ConnectAsync` and dispose paths on a 20,400-row store (40 small types, one type with
  20,000 rows over 200 keys): the two scenarios that failed on 5.3.1 (index created after
  the data, and data loaded after the index then reconnected), a store carrying 5.3.1's
  sampled rows repaired on connect with `autoOptimize` on and off, and one test per query
  shape in the table above. Each asserts the plan for the library's own SQL and that no
  statistics row exists.
- `TychoDB.UnitTests/OptimizeTests.cs` — the upgrade path from 4.4.2 through Tycho's own
  API: a store with 4.4.2's per-type and built-in indexes and no incremental auto-vacuum,
  declared under `autoOptimize: false` (nothing written, served by the 4.4.2 index) and
  then optimized (rebuilt, drops done, store compacted and converted, nothing written on
  the next launch); the same store under `autoOptimize: true`; a declaration the old index
  cannot serve; the string overload's same-named 4.4.2 index; the orphan a rollback leaves;
  a current store that connect, the declarations and `Optimize` leave byte-identical; and
  the compaction itself — reads and writes succeed throughout and run between its steps,
  `Disconnect` and `Dispose` during it leave a store that opens, a copy that cannot be
  written throws and keeps the store serving, a leftover copy is removed at connect, the
  file's permissions and a symbolic link survive, and it is skipped while another
  connection holds a lock on the file.

Bound parameters are fine in the library: SQLite re-plans when a bound value touches a
partial-index `WHERE`, so `EXPLAIN QUERY PLAN` on an unbound statement shows the generic
plan. The tests bind the parameters; the CLI script uses literals.

## Cost

Connect pays one `sqlite_master` lookup for `sqlite_stat1`, and one `SELECT … LIMIT 1` on
it when the table exists. A store written by 5.3.0 or 5.3.1 pays the delete of its small
statistics table once; a current store is not written to at all (the connect script's
`auto_vacuum` pragma, which rewrote page 1 at every connect on an auto-vacuum store, now
runs only for a new file).

The four `DROP INDEX IF EXISTS` for the indexes 4.x created were part of a 10–30 s stall
MoveScoutPro measured on the first launch of a large upgraded store (main-thread samples in
`sqlite3BtreeDropTable`/`clearDatabasePage`): freeing an index's pages is proportional to
its size. On a 1.76M-row, 2.37 GB production store the four took 9 s through the library,
and the six per-type rebuilds `CreateIndex` does for an upgraded store — drop the
whole-table index 4.x built, build the partial one — 15 s more. Each build scans the table,
and SQLite cannot build several indexes in one pass, so the builds are most of that time.
They stay on by default, because they make bulk writes faster: 25–45% measured for 5.0 with
the four built-ins alone, 2.1–3x on a production store once the six whole-table per-type
indexes go too (table below).

`autoOptimize: false` connects without the drops, and `CreateIndex` writes nothing while the
index an earlier release built can still serve a filter on the declaration's first property
— 90 ms for the connect and six declarations on that store. The whole-document index 4.x
built for a value-type property is left in place too: it never served a query, so waiting
loses nothing, and building at once would stall the caller for seconds. The deferred
declaration is kept in memory, because apps declare their indexes on every launch, and
`Optimize` builds it through the same code `CreateIndex` uses. A declaration whose old index
covers another property is built at once.

`Optimize`/`OptimizeAsync` then do the statistics delete, the drops, the rebuilds and a
compaction that returns the freed pages — 1.3 GB on that store — since a 4.x store has no
incremental auto-vacuum and nothing else can. Each is a step that takes the gate on its own,
so other operations run between steps and wait only for the step in progress, about 4 s at
the longest on that store. The compaction is one step: nothing can write during the copy.

The compaction is `VACUUM INTO` a copy next to the database, then a swap: the copy is
flushed to disk and takes the file's permissions, the connection closes, the copy is renamed
over the file, and the same connection object reopens on it. A full `VACUUM` writes the data
twice — into a temp copy, then back into the file through the WAL, which grew to 1.04 GB on
the 2.37 GB store — so it needs free space for about two compacted copies and, on a 49 MB
store with 75 MB free, failed with `database or disk is full`, left `temp_store = FILE` on
the connection, and was retried on every call. `VACUUM INTO` writes the live pages once,
needs free space for one compacted copy, uses no temp directory, and leaves the database
untouched until the rename, which is atomic: the same store compacted on the same volume.
The `-wal` and `-shm` are deleted before the rename, not after: SQLite checks a WAL only
against its own header, so a WAL an interruption left next to the new file would be replayed
into it. `Optimize` checks the volume first and skips the compaction, to retry on the next
call, when the space is not there or SQLite reports the disk full, and discards the copy
when another connection holds a lock on the file or the WAL survives the close, which happens
only when the close could not checkpoint it. A connection with the file open and no lock,
such as one retrying a busy file, cannot be detected, and after the rename it would read the
old file with the `-wal` of the new one, so the swap needs one instance per file. A store
converted this way runs with incremental auto-vacuum, as a store created by 5.1 or later
does, which costs about 7% on bulk inserts and 12% on partition deletes against the same
store without it.

## Measured on the real store

Through the library on a copy of a 4.4.2 production store (1,801,125 rows, 3,063 MB, eleven
indexes, no `TychoIndex`, `auto_vacuum` NONE), Release build, e_sqlite3 3.50.3, NVMe, the
file just copied so everything is warm. The lookups are the app's four: a pricing level by
brand, origin and load date; a distance by SPLC pair; a zip3; a zip.

| `autoOptimize: false` | Time |
|---|---|
| Connect | 72 ms |
| Six `CreateIndex`, all left to `Optimize`, no DDL | 69 ms (67 of it the first call's JIT) |
| Lookups before `Optimize`, on the 4.4.2 indexes | pricing 5.3 ms, SPLC 0.2 ms, zip3 and zip 0.05 ms |
| `Optimize()`: four drops, six rebuilds, full `VACUUM` (before the compaction below), conversion | 26.3 s |
| Lookups after | pricing 0.64 ms, the others 0.05 ms |
| File | 3,063 MB → 1,030 MB, `auto_vacuum` 2, freelist 0 |
| Next launch: Connect, six `CreateIndex`, `Optimize()` | 44 ms, 34 ms, 1 ms |

| `autoOptimize: true` | Time |
|---|---|
| Connect, the four drops | 9.2 s |
| Six `CreateIndex` inline, drop the 4.4.2 index and build | 9.0 s (0.7–4.7 s each) |
| File after launch | 2,922 MB, 476,124 free pages, still NONE |
| `Optimize()` on the next launch, the full `VACUUM` alone | 5.4 s → 1,030 MB; a further call 1 ms |

The string overload's same-named 4.4.2 index on a 5,000-row store: 4 ms to detect the
mismatch and leave it to `Optimize()`, 15 ms for `Optimize()` to rebuild it, 9 ms inline.

The compaction that replaced the full `VACUUM`, and the steps, measured through the library
on a copy of another 4.4.2 pricing store (1,763,583 rows, 2.37 GB, the same eleven indexes,
e_sqlite3 3.50.3, NVMe, warm), with one task running the app's four lookups in a loop, one
writing, and one making a synchronous call, for the whole of `Optimize()`:

| `autoOptimize: false` | |
|---|---|
| Connect, six `CreateIndex` | 38–52 ms, 32–39 ms |
| `Optimize()`: four drops, six rebuilds, `VACUUM INTO`, swap | 19–22 s |
| Calls during it | all succeeded, every write kept |
| Longest wait for one call | 3.4–4.3 s (the whole run before the steps) |
| File | 2.37 GB → 1.04 GB, `auto_vacuum` 2, freelist 0 |
| Largest WAL, peak memory | 11 MB, 187 MB |
| Next launch: `Optimize()` | 1.2 ms |

| `autoOptimize: true` | |
|---|---|
| Connect, the four drops | 9.2 s |
| Six `CreateIndex` inline | 14.6 s |
| `Optimize()` on the next launch, the compaction alone | 3.1 s |

A full `VACUUM` took the WAL to 1,043 MB on the same store. The same 49 MB store on a volume
with 75 MB free: the full `VACUUM` failed with the disk full, the compaction took 153 ms;
with 16 MB free it was skipped in 2.5 ms, the store untouched and in use.

Cold: the same runs with the file copied without caching (`F_NOCACHE`; a system purge
needs root), verified cold by a first sequential read at half the cached speed. Every phase
was within noise of the warm run — deferred connect 78 ms and declarations 62 ms, first
lookups 113 / 8.4 / 1.9 / 1.1 ms, `Optimize()` 23.8 s, inline connect 9.3 s and
declarations 9.6 s, the steady-state launch on the cold 1 GB file 44 / 37 / 1 ms with first
lookups 34 / 7.3 / 1.5 / 1.4 ms. Nothing left on the login path scales with random reads;
the 457 s cold `ANALYZE` of the statistics design was that.

Bulk writes on the same store, warm, 50,000 rows per `WriteObjectsAsync` into a fresh
partition and `DeleteObjectsAsync` of that partition, steady-state repetitions:

| | 4.4.2 shape, 11 indexes | Upgraded, 7 indexes | Gain |
|---|---|---|---|
| Write `PricingLevelModel` (three indexes on the type) | 37,700–38,000 rows/s | 78,200–82,900 rows/s | 2.1x |
| Write `DeltaDownloadDateModel` (no index on the type) | 59,000–65,600 rows/s | 184,000–194,000 rows/s | 3.0x |
| Delete the partition, indexed type | 1.0 s | 0.47 s | 2.2x |
| Delete the partition, unindexed type | 0.68 s | 0.25 s | 2.7x |

The 25–45% measured for 5.0 covered the four built-in drops on a synthetic store; the six
whole-table per-type indexes 4.4.2 built cost as much again, and a partial index costs
nothing for the types it does not cover.

## Verification

- Baseline, unmodified 5.3.1: 362 passed, 4 skipped, in both Debug (SQLite 3.50.4) and
  Encrypted (3.39.2). The two scenario tests fail on it in both configurations with
  `SEARCH JsonValue USING INDEX idx_jsonvalue_fulltypename_partition`.
- Final state: no failures in Debug or Encrypted, 4 skipped.
- Stores written by 14 real releases, 3.2.2 to 5.3.1, each upgraded in 16 combinations of
  `autoOptimize`, connection persistence, pooling and leftover sampled statistics: all 224
  matched the expected rows, schema and metadata, and the next launch wrote nothing.
- The process killed at random and at each step of the swap on a copy of the 2.37 GB store,
  with a writer running: no acknowledged write lost, `quick_check` clean, the next call
  finished the work.
- Disk full before and during the copy, and each step of the swap made to fail, including
  the reopen: nothing lost, and the instance serving or recovered by `Connect`.
- iOS simulator and an Android emulator (API 35), .NET 10: `DriveInfo`, the swap, the file
  mode, and reads and writes during `Optimize`.
- Code compiled against 5.3.1 runs against this build.
