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

TychoDB gathers no statistics, and removes any it finds. The connect script ends with
`DROP TABLE IF EXISTS sqlite_stat1; DROP TABLE IF EXISTS sqlite_stat4; ANALYZE sqlite_master;`
— the last statement is SQLite's documented way to reload statistics on an open connection,
and it leaves an empty `sqlite_stat1` behind. Nothing else runs `ANALYZE` or
`PRAGMA optimize`. The one-time index drops stay, behind `autoOptimize` and `Optimize`.

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
to matter.

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

Bound parameters are fine in the library: SQLite re-plans when a bound value touches a
partial-index `WHERE`, so `EXPLAIN QUERY PLAN` on an unbound statement shows the generic
plan. The tests bind the parameters; the CLI script uses literals.

## Cost

Connect pays two `DROP TABLE IF EXISTS` (a schema lookup each when there is nothing to
drop) and a statistics reload (a pass over the indexes in the schema). A store written by
5.3.0 or 5.3.1 pays the drop of its small statistics table once.

The four `DROP INDEX IF EXISTS` for the indexes 4.x created were most of a 10–30 s stall
MoveScoutPro measured on the first launch of a large upgraded store (main-thread samples in
`sqlite3BtreeDropTable`/`clearDatabasePage`): freeing an index's pages is proportional to
its size, and with `secure_delete` on — SQLCipher's default — every freed page is written
back. On the 165 MB synthetic store, dropping three ~15 MB legacy indexes took 6–7 ms each
plain and 39–54 ms each with `secure_delete`. They stay on by default, because they make
bulk writes 25–45% faster; `autoOptimize: false` connects without them and
`Optimize`/`OptimizeAsync` run them later. Both hold the single connection for the
duration and run on the calling thread — Microsoft.Data.Sqlite executes synchronously, so
the async form alone does not move the work off the caller — so apps call them from a
background thread. `CreateIndex` itself drops a per-type legacy index and builds the
partial one, which is the other piece of an upgrade stall and already under the app's
control through when it calls it.

## Verification

- Baseline, unmodified 5.3.1: 362 passed, 4 skipped, in both Debug (SQLite 3.50.4) and
  Encrypted (3.39.2). The two scenario tests fail on it in both configurations with
  `SEARCH JsonValue USING INDEX idx_jsonvalue_fulltypename_partition`.
- Final state: 373 passed, 4 skipped, 0 failed in both configurations (Debug 3 s,
  Encrypted 1 m 24 s).
