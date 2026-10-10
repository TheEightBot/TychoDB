# Sampled planner statistics made SQLite skip the partial indexes (5.0.1–5.3.1)

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

5.0.1 started running `PRAGMA optimize` on connect and disconnect, and 5.1.1 added an
`ANALYZE` after `CreateIndex`; both set `PRAGMA analysis_limit = 400`. With a limit,
`ANALYZE` reads only the first ~400 entries of each index:

- The general index is ordered by type name, and the types that sort first are tiny
  (22, 112, 10, 202 rows… in the real store). Its sample spans a handful of types, so
  `sqlite_stat1` credits every type with ~81 rows. The real type has 586,986.
- The partial index is ordered by `(Partition, GroupId)`, so its sample sits inside one key
  and credits every key with ~401 rows. The truth is ~542 across 1,083 keys.

The planner compares 81 against 401, chooses the general index, and reads and JSON-parses
every row of the type on each lookup.

## Fix

Statistics are kept, gathered in full, and owned by TychoDB.

- **When.** `CreateIndex` runs `PRAGMA analysis_limit = 0; ANALYZE JsonValue;` after it
  builds an index, and again when it re-declares an index that was built while its type
  was empty and has rows now (a re-declared index that is current and has a statistics
  row costs one metadata lookup, one `sqlite_master` probe and one `sqlite_stat1` lookup,
  no DDL). Nothing else gathers statistics: `PRAGMA optimize` is gone from connect,
  disconnect and dispose.
- **What.** `ANALYZE JsonValue` only — the one table with expression indexes. Rows
  recorded for an empty type (`0 0 0`) are deleted, so the planner uses its defaults
  until the type has rows and the index is re-declared. The general index's row is then
  pinned to `N N/2 N/2` and the connection reloads with `ANALYZE sqlite_master`.
- **Why pin.** `ANALYZE` writes the general index's row as the *average* rows per
  `FullTypeName`, and in a store where one type dominates that average is wrong for every
  type: taken while few types existed it says a type is the whole table and the planner
  scans instead of seeking (every read of a ten-row settings type scans the 586,986-row
  type); taken over many types it says the big type is small, and the planner prefers the
  general index to that type's partial index — the sampled-statistics regression again,
  in a milder form. In the planner's model a scan costs about 3N and an index visit
  between 1.1 and 3 per row, so N/2 keeps the general index cheaper than a scan for any
  type; and it is dearer than a partial index for any equality whose value repeats on
  fewer than half the rows, any one-sided range (estimated at a quarter of the index
  without stat4) and any sort with a limit. That leaves the partial indexes' own rows,
  which are accurate, to rank against each other — the one thing statistics are for.
  SQLite documents writing `sqlite_stat1` for this purpose
  (https://www.sqlite.org/lang_analyze.html, "Manual Control Of Query Plans").
- **Upgraded stores.** `PRAGMA user_version` gates a one-time repair: a store below the
  stamp may carry sampled rows, so when `autoOptimize` is on the first connect replaces
  them (full `ANALYZE` if the store has expression indexes, otherwise it discards them —
  statistics only rank one expression index against another) and stamps the store. A
  stamped store pays one header read at connect and writes nothing; `schema_version` is
  stable across reconnects. With `autoOptimize: false`, connect leaves the store as the
  previous release did and `Optimize()` / `OptimizeAsync()` performs the repair.

Plans on the 20,400-row test store (40 small types, one type with 20,000 rows over 200
keys, indexed on `GroupId`, a unique `Seq` and an 8-value `Bucket`), asserted by
`TychoDB.UnitTests/PlannerStatisticsTests.cs` on both bundled engines (SQLite 3.50.3 and
SQLCipher's 3.39.2):

| Query | Plan |
|---|---|
| equality on an indexed property (100 of 20,000) | partial index |
| equality on a non-selective indexed property (2,500 of 20,000) | partial index |
| range on an indexed numeric property | partial index |
| sort on an indexed numeric property, `LIMIT 20` | partial index, no temp b-tree |
| two indexed equalities, unique index created first **or** last | the unique index |
| equality on an unindexed property, wide type and small type | general index |
| whole type, wide (98% of the table) and small (0.05%) | general index |
| count | general index, covering |

## Why not "no statistics"

This branch tried both of the other designs before landing here.

*No statistics at all* (the connect script dropped `sqlite_stat1`) gets every single-index
shape right on the planner's defaults, and removes every way statistics can go stale or
inconsistent. But with two partial indexes on one type and a filter on both properties
the planner has nothing to rank them by and takes whichever was created last: measured
through `ReadObjectsAsync` at 40,000 rows, 0.022 ms → 4.56 ms (plain) and 0.019 ms →
57.5 ms (SQLCipher, where every extra page is decrypted) for a one-row result when the
less selective index came second. Which order a store ends up in is whatever order the
app first declared its indexes, so the regression is silent and permanent.

*Full statistics as `ANALYZE` writes them*, with `PRAGMA optimize` to keep them fresh,
was the branch's first design and ran into:

- `PRAGMA optimize`'s default mask includes bit `0x10`, a temporary 2,000-row limit
  regardless of `analysis_limit`, so it wrote `2001 501` against `101` and mis-ranked the
  indexes the same way (repro case 4).
- `PRAGMA optimize(0x10002)` lifts the limit but decides whether a table has grown from
  the cell counts down the leftmost path of its b-tree. A 2.2 GB production store (1.84M
  rows, estimated at 174,370) was re-analyzed on every call, 20–30 s each, because its
  lowest rowids held large documents.
- A full `ANALYZE` took 3.2 s on the 1.68M-row store and 17.5 s on the 2.2 GB store, on
  the connecting thread.
- The general index's averaged row misled the planner in both directions, as described
  under *Why pin*.

The design above keeps the full `ANALYZE` but runs it only where its result is used (an
index was built) or deferrable (`Optimize`), never on a timer or at every connect, and
removes the one row that `ANALYZE` gets wrong.

## Cost

- Connect on a current store: one `PRAGMA user_version` read. No DDL, no write.
- `CreateIndex` of a current index with statistics: one extra `sqlite_stat1` lookup.
- `CreateIndex` that builds an index, or re-declares one built on an empty type that has
  rows now: a full `ANALYZE JsonValue`, proportional to the store (0.115 s at 544k rows
  in the CLI repro; 3.2 s / 17.5 s on the production stores above).
- First connect of a store written by 5.0.1–5.3.1 with `autoOptimize` on: the same full
  `ANALYZE` once if it has expression indexes, otherwise a `DELETE` of its statistics
  rows; then the stamp.

The four `DROP INDEX IF EXISTS` for the indexes 4.x created were most of a 10–30 s stall
measured on the first launch of a large upgraded store (main-thread samples in
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

## Repro

- `docs/planner-statistics-sampling.sql` — sqlite3 CLI only, synthetic data, ~3 s. Forty
  tiny types that sort first, one type with 540,000 rows over 1,000 keys, the two partial
  index shapes. Case 1 is 5.3.1's sampled statistics (general index, ~0.2 s per lookup);
  case 2 is no statistics; case 3 is a full `ANALYZE`, the rows TychoDB now gathers before
  pinning the general index's row; case 4 is plain `PRAGMA optimize`.
- `TychoDB.UnitTests/PlannerStatisticsTests.cs` — through Tycho's own `CreateIndexAsync`,
  `ConnectAsync`, `OptimizeAsync` and dispose paths on the 20,400-row store: the two
  scenarios that failed on 5.3.1 (index created after the data; index created on an empty
  type, data loaded, reconnected), a store carrying 5.3.1's sampled rows repaired at
  connect and, with `autoOptimize: false`, left alone until `Optimize`, a store without
  indexes whose rows are discarded, a fresh store, `schema_version` and the statistics
  unchanged across reconnects of a current store, the pinned general-index row, and one
  test per query shape in the table above, including both creation orders of the two
  partial indexes.
- `TychoDB.Benchmarks/Benchmarks/PlannerStatistics.cs` — the same layout at 40,000 rows
  through the public API across three "launches": an indexed equality (the 5.3.1
  regression), two indexed equalities with the unique index created first or last (the
  no-statistics regression), and a whole-type read of a ten-row type (the averaged-row
  scan). `dotnet run --project TychoDB.Benchmarks -c Release -- --filter '*PlannerStatistics*'`.

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

- Baseline, unmodified 5.3.1: 362 passed, 4 skipped, in both Debug (SQLite 3.50.3) and
  Encrypted (3.39.2). The two scenario tests fail on it in both configurations with
  `SEARCH JsonValue USING INDEX idx_jsonvalue_fulltypename_partition`.
- Final state: 379 passed, 4 skipped, 0 failed in both configurations.
- Benchmark before/after: see the pull request.
