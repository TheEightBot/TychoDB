# Sampled planner statistics made SQLite skip the partial indexes (5.3.1)

## Symptom

On a 1.68M-row store whose largest type held 586,986 rows, an equality lookup on an indexed
property of that type took 82–366 ms, and a lookup for a missing key 310 ms. The same query
with correct statistics takes 0–1 ms. Results were identical in every case; only speed
changed. Stores upgraded from 4.4.2, whose indexes are the legacy non-partial shape, were
not affected.

## Why

All objects share `JsonValue (Key, FullTypeName, Partition, Data)` with the general index
`idx_jsonvalue_fulltypename_partition (FullTypeName, Partition)`. 5.x `CreateIndex` adds a
per-type partial index:

```sql
CREATE INDEX idx_GroupId_WideModel_0badcafe
ON JsonValue (Partition, JSON_EXTRACT(Data, '$.GroupId'))
WHERE FullTypeName = 'Zzz.Models.WideModel';
```

Two places gathered statistics with `PRAGMA analysis_limit = 400`: `Tycho.RunOptimize`
(`PRAGMA analysis_limit = 400; PRAGMA optimize;` on connect, disconnect and dispose) and
`Queries.AnalyzeBounded` (`PRAGMA analysis_limit = 400; ANALYZE;` after `CreateIndex`).
With a limit, `ANALYZE` reads only the first ~400 entries of each index:

- The general index is ordered by type name, and the types that sort first are tiny
  (22, 112, 10, 202 rows… in the real store). Its sample spans a handful of types, so
  `sqlite_stat1` credits every type with ~81 rows. The real type has 586,986.
- The partial index is ordered by `(Partition, GroupId)`, so its sample sits inside one key
  and credits every key with ~401 rows. The truth is ~542 across 1,083 keys.

The planner compares 81 against 401 and chooses the general index, then reads and
JSON-parses every row of the type on each lookup. Full statistics (`586986 586986 542`
against `1682261 34332 31741`) or no statistics at all keep the partial index chosen. So
does a partial index with no statistics row next to a full, stale or zero-row general-index
row, and statistics taken on a few hundred rows before 540,000 arrive (probes on the
synthetic store); the only snapshots that mislead the planner are a sampled pair, a full
partial-index row next to a stale general-index row, or a partial-index row that says one
key holds the whole index.

Plain `PRAGMA optimize` cannot be used either: the default mask (`0xfffe`) includes bit
`0x10`, which applies a temporary 2,000-row limit regardless of `analysis_limit`. It wrote
`2001 501` against `101` and mis-ranked the indexes the same way.

The bundled SQLite is 3.50.4 (`SQLitePCLRaw.bundle_e_sqlite3` 3.0.1) and 3.39.2 for the
Encrypted package (`bundle_e_sqlcipher` 2.1.10). Both are affected: the explicit `ANALYZE`
behaves identically, and the regression tests fail on both.

## Repro

- `docs/planner-statistics-sampling.sql` — sqlite3 CLI only, synthetic data, ~3 s. Forty
  tiny types that sort first, one type with 540,000 rows over 1,000 keys, the same partial
  index shape. Output on SQLite 3.51.0:

  | State | `sqlite_stat1` (partial / general) | Plan | Lookup |
  |---|---|---|---|
  | `analysis_limit = 400; ANALYZE` (stock 5.3.1) | `540000 401 401` / `543990 81 81` | general index | 196–217 ms, missing key 205 ms |
  | `analysis_limit = 0; ANALYZE` | `540000 540000 540` / `543990 13269 13269` | partial index | 0–1 ms |
  | no statistics | — | partial index | 0–1 ms |
  | `PRAGMA optimize` (default mask), `analysis_limit = 0` | `540000 2001 501` / `543990 101 101` | general index | 220 ms |
  | `PRAGMA optimize(0x10002)` | `540000 540000 540` / `543990 13269 13269` | partial index | 1 ms |

  Two more cases that shaped the fix: sampled rows already in the file are left alone by
  `PRAGMA optimize(0x10002)` (nothing is missing, nothing grew), and a full row for the
  partial index next to a sampled row for the general index still picks the general index.

- `TychoDB.UnitTests/PlannerStatisticsTests.cs` — three tests through Tycho's own
  `CreateIndexAsync`, `ConnectAsync` and dispose paths on a 20,400-row store (40 small
  types, one type with 20,000 rows over 200 keys). Each asserts the plan for the library's
  own filter SQL uses the partial index and that its statistics row reads from the whole
  index (`20000 20000 100`). On 5.3.1 all three fail in both configurations with
  `SEARCH JsonValue USING INDEX idx_jsonvalue_fulltypename_partition`. On the same
  Tycho-written file, 5.3.1's statement leaves `20000 401 81` / `20400 10 10` and a lookup
  takes 7–11 ms; the fixed build leaves `20000 20000 100` / `20400 498 498` and 0.03 ms.

Bound parameters are fine in the library: SQLite re-plans when a bound value touches a
partial-index `WHERE`, so `EXPLAIN QUERY PLAN` on an unbound statement shows the generic
plan. The tests bind the parameters; the CLI script uses literals.

## Fix

- At connect and at disconnect/dispose, Tycho re-analyzes when an index of `JsonValue` has
  no `sqlite_stat1` row or the exact `count(*)` differs tenfold from the row count recorded
  in the general index's row, and otherwise does nothing. The branch first used SQLite's
  `PRAGMA optimize(0x10002)` for this (the documented form for a long-lived connection at
  open, without the default mask's 2,000-row cap, which reproduces the problem); the
  follow-up below explains why it was replaced.
- After `CreateIndex`, `Queries.Analyze` runs `PRAGMA analysis_limit = 0; ANALYZE;`. It is
  a full `ANALYZE`, not `ANALYZE <new index>`: a full row next to a sampled or stale row
  for the general index still mis-ranks. It is an explicit `ANALYZE`, not `PRAGMA optimize`:
  on 3.39.2 `optimize` only considers tables the planner has used on the connection, so it
  would do nothing right after `CREATE INDEX`.
- A store written by an earlier 5.x release already holds sampled rows, which look complete
  and current (repro case 6). On its first connect, Tycho runs one full `ANALYZE` and stamps
  the store with `PRAGMA user_version = 1` (unused by Tycho until now); later connects use
  the check above.
- `autoOptimize: false` connects without the one-time upgrade work — the four
  `DROP INDEX IF EXISTS` for the indexes 4.x created (now `Queries.DropLegacyIndexes`,
  appended to the connect script only when the option is on) and the statistics step above
  — and skips the `ANALYZE` after `CreateIndex`; a new index without a statistics row is
  chosen by default heuristics until its row arrives. `Optimize`/`OptimizeAsync` do exactly
  what connect skipped: the drops, then the same conditional statistics step, so a call
  with nothing to do costs ~0 ms and the app can run it on every launch. The drops stay:
  they are what makes bulk writes 25–45% faster, and nothing else is removed. Disconnect
  still runs `optimize(0x10002)` in both modes.
- README and CHANGELOG describe the new behavior.

## Cost

| Store | Sampled `ANALYZE` | Full `ANALYZE` |
|---|---|---|
| synthetic, 544k rows (CLI) | 0.017 s | 0.13–0.26 s |
| real, 1.68M rows, fresh | 2.9 s | 3.2 s |
| real, 2.2 GB, upgraded from 4.4.2 | — | 17.5 s |

The full `ANALYZE` runs after each new index, as before. Connect, disconnect and `Optimize`
run it only when an index has no statistics or the exact row count moved tenfold; otherwise
they cost the exact count, a scan of the covering general index (36 ms warm on 1.8M rows),
so the analysis is paid once per store, not per launch.

## Decisions

1. **Stores that already hold 5.3.1's sampled rows** are repaired once at connect: nothing
   is missing and nothing grew, so `PRAGMA optimize(0x10002)` would leave them, and
   re-declaring an unchanged index runs no `ANALYZE`; without the repair they would stay slow
   until a table grew tenfold or an index changed. The repair costs one full `ANALYZE` per
   existing store and is on by default.
2. **The connect-time work blocks the first launch of an upgraded store.** The `ANALYZE`
   costs the 3.2 s / 17.5 s above the first time it runs on a store with no statistics (a
   4.x upgrade), one written by an earlier release, or one that grew tenfold. The four
   legacy index drops were most of a 10–30 s stall MoveScoutPro measured on a large
   upgraded store (main-thread samples in `sqlite3BtreeDropTable`/`clearDatabasePage`):
   freeing an index's pages is proportional to its size, and with `secure_delete` on —
   SQLCipher's default — every freed page is written back. On the 165 MB synthetic store,
   dropping three ~15 MB legacy indexes took 6–7 ms each plain and 39–54 ms each with
   `secure_delete`, in both auto-vacuum modes. Both stay on by default, with
   `autoOptimize: false` as the opt-out and `Optimize`/`OptimizeAsync` for the app to run
   them when convenient — before a large download for the drops (faster writes), after it
   for the statistics (fresh plans), never during it. They hold the single connection for
   the duration (17–24 s on the 2.2 GB store the first time, ~0 ms afterwards) and run on
   the calling thread — Microsoft.Data.Sqlite executes synchronously, so the async form
   alone does not move the work off the caller — so apps call them from a background
   thread. The disconnect-time check is not affected by the opt-out. `CreateIndex` itself
   drops a per-type legacy index and builds the partial one, which is the third piece of an
   upgrade stall and already under the app's control through when it calls it.
3. **Review outcomes.** The opted-out `CreateIndex` no longer analyzes (an opted-out app
   declaring N indexes would otherwise pay N full analyzes, ~2× each while the legacy
   indexes survive); `user_version` is stamped only when lower, so a higher store version
   is never clobbered; `OptimizeAsync` stays inline like every other async method, with the
   threading caveat documented. Not changed: disconnect stays automatic (a mid-`ANALYZE`
   kill rolls back and the next disconnect retries), and the opted-out connect skips the
   statistics check entirely — when it is not a no-op it is the multi-second `ANALYZE` the
   opt-out exists to avoid, and the launch-page `Optimize` covers it.
4. **Known limitation.** The stamp is never invalidated, so if a 5.3.1-or-earlier build
   writes to the store after 5.3.2 has stamped it, its sampled rows return and nothing
   detects them; `Optimize` sees a stamped store with nothing missing and nothing grown.
   Downgrades are out of scope; a `force` flag on `Optimize` would be the remedy if it is
   ever needed.

## Follow-up: SQLite's tenfold check

Testing the branch against four real device stores, MoveScoutPro found one (1.84M rows,
2.2 GB) on which `PRAGMA optimize(0x10002)` ran a full `ANALYZE` on every call — 2–30 s
each, at connect and disconnect by default and at every `Optimize()` otherwise — although
every index had a full, current statistics row. The pragma's tenfold check estimates the
table's row count from the cell counts down the leftmost path of its b-tree; on that store
the path was 265 × 329 × 2 = 174,370 against 1,841,652 rows, a ratio just past the
threshold, because the lowest rowids hold large documents and the leftmost leaf has two
cells. Reproduced synthetically (`docs/planner-statistics-row-estimate.sql`): a few ~3 KB
documents inserted before 540,000 small rows make `PRAGMA optimize(0x10003)` report
`ANALYZE "main"."JsonValue"` immediately after a full `ANALYZE`, and `optimize(0x10002)`
re-runs it on every call. Any store can drift into this layout as rows are rewritten.

Tycho now decides staleness itself and no longer calls `PRAGMA optimize`: a stamped store
is re-analyzed only when an index of `JsonValue` has no `sqlite_stat1` row or the exact
`count(*)` differs tenfold from the row count recorded in the general index's row. The
count is a scan of the covering general index, 36 ms warm on 1.8M rows. This also removed
the probe read that SQLite 3.39.2 (the Encrypted package) had needed, since that build lets
`PRAGMA optimize` consider only tables the connection's planner has already used. The
regression test stores eight large documents first, plants a sentinel `sqlite_stat1` row
(a whole-database `ANALYZE` empties the table before rewriting it), connects twice, and
checks the sentinel survived; two more tests check that the analysis does run after a
tenfold growth or shrink and not after a sixfold one. One thing learned on the way: SQLite
reads a `sqlite_stat1` row whose index it does not know as that table's row count, so the
sentinel names `StreamValue`, not `JsonValue`.

## Considered and not done

Taking the choice away from the planner — a `+FullTypeName` term or `INDEXED BY` in the
generated SQL whenever a partial index applies — would make index choice independent of
statistics. It was not done: `+FullTypeName` would turn every filter on an unindexed
property into a scan of all types, SQLite documents `INDEXED BY` as a regression-testing
aid rather than a tuning tool, and the query builder would have to know which partial
indexes exist. The planner also does not need the help when statistics are consistent: a
partial index with no statistics row is chosen against a full, stale or sampled row for
the general index. The general index wins only when both rows were sampled, or when the
partial row is full and the general row stale — the inconsistent states, which the fix
removes by never sampling and never refreshing one index without the others.

## Verification

- Baseline, unmodified 5.3.1: 362 passed, 4 skipped, in both Debug (SQLite 3.50.4) and
  Encrypted (3.39.2).
- New tests on 5.3.1: 3 failed in both configurations. With the fix: 3 passed in both.
- The change lands as four code commits, each building and passing the Debug suite on its
  own: the core fix (3 tests, 365 passed); the first-connect repair of sampled stores and
  the never-lowered `user_version` stamp (2 tests, 367); the `autoOptimize` opt-out with
  `Optimize`/`OptimizeAsync` (5 tests, 372 — including one that observes, through a
  non-persistent connection, that an opted-out `CreateIndex` leaves no statistics row and
  the partial index is chosen anyway, and that `Optimize` then fills it in; and one in
  `IndexDdlTests` showing the opt-out keeps the legacy indexes until `Optimize` drops
  them); and the probe read that makes `PRAGMA optimize` consider `JsonValue` on SQLite
  3.39.2, which that observing test caught on the Encrypted build.
- Replacing `PRAGMA optimize` with Tycho's own staleness check added three tests (large
  documents first; tenfold growth, with a sixfold growth as the negative; tenfold shrink)
  and removed the need for the 3.39.2 probe.
- Final state: 375 passed, 4 skipped, 0 failed in both configurations (Debug 5 s,
  Encrypted 1 m 36 s).
