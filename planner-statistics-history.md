# Query Planner Statistics: History and Decisions

TychoDB stores every object in one table, `JsonValue`. Two kinds of index matter here:

- **General index**: `(FullTypeName, Partition)`, shared by all types.
- **Per-type index**: a partial index that `CreateIndex` builds for one property of one type.

SQLite's query planner chooses between them. If `ANALYZE` has stored statistics in `sqlite_stat1`, the planner uses them; if not, it uses its built-in rules.

---

## 1. The 5.3.1 problem

**What happened:** Lookups on an indexed property became slow on large stores. On a 1.68M-row store, a lookup took 82–366 ms instead of 0–1 ms. The results were still correct.

**Why:** 5.3.1 ran `ANALYZE` with `analysis_limit = 400`, so SQLite read only the first 400 entries of each index. That sample told the planner each type had about 81 rows, but the largest type had 586,986. So the planner picked the general index and read every row of the type on each lookup.

---

## 2. First fix: full statistics

**Changed:** `ANALYZE` ran without a limit after `CreateIndex`, and once on stores that already held sampled statistics.

**Why:** Full statistics give the planner the right row counts, so it picks the per-type index again.

**Result:** Lookups were fast again, but a full `ANALYZE` took 3.2 s on the 1.68M-row store and 17.5 s on a 2.2 GB store.

---

## 3. Connecting became slow

**What happened:** On the first launch after upgrading, connecting took 10–34 s on MoveScoutPro's device stores.

**Why:** Connecting did two slow things. It dropped four indexes that 4.x created and 5.x no longer needs, which took 3.8–9.3 s, and it ran the statistics work.

---

## 4. Opt-out for the slow connect

**Changed:** Added `autoOptimize: false`, plus `Optimize()` / `OptimizeAsync()` to do the same work later.

**Why:** It lets an app connect fast and do the slow, one-time work on a background thread.

---

## 5. `PRAGMA optimize` could not be trusted

**What happened:**
- **Sampling came back:** by default, `PRAGMA optimize` limits `ANALYZE` to 2,000 rows, which recreated the 5.3.1 problem.
- **It re-analyzed on every call:** with the limit removed, it estimated a 1.84M-row store at 174,370 rows, so it decided the table had grown and re-analyzed on every call, 20–30 s each time.

**Changed:** TychoDB stopped using `PRAGMA optimize`, and decided for itself when statistics were out of date.

**Why:** SQLite's own check gave wrong answers on real data.

---

## 6. Step back: no statistics at all

**Changed:** All statistics work was removed. Connecting now deletes any statistics an earlier release left behind. The 4.x index drops stay.

**Why:**
- **Each fix needed another fix,** so it was worth asking whether statistics were needed at all.
- **Built-in rules choose correctly without them.** The planner picks the right index for every single-index query TychoDB runs: equality, range, sort with a limit, an unindexed property, a whole type, and a count.
- **Nothing left to go wrong:** with no statistics, nothing can be sampled or go out of date.
- **The drops make writes faster:** bulk writes are 25–45% faster without the old indexes.

**Known trade-off:** when a query filters on two indexed properties, the planner can't tell which index is better, so it picks one by rule. This was accepted without being measured.

---

## 7. Tested on real device stores

MoveScoutPro's test suite ran on four device stores (682k–1.85M rows, 2.4–3.2 GB), with `autoOptimize: false`:

| | Stock 5.3.1 | This branch |
|---|---|---|
| First connect after upgrade | 10–34 s | 8–40 ms |
| `Optimize()`, first call | – | 3.8–9.3 s |
| `Optimize()`, later calls | – | 0–1 ms |
| App login to launch page | 18.2 s | 8.8 s (4.4.2: 9.3 s) |

Results matched 4.4.2 for every read and write.

---

## 8. Rename (later reset)

**Changed:** `autoOptimize` became `dropLegacyIndexes`, and `Optimize()` became `DropLegacyIndexes()`.

**Why:** At that point dropping the old indexes was the only work left, so the names should say that.

**Now:** Reset off the branch. The branch keeps `autoOptimize` / `Optimize()`, which now do more than the drops (step 12).

---

## 9. Commit 120fc5c: statistics come back

**Why:** It found two real problems with step 6:

1. **Two indexed filters:** the planner used whichever index was created last. That cost 4.6 ms instead of 0.02 ms at 40,000 rows.
2. **Writes on every connect:** each connect dropped and re-created `sqlite_stat1`, which changes the schema twice. With `persistConnection: false`, that happens on every operation.

**Changed:**
- **Gathered on index build:** a full `ANALYZE JsonValue` ran whenever `CreateIndex` built an index.
- **General index overwritten:** its statistics were set to half the table's row count, so it only wins when one value covers more than half the table.
- **One-time repair:** stores with sampled statistics were fixed once, and `PRAGMA user_version` recorded that it had run.

---

## 10. 120fc5c reverted

**Why:**
- **Statistics never refreshed.** They were taken once, when an index was built. In two synthetic tests (544k rows) they went out of date and lookups took 174 ms and 180 ms instead of 0 ms. That's the 5.3.1 problem again, and nothing fixed it.
- **Upgrades got slower.** Every index build ran a full `ANALYZE`, which takes 3.2–17.5 s on real stores, and a 4.x upgrade rebuilds every index.
- **No gain on a real store.** On a 1.8M-row, 3 GB production store, plans and timings were the same with and without statistics.
- **Composite indexes cover two filters.** A query filtering on two properties is served by a composite index, which needs no statistics. The README now says so.

**Kept:** the connect-write problem it found, fixed in step 11.

---

## 11. Connecting writes nothing

**What happened:** Connect wrote to the file every time, for two reasons:
- It dropped and re-created `sqlite_stat1`.
- It ran `PRAGMA auto_vacuum = INCREMENTAL`, which rewrites the first page of an existing store.

**Changed:**
- **Statistics:** connect deletes leftover `sqlite_stat1` rows only when there are any, and never drops the table.
- **Auto-vacuum:** the pragma runs only for a new file.

**Why:** A write on every connect is wasted work, and with `persistConnection: false` it happened on every operation.

**Result:** Connecting to a current store leaves the file byte-identical.

---

## 12. Defer the index rebuilds too

**What happened:** On a 4.x upgrade, each first `CreateIndex` drops the old 4.x index and builds the new one. On a 1.8M-row, 3 GB store that took 0.7–4.7 s per index, 9.0 s for six, on top of 9.2 s of drops at connect.

**Changed:**
- **Pending indexes:** with `autoOptimize: false`, `CreateIndex` marks the index as pending, and the old 4.x index keeps serving queries.
- **`Optimize()` does the rest:** the drops, the pending rebuilds, and a `VACUUM` to return the freed space. 4.x stores never got incremental auto-vacuum, so the space can only come back this way.
- **Two related fixes:**
  - The string overload of `CreateIndex` no longer keeps the 4.x index by mistake.
  - Indexes a 4.x rollback recreated are dropped.

**Result** on that store:

| | |
|---|---|
| Connect plus six `CreateIndex` | about 140 ms |
| `Optimize()` | 26.3 s |
| File size | 3,063 MB → 1,030 MB |
| Next launch: connect, six `CreateIndex`, `Optimize()` | 44 ms, 34 ms, 1 ms |
| Bulk writes afterwards | 2.1x faster (indexed type), 3x (unindexed) |

---

## 13. Compact with `VACUUM INTO` instead of `VACUUM`

**What happened:** A full `VACUUM` writes the data twice: into a temp copy, then back through the WAL, which grew to 1.04 GB on a 2.37 GB store. It needs room for about two copies. On a 49 MB store with 75 MB free it failed with "database or disk is full", and was retried on every call.

**Changed:** `Optimize()` now:
1. writes a compacted copy with `VACUUM INTO`,
2. closes the connection,
3. deletes the old `-wal` and `-shm` files,
4. renames the copy over the database,
5. reopens.

It skips the compaction, and retries on the next call, when there isn't room or a WAL with data is still there after the close.

**Why:**
- **One copy:** `VACUUM INTO` writes the data once, so it only needs room for one compacted copy.
- **Safe interruption:** the database isn't touched until the rename, which is atomic.
- **WAL first:** SQLite would replay an old WAL into the new file, so the WAL is deleted before the rename.

**Result:**

| Store | Full `VACUUM` | `VACUUM INTO` |
|---|---|---|
| 1.76M rows, 2,374 MB: whole `Optimize()` | 22.4 s | 19.0 s |
| Same store: largest WAL | 1,043 MB | 12 MB |
| 49 MB store, 75 MB free | failed, disk full | 153 ms |
| 49 MB store, 16 MB free | – | skipped in 2.5 ms |

---

## 14. Smaller fixes

- **Both overloads, one name:** when the same index name is declared through both `CreateIndex` overloads, each now leaves the other's index alone. Before, one dropped and rebuilt the other's index on every launch.
- **Pending definitions are checked:** a pending index's saved definition is rebuilt from its parts and compared exactly before `Optimize()` runs it, and anything else is refused. It's read back from the file, so it could have been changed.
- **Read-only files:** connecting works even when leftover statistics can't be deleted.
- **Version:** set to 5.4.0.

---

## Where it ends up

- **No statistics:** no planner statistics are gathered, and connect deletes leftover ones only if there are any.
- **Quiet connect:** connecting to a current store writes nothing.
- **`autoOptimize: true` (default):** connect drops the 4.x indexes and `CreateIndex` rebuilds on the spot.
- **`autoOptimize: false`:** connect and `CreateIndex` take milliseconds, and `Optimize()` on a background thread does the drops, rebuilds and compaction.
- **Two filters:** filtering on two properties needs a composite index.
