# TychoDB Indexing Analysis

**Bucket 1 — measurement baseline.** This document records the evidence for the reported
indexing defects, a verdict per claim, and the empirical results that gate the redesign
buckets. All evidence below was produced by the repeatable harness in this repo:

```bash
cd TychoDB.Benchmarks
dotnet run -c Release -- diagnose                                    # DDL / EXPLAIN QUERY PLAN / file sizes
dotnet run -c Release -- --filter '*IndexedQuerying*' '*InsertionWithIndexes*'   # timings
```

Environment: macOS (arm64), .NET 9, Microsoft.Data.Sqlite 9.0.8, **SQLite 3.39.2** as
reported by `sqlite_version()` at runtime. Note: this is older than the version implied by
the `SQLitePCLRaw.bundle_e_sqlite3` package version, but comfortably supports everything
the redesign needs — partial indexes (3.8.0+), indexes on expressions (3.9.0+), and
generated columns (3.31.0+). The diagnose database seeds 25,000 `TestClassA` rows and
5,000 `TestClassB` rows (a second type sharing the `JsonValue` table, as any real app has).

## Summary of verdicts

| # | Claim | Verdict |
|---|---|---|
| 1 | Value-type properties (int, long, bool, DateTime, Guid, enum) produce an index on the **entire document** | **Confirmed** — DDL dump shows `JSON_EXTRACT(Data, '$')` |
| 2 | Indexes bloat the database | **Confirmed** — 3 indexes added +83% to file size |
| 3 | Indexed queries can be no faster / slower than unindexed | **Confirmed for numeric filters and all sorts** — read timings identical, writes up to 95% slower with indexes present |
| 4 | Built-in schema indexes are largely redundant | **Confirmed** — 3 of 4 `JsonValue` indexes + the `StreamValue` index duplicate the PK autoindexes |
| 5 | Planner statistics are never available in practice | **Confirmed** — `sqlite_stat1` absent even after `Disconnect`'s `PRAGMA optimize` |
| 6 | Partial indexes with a bound `$fullTypeName` parameter are usable (Bucket 3 design gate) | **Confirmed viable** — planner selected both partial indexes |
| 7 | Sort can use an index if `SortBuilder` emits `JSON_EXTRACT` instead of `->>` | **Confirmed** — aligned ORDER BY plan drops the temp b-tree |

## 1. The whole-document index defect (root cause)

`CreateIndex<TObj>(Expression<Func<TObj, object>>, string)` boxes value-type properties,
wrapping the member access in a `Convert` node. `QueryPropertyPath.BuildPath`
(TychoDB/QueryPropertyPath.cs:30-51) only walks `MemberExpression` nodes, so the loop never
runs, zero segments are collected, and the path falls back to `"$"`. `IsNumeric` has the
same blind spot, so the `CAST(... as NUMERIC)` index form is never emitted by the
expression API.

Actual DDL created by the public API (from the diagnose harness, verbatim):

```sql
-- CreateIndexAsync<TestClassA>(x => x.LongProperty, "long_prop")   <- value type
CREATE INDEX idx_long_prop_TestClassA
ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '$'));

-- CreateIndexAsync<TestClassA>(x => x.StringProperty, "str_prop")  <- reference type: correct path
CREATE INDEX idx_str_prop_TestClassA
ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '$.StringProperty'));

-- Composite (StringProperty, TimestampMillis): the value-type member degrades to '$'
CREATE INDEX idx_str_ts_TestClassA
ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '$.StringProperty'), JSON_EXTRACT(Data, '$'));
```

`JSON_EXTRACT(Data, '$')` returns the whole minified document, so each such index stores a
complete second copy of every document — for **every row of every type** in the table.

## 2. Bloat, quantified

25,000 `TestClassA` + 5,000 `TestClassB` rows, sizes after `VACUUM`:

| Configuration | File size | Delta |
|---|---|---|
| Baseline (built-in indexes only) | 11.03 MiB | — |
| + the 3 public-API indexes above | 20.17 MiB | **+9.14 MiB (+83%)** |
| Single broken index (`'$'` shape) on LongProperty | 14.54 MiB | +3.51 MiB |
| Single corrected index (`CAST(JSON_EXTRACT(Data,'$.LongProperty') as NUMERIC)`) | 12.18 MiB | +1.15 MiB |
| Pair of partial indexes (string + numeric, `WHERE FullTypeName = ...`) | 11.92 MiB | +0.90 MiB (≈0.45 MiB each) |

A broken value-type index costs **~3× a corrected full index** and **~7.8× a partial
index** — and the corrected/partial variants are also the only ones the planner will use.

## 3. Query plans: what the planner actually does

The harness runs `EXPLAIN QUERY PLAN` on the exact SQL the library's own
`FilterBuilder`/`SortBuilder` emit (via `InternalsVisibleTo`), with parameters bound the
way `Tycho` binds them.

**Before any user indexes** every filter shape uses the built-in
`idx_jsonvalue_fulltypename_partition` (two equality columns), then scans and evaluates the
JSON predicate per row:

```
equals-numeric   SEARCH JsonValue USING INDEX idx_jsonvalue_fulltypename_partition (FullTypeName=? AND Partition=?)
```

**After creating the three public-API indexes**, the numeric and range filters are
*unchanged* — the `'$'` index cannot match `CAST(JSON_EXTRACT(Data,'$.LongProperty') as
NUMERIC)`, so consumers pay 9 MiB of storage and full write amplification for zero query
benefit:

```
equals-string    SEARCH JsonValue USING INDEX idx_str_ts_TestClassA (FullTypeName=? AND <expr>=?)
equals-numeric   SEARCH JsonValue USING INDEX idx_jsonvalue_fulltypename_partition (FullTypeName=? AND Partition=?)   <- unchanged
range-numeric    SEARCH JsonValue USING INDEX idx_jsonvalue_fulltypename_partition (FullTypeName=? AND Partition=?)   <- unchanged
sort-*           ... USE TEMP B-TREE FOR ORDER BY                                                                     <- always
```

(Note the one "win", equals-string, is served by the *composite* index whose third column
is the whole document — the most bloated possible way to index a string property.)

**With the corrected numeric index**, equality and range flip to index searches
immediately:

```
equals-numeric   SEARCH JsonValue USING INDEX idx_corrected (FullTypeName=? AND <expr>=?)
range-numeric    SEARCH JsonValue USING INDEX idx_corrected (FullTypeName=? AND <expr>>?)
```

## 4. Redundant built-in indexes

From the DDL dump: `JsonValue` has `PRIMARY KEY (Key, FullTypeName, Partition)` (implicit
autoindex) plus four explicit indexes, of which three add nothing:

- `idx_jsonvalue_key_fulltypename_partition` — exact duplicate of the PK autoindex
- `idx_jsonvalue_key_fulltypename` — strict prefix of the PK autoindex
- `idx_jsonvalue_fulltypename` — strict prefix of `idx_jsonvalue_fulltypename_partition`
- `idx_streamvalue_key_partition` — exact duplicate of `StreamValue`'s PK autoindex

Every document write maintains all five `JsonValue` b-trees. Removal (Bucket 4) is pure
win; the query-plan sweep in that bucket must confirm no query regresses to a table scan.

## 5. Statistics

`sqlite_stat1` does not exist even after a full connect→index-create→dispose cycle —
`Disconnect`'s `PRAGMA optimize` produced no statistics table in the diagnose run. All
plans above are chosen by default heuristics. Bucket 3 adds a bounded `ANALYZE` after
index creation and `PRAGMA optimize` at the end of `Connect`.

## 6. Partial-index spike — Bucket 3 design gate: PASSED

The redesign's target shape is a partial expression index:

```sql
CREATE INDEX idx_partial_long ON JsonValue(Partition, CAST(JSON_EXTRACT(Data, '$.LongProperty') as NUMERIC))
WHERE FullTypeName = 'TychoDB.Benchmarks.TestClassA';
```

The open question was whether SQLite would use it when the query constrains
`FullTypeName = $fullTypeName` as a **bound parameter** (partial-index usage requires the
query's WHERE to imply the index's WHERE). On SQLite 3.39.2 via Microsoft.Data.Sqlite,
with the parameter bound before execution:

```
equals-string    SEARCH JsonValue USING INDEX idx_partial_str (Partition=? AND <expr>=?)
equals-numeric   SEARCH JsonValue USING INDEX idx_partial_long (Partition=? AND <expr>=?)
range-numeric    SEARCH JsonValue USING INDEX idx_partial_long (Partition=? AND <expr>>?)
```

**The planner uses the partial indexes.** Leading with `Partition` also absorbs the
partition equality present in every query. This confirms the Bucket 3 shape; the
non-partial fallback `(FullTypeName, Partition, <expr>)` is not needed.

Caveat for Bucket 3: this holds when the parameter is bound before the statement is
prepared/stepped (Microsoft.Data.Sqlite's behavior). The Bucket 3 test suite must assert
this plan through the real `ReadObjectsAsync` path, not only through the harness.

## 7. Sort alignment

`SortBuilder` emits `ORDER BY Data ->> '$.Prop'`; an expression index is defined on
`JSON_EXTRACT(Data, '$.Prop')`. SQLite matches expression indexes structurally, so no sort
can ever use an index today — every sorted query gets `USE TEMP B-TREE FOR ORDER BY`.

Synthetic check: the same query with `ORDER BY JSON_EXTRACT(Data, '$.StringProperty')`
against the partial string index:

```
sort-aligned-string   SEARCH JsonValue USING INDEX idx_partial_str (Partition=?)
```

No temp b-tree — rows stream out of the index pre-sorted, so a `top: 50` query stops
after 50 rows instead of sorting the full result set. This validates Bucket 4's
`SortBuilder` change (`->>` → `JSON_EXTRACT`), with the null/mixed-type edge-case review
noted in the plan.

## 8. Timing benchmarks

BenchmarkDotNet, Apple M4 Max, .NET 9.0.17, System.Text.Json serializer. Indexes created
through the real public API (`str_prop`, `long_prop`, `str_ts` — the same three as the
diagnose harness). Full reports in `TychoDB.Benchmarks/BenchmarkDotNet.Artifacts/results/`.

### Read latency (`IndexedQuerying`), 25,000 rows

| Benchmark | Unindexed | Indexed | Effect |
|---|--:|--:|---|
| EqualsString | 6,856 μs | **14.4 μs** | **476× faster** — the one shape whose index matches |
| EqualsStringWithPartition | 6,928 μs | **12.5 μs** | ~554× faster (same mechanism) |
| EqualsNumeric | 6,715 μs | 6,743 μs | **no change** — `'$'` index unusable |
| RangeNumeric | 6,732 μs | 6,715 μs | **no change** |
| SortByStringTop50 | 7,131 μs | 7,164 μs | **no change** — `->>` never matches |
| SortByNumericTop50 | 7,085 μs | 6,990 μs | **no change** |

At 1,000 rows the pattern is identical (EqualsString 285 μs → 12.5 μs; everything else
unchanged within noise).

Two conclusions in one table: (1) for every value-type filter and every sort, consumers
pay the full index cost for **zero** read benefit; (2) when an index *does* match
(string equality), the win is enormous — which is exactly the upside the redesign
unlocks for the numeric/date/Guid/bool cases and for sorts.

### Write amplification (`InsertionWithIndexes`), 5,000-row db, replace writes

| Benchmark | 0 indexes | 1 index (string) | 3 indexes (incl. two `'$'` shapes) |
|---|--:|--:|--:|
| WriteSingle | 30.6 μs | 34.6 μs (+13%) | 56.0 μs (**+83%**) |
| WriteBatch1000 | 6,364 μs | 7,598 μs (+19%) | 12,391 μs (**+95%**) |

The jump from 1→3 indexes is dominated by the two `'$'`-bearing indexes, each of which
rewrites a full copy of every document into its b-tree on every write. Today's
value-type indexes therefore make writes nearly **2× slower** while leaving reads
untouched — the precise mechanism behind "worse than no index at all."

## 9. Results after the fix

All work landed across four buckets. Same harness, same hardware, same data.

### 9.1 What changed

| Bucket | Change |
|---|---|
| 2 | Unwrap the boxing `Convert` node in `QueryPropertyPath` so value-type properties resolve to real JSON paths and the numeric `CAST` form; consolidate four duplicated `CreateIndex` bodies into one core |
| 3 | Partial index shape `ON JsonValue(Partition, <expr>) WHERE FullTypeName = '<full name>'`; `TychoIndex` metadata table (dedup, rebuild-on-change, legacy migration); hash-suffixed physical names; bounded `ANALYZE` after index creation and `PRAGMA optimize` on connect |
| 4 | `SortBuilder` emits the same `JSON_EXTRACT` / `CAST(... as NUMERIC)` expressions as the index; the three redundant `JsonValue` indexes and the redundant `StreamValue` index are removed, with idempotent `DROP INDEX` so existing databases shed them |
| 5 | `DropIndex`/`DropIndexAsync`/`ListIndexes` (additive API) |

Generated DDL now, for `CreateIndexAsync<TestClassA>(x => x.LongProperty, "long_prop")`:

```sql
CREATE INDEX idx_long_prop_TestClassA_5958e107
ON JsonValue(Partition, CAST(JSON_EXTRACT(Data, '$.LongProperty') as NUMERIC))
WHERE FullTypeName = 'TychoDB.Benchmarks.TestClassA';
```

### 9.2 Query plans — every shape now uses an index

```
equals-string        SEARCH JsonValue USING INDEX idx_str_ts_TestClassA_5958e107 (Partition=? AND <expr>=?)
equals-numeric       SEARCH JsonValue USING INDEX idx_long_prop_TestClassA_5958e107 (Partition=? AND <expr>=?)
range-numeric        SEARCH JsonValue USING INDEX idx_long_prop_TestClassA_5958e107 (Partition=? AND <expr>>?)
sort-string-top50    SEARCH JsonValue USING INDEX idx_str_ts_TestClassA_5958e107 (Partition=?)
sort-numeric-top50   SEARCH JsonValue USING INDEX idx_long_prop_TestClassA_5958e107 (Partition=?)
```

No `USE TEMP B-TREE FOR ORDER BY` remains. `sqlite_stat1` is now populated. (`contains-string` still scans — `LIKE '%x%'` is not index-able by design.)

### 9.3 Read latency, 25,000 rows

| Benchmark | Before (unindexed) | Before (indexed) | **After (indexed)** | Speedup |
|---|--:|--:|--:|--:|
| EqualsString | 6,856 μs | 14.4 μs | **16.9 μs** | 388× |
| EqualsNumeric | 6,715 μs | 6,743 μs *(no benefit)* | **18.5 μs** | **341×** |
| RangeNumeric | 6,732 μs | 6,715 μs *(no benefit)* | **84.9 μs** | **75×** |
| SortByStringTop50 | 7,131 μs | 7,164 μs *(no benefit)* | **53.0 μs** | **127×** |
| SortByNumericTop50 | 7,085 μs | 6,990 μs *(no benefit)* | **54.5 μs** | **123×** |
| EqualsStringWithPartition | 6,928 μs | 12.5 μs | **18.0 μs** | 369× |

The four shapes that previously gained *nothing* from an index are the headline: numeric equality, numeric range, and both sorts.

### 9.4 Write throughput

Writes improved even with no user indexes, because three redundant built-in b-trees no longer have to be maintained.

| Benchmark | Before | After | Change |
|---|--:|--:|--:|
| WriteSingle, 0 indexes | 30.6 μs | **19.5 μs** | −36% |
| WriteBatch1000, 0 indexes | 6,364 μs | **3,572 μs** | **−44%** |
| WriteSingle, 3 indexes | 56.0 μs | **32.4 μs** | −42% |
| WriteBatch1000, 3 indexes | 12,391 μs | **6,672 μs** | **−46%** |

### 9.5 Database size — 25,000 + 5,000 rows, after VACUUM

| Configuration | Before | After | Change |
|---|--:|--:|--:|
| Baseline (no user indexes) | 11.03 MiB | **6.78 MiB** | **−38%** |
| With 3 user indexes | 20.17 MiB | **8.42 MiB** | **−58%** |
| Cost of those 3 indexes | +9.14 MiB | **+1.64 MiB** | **−82%** |

Per-index cost on this dataset: broken `'$'` shape 3.51 MiB → corrected non-partial 1.15 MiB → **partial 0.46 MiB**.

### 9.6 Verdict per original claim

| Claim | Was | Now |
|---|---|---|
| Value types index the whole document | Confirmed | Fixed — real paths, asserted by regression tests |
| Indexes bloat the database | Confirmed (+83%) | Fixed — index cost down 82%, base database down 38% |
| Indexes can be worse than none | Confirmed (no read gain, ~2× write cost) | Fixed — 75–388× read gains, writes 36–46% faster |
| Redundant built-in indexes | Confirmed (5 b-trees for 2) | Fixed — 2 b-trees, plans proven unchanged |
| Planner never has statistics | Confirmed | Fixed — ANALYZE on index creation, optimize on connect |
| Cross-namespace name collisions | Confirmed | Fixed — hash-suffixed physical names |
| No drop/dedup/rebuild | Confirmed | Fixed — metadata table + DropIndex/ListIndexes |

## Known limitations

- `Contains` / `StartsWith` / `EndsWith` use `LIKE`, which cannot use these indexes. A leading-wildcard `LIKE` is inherently a scan; only `StartsWith` could be made index-able (via a range rewrite) and that is not implemented.
- Range filters (`>`, `>=`, `<`, `<=`) always emit `CAST(... as NUMERIC)`. On a non-numeric property (e.g. `DateTime`) the equality index uses the plain form, so a *range* filter over a DateTime property still will not match its index. Indexing such properties for range queries needs a numeric representation.
- The manual `CreateIndex(string, bool, string, string)` overload only receives a short type name and so cannot build a partial index; it falls back to the non-partial `(FullTypeName, Partition, <expr>)` shape — matched by every query, just larger. Prefer the generic overloads.
- JSONB storage was evaluated and deliberately deferred: it requires a full-table rewrite migration and its main benefit (cheaper repeated JSON parsing) applies mostly to scans, which working indexes now largely eliminate.

## Implications for the redesign buckets

1. **Bucket 2 (correctness)** is justified: unwrapping `Convert` in
   `QueryPropertyPath.BuildPath`/`IsNumeric` makes the expression API emit real paths and
   the `CAST` numeric form, which the plans above show flip numeric filters to index
   searches with zero schema change.
2. **Bucket 3 (partial-index shape)** is confirmed by the spike — adopt
   `(Partition, <expr>) WHERE FullTypeName = '<full name>'`; per-index cost drops ~7.8×
   vs the current broken shape and ~2.5× vs the corrected non-partial shape.
3. **Bucket 4 (sort + built-ins)** is validated by the aligned-ORDER-BY plan and the
   redundancy dump.
4. Statistics work (ANALYZE placement) is needed everywhere — no configuration produced
   `sqlite_stat1` today.
