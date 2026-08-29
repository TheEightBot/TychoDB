# Changelog

## 5.0.0 (unreleased) — Security & performance hardening

This release closes a critical SQL-injection vector and a data-integrity bug, and
adds proven write/startup performance improvements. It is a **major** version because
some query behavior changes (see Breaking changes).

### Security

- **Critical: SQL injection via filter values fixed.** Filter comparison values were
  concatenated directly into the SQL text, allowing full data disclosure and
  destruction (a stacked-statement value on a *read* could `DELETE` rows). All filter
  values are now bound as parameters. Genuine numeric/boolean CLR values are emitted
  as validated literals; everything else is parameterized.
- **Path & identifier validation.** The raw-string overloads
  `FilterBuilder.Filter(FilterType, string propertyPath, …)`,
  `SortBuilder.OrderBy(SortDirection, string)`, and `CreateIndex(…)` (property path,
  object type name, and index name) now validate their inputs against a strict grammar
  and throw `ArgumentException` on anything that could be an injection vector.
- **LIKE escaping.** `Contains`/`StartsWith`/`EndsWith` now escape `%`, `_`, and `\`
  with an explicit `ESCAPE` clause, so those characters match literally and cannot be
  used to force full-table scans.
- The `CA2100` analyzer suppressions were narrowed and justified (values are
  parameterized; only validated identifiers/paths remain concatenated).

### Fixed

- **Critical: an ungrouped `Or()` escaped the partition and type predicates.** The caller's
  filter was appended to the generated `WHERE` clause without being bound as a unit:

  ```sql
  WHERE FullTypeName = ? AND Partition = ? AND <term1> OR <term2>
  ```

  `AND` binds tighter than `OR`, so SQL read that as
  `(FullTypeName = ? AND Partition = ? AND term1) OR (term2)` — every term after the first
  `Or()` was matched against **the whole table**. A two-term `Or()` returned rows from other
  partitions, and rows of *other stored types*, which the reader then deserialized as `T` with
  no error. The same clause is used by `DeleteObjectsAsync`, so an ungrouped `Or()` could
  **delete rows in other partitions and of other types**, and by `CountObjectsAsync`, which
  over-counted. The caller's filter is now emitted inside its own parentheses. Losing the
  `Partition` predicate also cost the partition-prefixed indexes, so this was a large
  performance regression as well as a correctness one; grouped OR-chains now use the index.
  Filters already wrapped in `StartGroup()`/`EndGroup()` were unaffected and still are.
- **LINQ predicates lost their own precedence.** `TychoQueryable` translated `&&` and `||` by
  emitting their operands flat, so `Where(x => (x.A || x.B) && x.C)` became `A OR B AND C` —
  read by SQL as `A OR (B AND C)` — and returned rows matching only `A` despite their failing
  `C`. Each composite boolean node is now emitted in its own group. (`.Where(a).Where(b)`
  chains were affected the same way when either predicate contained an `||`.)
- **Data integrity: filter values are now compared in the form the serializer wrote.**
  A filter value was rendered with `ToString()`, which is not how the serializer stores it for
  every type. The clearest case is an enum: both serializers write it as a **number** by
  default, so `Filter(Equals, x => x.StoreAllocation, StoreAllocationType.Produce)` compared
  the stored `0` against the text `"Produce"` and matched **nothing**, while the
  `(int)`-cast workaround matched. With a string-enum converter the name happened to line up —
  unless a naming policy renamed it, which broke it again. A full sweep of the scalar type
  surface found five types affected on both serializers: `enum`, enums renamed by a converter
  or naming policy, nullable enums, `DateOnly`, and `TimeOnly`. The two date types were
  additionally **culture-dependent** — `DateOnly.ToString()` yields `8/28/2026` under `en-US`
  against a stored `2026-08-28` — so the same code matched or failed depending on the
  machine's locale. Values are now resolved through the new `IJsonValueResolver`.
  `string`, `bool`, the numeric primitives, `DateTime` and `DateTimeOffset` are unchanged;
  `Guid`, `TimeSpan`, `Uri` and `char` already agreed with their JSON form and still do.
- **Data integrity: property expressions now honour the serializer's member names.**
  Expressions such as `x => x.Description` built the JSON path from the **CLR** property
  name (`$.Description`), ignoring `PropertyNamingPolicy`, `[JsonPropertyName]`,
  `[JsonProperty]`, and Newtonsoft contract resolvers. Any serializer configuration that
  renames members therefore produced a path matching nothing in the stored document.
  Because an unmatched JSON path is not an error in SQLite, this failed **silently**:
  `ReadObjectsAsync` returned zero rows, `SortBuilder.OrderBy` did not sort, and
  `CreateIndex`/`CreateIndexAsync` built indexes that never matched a row — with no
  exception and nothing logged. Serializers now report their JSON member names via the
  new `IJsonPropertyNameResolver`, and expression paths are resolved against them.
- **The projection overloads now handle every JSON value kind.**
  `ReadObjectsAsync<TIn, TOut>` / `ReadObjectsWithKeysAsync<TIn, TOut>` selected the member
  with `JSON_EXTRACT`, which converts the match to an *SQL* value: a JSON string was
  unwrapped to bare text (`target`, not `"target"`) and `true`/`false` collapsed to the
  integers `1`/`0`. Handing those to a JSON deserializer failed — projecting a `string`
  threw "invalid JSON literal", and projecting a `bool` threw "cannot get the value of a
  token type 'Number' as a boolean" under `System.Text.Json` (Newtonsoft silently coerced
  `1` to `true`). Projection now uses SQLite's `->` operator, which returns the JSON
  representation, so strings, numbers, booleans, objects and arrays all round-trip.
- **Projecting a member that is absent no longer throws.** A member that was never written
  (or stored as JSON null) produced SQL NULL, and the reader called `GetStream` on it —
  failing with `InvalidOperationException: The data is NULL at ordinal 2`. An absent member
  is now reported as `default(TOut)`: `null` for reference and nullable types, zero/`false`
  for value types.
- **Data integrity: `NewtonsoftJsonSerializer` no longer emits a UTF-8 BOM.** The BOM
  made stored JSON malformed for SQLite's `json()` on stricter/older builds — notably
  the SQLCipher bundle — breaking **every** Newtonsoft-serialized write on
  `TychoDB.Encrypted`. Serialization now uses BOM-less UTF-8.

### Indexing overhaul

Indexing was measured end-to-end and rebuilt. Full evidence, before/after benchmarks and
query plans: [docs/indexing-analysis.md](docs/indexing-analysis.md).

- **Critical: indexes on value-type properties indexed the entire document.**
  `CreateIndex<T>(x => x.Age, …)` — and every `int`, `long`, `double`, `bool`,
  `DateTime`, `Guid`, enum, or nullable property — generated
  `JSON_EXTRACT(Data, '$')`, storing a **complete second copy of every document** in the
  index. Those indexes could never be used by any query, so they cost storage and write
  throughput for zero benefit. The boxing `Convert` node introduced by
  `Expression<Func<T, object>>` is now unwrapped, producing the real property path and
  the correct numeric form.
- **Partial expression indexes.** Indexes are now
  `ON JsonValue(Partition, <expr>…) WHERE FullTypeName = '<type>'`: scoped to one stored
  type, led by the `Partition` column every query constrains. Index size on the
  benchmark dataset dropped ~82%.
- **Sorting can now use an index.** `SortBuilder` emitted `Data ->> '$.x'`, which can
  never match a `JSON_EXTRACT` expression index, so every sorted read built a temporary
  b-tree. It now emits the same expression the index is built on.
- **Redundant built-in indexes removed.** Three of the four `JsonValue` indexes and the
  `StreamValue` index duplicated the primary-key autoindexes or a prefix of another
  index. They are dropped (idempotently, so existing databases shed them on connect),
  cutting maintained b-trees from five to two. Query plans are unchanged, which is
  covered by a regression test.
- **Index metadata, dedup and migration.** A `TychoIndex` table records each index, so
  re-declaring an unchanged index is a cheap metadata lookup, changing an index's
  definition rebuilds it and drops the stale b-tree, and indexes from older versions are
  migrated automatically on the next `CreateIndex` call.
- **Cross-namespace index-name collisions fixed.** Physical index names carry a stable
  hash of the full type name. Previously two same-named types in different namespaces
  shared one index name and the second `CREATE INDEX IF NOT EXISTS` silently did nothing.
- **Planner statistics.** A bounded `ANALYZE` runs after an index is created, and
  `PRAGMA optimize` now also runs on connect. Previously `sqlite_stat1` was never
  created at all, so the planner always ran on default heuristics.
- **New API:** `DropIndex<T>`, `DropIndexAsync<T>`, and `ListIndexes()` (additive), plus
  `SortBuilder.OrderBy(SortDirection, string propertyPath, bool isPropertyPathNumeric)`
  so the raw-string sort overload can emit the numeric form its index is built on —
  matching the existing raw-string `FilterBuilder.Filter` overload.
- **Closed generic types are indexable.** Derived type names for closed generics contain
  characters that are not valid in a SQL identifier (e.g. `Dictionary_2__String,Int32__`);
  they are now normalized instead of rejected. Caller-supplied identifiers are still
  validated strictly.

Measured on 25,000 rows: numeric equality **6,320 → 18.5 µs**, numeric range
**6,372 → 85 µs**, sorts **~6,700 → ~53 µs** (all previously gained nothing from an
index); batch writes **−44%**; database file with three indexes **20.2 → 8.4 MiB**.

### Performance

- **`CountObjectsAsync` no longer counts rows on the client.** It issued
  `SELECT 1 FROM JsonValue WHERE …` and incremented a counter once per matching row, costing a
  reader round trip per row. It now issues `SELECT COUNT(*)` and reads the single scalar:
  **16.0 ms → 6.5 ms** counting a 250,000-row partition (2.5x). The same query backs the
  pre-count a progress-reporting `ReadObjectsAsync` performs, so progress-enabled reads pay
  half of what they did. A *filtered* count is still bounded by whether the filtered property
  is indexed — counting a 1-in-200 selective filter on an unindexed path takes ~79 ms on the
  same store, essentially all of it the `JSON_EXTRACT` scan.

- **`PRAGMA optimize` on connect and disconnect.** `Connect`/`ConnectAsync` and
  `Disconnect`/`DisconnectAsync`/`Dispose` run SQLite's recommended `PRAGMA optimize`
  (bounded by `analysis_limit = 400`) so the query planner keeps fresh statistics and
  continues to choose indexes — including expression indexes over `JSON_EXTRACT`. The
  connect-time call matters for long-lived mobile apps that never cleanly disconnect.
- **Bounded WAL on mobile.** The `Mobile` profile sets `journal_size_limit = 8 MB` so
  the WAL file truncates after a checkpoint instead of growing unbounded; `Desktop`
  leaves it unlimited.
- **`Cleanup` truncates the WAL.** `Cleanup(vacuum: true)` now runs
  `wal_checkpoint(TRUNCATE)` after reclaiming free space, returning the WAL file's space
  to disk as well.
- **Device-aware SQLite tuning.** A new `TychoPerformanceProfile` (`Mobile` /
  `Desktop`) constructor parameter selects a preset of PRAGMA tuning, with optional
  `cacheSizeKb` / `mmapSizeBytes` overrides. `Mobile` (the default) uses a small page
  cache (8 MB), a modest 32 MB memory-map, and frequent WAL checkpoints to keep memory
  and the WAL file small; `Desktop` uses a 64 MB cache, a 256 MB memory-map, and less
  frequent checkpoints for read/write throughput. Previously a single fixed set
  (16 MB cache / 128 MB mmap) was used for all devices.
- `Cache=Shared` was removed from the connection string; it contradicted
  `locking_mode = EXCLUSIVE` (single persistent connection), so a private cache is used.
- **Bulk writes batched.** `WriteObjectsAsync` now writes rows in multi-row
  `INSERT OR REPLACE` batches (100 rows/execution) and no longer runs a redundant
  `SELECT last_insert_rowid()` per row. Measured (System.Text.Json, 1000 objects):
  **−16% time, −62% allocations** (1.66 MB → 631 KB). Individual writes: **−21% time**.
- **`cache_size`/`mmap_size` PRAGMAs applied.** The intended page-cache tuning was
  defined but never wired up; it is now applied on connect (helps datasets larger than
  the default cache).
- **Lighter connection gate.** The per-operation `ConcurrencyLimiter` was replaced with
  a `SemaphoreSlim(1,1)`, which is lighter and also genuinely serializes *synchronous*
  callers (the previous `AttemptAcquire()` path did not).
- **Cheaper connect.** The SQLite JSON/version support check is now performed once per
  process instead of on every `Connect()` (**−16%** connect time).
- Single-object writes avoid an extra `List` allocation (`IList<T>` fast path).

### Added

- **`ReadObjectsByKeysAsync<T>(keys, partition, sort, …)`.** Reads a batch of keys in one round
  trip. The key set is bound as a **single JSON array** expanded by `JSON_EACH`, not as one
  parameter per key, so there is no `SQLITE_MAX_VARIABLE_NUMBER` ceiling (999 on older SQLite
  builds), no chunking for callers to think about, and one prepared statement regardless of
  batch size. Keys lead the primary key, so each is a primary-key probe. Measured against a
  loop of `ReadObjectAsync` on a 250,000-row store (best of five, after warm-up):

  | batch | looped `ReadObjectAsync` | `ReadObjectsByKeysAsync` |
  |------:|------------------------:|-------------------------:|
  |   200 |                  1.9 ms |                   0.9 ms |
  |   999 |                 10.6 ms |                   4.5 ms |
  | 4,949 |                 36.8 ms |                  16.9 ms |
  |23,784 |                183.2 ms |                  67.3 ms |

  That is 2.1–2.7x end to end. Both figures include deserialization, which is identical between
  them and dominates what is left — the query alone is 27.5 ms at 23,784 keys. The `JSON_EACH`
  shape was chosen by measurement: a single `IN (@p0…@pN)` collapses at scale (1,297.7 ms at
  23,784 keys, because the statement text and plan grow with the batch), a chunked `IN` is
  91.8 ms, and a temp-table join carries ~40 ms of fixed setup. Keys not present are simply
  absent from the result.
- **`FilterType.In` and `FilterType.NotIn`.** Set membership as a single atomic term, via new
  `Filter` overloads taking an `IEnumerable`:

  ```csharp
  FilterBuilder<Item>.Create().Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47 });
  ```

  It renders to `<path> IN (…)` through the same numeric `CAST` the scalar comparisons use, so
  an expression index over the property still serves the query. Being one term, it cannot be
  mis-grouped the way an `Or()` chain can. Details:
  - Duplicate values are removed; the caller's order is preserved.
  - An empty set matches nothing for `In` and everything for `NotIn` — never `IN ()`, which is
    a syntax error, and never a silently dropped term, which would widen the result set.
  - A `null` in the set is matched against a missing or null member with `IS NULL`, which SQL's
    own `IN` would never do. `NotIn` keeps SQL's semantics for rows whose member is null: they
    are not returned, exactly as `NotEquals` already behaves.
  - Longer lists are split across several `IN` terms rather than exceeding
    `SQLITE_MAX_VARIABLE_NUMBER`, which is only 999 on older SQLite builds, so a large set works
    regardless of which build the host application ships.
  - The raw-path overload takes `IEnumerable<object>` rather than a generic parameter on
    purpose: a generic overload there captures an ordinary `string` comparison value, since
    `string` is an `IEnumerable<char>`. A value-type collection needs `Cast<object>()`; the
    expression overload infers the element type from the property and needs no cast.
- **`IJsonValueResolver`.** A second optional serializer capability, feature-detected the same
  way, reporting the scalar form a CLR value takes in JSON so filter comparisons are made
  against what was stored. Implemented by `SystemTextJsonSerializer` and
  `NewtonsoftJsonSerializer`; serializers that do not implement it fall back to `ToString()`.
- **`IJsonPropertyNameResolver`.** An optional serializer capability (feature-detected,
  like `IUtf8JsonDeserializer`) that reports the JSON member name a CLR property is
  serialized as. Implemented by `SystemTextJsonSerializer` and `NewtonsoftJsonSerializer`.
  Third-party serializers that do not implement it keep working unchanged, falling back to
  CLR property names. Resolved names are validated before being emitted into a JSON path,
  so a name carrying a quote is rejected with `ArgumentException` rather than escaping the
  SQL literal.

### Breaking changes

- **Passing a collection to a scalar `FilterType` now throws `ArgumentException`.** Adding the
  `IEnumerable` overloads changes overload resolution for a collection argument, which
  previously bound to `object` and was rendered as `ToString()` (`"System.Int32[]"`), matching
  nothing silently. Use `FilterType.In`. A literal `null` argument also now binds to the new
  overload, but keeps its old meaning — `Filter(Equals, x => x.Value, null)` is still the
  null comparison.
- **An ungrouped `Or()` now means what it reads as.** Code that (unknowingly) depended on the
  leaked rows — most plausibly a query written against a single-partition, single-type database
  where the bug was invisible — returns fewer rows now. This is the fix, not a regression.
- **Enum, `DateOnly` and `TimeOnly` filter values now compare against their JSON form.** Code
  that worked around the enum mismatch by casting to `(int)` keeps working. Code that relied on
  a string-enum converter's name matching by coincidence also keeps working, and now stays
  correct when a naming policy renames the member.
- **Property expressions now resolve to the serializer's JSON member names.** Code using
  a naming policy or renaming attributes will start matching rows, sorting, and indexing
  correctly — but the emitted SQL paths change. Indexes created by earlier versions on
  the CLR-named path (e.g. `$.Description`) are now unused and should be dropped and
  recreated. Applications that worked around the bug by storing PascalCase JSON while
  configuring a camelCase policy will see behavior change.
- Filter values are now **bound**, not concatenated. Values containing `'`, `%`, `_`,
  etc. are treated as literal data — correct behavior, but different from before for
  any code that (accidentally or intentionally) relied on the old concatenation.
- `LIKE` metacharacters (`%`, `_`) in `Contains`/`StartsWith`/`EndsWith` values now
  match **literally**; previously they acted as wildcards.
- The raw-string path/index-name overloads now throw `ArgumentException` for inputs
  outside `[A-Za-z0-9_.$\[\]]` (paths) / `[A-Za-z0-9_]` (identifiers).
- **Index DDL and physical index names changed.** Indexes are rebuilt in the new partial
  shape the next time `CreateIndex` is called for them, and the old index is dropped;
  no application change is required, but the first launch after upgrading pays a
  one-time rebuild. Code that inspected TychoDB's index names directly in `sqlite_master`
  must account for the hash suffix — use `ListIndexes()` instead.
- **Sort SQL changed** from `Data ->> '$.x'` to `JSON_EXTRACT(Data, '$.x')` (and
  `CAST(… as NUMERIC)` for numeric properties). Ordering of scalar values is unchanged;
  this is what allows sorts to use an index.
- The three redundant `JsonValue` indexes and `idx_streamvalue_key_partition` are dropped
  on connect. Applications that created their own indexes with those exact names would
  lose them.

### Packaging

- **`TychoDB.Encrypted` now uses the same SQLite version as `TychoDB`.** The encrypted
  build's `Microsoft.Data.Sqlite.Core` was aligned to 9.0.8 (was 8.0.0) and the
  SQLCipher bundle bumped to 2.1.10 (was 2.1.4), so the encrypted package no longer
  ships an older SQLite engine than the standard one.
- The serializer packages (`TychoDB.JsonSerializer`,
  `TychoDB.JsonSerializer.SystemTextJson`, `TychoDB.JsonSerializer.NewtonsoftJson`) now
  multi-target `netstandard2.1;net9.0`.
- **The legacy `Tycho` / older-TFM (netstandard2.1;net7.0) package is not shipped in
  this release.** Its shared source relies on net9-only APIs (`System.Threading.Lock`,
  `FrozenDictionary`) and it had not been building. Reviving it for Xamarin/MAUI
  (via portable-type fallbacks) is tracked as follow-up work. `net9.0` `TychoDB` and
  `TychoDB.Encrypted` are the supported packages.

### Notes

- **Serializer choice is the largest remaining lever on read throughput.** Reading a whole
  250,000-row partition measured 254.7 ms with `SystemTextJsonSerializer` against 358.9 ms with
  `NewtonsoftJsonSerializer` (~1.4x), because the former implements `IUtf8JsonDeserializer` and
  receives rows as UTF-8 spans. Deserialization dominates any large read: of the 67.3 ms
  `ReadObjectsByKeysAsync` takes for 23,784 keys, only 27.5 ms is the query.

- Performance guidance: prefer `WriteObjectsAsync` for writing many objects — it is
  ~10× faster and ~6× lower-allocation than looping `WriteObjectAsync`, and
  `withTransaction: true` is faster than `false` for bulk writes.
