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

- **Data integrity: `NewtonsoftJsonSerializer` no longer emits a UTF-8 BOM.** The BOM
  made stored JSON malformed for SQLite's `json()` on stricter/older builds — notably
  the SQLCipher bundle — breaking **every** Newtonsoft-serialized write on
  `TychoDB.Encrypted`. Serialization now uses BOM-less UTF-8.

### Performance

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

### Breaking changes

- Filter values are now **bound**, not concatenated. Values containing `'`, `%`, `_`,
  etc. are treated as literal data — correct behavior, but different from before for
  any code that (accidentally or intentionally) relied on the old concatenation.
- `LIKE` metacharacters (`%`, `_`) in `Contains`/`StartsWith`/`EndsWith` values now
  match **literally**; previously they acted as wildcards.
- The raw-string path/index-name overloads now throw `ArgumentException` for inputs
  outside `[A-Za-z0-9_.$\[\]]` (paths) / `[A-Za-z0-9_]` (identifiers).

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

- Performance guidance: prefer `WriteObjectsAsync` for writing many objects — it is
  ~10× faster and ~6× lower-allocation than looping `WriteObjectAsync`, and
  `withTransaction: true` is faster than `false` for bulk writes.
