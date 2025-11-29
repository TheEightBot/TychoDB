# TychoDB Performance Optimization Checklist

This document tracks performance and memory optimization work for TychoDB and TychoDB.JsonSerializer.\* projects.

## Overview

The goal is to identify and implement optimizations that:

-   Convert classes to structs where appropriate (reducing heap allocations)
-   Eliminate closures (reducing delegate allocations)
-   Aid garbage collection without being aggressive
-   Apply other critical performance and memory improvements

---

## Optimization Items

### 1. Filter.cs - Convert Filter class to readonly struct

-   [x] **Status: Completed**
-   **File:** `TychoDB/Filter.cs`
-   **Current:** `Filter` is a class with multiple constructors
-   **Proposed:** Convert to `readonly struct` with required properties
-   **Rationale:**
    -   Filter instances are short-lived, created during query building
    -   They are stored in a `List<Filter>` which boxes structs, BUT the overhead of heap allocation for many small Filter objects is worse
    -   Using `readonly struct` ensures no defensive copies
-   **Risk:** Low - internal class, immutable data
-   **Commit:** `93531c0`

### 2. SortInfo.cs - Convert SortInfo class to readonly struct

-   [x] **Status: Completed**
-   **File:** `TychoDB/SortInfo.cs`
-   **Current:** `SortInfo` is a class with a constructor
-   **Proposed:** Convert to `readonly struct`
-   **Rationale:**
    -   Small, immutable data structure (SortDirection enum + string path)
    -   Short-lived, used only during query building
    -   Reduced heap allocations
-   **Risk:** Low - internal class, simple structure
-   **Commit:** `852ba97`

### 3. QueryPropertyPath.cs - Optimize PropertyPathVisitor to use ValueListBuilder

-   [x] **Status: Completed**
-   **File:** `TychoDB/QueryPropertyPath.cs`
-   **Current:** Uses `List<string>` in `PropertyPathVisitor`
-   **Proposed:** Use stack-allocated `Span<string>` or `ValueStringBuilder` pattern with pooling
-   **Rationale:**
    -   Property paths are typically short (1-4 segments)
    -   Avoid List allocation for short paths
-   **Risk:** Low - internal implementation detail
-   **Commit:** `eaf1f9a`**
    -   Property paths are typically short (1-3 segments)
    -   Avoid List allocation for short paths
-   **Risk:** Low - internal implementation detail

### 4. FilterBuilder.cs - Use Span-based string building

-   [x] **Status: Completed**
-   **File:** `TychoDB/FilterBuilder.cs`
-   **Current:** Uses string interpolation extensively in `Build()` method
-   **Proposed:**
    -   Pre-allocate string constants for repeated SQL fragments
    -   Replace string interpolation with direct `StringBuilder.Append()` calls
    -   Extract helper methods with `MethodImpl(MethodImplOptions.AggressiveInlining)`
    -   Replace `_filters.Any()` with `_filters.Count > 0` to avoid LINQ overhead
-   **Rationale:**
    -   Reduces intermediate string allocations from interpolation
    -   Constant strings are interned and reused
    -   Helper methods improve code organization while being inlined
-   **Risk:** Low - internal implementation
-   **Commit:** `3fc77f7`

### 5. SortBuilder.cs - Optimize string building in Build()

-   [x] **Status: Completed**
-   **File:** `TychoDB/SortBuilder.cs`
-   **Current:** Uses LINQ `.Select()` with string interpolation and `string.Join`
-   **Proposed:**
    -   Pre-allocate string constants for SQL fragments
    -   Replace LINQ Select + AppendJoin with direct for loop and Append calls
    -   Remove System.Linq dependency
-   **Rationale:**
    -   Eliminates LINQ overhead and intermediate string array from Select
    -   Direct StringBuilder manipulation is more efficient
-   **Risk:** Low - internal implementation
-   **Commit:** `159adac`

### 6. Tycho.cs - Eliminate closure allocations in WithConnectionBlockAsync delegates

-   [ ] **Status: Deferred**
-   **File:** `TychoDB/Tycho.cs`
-   **Current:** Many methods pass lambdas to `WithConnectionBlockAsync` that capture local variables (closures)
-   **Proposed:**
    -   Create static lambda overloads that accept state parameter
    -   Use tuple or struct to pass captured state without closure allocation
-   **Rationale:**
    -   Each closure creates a hidden class instance on the heap
    -   High-frequency operations like ReadObjectsAsync benefit most
-   **Risk:** Medium - requires careful refactoring to maintain behavior
-   **Decision:** Deferred - 20+ closure sites with varying state; database I/O dominates performance. Low ROI for the refactoring effort.

### 7. Tycho.cs - Cache RegisteredTypeInformation lookups

-   [x] **Status: Completed**
-   **File:** `TychoDB/Tycho.cs`, `TychoDB/TypeCache.cs` (new file)
-   **Current:** `CheckHasRegisteredType<T>()` uses `typeof(T)` which allocates
-   **Proposed:**
    -   Created static generic `TypeCache<T>` class with cached FullName
    -   Replaced 6 occurrences of `typeof(T).FullName` with `TypeCache<T>.FullName`
-   **Rationale:**
    -   `typeof(T)` is cheap but `.FullName` creates a string each time
    -   Static generic pattern means each type T gets its own cached string once
-   **Risk:** Low - optimization only
-   **Commit:** TBD

### 8. ObjectExtensions.cs - Cache GetSafeTypeName results

-   [ ] **Status: Pending Approval**
-   **File:** `TychoDB/ObjectExtensions.cs`
-   **Current:** `GetSafeTypeName()` performs string manipulation on each call
-   **Proposed:** Add `ConcurrentDictionary<Type, string>` cache for computed type names
-   **Rationale:**
    -   Type names are immutable; caching eliminates repeated computation
    -   Same type will have same safe name always
-   **Risk:** Low - pure optimization

### 9. RegisteredTypeInformation.cs - Consider using init-only properties

-   [ ] **Status: Pending Approval**
-   **File:** `TychoDB/RegisteredTypeInformation.cs`
-   **Current:** Record with private setters
-   **Proposed:**
    -   Use `init` accessors where applicable
    -   Consider if this could be a `readonly record struct` (likely not due to Delegate storage)
-   **Rationale:**
    -   Minor optimization for clarity and potential JIT optimization
-   **Risk:** Low - API remains the same

### 10. NewtonsoftJsonSerializer.cs - Improve buffer management

-   [ ] **Status: Pending Approval**
-   **File:** `TychoDB.JsonSerializer.NewtonsoftJson/NewtonsoftJsonSerializer.cs`
-   **Current:** Allocates new `byte[]` result array after copying from pooled buffer
-   **Proposed:**
    -   Return `Memory<byte>` or `ReadOnlyMemory<byte>` to avoid copy
    -   Or use `RecyclableMemoryStream` from Microsoft.IO.RecyclableMemoryStream
-   **Rationale:**
    -   Current implementation copies from pooled buffer to new array, negating some pooling benefits
-   **Risk:** Medium - would require interface change to `IJsonSerializer`

### 11. SystemTextJsonSerializer.cs - Add JsonSerializerContext caching

-   [ ] **Status: Pending Approval**
-   **File:** `TychoDB.JsonSerializer.SystemTextJson/SystemTextJsonSerializer.cs`
-   **Current:** Good use of source-generated serializers via `JsonTypeInfo`
-   **Proposed:**
    -   Ensure all commonly used types have pre-generated `JsonTypeInfo`
    -   Add documentation encouraging users to provide source-generated contexts
-   **Rationale:**
    -   Source generation eliminates reflection overhead
-   **Risk:** Low - documentation/best practices

### 12. TychoQueryable.cs - Optimize expression tree processing

-   [ ] **Status: Pending Approval**
-   **File:** `TychoDB/TychoQueryable.cs`
-   **Current:** `BuildFilterFromExpressionInternal` uses reflection via `GetMethod`
-   **Proposed:**
    -   Cache the `MethodInfo` for `CreateFilterWithType` as a static field
    -   Use expression compilation caching for repeated queries
-   **Rationale:**
    -   Reflection is expensive; caching reduces overhead
-   **Risk:** Low - internal optimization

### 13. TychoQueryable.cs - Avoid repeated Expression.Lambda creation

-   [ ] **Status: Pending Approval**
-   **File:** `TychoDB/TychoQueryable.cs`
-   **Current:** Creates new `Expression.Lambda` and `Expression.Parameter` repeatedly
-   **Proposed:**
    -   Cache the parameter expression for type T
    -   Reuse compiled expressions where possible
-   **Rationale:**
    -   Expression tree creation has overhead that can be avoided
-   **Risk:** Low - internal optimization

### 14. ProgressStream.cs - Make it a struct wrapper or use span-based approach

-   [ ] **Status: Pending Approval**
-   **File:** `TychoDB/ProgressStream.cs`
-   **Current:** Class that wraps another stream
-   **Proposed:**
    -   Keep as class (Stream inheritance requires class)
    -   Add `sealed` keyword to enable devirtualization
-   **Rationale:**
    -   `sealed` allows JIT to devirtualize virtual calls
-   **Risk:** Very Low - adding `sealed` is safe

---

## Testing Requirements

After each optimization:

1. Run all unit tests to ensure functionality is preserved
2. Verify no breaking changes to public API
3. Consider running benchmarks if available

---

## Approval Status

**Please review the items above and confirm which optimizations should proceed.**

Once approved, each item will be:

1. Implemented
2. Tested
3. Committed individually

---

## Progress Tracking

| Item | Description                       | Status      |
| ---- | --------------------------------- | ----------- |
| 1    | Filter class to struct            | ✅ Complete |
| 2    | SortInfo class to struct          | ✅ Complete |
| 3   | PropertyPathVisitor optimization     | ✅ Complete |
| 4    | FilterBuilder string building     | ⏳ Pending  |
| 5    | SortBuilder string building       | ⏳ Pending  |
| 6    | Closure elimination in Tycho.cs   | ⏳ Pending  |
| 7    | TypeCache for type lookups        | ⏳ Pending  |
| 8    | GetSafeTypeName caching           | ⏳ Pending  |
| 9    | RegisteredTypeInformation init    | ⏳ Pending  |
| 10   | NewtonsoftJsonSerializer buffers  | ⏳ Pending  |
| 11   | SystemTextJsonSerializer docs     | ⏳ Pending  |
| 12   | TychoQueryable reflection caching | ⏳ Pending  |
| 13   | Expression.Lambda caching         | ⏳ Pending  |
| 14   | ProgressStream sealed             | ⏳ Pending  |

---

_Last updated: November 29, 2025_
