# TychoDB

<p align="center">
  <img src="images/logo.png" alt="TychoDB Logo" width="200">
</p>

TychoDB is a high-performance .NET library that provides a simple and efficient way to store and retrieve JSON objects using SQLite. It's designed to be a lightweight and fast database solution for .NET applications, with a focus on ease of use and flexibility.

![License](https://img.shields.io/github/license/TheEightBot/TychoDB)
![NuGet](https://img.shields.io/nuget/v/TychoDB)

## Features

- **Simple API**: Intuitive methods for storing, retrieving, and querying JSON data
- **Type Registration**: Flexible registration of C# types with custom ID selectors
- **Advanced Querying**: Rich filtering and sorting capabilities for complex data retrievals
- **Partitioning**: Organize your data using logical partitions
- **Binary Data Support**: Store and retrieve binary large objects (BLOBs)
- **Indexing**: Create indexes on properties for faster query performance
- **Encryption**: Optional AES-256 full-database encryption via SQLCipher (`TychoDB.Encrypted`)
- **Multiple Serialization Options**: Support for System.Text.Json and Newtonsoft.Json
- **Asynchronous Operations**: Full async/await support for all database operations
- **LINQ-like Syntax**: Familiar querying patterns for .NET developers
- **Nested Object Support**: Query and filter on nested object properties
- **Optimized Performance**: Efficient memory usage and connection management

## Installation

Install TychoDB via NuGet:

```bash
dotnet add package TychoDB
```

Depending on your preferred JSON serializer, you can also install one of the following:

```bash
dotnet add package TychoDB.JsonSerializer.SystemTextJson
dotnet add package TychoDB.JsonSerializer.NewtonsoftJson
```

If you need **full database encryption** (powered by SQLCipher), use the encrypted variant instead of the standard package:

```bash
dotnet add package TychoDB.Encrypted
```

## Encryption

TychoDB supports full database encryption via [SQLCipher](https://www.zetetic.net/sqlcipher/). Encryption is provided through a separate NuGet package that replaces the standard SQLite driver with the SQLCipher-backed variant.

### Installing the Encrypted Package

Instead of the standard `TychoDB` package, install `TychoDB.Encrypted`:

```bash
dotnet add package TychoDB.Encrypted
```

> **Note:** `TychoDB` and `TychoDB.Encrypted` are mutually exclusive — only reference one of them in a given project.

### Creating an Encrypted Database

Pass a `password` to the `Tycho` constructor. When a password is provided the underlying SQLCipher driver automatically encrypts the entire database file using AES-256.

```csharp
using TychoDB;

var jsonSerializer = new SystemTextJsonSerializer();

using var db = new Tycho(
        dbPath: "./data",
        jsonSerializer: jsonSerializer,
        password: "your-strong-password")
    .Connect();
```

All subsequent reads and writes are transparently encrypted/decrypted — the rest of the API is identical to the non-encrypted version.

### Opening an Existing Encrypted Database

Supply the same password that was used when the database was first created:

```csharp
using var db = new Tycho(
        dbPath: "./data",
        jsonSerializer: jsonSerializer,
        dbName: "tycho_cache.db",
        password: "your-strong-password")
    .Connect();
```

Providing the wrong password (or no password) will cause a `SqliteException` when the connection is opened.

### Security Considerations

- Store the password securely — use platform secret stores (e.g. Android Keystore, iOS Keychain, Windows DPAPI, or a secrets manager) rather than hard-coding it.
- The `TychoDB.Encrypted` package pulls in `SQLitePCLRaw.bundle_e_sqlcipher`, which links against the SQLCipher native library. Ensure your target platforms are supported by SQLCipher.
- Changing the password of an existing database requires re-keying (outside the scope of the TychoDB API); use raw SQLCipher `PRAGMA rekey` if needed.

## Quick Start

Here's a simple example to get you started:

```csharp
using TychoDB;
using System;
using System.Threading.Tasks;

// Define a class to store
public class Person
{
    public string Id { get; set; }
    public string Name { get; set; }
    public int Age { get; set; }
    public DateTime DateOfBirth { get; set; }
}

public class Program
{
    public static async Task Main()
    {
        // Create a JSON serializer (System.Text.Json implementation)
        var jsonSerializer = new SystemTextJsonSerializer();
        
        // Initialize Tycho and connect to a database.
        //
        // requireTypeRegistration defaults to true, so register each type you store.
        // Registration tells Tycho how to find an object's key, and the query methods
        // (ReadObjectsAsync, CountObjectsAsync, the LINQ surface) throw without it.
        // Pass requireTypeRegistration: false to opt out and supply keys at every call.
        using var db = new Tycho("./data", jsonSerializer)
            .AddTypeRegistration<Person, string>(x => x.Id)
            .Connect();
            
        // Create a person object
        var person = new Person
        {
            Id = "123",
            Name = "John Doe",
            Age = 30,
            DateOfBirth = new DateTime(1992, 5, 15)
        };
            
        // Write the object to the database
        await db.WriteObjectAsync(person, x => x.Id);
            
        // Read the object back by its key
        var retrievedPerson = await db.ReadObjectAsync<Person>("123");
            
        Console.WriteLine($"Retrieved: {retrievedPerson.Name}, Age: {retrievedPerson.Age}");
    }
}
```

## Type Registration

TychoDB provides several ways to register your types, which helps with ID selection and comparison:

```csharp
// Register a type with a specific ID property
db.AddTypeRegistration<Person, string>(x => x.Id);

// Register using a custom key selector function
db.AddTypeRegistrationWithCustomKeySelector<Person>(x => $"{x.Id}_{x.Name}");

// Register using convention-based ID property detection
db.AddTypeRegistration<Person>();
```

After registration, you can use simplified write/read operations:

```csharp
// Write without specifying a key selector
await db.WriteObjectAsync(person);

// Tycho knows how to extract the ID
await db.ReadObjectAsync<Person>(person);
```

`AddTypeRegistration<T>()` finds the id property by convention: `Id`, then `<TypeName>Id`
(matched case-insensitively, and it must have a public getter). A type with no such property
still registers, but without an id mapping — it can then only be reached by keys you supply at
the call site.

> **Supplying a key that disagrees with the registration.** `WriteObjectsAsync(objs, keySelector, …)`
> takes a key selector at the call site and overrides the registration. A row written under a key the
> registration would not produce is unreachable by every by-object overload —
> `ReadObjectAsync(obj)` returns null and `DeleteObjectAsync(obj)` returns **false while the row
> survives**. Under `requireTypeRegistration` (the default), a type registered by id property
> rejects such a write with a `TychoException` naming both keys rather than storing a row you
> cannot reach. Types registered with `AddTypeRegistrationWithCustomKeySelector` have no id
> property to compare against and are unaffected.

## Querying Objects

TychoDB offers rich querying capabilities.

> The examples below use a fuller `Person` than the Quick Start one — assume it also carries
> `DepartmentId`, `IsActive`, `Email`, `Points`, `RegistrationDate`, `FirstName` and `LastName`.
> They also assume the type has been registered, as the Quick Start shows.

### Basic Querying

```csharp
// Read all objects of a type
var allPeople = await db.ReadObjectsAsync<Person>();

// Read by ID
var person = await db.ReadObjectAsync<Person>("123");

// Check if an object exists
var exists = await db.ObjectExistsAsync<Person>("123");

// Count objects
var count = await db.CountObjectsAsync<Person>();

// Read many objects by key in one round trip. Prefer this over a loop of ReadObjectAsync:
// keys lead the primary key, and there is no limit on how many may be passed.
var people = await db.ReadObjectsByKeysAsync<Person>(new object[] { "id-1", "id-2", "id-3" });
```

> **Filtering on the property that is also the Tycho key** (`x => x.Id`) would normally go
> through `JSON_EXTRACT` and scan, because a write may supply its own key selector and Tycho
> cannot otherwise assume the property still matches the stored key.
>
> Under `requireTypeRegistration` — **the default** — with the type registered by id property,
> that assumption is enforced (see [Type Registration](#type-registration)), so `Equals` and
> `In` filters on the id property are answered from the indexed `Key` column instead:
> **79.3 ms → 0.0 ms** for `Equals`, **101.2 ms → 0.2 ms** for `In` over 100 keys, on a
> 250,000-row store. Negated forms and a null comparison value are not rewritten.
>
> If you turn registration off, or register with a custom key selector delegate, reach those
> rows through `ReadObjectAsync` / `ReadObjectsByKeysAsync`, or index the property like any
> other — both measured 0.0 ms against the 71.6 ms scan.

### Filtering

```csharp
// Create a filter for people older than 25
var filter = FilterBuilder<Person>
    .Create()
    .Filter(FilterType.GreaterThan, x => x.Age, 25);

// Apply the filter
var olderPeople = await db.ReadObjectsAsync<Person>(filter: filter);

// Chain multiple filters
var complexFilter = FilterBuilder<Person>
    .Create()
    .Filter(FilterType.GreaterThan, x => x.Age, 25)
    .And()
    .Filter(FilterType.Contains, x => x.Name, "Doe");

// Set membership: one atomic term, so it needs no grouping
var inDepartments = FilterBuilder<Person>
    .Create()
    .Filter(FilterType.In, x => x.DepartmentId, new[] { 33, 47, 51 });

// ...and its negation
var outsideDepartments = FilterBuilder<Person>
    .Create()
    .Filter(FilterType.NotIn, x => x.DepartmentId, new[] { 33, 47 });

// Mixing OR with other terms: group the alternatives so the intent is explicit
var grouped = FilterBuilder<Person>
    .Create()
    .StartGroup()
        .Filter(FilterType.Equals, x => x.DepartmentId, 33)
        .Or()
        .Filter(FilterType.Equals, x => x.DepartmentId, 47)
    .EndGroup()
    .And()
    .Filter(FilterType.GreaterThan, x => x.Age, 25);

// Get a single object matching the filter
var johnDoe = await db.ReadObjectAsync<Person>(filter: complexFilter);

// Get the first object matching the filter
var firstPerson = await db.ReadFirstObjectAsync<Person>(filter: complexFilter);
```

`In` / `NotIn` are worth knowing the edges of:

- An **empty set** matches nothing for `In` and everything for `NotIn`. The term is never
  dropped, which would silently widen the result.
- A **`null` in the set** is matched against a missing or null member with `IS NULL`, which
  SQL's own `IN` would never do.
- `NotIn` **excludes rows whose member is null**, exactly as `NotEquals` already does. Add an
  explicit `Or(Equals(path, null))` term if you want them.
- The **raw-string overload** takes `IEnumerable<object>`, so a value-type collection needs
  `.Cast<object>()`. The expression overload infers the element type and needs no cast.

### Sorting

```csharp
// Create a sort builder
var sort = SortBuilder<Person>
    .Create()
    .OrderBy(SortDirection.Ascending, x => x.Age)
    .OrderBy(SortDirection.Descending, x => x.Name);

// Apply sorting with optional filtering
var sortedPeople = await db.ReadObjectsAsync<Person>(
    filter: complexFilter,
    sort: sort
);

// Limit the number of results
var topFivePeople = await db.ReadObjectsAsync<Person>(
    filter: complexFilter,
    sort: sort,
    top: 5
);
```

### Nested Object Queries

```csharp
public class Address 
{
    public string Street { get; set; }
    public string City { get; set; }
    public string Country { get; set; }
}

public class Customer
{
    public string Id { get; set; }
    public string Name { get; set; }
    public Address HomeAddress { get; set; }
}

// Query nested properties
var usCustomers = await db.ReadObjectsAsync<Customer>(
    filter: FilterBuilder<Customer>
        .Create()
        .Filter(FilterType.Equals, x => x.HomeAddress.Country, "USA")
);

// Extract nested objects
var addresses = await db.ReadObjectsAsync<Customer, Address>(
    x => x.HomeAddress
);

// Extract nested objects with keys
var addressesWithCustomerIds = await db.ReadObjectsWithKeysAsync<Customer, Address>(
    x => x.HomeAddress
);

// Extract specific property from nested objects
var countries = await db.ReadObjectsAsync<Customer, string>(
    x => x.HomeAddress.Country
);
```

## Partitioning

Partitions allow you to organize your data logically:

```csharp
// Write objects to different partitions
await db.WriteObjectAsync(activePerson, x => x.Id, "active_users");
await db.WriteObjectAsync(inactivePerson, x => x.Id, "inactive_users");

// Read from a specific partition
var activeUsers = await db.ReadObjectsAsync<Person>(partition: "active_users");

// Count objects in a partition
var inactiveCount = await db.CountObjectsAsync<Person>(partition: "inactive_users");

// Delete all objects in a partition
var deletedCount = await db.DeleteObjectsAsync("inactive_users");
```

## BLOB Storage

For binary data:

```csharp
// Store a binary file
using var fileStream = File.OpenRead("document.pdf");
await db.WriteBlobAsync(fileStream, "doc_123", "documents");

// Check if a blob exists
var exists = await db.BlobExistsAsync("doc_123", "documents");

// Read a blob
using var blobStream = await db.ReadBlobAsync("doc_123", "documents");
// Use the stream...

// Delete a blob
await db.DeleteBlobAsync("doc_123", "documents");

// Delete all blobs in a partition
var result = await db.DeleteBlobsAsync("documents");
Console.WriteLine($"Deleted {result.Count} blobs");
```

## Performance notes

- **Reach rows by key through the key APIs.** A filter on the key property scans; see the note
  under [Basic Querying](#basic-querying). `ReadObjectsByKeysAsync` fetches a whole batch in one
  round trip and has no limit on batch size.
- **`SystemTextJsonSerializer` deserializes faster.** It implements `IUtf8JsonDeserializer`, so
  rows are handed to it as UTF-8 spans and skip an intermediate stream. Reading a whole
  250,000-row partition measured **254.7 ms** with `SystemTextJsonSerializer` against
  **358.9 ms** with `NewtonsoftJsonSerializer` — about 1.4x. Deserialization dominates any large
  read, so this is usually the largest single lever on read throughput.
- **Index anything you filter or sort on.** An unindexed `JSON_EXTRACT` predicate scans the
  partition; see below.

## Indexing

Create indexes to improve query performance:

```csharp
// Create a simple index on a property
db.CreateIndex<Person>(x => x.Age, "age_index");

// Create an index asynchronously
await db.CreateIndexAsync<Person>(x => x.Name, "name_index");

// Create a composite index on multiple properties
db.CreateIndex<Person>(
    new Expression<Func<Person, object>>[] 
    {
        x => x.Age,
        x => x.Name
    }, 
    "age_name_index"
);

// Create an index on a nested property
db.CreateIndex<Customer>(x => x.HomeAddress.Country, "country_index");

// List the indexes Tycho has created
foreach (var index in db.ListIndexes())
{
    Console.WriteLine($"{index.IndexName} on {index.FullTypeName}");
}

// Drop an index by its logical name
await db.DropIndexAsync<Person>("age_index");
```

### How indexes are built

Indexes are **partial expression indexes** scoped to a single stored type:

```sql
CREATE INDEX idx_age_index_Person_1a2b3c4d
ON JsonValue(Partition, CAST(JSON_EXTRACT(Data, '$.Age') as NUMERIC))
WHERE FullTypeName = 'MyApp.Models.Person';
```

This matters for how they behave:

- **They are scoped to one type.** `JsonValue` holds every stored type, so a partial
  index keeps entries only for rows of the indexed type. Writes of *other* types cost
  nothing to maintain it.
- **The indexed expression must match the query.** SQLite matches expression indexes
  structurally, so TychoDB derives filters, sorts, and index DDL from the same helpers —
  a numeric property is `CAST(... as NUMERIC)` everywhere, a non-numeric property is
  plain `JSON_EXTRACT` everywhere.
- **Calling `CreateIndex` repeatedly is cheap and safe.** Definitions are recorded in a
  `TychoIndex` metadata table; re-declaring an unchanged index is a metadata lookup with
  no DDL. Declaring the same index name with a *different* property rebuilds it and drops
  the old one, and indexes created by older versions are migrated away automatically.
  Declaring your indexes on every app launch is the intended usage.
- **Statistics are refreshed** with a bounded `ANALYZE` after an index is created, so a
  new index is usable by the very next query.

Index names are scoped per type, so the same name can be reused for different types
(including two types that share a short name in different namespaces).

**What indexes cannot help.** `Contains`, `StartsWith`, and `EndsWith` compile to `LIKE`
and always scan. Range filters (`>`, `>=`, `<`, `<=`) compile to a numeric comparison, so
they use an index only on numeric properties.

**Raw-string paths.** The expression overloads infer whether a property is numeric. If you
use the raw-string overloads, say so explicitly, or the ordering/comparison will not match
a numeric index:

```csharp
SortBuilder<Person>.Create()
    .OrderBy(SortDirection.Ascending, "$.Age", isPropertyPathNumeric: true);
```

For the measurements behind this design — including why the previous implementation could
be slower than no index at all — see [docs/indexing-analysis.md](docs/indexing-analysis.md).

## Connection Management

TychoDB offers options for connection management:

```csharp
// Create a database with persistent connection (default)
var db = new Tycho(
    dbPath: "./data",
    jsonSerializer: serializer,
    persistConnection: true
);

// Connect explicitly
db.Connect();

// Or connect asynchronously
await db.ConnectAsync();

// Disconnect when needed
db.Disconnect();

// Or disconnect asynchronously
await db.DisconnectAsync();
```

### Device Performance Profiles

TychoDB tunes its SQLite PRAGMAs (page cache, memory-map, WAL checkpointing) for the
target device class. Choose a profile via the constructor — `Mobile` is the default:

```csharp
// Mobile (default): small page cache (8 MB), 32 MB mmap, frequent WAL checkpoints —
// keeps memory footprint and the WAL file small on phones/tablets.
var mobileDb = new Tycho(dbPath, serializer);

// Desktop: large page cache (64 MB), 256 MB mmap, less frequent checkpoints —
// favors read/write throughput on desktops and servers.
var desktopDb = new Tycho(dbPath, serializer,
    performanceProfile: TychoPerformanceProfile.Desktop);

// Fine-tune individual values (override the profile defaults):
var tunedDb = new Tycho(dbPath, serializer,
    performanceProfile: TychoPerformanceProfile.Desktop,
    cacheSizeKb: 131072,        // 128 MB page cache
    mmapSizeBytes: 0);          // disable memory-mapped I/O
```

> **Note:** memory-mapped I/O (`mmap_size`) is a no-op on the encrypted (SQLCipher)
> build, which does not support mmap.

## Advanced Features

### Batch Operations

```csharp
// Write multiple objects at once
var people = GetManyPeople(); // Returns List<Person>
await db.WriteObjectsAsync(people, x => x.Id);

// Delete multiple objects with a filter
var deletedCount = await db.DeleteObjectsAsync<Person>(
    filter: FilterBuilder<Person>
        .Create()
        .Filter(FilterType.LessThan, x => x.Age, 18)
);

// Delete all objects
await db.DeleteObjectsAsync();
```

### Database Maintenance

```csharp
// Optimize database performance and reduce size
db.Cleanup(shrinkMemory: true, vacuum: true);
```

## LINQ Support

TychoDB offers comprehensive LINQ support for more natural and familiar querying in C#. The LINQ interface lets you write type-safe queries with IntelliSense support and compile-time checking.

### Basic Querying with LINQ

```csharp
// Start a LINQ query for a specific type
var query = db.Query<Person>();

// Apply filters
var activeUsers = await db.Query<Person>()
    .Where(p => p.IsActive)
    .ToListAsync();

// Use multiple conditions
var seniorActiveUsers = await db.Query<Person>()
    .Where(p => p.IsActive && p.Age > 65)
    .ToListAsync();

// String operations
var gmailUsers = await db.Query<Person>()
    .Where(p => p.Email.EndsWith("@gmail.com"))
    .ToListAsync();
```

### Sorting and Paging

```csharp
// Order results
var orderedByAge = await db.Query<Person>()
    .OrderBy(p => p.Age)
    .ToListAsync();

// Order descending
var newestFirst = await db.Query<Person>()
    .OrderByDescending(p => p.RegistrationDate)
    .ToListAsync();

// Multiple ordering criteria
var sortedPeople = await db.Query<Person>()
    .OrderBy(p => p.LastName)
    .ThenBy(p => p.FirstName)
    .ToListAsync();

// Limit results (pagination)
var topFive = await db.Query<Person>()
    .OrderByDescending(p => p.Points)
    .Take(5)
    .ToListAsync();
```

### Single Result Operations

```csharp
// Get first matching result or default
var person = await db.Query<Person>()
    .Where(p => p.Id == "abc123")
    .FirstOrDefaultAsync();

// Get single matching result or default (throws if multiple matches)
var uniquePerson = await db.Query<Person>()
    .Where(p => p.Email == "unique@example.com")
    .SingleOrDefaultAsync();
```

### Aggregation Operations

```csharp
// Count results
int activeCount = await db.Query<Person>()
    .Where(p => p.IsActive)
    .CountAsync();

// Check existence
bool hasInactivePeople = await db.Query<Person>()
    .Where(p => !p.IsActive)
    .AnyAsync();
```

### Working with Partitions

```csharp
// Query within a specific partition
var europeUsers = await db.Query<Person>("europe")
    .Where(p => p.Age > 18)
    .ToListAsync();
```

### Complex Queries

```csharp
// Complex multi-condition queries
var result = await db.Query<Person>()
    .Where(p => p.IsActive && p.Age > 25)
    .Where(p => p.Email.EndsWith("@gmail.com") || p.Points >= 150)
    .OrderByDescending(p => p.Points)
    .Take(10)
    .ToListAsync();
```

### Saving Collections with LINQ Extensions

```csharp
// Save a collection of objects
var people = new List<Person>
{
    new Person { Id = "1", Name = "John Doe", Age = 30 },
    new Person { Id = "2", Name = "Jane Smith", Age = 25 },
    new Person { Id = "3", Name = "Bob Johnson", Age = 40 }
};

// Save all objects with a single call
await db.SaveAllAsync(people);

// Save to a specific partition
await db.SaveAllAsync(people, "active_users");
```

## Performance Considerations

- **Write many objects with `WriteObjectsAsync`, not a loop of `WriteObjectAsync`.**
  The bulk method batches rows into multi-row inserts inside a single transaction —
  roughly **10× faster** and **~6× lower allocation** than calling `WriteObjectAsync`
  in a loop (each single write is its own transaction/commit). Keep the default
  `withTransaction: true` for bulk writes; it is faster than `withTransaction: false`.
- Create indexes for frequently queried properties (`CreateIndex`) using the
  expression-based overloads, and index the same property you filter or sort on.
  Equality filters, numeric range filters, and sorts can all use an index; `Contains`
  and the other `LIKE`-based filters cannot.
- Use the appropriate serializer for your needs. System.Text.Json with a
  `JsonSerializerContext` (source generation) is the recommended default; Newtonsoft
  works but allocates more.
- **Counting is done in the engine.** `CountObjectsAsync` issues `SELECT COUNT(*)` rather than
  counting rows client-side (16.0 ms → 6.5 ms over a 250,000-row partition). A *filtered* count
  is still bounded by whether the filtered property is indexed.
- Consider partitioning for large datasets.
- **Batch your reads by key.** `ReadObjectsByKeysAsync` fetches a whole set in one round trip
  and takes the connection gate once instead of once per key — 2.1–2.7x faster end to end than
  a loop of `ReadObjectAsync`, and the gap widens under contention.
- **Concurrency is serialized.** All database access runs on a single connection behind a gate,
  so concurrent callers are executed one operation at a time. Prefer fewer, larger operations
  (`WriteObjectsAsync`, `ReadObjectsByKeysAsync`) over many small concurrent ones; the
  `useConnectionPooling` constructor parameter controls the underlying driver pool, not this
  serialization.

## Security Considerations (Querying)

Filter comparison values are always bound as SQL parameters, so it is safe to pass
user-supplied values into filters. When you use the **raw-string overloads** —
`FilterBuilder.Filter(FilterType, string propertyPath, …)`,
`SortBuilder.OrderBy(SortDirection, string)`, or the string-based `CreateIndex` — the
property path and index name are validated against a strict grammar and will throw
`ArgumentException` if they contain anything other than letters, digits, `_`, `.`, `$`,
`[`, `]` (paths) or a valid identifier (index names). Prefer the expression-based
overloads (`x => x.Property`) where possible.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- SQLite for providing the underlying database engine
- The .NET community for support and inspiration
