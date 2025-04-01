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
- **Multiple Serialization Options**: Support for System.Text.Json, Newtonsoft.Json, and MessagePack
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
dotnet add package TychoDB.JsonSerializer.MessagePack
```

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
        
        // Initialize Tycho and connect to a database
        using var db = new Tycho("./data", jsonSerializer)
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

## Querying Objects

TychoDB offers rich querying capabilities:

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
```

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

// Get a single object matching the filter
var johnDoe = await db.ReadObjectAsync<Person>(filter: complexFilter);

// Get the first object matching the filter
var firstPerson = await db.ReadFirstObjectAsync<Person>(filter: complexFilter);
```

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
```

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

- Use batch operations when dealing with multiple objects
- Create indexes for frequently queried properties
- Use the appropriate serializer for your needs (MessagePack for best performance)
- Consider partitioning for large datasets
- Use connection pooling for multi-threaded applications

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- SQLite for providing the underlying database engine
- The .NET community for support and inspiration
