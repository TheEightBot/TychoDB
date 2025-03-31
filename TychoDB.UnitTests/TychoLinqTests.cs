using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TychoDB.UnitTests;

[TestClass]
public class TychoLinqTests
{
    private static readonly IJsonSerializer _systemTextJsonSerializer =
        new SystemTextJsonSerializer(
            jsonTypeSerializers:
            new Dictionary<Type, System.Text.Json.Serialization.Metadata.JsonTypeInfo>
            {
                [typeof(User)] = TestJsonContext.Default.TestClassA,
            });

    [TestMethod]
    public async Task Query_SimpleFilter_ShouldReturnFilteredResults()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users with varying properties
        var users = new List<User>
        {
            new User
            {
                UserId = 1,
                FirstName = "John",
                LastName = "Doe",
                Age = 25,
                IsActive = true,
                Email = "john.doe@example.com",
                IsAdmin = false,
                Points = 100,
            },
            new User
            {
                UserId = 2,
                FirstName = "Jane",
                LastName = "Smith",
                Age = 30,
                IsActive = true,
                Email = "jane.smith@example.com",
                IsAdmin = true,
                Points = 200,
            },
            new User
            {
                UserId = 3,
                FirstName = "Alice",
                LastName = "Johnson",
                Age = 22,
                IsActive = false,
                Email = "alice@gmail.com",
                IsAdmin = false,
                Points = 50,
            },
            new User
            {
                UserId = 4,
                FirstName = "Bob",
                LastName = "Brown",
                Age = 45,
                IsActive = true,
                Email = "bob.brown@example.com",
                IsAdmin = false,
                Points = 300,
            },
            new User
            {
                UserId = 5,
                FirstName = "Charlie",
                LastName = "Wilson",
                Age = 35,
                IsActive = true,
                Email = "charlie@gmail.com",
                IsAdmin = false,
                Points = 150,
            },
            new User
            {
                UserId = 6,
                FirstName = "Diana",
                LastName = "Miller",
                Age = 28,
                IsActive = false,
                Email = "diana@example.com",
                IsAdmin = false,
                Points = 75,
            },
        };

        // Save all users
        await db.SaveAllAsync(users);

        // Act & Assert - Demonstrate various query capabilities

        // Simple equality filter
        var activeUsers = await db.Query<User>()
            .Where(u => u.IsActive)
            .ToListAsync();

        activeUsers.Should().HaveCount(4);
        activeUsers.All(u => u.IsActive).Should().BeTrue();

        // Numeric comparison
        var adultUsers = await db.Query<User>()
            .Where(u => u.Age >= 30)
            .ToListAsync();

        adultUsers.Should().HaveCount(3);
        adultUsers.All(u => u.Age >= 30).Should().BeTrue();

        // Multiple filters using string operations
        var gmailUsers = await db.Query<User>()
            .Where(u => u.Email.EndsWith("@gmail.com"))
            .ToListAsync();

        gmailUsers.Should().HaveCount(2);
        gmailUsers.All(u => u.Email.EndsWith("@gmail.com")).Should().BeTrue();

        // Boolean conditions
        var activeNonAdminUsers = await db.Query<User>()
            .Where(u => u.IsActive && !u.IsAdmin)
            .ToListAsync();

        activeNonAdminUsers.Should().HaveCount(3);
        activeNonAdminUsers.All(u => u.IsActive && !u.IsAdmin).Should().BeTrue();
    }

    [TestMethod]
    public async Task Query_Ordering_ShouldReturnOrderedResults()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users with varying properties
        var users = new List<User>
        {
            new User
            {
                UserId = 1,
                FirstName = "John",
                LastName = "Doe",
                Age = 25,
                IsActive = true,
                Points = 100,
            },
            new User
            {
                UserId = 2,
                FirstName = "Jane",
                LastName = "Doe",
                Age = 30,
                IsActive = true,
                Points = 200,
            },
            new User
            {
                UserId = 3,
                FirstName = "Alice",
                LastName = "Johnson",
                Age = 22,
                IsActive = false,
                Points = 50,
            },
            new User
            {
                UserId = 4,
                FirstName = "Bob",
                LastName = "Brown",
                Age = 45,
                IsActive = true,
                Points = 300,
            },
        };

        // Save all users
        await db.SaveAllAsync(users);

        // Act & Assert - Demonstrate ordering capabilities

        // Simple ordering
        var orderedByAge = await db.Query<User>()
            .OrderBy(u => u.Age)
            .ToListAsync();

        orderedByAge.Should().HaveCount(4);
        orderedByAge[0].Age.Should().Be(22);
        orderedByAge[3].Age.Should().Be(45);

        // Descending order
        var orderedByPointsDesc = await db.Query<User>()
            .OrderByDescending(u => u.Points)
            .ToListAsync();

        orderedByPointsDesc.Should().HaveCount(4);
        orderedByPointsDesc[0].Points.Should().Be(300);
        orderedByPointsDesc[3].Points.Should().Be(50);

        // Multiple ordering criteria
        var orderedByLastNameThenFirstName = await db.Query<User>()
            .OrderBy(u => u.LastName)
            .ThenBy(u => u.FirstName)
            .ToListAsync();

        orderedByLastNameThenFirstName.Should().HaveCount(4);
        orderedByLastNameThenFirstName[0].LastName.Should().Be("Brown");
        orderedByLastNameThenFirstName[1].LastName.Should().Be("Doe");
        orderedByLastNameThenFirstName[1].FirstName.Should().Be("Jane");
        orderedByLastNameThenFirstName[2].LastName.Should().Be("Doe");
        orderedByLastNameThenFirstName[2].FirstName.Should().Be("John");
    }

    [TestMethod]
    public async Task Query_FilterAndSort_ShouldReturnFilteredAndSortedResults()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users with varying properties
        var users = new List<User>
        {
            new User
            {
                UserId = 1,
                FirstName = "John",
                LastName = "Doe",
                Age = 25,
                IsActive = true,
                Email = "john@example.com",
                Points = 100,
            },
            new User
            {
                UserId = 2,
                FirstName = "Jane",
                LastName = "Smith",
                Age = 30,
                IsActive = true,
                Email = "jane@example.com",
                Points = 200,
            },
            new User
            {
                UserId = 3,
                FirstName = "Alice",
                LastName = "Johnson",
                Age = 22,
                IsActive = false,
                Email = "alice@example.com",
                Points = 50,
            },
            new User
            {
                UserId = 4,
                FirstName = "Bob",
                LastName = "Brown",
                Age = 45,
                IsActive = true,
                Email = "bob@example.com",
                Points = 300,
            },
            new User
            {
                UserId = 5,
                FirstName = "Charlie",
                LastName = "Wilson",
                Age = 35,
                IsActive = true,
                Email = "charlie@example.com",
                Points = 150,
            },
        };

        // Save all users
        await db.SaveAllAsync(users);

        // Act
        var result = await db.Query<User>()
            .Where(u => u.IsActive)
            .Where(u => u.Age > 25)
            .OrderBy(u => u.LastName)
            .ToListAsync();

        // Assert
        result.Should().HaveCount(3);
        result.All(u => u.IsActive && u.Age > 25).Should().BeTrue();
        result[0].LastName.Should().Be("Brown");
        result[1].LastName.Should().Be("Smith");
        result[2].LastName.Should().Be("Wilson");
    }

    [TestMethod]
    public async Task Query_LimitResults_ShouldReturnLimitedResults()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users
        var users = new List<User>();
        for (int i = 1; i <= 20; i++)
        {
            users.Add(new User { UserId = i, FirstName = $"User{i}", Age = 20 + i, Points = i * 10, });
        }

        // Save all users
        await db.SaveAllAsync(users);

        // Act
        var topUsers = await db.Query<User>()
            .OrderByDescending(u => u.Points)
            .Take(5)
            .ToListAsync();

        // Assert
        topUsers.Should().HaveCount(5);
        topUsers[0].Points.Should().Be(200);
        topUsers[4].Points.Should().Be(160);
    }

    [TestMethod]
    public async Task Query_SingleResult_ShouldReturnSingleResult()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users
        var users = new List<User>
        {
            new User { UserId = 1, FirstName = "John", LastName = "Doe", Age = 25, },
            new User { UserId = 2, FirstName = "Jane", LastName = "Smith", Age = 30, },
            new User { UserId = 3, FirstName = "Alice", LastName = "Johnson", Age = 22, },
        };

        // Save all users
        await db.SaveAllAsync(users);

        // Act & Assert
        var user = await db.Query<User>()
            .Where(u => u.UserId == 2)
            .FirstOrDefaultAsync();

        user.Should().NotBeNull();
        user.FirstName.Should().Be("Jane");

        // Test SingleOrDefault
        user = await db.Query<User>()
            .Where(u => u.UserId == 3)
            .SingleOrDefaultAsync();

        user.Should().NotBeNull();
        user.FirstName.Should().Be("Alice");

        // Test non-existent user
        user = await db.Query<User>()
            .Where(u => u.UserId == 99)
            .FirstOrDefaultAsync();

        user.Should().BeNull();
    }

    [TestMethod]
    public async Task Query_Count_ShouldReturnCorrectCount()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users
        var users = new List<User>
        {
            new User { UserId = 1, FirstName = "John", Age = 25, IsActive = true, },
            new User { UserId = 2, FirstName = "Jane", Age = 30, IsActive = true, },
            new User { UserId = 3, FirstName = "Alice", Age = 22, IsActive = false, },
            new User { UserId = 4, FirstName = "Bob", Age = 45, IsActive = true, },
            new User { UserId = 5, FirstName = "Charlie", Age = 35, IsActive = false, },
        };

        // Save all users
        await db.SaveAllAsync(users);

        // Act & Assert
        int activeCount = await db.Query<User>()
            .Where(u => u.IsActive)
            .CountAsync();

        activeCount.Should().Be(3);

        // Test Any
        bool hasInactiveUsers = await db.Query<User>()
            .Where(u => !u.IsActive)
            .AnyAsync();

        hasInactiveUsers.Should().BeTrue();

        bool hasOldUsers = await db.Query<User>()
            .Where(u => u.Age > 50)
            .AnyAsync();

        hasOldUsers.Should().BeFalse();
    }

    [TestMethod]
    public async Task Query_WithPartition_ShouldRespectPartition()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users in different partitions
        var usersPartition1 = new List<User>
        {
            new User { UserId = 1, FirstName = "John", LastName = "Doe", Region = "Europe", },
            new User { UserId = 2, FirstName = "Jane", LastName = "Smith", Region = "Europe", },
        };

        var usersPartition2 = new List<User>
        {
            new User { UserId = 3, FirstName = "Alice", LastName = "Johnson", Region = "America", },
            new User { UserId = 4, FirstName = "Bob", LastName = "Brown", Region = "America", },
            new User { UserId = 5, FirstName = "Charlie", LastName = "Wilson", Region = "America", },
        };

        // Save users to different partitions
        await db.SaveAllAsync(usersPartition1, "europe");
        await db.SaveAllAsync(usersPartition2, "america");

        // Act & Assert
        var europeUsers = await db.Query<User>("europe")
            .ToListAsync();

        europeUsers.Should().HaveCount(2);
        europeUsers.All(u => u.Region == "Europe").Should().BeTrue();

        var americaUsers = await db.Query<User>("america")
            .ToListAsync();

        americaUsers.Should().HaveCount(3);
        americaUsers.All(u => u.Region == "America").Should().BeTrue();
    }

    [TestMethod]
    public async Task Query_ComplexQuery_ShouldHandleComplexFiltering()
    {
        // Arrange
        using var db = TychoDbTests.BuildDatabaseConnection(_systemTextJsonSerializer)
            .AddTypeRegistration<User, int>(u => u.UserId)
            .Connect();

        // Create test users with varied properties
        var users = new List<User>
        {
            new User
            {
                UserId = 1,
                FirstName = "John",
                LastName = "Doe",
                Age = 25,
                IsActive = true,
                Email = "john@example.com",
                Points = 100,
                IsVerified = true,
            },
            new User
            {
                UserId = 2,
                FirstName = "Jane",
                LastName = "Smith",
                Age = 30,
                IsActive = true,
                Email = "jane@gmail.com",
                Points = 200,
                IsVerified = true,
            },
            new User
            {
                UserId = 3,
                FirstName = "Alice",
                LastName = "Johnson",
                Age = 22,
                IsActive = false,
                Email = "alice@example.com",
                Points = 50,
                IsVerified = false,
            },
            new User
            {
                UserId = 4,
                FirstName = "Bob",
                LastName = "Brown",
                Age = 45,
                IsActive = true,
                Email = "bob@gmail.com",
                Points = 300,
                IsVerified = true,
            },
            new User
            {
                UserId = 5,
                FirstName = "Charlie",
                LastName = "Wilson",
                Age = 35,
                IsActive = true,
                Email = "charlie@example.com",
                Points = 150,
                IsVerified = false,
            },
        };

        // Save all users
        await db.SaveAllAsync(users);

        // Act - Complex query with multiple conditions
        var result = await db.Query<User>()
            .Where(u => u.IsActive && u.Age > 25)
            .Where(u => u.Email.EndsWith("@gmail.com") || u.Points >= 150)
            .OrderByDescending(u => u.Points)
            .ToListAsync();

        // Assert
        result.Should().HaveCount(3);
        result[0].UserId.Should().Be(4); // Bob: highest points
        result[1].UserId.Should().Be(2); // Jane: gmail & high points
        result[2].UserId.Should().Be(5); // Charlie: high points
    }
}

// Test entity class for the LINQ query examples
public class User
{
    public int UserId { get; set; }

    public string FirstName { get; set; }

    public string LastName { get; set; }

    public int Age { get; set; }

    public bool IsActive { get; set; }

    public string Email { get; set; }

    public bool IsAdmin { get; set; }

    public bool IsVerified { get; set; }

    public int Points { get; set; }

    public string Region { get; set; }
}
