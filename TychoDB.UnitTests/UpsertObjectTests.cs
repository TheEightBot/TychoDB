#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace TychoDB.UnitTests;

/// <summary>
/// <c>UpsertObjectAsync</c> exists so a caller can learn whether a write created a row or
/// replaced one without a read-then-write pair of its own. The tests pin the answer on the
/// three axes of the primary key (key, type, partition) and check the stored data matches the
/// last write.
/// </summary>
[TestClass]
public class UpsertObjectTests
{
    private const string PartitionA = "partitionA";
    private const string PartitionB = "partitionB";

    [TestMethod]
    public async Task FirstWriteOfAKey_IsInserted()
    {
        using var db = Connect();

        var result = await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key, PartitionA);

        result.ShouldBe(UpsertResult.Inserted);
        (await db.ReadObjectAsync<Doc>("k1", PartitionA))!.Description.ShouldBe("one");
    }

    [TestMethod]
    public async Task SecondWriteOfTheSameKey_IsUpdated_AndReplacesTheData()
    {
        using var db = Connect();
        await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key, PartitionA);

        var result = await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "two" }, x => x.Key, PartitionA);

        result.ShouldBe(UpsertResult.Updated);
        (await db.ReadObjectAsync<Doc>("k1", PartitionA))!.Description.ShouldBe("two");
        (await db.CountObjectsAsync<Doc>(PartitionA)).ShouldBe(1);
    }

    [TestMethod]
    public async Task RewritingIdenticalData_IsUpdated_NotAFailure()
    {
        // SQLite's change count is the number of rows the UPDATE matched, not the number whose
        // bytes differed, so an idempotent rewrite reports one affected row and must come back
        // Updated rather than tripping the exactly-one-row check.
        using var db = Connect();
        var doc = new Doc { Key = "k1", Description = "same" };
        await db.UpsertObjectAsync(doc, x => x.Key, PartitionA);

        var result = await db.UpsertObjectAsync(doc, x => x.Key, PartitionA);

        result.ShouldBe(UpsertResult.Updated);
        (await db.ReadObjectAsync<Doc>("k1", PartitionA))!.Description.ShouldBe("same");
    }

    [TestMethod]
    public async Task ARowWrittenByWriteObjectAsync_CountsAsExisting()
    {
        using var db = Connect();
        await db.WriteObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key, PartitionA);

        var result = await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "two" }, x => x.Key, PartitionA);

        result.ShouldBe(UpsertResult.Updated);
    }

    [TestMethod]
    public async Task SameKeyInAnotherPartition_IsInserted()
    {
        using var db = Connect();
        await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "a" }, x => x.Key, PartitionA);

        var result = await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "b" }, x => x.Key, PartitionB);

        result.ShouldBe(UpsertResult.Inserted);
        (await db.ReadObjectAsync<Doc>("k1", PartitionA))!.Description.ShouldBe("a");
        (await db.ReadObjectAsync<Doc>("k1", PartitionB))!.Description.ShouldBe("b");
    }

    [TestMethod]
    public async Task SameKeyForAnotherType_IsInserted()
    {
        using var db = Connect();
        await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "doc" }, x => x.Key, PartitionA);

        var result = await db.UpsertObjectAsync(new Other { Key = "k1", Name = "other" }, x => x.Key, PartitionA);

        result.ShouldBe(UpsertResult.Inserted);
    }

    [TestMethod]
    public async Task NoPartition_BehavesLikeTheEmptyPartition()
    {
        using var db = Connect();
        await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key);

        var result = await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "two" }, x => x.Key);

        result.ShouldBe(UpsertResult.Updated);
        (await db.ReadObjectAsync<Doc>("k1"))!.Description.ShouldBe("two");
    }

    [TestMethod]
    public async Task RegisteredIdOverload_UsesTheRegisteredKey()
    {
        using var db = Connect(register: true);
        await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, PartitionA);

        var result = await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "two" }, PartitionA);

        result.ShouldBe(UpsertResult.Updated);
        (await db.ReadObjectAsync<Doc>("k1", PartitionA))!.Description.ShouldBe("two");
    }

    [TestMethod]
    public async Task WithoutATransaction_StillReportsTheOutcome()
    {
        using var db = Connect();
        await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key, PartitionA, withTransaction: false);

        var result = await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "two" }, x => x.Key, PartitionA, withTransaction: false);

        result.ShouldBe(UpsertResult.Updated);
    }

    [TestMethod]
    public async Task NullObject_Throws()
    {
        using var db = Connect();

        await Should.ThrowAsync<ArgumentNullException>(
            async () => await db.UpsertObjectAsync<Doc>(null!, x => x.Key, PartitionA));
    }

    [TestMethod]
    public async Task NullKeySelector_Throws()
    {
        using var db = Connect();

        await Should.ThrowAsync<ArgumentNullException>(
            async () => await db.UpsertObjectAsync(new Doc { Key = "k1" }, null!, PartitionA));
    }

    [TestMethod]
    public async Task InsertPathFailure_Throws_AndWritesNothing()
    {
        // The serializer blows up, so nothing reaches SQLite. That must surface as a
        // TychoException (never as an outcome) and leave no row behind.
        using var db = Connect(serializer: new ThrowingSerializer(failFromCall: 1));

        await Should.ThrowAsync<TychoException>(
            async () => await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key, PartitionA));

        (await db.CountObjectsAsync<Doc>(PartitionA)).ShouldBe(0);
    }

    [TestMethod]
    public async Task UpdatePathFailure_Throws_AndLeavesTheExistingRowUntouched()
    {
        // The row exists, so INSERT OR IGNORE is ignored and the follow-up UPDATE runs into a
        // trigger that aborts it. The failure must be an exception, and the transaction must
        // roll back to the original data - the "ignored, then failed" case from the review.
        var (db, path) = ConnectWithPath(persistConnection: false);
        using (db)
        {
            (await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key, PartitionA))
                .ShouldBe(UpsertResult.Inserted);

            AbortEveryUpdate(path);

            await Should.ThrowAsync<TychoException>(
                async () => await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "two" }, x => x.Key, PartitionA));

            (await db.ReadObjectAsync<Doc>("k1", PartitionA))!.Description.ShouldBe("one");
            (await db.CountObjectsAsync<Doc>(PartitionA)).ShouldBe(1);
        }
    }

    [TestMethod]
    public async Task AfterAFailure_TheNextUpsertStillWorks()
    {
        using var db = Connect(serializer: new ThrowingSerializer(failFromCall: 1, failCount: 1));

        await Should.ThrowAsync<TychoException>(
            async () => await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "one" }, x => x.Key, PartitionA));

        (await db.UpsertObjectAsync(new Doc { Key = "k1", Description = "two" }, x => x.Key, PartitionA))
            .ShouldBe(UpsertResult.Inserted);
        (await db.ReadObjectAsync<Doc>("k1", PartitionA))!.Description.ShouldBe("two");
    }

    private static Tycho Connect(bool register = false, IJsonSerializer? serializer = null)
    {
        return ConnectWithPath(register, serializer).Db;
    }

    private static (Tycho Db, string Path) ConnectWithPath(bool register = false, IJsonSerializer? serializer = null, bool persistConnection = true)
    {
        var dir = Path.GetTempPath();
        var name = $"{Guid.NewGuid()}.db";

        // Pooling keeps a closed connection's handle alive; a test that installs DDL from a
        // second connection needs the first one really closed, so pooling follows persistence.
        var db = new Tycho(dir, serializer ?? new NewtonsoftJsonSerializer(), dbName: name, persistConnection: persistConnection, rebuildCache: true, requireTypeRegistration: false, useConnectionPooling: persistConnection);

        if (register)
        {
            db.AddTypeRegistrationWithCustomKeySelector<Doc>(x => x.Key);
        }

        return (db.Connect(), Path.Combine(dir, name));
    }

    /// <summary>
    /// Installs a trigger through a second connection so every UPDATE on JsonValue aborts.
    /// Needs a Tycho opened with persistConnection: false, otherwise Tycho's held connection
    /// keeps the writer lock and the DDL fails with "database is locked".
    /// </summary>
    private static void AbortEveryUpdate(string path)
    {
        using var connection = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={path}");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText =
            """
            CREATE TRIGGER abort_every_update BEFORE UPDATE ON JsonValue
            BEGIN
                SELECT RAISE(ABORT, 'update refused by test trigger');
            END;
            """;
        command.ExecuteNonQuery();
    }

    public class Doc
    {
        public string Key { get; set; } = string.Empty;

        public string Description { get; set; } = string.Empty;
    }

    /// <summary>
    /// Delegates to the real serializer except for a window of calls (1-based, counted per
    /// serialize) during which it throws, standing in for any failure ahead of the INSERT.
    /// </summary>
    private sealed class ThrowingSerializer(int failFromCall, int failCount = int.MaxValue) : IJsonSerializer
    {
        private readonly NewtonsoftJsonSerializer _inner = new();

        private int _calls;

        public string DateTimeSerializationFormat => _inner.DateTimeSerializationFormat;

        public object Serialize<T>(T obj)
        {
            return ShouldFail() ? throw new InvalidOperationException("serializer refused") : _inner.Serialize(obj);
        }

        public void Serialize<T>(T obj, System.Buffers.IBufferWriter<byte> bufferWriter)
        {
            if (ShouldFail())
            {
                throw new InvalidOperationException("serializer refused");
            }

            _inner.Serialize(obj, bufferWriter);
        }

        public System.Threading.Tasks.ValueTask<T> DeserializeAsync<T>(Stream stream, System.Threading.CancellationToken cancellationToken)
        {
            return _inner.DeserializeAsync<T>(stream, cancellationToken);
        }

        private bool ShouldFail()
        {
            var call = ++_calls;
            return call >= failFromCall && call - failFromCall < failCount;
        }
    }

    public class Other
    {
        public string Key { get; set; } = string.Empty;

        public string Name { get; set; } = string.Empty;
    }
}
