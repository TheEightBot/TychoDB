using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;
using TychoDB;
using static TychoDB.UnitTests.StoreFixture;

namespace TychoDB.UnitTests;

/// <summary>
/// Proves <c>Connect</c>, <c>Disconnect</c>, <c>Dispose</c> and <c>Backup</c> take the
/// connection gate like every other operation: they wait for the operation in progress
/// instead of closing the connection under it, and leave no operation waiting for good.
/// </summary>
[TestClass]
public class ConnectionLifecycleTests
{
    [TestMethod]
    public async Task ConcurrentConnects_OpenOneConnection()
    {
        var (path, dbName) = NewDbPath();

        using var db = BuildDb(path, dbName);

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(db.Connect)));

        await db.WriteObjectsAsync(Items(), x => x.Key);
        (await db.CountObjectsAsync<Item>()).ShouldBe(100);
    }

    [TestMethod]
    public async Task Disconnect_DuringOperations_LeavesAnInstanceThatReconnects()
    {
        const int rows = 5_000;

        var dbFile = await SeedLegacyStore(rows);
        var (path, dbName) = Split(dbFile);

        using var db = await ReopenDb(path, dbName).ConnectAsync();

        foreach (int delay in new[] { 0, 1, 2, 5, 10 })
        {
            var reads = StartReads(db);

            await Task.Delay(delay);
            db.Disconnect();

            (await Task.WhenAll(reads)).ShouldAllBe(count => count == rows || count == -1);

            db.Connect();
            (await db.CountObjectsAsync<Item>()).ShouldBe(rows);
        }
    }

    [TestMethod]
    public async Task Dispose_WithOperationsWaiting_LeavesNoneOfThemHanging()
    {
        const int rows = 5_000;

        var dbFile = await SeedLegacyStore(rows);
        var (path, dbName) = Split(dbFile);

        foreach (int delay in new[] { 0, 1, 2, 5, 10 })
        {
            var db = await ReopenDb(path, dbName).ConnectAsync();
            var reads = StartReads(db);

            await Task.Delay(delay);
            db.Dispose();

            (await Task.WhenAll(reads).WaitAsync(TimeSpan.FromSeconds(30)))
                .ShouldAllBe(count => count == rows || count == -1);
        }
    }

    [TestMethod]
    public async Task Operations_AfterDispose_ThrowObjectDisposed()
    {
        var (path, dbName) = NewDbPath();

        var db = await BuildDb(path, dbName).ConnectAsync();
        db.Dispose();

        Should.Throw<ObjectDisposedException>(() => db.Connect());
        await Should.ThrowAsync<ObjectDisposedException>(async () => await db.ConnectAsync());
        await Should.ThrowAsync<ObjectDisposedException>(async () => await db.CountObjectsAsync<Item>());
    }

#if !ENCRYPTED
    [DataTestMethod]
    [DataRow(true)]
    [DataRow(false)]
    public async Task Backup_CopiesTheStore(bool persistConnection)
    {
        var dbFile = await SeedLegacyStore();
        var (path, dbName) = Split(dbFile);

        using var db = await ReopenDb(path, dbName, persistConnection: persistConnection).ConnectAsync();
        using var destination = new SqliteConnection("Data Source=:memory:");
        destination.Open();

        db.Backup(destination);

        using var command = destination.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM JsonValue";
        command.ExecuteScalar().ShouldBe(100L);
    }
#endif

    /// <summary>
    /// Reads that run to their end when they have the gate first, and fail as a call on a
    /// closed instance does when they do not.
    /// </summary>
    private static List<Task<int>> StartReads(Tycho db)
        => Enumerable.Range(0, 4)
            .Select(_ => Task.Run(async () =>
            {
                try
                {
                    return (await db.ReadObjectsAsync<Item>()).Count();
                }
                catch (Exception ex) when (ex is TychoException or ArgumentNullException or ObjectDisposedException)
                {
                    return -1;
                }
            }))
            .ToList();
}
