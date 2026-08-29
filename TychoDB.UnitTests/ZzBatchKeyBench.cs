#nullable enable
#pragma warning disable CA1305, CA1307, CA1848, CA2100, SA1600, SA1601, SA1201, SA1202, SA1204, SA1516

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TychoDB.UnitTests;

/// <summary>
/// Scratch harness comparing the ways a batch of keys can be fetched. Not a correctness test —
/// it prints timings. Run explicitly.
/// </summary>
[TestClass]
public class ZzBatchKeyBench
{
    private const int RowCount = 250_000;
    private const string Partition = "itemMaster|v1";

    private static readonly IJsonSerializer Serializer = new NewtonsoftJsonSerializer();

    public class Item
    {
        public string Key { get; set; } = string.Empty;

        public int DepartmentId { get; set; }

        public string Description { get; set; } = string.Empty;

        public string Filler { get; set; } = string.Empty;
    }

    [TestMethod]
    [Ignore("Scratch performance harness: seeds 250k rows. Run it by removing this attribute.")]
    public async Task Compare()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tycho_batchkey", Guid.NewGuid().ToString());
        Directory.CreateDirectory(dir);
        var name = "bench.db";
        var file = Path.Combine(dir, name);

        var sw = Stopwatch.StartNew();
        using (var db = new Tycho(dir, Serializer, dbName: name, rebuildCache: true, requireTypeRegistration: false).Connect())
        {
            const int batch = 10_000;
            for (var offset = 0; offset < RowCount; offset += batch)
            {
                var slice =
                    Enumerable.Range(offset, Math.Min(batch, RowCount - offset))
                        .Select(i => new Item
                        {
                            Key = "K" + i.ToString(CultureInfo.InvariantCulture),
                            DepartmentId = i % 200,
                            Description = "Item number " + i.ToString(CultureInfo.InvariantCulture),
                            Filler = new string('x', 200),
                        });

                await db.WriteObjectsAsync(slice, x => x.Key, Partition);
            }
        }

        SqliteConnection.ClearAllPools();
        Console.WriteLine($"seed {RowCount} rows: {sw.ElapsedMilliseconds} ms, {new FileInfo(file).Length / 1_048_576} MB");

        var rng = new Random(20260828);
        var allKeys = Enumerable.Range(0, RowCount).Select(i => "K" + i.ToString(CultureInfo.InvariantCulture)).ToArray();

        foreach (var batchSize in new[] { 200, 1_000, 5_000, 25_000 })
        {
            var keys = Enumerable.Range(0, batchSize).Select(_ => allKeys[rng.Next(RowCount)]).Distinct().ToArray();

            Console.WriteLine($"\n=== batch of {keys.Length} keys ===");

            // Tycho paths: end to end, including deserialization.
            SqliteConnection.ClearAllPools();
            using (var db = new Tycho(dir, Serializer, dbName: name, rebuildCache: false, requireTypeRegistration: false).Connect())
            {
                await Timed("looped ReadObjectAsync", async () =>
                {
                    var found = 0;
                    foreach (var k in keys)
                    {
                        if (await db.ReadObjectAsync<Item>(k, Partition) is not null)
                        {
                            found++;
                        }
                    }

                    return found;
                });

                await Timed("ReadObjectsByKeysAsync", async () =>
                    (await db.ReadObjectsByKeysAsync<Item>(keys, Partition)).Count());
            }

            // Raw SQL shapes: query cost only, no deserialization, so they are not comparable
            // with the two above — they are here to compare the shapes against each other.
            SqliteConnection.ClearAllPools();
            var conn = Open(file);
            try
            {
                await Timed("  raw json_each(@keys)", () => Task.FromResult(RunJsonEach(conn, keys)));
                await Timed("  raw single IN", () => Task.FromResult(RunIn(conn, keys, keys.Length)));
                await Timed("  raw chunked IN (900)", () => Task.FromResult(RunIn(conn, keys, 900)));
                await Timed("  raw temp table + join", () => Task.FromResult(RunTempTable(conn, keys)));
            }
            finally
            {
                conn.Close();
                conn.Dispose();
                SqliteConnection.ClearAllPools();
            }
        }

        Directory.Delete(dir, true);
    }

    [TestMethod]
    [Ignore("Scratch performance harness: seeds 250k rows. Run it by removing this attribute.")]
    public async Task CountShapes()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tycho_countbench", Guid.NewGuid().ToString());
        Directory.CreateDirectory(dir);
        var name = "bench.db";
        var file = Path.Combine(dir, name);

        using (var db = new Tycho(dir, Serializer, dbName: name, rebuildCache: true, requireTypeRegistration: false).Connect())
        {
            const int batch = 10_000;
            for (var offset = 0; offset < RowCount; offset += batch)
            {
                await db.WriteObjectsAsync(
                    Enumerable.Range(offset, Math.Min(batch, RowCount - offset))
                        .Select(i => new Item
                        {
                            Key = "K" + i.ToString(CultureInfo.InvariantCulture),
                            DepartmentId = i % 200,
                            Description = "Item number " + i.ToString(CultureInfo.InvariantCulture),
                            Filler = new string('x', 200),
                        }),
                    x => x.Key,
                    Partition);
            }
        }

        SqliteConnection.ClearAllPools();

        using (var db = new Tycho(dir, Serializer, dbName: name, rebuildCache: false, requireTypeRegistration: false).Connect())
        {
            await Timed("CountObjectsAsync (all)", async () => await db.CountObjectsAsync<Item>(Partition));
            await Timed("CountObjectsAsync (1/200)", async () => await db.CountObjectsAsync(
                Partition, FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.DepartmentId, 7)));
        }

        SqliteConnection.ClearAllPools();

        var conn = Open(file);
        try
        {
            await Timed("  raw SELECT 1 + client loop", () => Task.FromResult(CountVia(conn, "SELECT 1")));
            await Timed("  raw SELECT COUNT(*)", () => Task.FromResult(CountVia(conn, "SELECT COUNT(*)")));
        }
        finally
        {
            conn.Close();
            conn.Dispose();
            SqliteConnection.ClearAllPools();
        }

        Directory.Delete(dir, true);
    }

    /// <summary>
    /// Two questions at once: what a filter on the key property costs versus reaching the key
    /// through the primary key, and what the serializer choice costs on the deserialization
    /// that dominates a large read.
    /// </summary>
    [TestMethod]
    [Ignore("Scratch performance harness: seeds 250k rows. Run it by removing this attribute.")]
    public async Task KeyFilterAndSerializerShapes()
    {
        foreach (var (serializer, label) in new (IJsonSerializer, string)[]
                 {
                     (new NewtonsoftJsonSerializer(), "Newtonsoft"),
                     (new SystemTextJsonSerializer(), "SystemTextJson"),
                 })
        {
            var dir = Path.Combine(Path.GetTempPath(), "tycho_keybench", Guid.NewGuid().ToString());
            Directory.CreateDirectory(dir);
            var name = "bench.db";

            using (var db = new Tycho(dir, serializer, dbName: name, rebuildCache: true, requireTypeRegistration: false).Connect())
            {
                const int batch = 10_000;
                for (var offset = 0; offset < RowCount; offset += batch)
                {
                    await db.WriteObjectsAsync(
                        Enumerable.Range(offset, Math.Min(batch, RowCount - offset))
                            .Select(i => new Item
                            {
                                Key = "K" + i.ToString(CultureInfo.InvariantCulture),
                                DepartmentId = i % 200,
                                Description = "Item number " + i.ToString(CultureInfo.InvariantCulture),
                                Filler = new string('x', 200),
                            }),
                        x => x.Key,
                        Partition);
                }
            }

            SqliteConnection.ClearAllPools();
            Console.WriteLine($"\n=== {label} ===");

            using (var db = new Tycho(dir, serializer, dbName: name, rebuildCache: false, requireTypeRegistration: false).Connect())
            {
                // Reaching one row three ways.
                await Timed("filter on key property", async () =>
                    (await db.ReadObjectsAsync<Item>(
                        Partition,
                        FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Key, "K123456"))).Count());

                await Timed("ReadObjectAsync (PK)", async () =>
                    await db.ReadObjectAsync<Item>("K123456", Partition) is null ? 0 : 1);

                await Timed("ReadObjectsByKeysAsync (PK)", async () =>
                    (await db.ReadObjectsByKeysAsync<Item>(new object[] { "K123456" }, Partition)).Count());

                // The safe alternative to rewriting the filter onto the Key column: index the
                // key property like any other.
                await db.CreateIndexAsync<Item>(x => x.Key, "ix_key_property");

                await Timed("filter on key property, indexed", async () =>
                    (await db.ReadObjectsAsync<Item>(
                        Partition,
                        FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Key, "K123456"))).Count());

                // Deserialization-dominated read: every row in the partition.
                await Timed("read all 250k", async () => (await db.ReadObjectsAsync<Item>(Partition)).Count());
            }

            SqliteConnection.ClearAllPools();
            Directory.Delete(dir, true);
        }
    }

    [TestMethod]
    [Ignore("Scratch performance harness: seeds 250k rows. Run it by removing this attribute.")]
    public async Task KeyColumnRewrite()
    {
        var dir = Path.Combine(Path.GetTempPath(), "tycho_rewrite", Guid.NewGuid().ToString());
        Directory.CreateDirectory(dir);
        var name = "bench.db";

        using (var db = new Tycho(dir, Serializer, dbName: name, rebuildCache: true, requireTypeRegistration: false).Connect())
        {
            const int batch = 10_000;
            for (var offset = 0; offset < RowCount; offset += batch)
            {
                await db.WriteObjectsAsync(
                    Enumerable.Range(offset, Math.Min(batch, RowCount - offset))
                        .Select(i => new Item
                        {
                            Key = "K" + i.ToString(CultureInfo.InvariantCulture),
                            DepartmentId = i % 200,
                            Description = "Item number " + i.ToString(CultureInfo.InvariantCulture),
                            Filler = new string('x', 200),
                        }),
                    x => x.Key,
                    Partition);
            }
        }

        SqliteConnection.ClearAllPools();

        foreach (var strict in new[] { false, true })
        {
            using var db = new Tycho(dir, Serializer, dbName: name, rebuildCache: false, requireTypeRegistration: strict)
                .AddTypeRegistration<Item, string>(x => x.Key)
                .Connect();

            Console.WriteLine($"\n=== requireTypeRegistration: {strict} ===");

            // First call pays the one-time divergence probe (a scan of this type's rows).
            var sw = Stopwatch.StartNew();
            _ = (await db.ReadObjectsAsync<Item>(
                Partition, FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Key, "K123456"))).Count();
            sw.Stop();
            Console.WriteLine($"  {"first call (incl. probe)",-28} {sw.Elapsed.TotalMilliseconds,9:F1} ms   (1 rows)");

            await Timed("steady state", async () =>
                (await db.ReadObjectsAsync<Item>(
                    Partition, FilterBuilder<Item>.Create().Filter(FilterType.Equals, x => x.Key, "K123456"))).Count());

            await Timed("In (100 keys)", async () =>
                (await db.ReadObjectsAsync<Item>(
                    Partition,
                    FilterBuilder<Item>.Create().Filter(
                        FilterType.In,
                        x => x.Key,
                        Enumerable.Range(0, 100).Select(i => "K" + i.ToString(CultureInfo.InvariantCulture))))).Count());

            SqliteConnection.ClearAllPools();
        }

        Directory.Delete(dir, true);
    }

    private static int CountVia(SqliteConnection conn, string projection)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"{projection} FROM JsonValue WHERE FullTypeName = $t AND Partition = $p";
        cmd.Parameters.AddWithValue("$t", typeof(Item).FullName!);
        cmd.Parameters.AddWithValue("$p", Partition);

        using var reader = cmd.ExecuteReader();
        if (projection == "SELECT COUNT(*)")
        {
            return reader.Read() ? reader.GetInt32(0) : 0;
        }

        var n = 0;
        while (reader.Read())
        {
            n++;
        }

        return n;
    }

    private static SqliteConnection Open(string file)
    {
        var conn = new SqliteConnection($"Data Source={file}");
        conn.Open();
        using var pragma = conn.CreateCommand();
        pragma.CommandText =
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA temp_store = MEMORY;" +
            "PRAGMA cache_size = -65536; PRAGMA mmap_size = 268435456;";
        pragma.ExecuteNonQuery();
        return conn;
    }

    private static async Task Timed(string label, Func<Task<int>> action)
    {
        // Two warm-up passes (JIT, page cache, statement cache), then the best of five. Best
        // rather than mean: the run without a GC pause or a scheduler hiccup is the one that
        // reflects the work actually being done.
        await action().ConfigureAwait(false);
        await action().ConfigureAwait(false);

        var best = double.MaxValue;
        var n = 0;

        for (var i = 0; i < 5; i++)
        {
            var sw = Stopwatch.StartNew();
            n = await action().ConfigureAwait(false);
            sw.Stop();
            best = Math.Min(best, sw.Elapsed.TotalMilliseconds);
        }

        Console.WriteLine($"  {label,-28} {best,9:F1} ms   ({n} rows)");
    }

    private static int RunIn(SqliteConnection conn, string[] keys, int chunkSize)
    {
        var total = 0;

        for (var start = 0; start < keys.Length; start += chunkSize)
        {
            var end = Math.Min(start + chunkSize, keys.Length);

            var sb = new StringBuilder();
            sb.Append("SELECT Data FROM JsonValue WHERE FullTypeName = $t AND Partition = $p AND Key IN (");
            for (var i = start; i < end; i++)
            {
                if (i > start)
                {
                    sb.Append(',');
                }

                sb.Append("$k").Append(i.ToString(CultureInfo.InvariantCulture));
            }

            sb.Append(')');

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sb.ToString();
            cmd.Parameters.AddWithValue("$t", typeof(Item).FullName!);
            cmd.Parameters.AddWithValue("$p", Partition);
            for (var i = start; i < end; i++)
            {
                cmd.Parameters.AddWithValue("$k" + i.ToString(CultureInfo.InvariantCulture), keys[i]);
            }

            total += Drain(cmd);
        }

        return total;
    }

    private static int RunJsonEach(SqliteConnection conn, string[] keys)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT Data FROM JsonValue WHERE FullTypeName = $t AND Partition = $p" +
            " AND Key IN (SELECT value FROM json_each($keys))";
        cmd.Parameters.AddWithValue("$t", typeof(Item).FullName!);
        cmd.Parameters.AddWithValue("$p", Partition);
        cmd.Parameters.AddWithValue("$keys", JsonSerializer.Serialize(keys));

        return Drain(cmd);
    }

    private static int RunTempTable(SqliteConnection conn, string[] keys)
    {
        using (var ddl = conn.CreateCommand())
        {
            ddl.CommandText = "DROP TABLE IF EXISTS temp.BatchKeys; CREATE TEMP TABLE BatchKeys(Key TEXT PRIMARY KEY);";
            ddl.ExecuteNonQuery();
        }

        using (var tx = conn.BeginTransaction())
        {
            using var ins = conn.CreateCommand();
            ins.Transaction = tx;
            ins.CommandText = "INSERT OR IGNORE INTO temp.BatchKeys(Key) VALUES ($k)";
            var p = ins.Parameters.Add("$k", SqliteType.Text);
            foreach (var k in keys)
            {
                p.Value = k;
                ins.ExecuteNonQuery();
            }

            tx.Commit();
        }

        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT j.Data FROM temp.BatchKeys b JOIN JsonValue j" +
            " ON j.Key = b.Key AND j.FullTypeName = $t AND j.Partition = $p";
        cmd.Parameters.AddWithValue("$t", typeof(Item).FullName!);
        cmd.Parameters.AddWithValue("$p", Partition);

        return Drain(cmd);
    }

    private static int Drain(SqliteCommand cmd)
    {
        var n = 0;
        using var reader = cmd.ExecuteReader(System.Data.CommandBehavior.SequentialAccess);
        while (reader.Read())
        {
            _ = reader.GetString(0);
            n++;
        }

        return n;
    }
}
