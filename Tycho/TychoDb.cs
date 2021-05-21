using System;
using System.Buffers;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Schema;
using Microsoft.Data.Sqlite;

namespace Tycho
{
    public class TychoDb : IDisposable
    {
        private const string DataColumn = "Data";

        private readonly object _connectionLock = new object ();

        private readonly string _dbConnectionString;

        private readonly JsonSerializerOptions _jsonSerializerOptions;

        private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypeInformation = new Dictionary<Type, RegisteredTypeInformation>();

        private readonly ProcessingQueue _processingQueue = new ProcessingQueue();

        private SqliteConnection _connection;

        private bool _isDisposed;

        public TychoDb (string dbPath, string dbName = "tycho_cache.db", string password = null, JsonSerializerOptions jsonSerializerOptions = null, bool rebuildCache = false)
        {
            SQLitePCL.Batteries_V2.Init ();

            var databasePath = Path.Join (dbPath, dbName);

            if(rebuildCache && File.Exists(databasePath))
            {
                File.Delete (databasePath);
            }

            var connectionStringBuilder =
                new SqliteConnectionStringBuilder
                {
                    ConnectionString = $"Filename={databasePath}",
                    Cache = SqliteCacheMode.Default,
                    Mode = SqliteOpenMode.ReadWriteCreate,
                };

            if(password != null)
            {
                connectionStringBuilder.Password = password;
            }

            _dbConnectionString = connectionStringBuilder.ToString ();

            _jsonSerializerOptions =
                jsonSerializerOptions ??
                new JsonSerializerOptions
                {
                    IgnoreReadOnlyProperties = true,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                };
        }

        public TychoDb AddTypeRegistration<T> (Expression<Func<T, object>> idSelector)
            where T : class
        {
            var rti = RegisteredTypeInformation.Create (idSelector);

            _registeredTypeInformation[rti.ObjectType] = rti;

            return this;
        }

        public TychoDb Connect ()
        {

            if(_connection == null)
            {
                _connection = BuildConnection ();
            }

            return this;
        }

        public async ValueTask<TychoDb> ConnectAsync ()
        {

            if (_connection == null)
            {
                _connection = await BuildConnectionAsync ().ConfigureAwait(false);
            }

            return this;
        }

        public void Disconnect ()
        {
            lock(_connectionLock)
            {
                _connection?.Dispose ();
            }
        }

        public ValueTask DisconnectAsync ()
        {
            if(_connection == null)
            {
                return new ValueTask (Task.CompletedTask);
            }

            return _connection.DisposeAsync ();
        }

        public ValueTask<bool> WriteObjectAsync<T> (T obj, string partition = null, CancellationToken cancellationToken = default)
        {
            return WriteObjectsAsync (new[] { obj }, GetIdFor<T>(), partition, cancellationToken);
        }

        public ValueTask<bool> WriteObjectAsync<T> (T obj, Func<T, object> keySelector, string partition = null, CancellationToken cancellationToken = default)
        {
            return WriteObjectsAsync (new[] { obj }, keySelector, partition, cancellationToken);
        }

        public ValueTask<bool> WriteObjectsAsync<T> (IEnumerable<T> objs, string partition = null, CancellationToken cancellationToken = default)
        {
            return WriteObjectsAsync (objs, GetIdFor<T>(), partition, cancellationToken);
        }

        public ValueTask<bool> WriteObjectsAsync<T>(IEnumerable<T> objs, Func<T, object> keySelector, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    async conn =>
                    {
                        var writeCount = 0;
                        var totalCount = objs.Count ();

                        var transaction = await conn.BeginTransactionAsync (IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

                        try
                        {

                            foreach (var obj in objs)
                            {
                                var rowId = 0L;

                                using var insertCommand = conn.CreateCommand ();
                                insertCommand.CommandText =
                                    @"
                                    INSERT OR REPLACE INTO JsonValue(Key, FullTypeName, Data, Partition)
                                    VALUES ($key, $fullTypeName, json($json), $partition);

                                    SELECT last_insert_rowid();
                                    ";

                                insertCommand.Parameters.Add ("$key", SqliteType.Text).Value = keySelector (obj);
                                insertCommand.Parameters.Add ("$partition", SqliteType.Text).Value = partition.AsValueOrDbNull();
                                insertCommand.Parameters.Add ("$fullTypeName", SqliteType.Text).Value = typeof (T).FullName;

                                var jsonBytes = JsonSerializer.SerializeToUtf8Bytes (obj, _jsonSerializerOptions);

                                try
                                {
                                    insertCommand.Parameters.Add ("$json", SqliteType.Text).Value = jsonBytes;

                                    rowId = (long)await insertCommand.ExecuteScalarAsync (cancellationToken).ConfigureAwait(false);
                                }
                                finally
                                {
                                    jsonBytes = null;
                                }

                                writeCount += rowId > 0 ? 1 : 0;
                            }

                            await transaction.CommitAsync (cancellationToken).ConfigureAwait (false);
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine ($"{ex}");
                            await transaction.RollbackAsync (cancellationToken).ConfigureAwait (false);
                        }

                        return writeCount == totalCount;
                    });         
        }

        public ValueTask<T> ReadObjectAsync<T> (object key, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    async conn =>
                    {
                        var transaction = await conn.BeginTransactionAsync (IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait (false);

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            selectCommand.CommandText =
                                @"
                                    SELECT Data
                                    FROM JsonValue
                                    Where
                                        Key = $key
                                        AND
                                        FullTypeName = $fullTypeName
                                ";

                            selectCommand.Parameters.Add ("$key", SqliteType.Text).Value = key;
                            selectCommand.Parameters.Add ("$fullTypeName", SqliteType.Text).Value = typeof (T).FullName;

                            if (!string.IsNullOrEmpty(partition))
                            {
                                selectCommand.CommandText +=
                                    @"
                                        AND
                                        Partition = $partition
                                    ";

                                selectCommand.Parameters.Add ("$partition", SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }

                            using var reader = await selectCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                            while (await reader.ReadAsync (cancellationToken).ConfigureAwait (false))
                            {
                                using var stream = reader.GetStream (0);
                                return await JsonSerializer.DeserializeAsync<T> (stream, _jsonSerializerOptions, cancellationToken).ConfigureAwait (false);
                            }
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine ($"{ex}");
                        }
                        finally
                        {
                            await transaction.CommitAsync ().ConfigureAwait (false);
                        }

                        return default;
                    });
        }

        public async ValueTask<T> ReadObjectAsync<T> (FilterBuilder<T> filter, string partition = null, CancellationToken cancellationToken = default)
        {
            var result = await ReadObjectsAsync (partition: partition, filter: filter, cancellationToken: cancellationToken).ConfigureAwait(false);

            if(result?.Count() > 1)
            {
                throw new TychoDbException ("Too many matching values were found, please refine your query to limit it to a single match");
            }

            return result.FirstOrDefault ();
        }

        public ValueTask<IEnumerable<T>> ReadObjectsAsync<T> (string partition = null, FilterBuilder<T> filter = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    async conn =>
                    {
                        var transaction = await conn.BeginTransactionAsync (IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait (false);

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            selectCommand.CommandText =
                                @"
                                    SELECT Data
                                    FROM JsonValue
                                    Where
                                    FullTypeName = $fullTypeName
                                ";

                            selectCommand.Parameters.Add ("$fullTypeName", SqliteType.Text).Value = typeof (T).FullName;

                            if (!string.IsNullOrEmpty (partition))
                            {
                                selectCommand.CommandText +=
                                @"
                                AND
                                Partition = $partition
                                ";

                                selectCommand.Parameters.Add ("$partition", SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }

                            if(filter != null)
                            {
                                filter.Build (selectCommand);
                            }

                            using var reader = await selectCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                            var objects = new List<T> ();

                            while (await reader.ReadAsync (cancellationToken).ConfigureAwait (false))
                            {
                                using var stream = reader.GetStream (0);
                                objects.Add(await JsonSerializer.DeserializeAsync<T> (stream, _jsonSerializerOptions, cancellationToken).ConfigureAwait (false));
                            }

                            return objects;
                        }
                        catch (Exception ex)
                        {
                            System.Diagnostics.Debug.WriteLine ($"{ex}");
                        }
                        finally
                        {
                            await transaction.CommitAsync ().ConfigureAwait (false);
                        }

                        return Enumerable.Empty<T>();
                    });
        }

        public ValueTask<IEnumerable<TOut>> ReadObjectsAsync<TIn, TOut> (Expression<Func<TIn,TOut>> innerObjectSelection, string partition = null, FilterBuilder<TIn> filter = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock<IEnumerable<TOut>> (
                    _processingQueue,
                    async conn =>
                    {
                        var transaction = await conn.BeginTransactionAsync (IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait (false);

                        var objects = new List<TOut> ();

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var selectionPath = QueryPropertyPath.BuildPath (innerObjectSelection);

                            selectCommand.CommandText =
                                @$"
                                    SELECT JSON_EXTRACT(Data, '{selectionPath}') AS Data
                                    FROM JsonValue
                                    Where
                                    FullTypeName = $fullTypeName
                                ";

                            selectCommand.Parameters.Add ("$fullTypeName", SqliteType.Text).Value = typeof (TIn).FullName;

                            if (!string.IsNullOrEmpty (partition))
                            {
                                selectCommand.CommandText +=
                                @"
                                    AND
                                    Partition = $partition
                                ";

                                selectCommand.Parameters.Add ("$partition", SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }

                            if (filter != null)
                            {
                                filter.Build (selectCommand);
                            }

                            using var reader = await selectCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                            while (await reader.ReadAsync (cancellationToken).ConfigureAwait (false))
                            {
                                using var stream = reader.GetStream (0);
                                objects.Add (await JsonSerializer.DeserializeAsync<TOut> (stream, _jsonSerializerOptions, cancellationToken).ConfigureAwait (false));
                            }

                        }
                        catch (Exception ex)
                        {
                            throw new TychoDbException ("Failed Reading Objects", ex);
                        }
                        finally
                        {
                            await transaction.CommitAsync ().ConfigureAwait (false);
                        }

                        return objects;
                    });
        }

        public TychoDb CreateIndex<TObj> (Expression<Func<TObj, object>> propertyPath, string indexName)
        {
            lock (_connectionLock)
            {
                try
                {
                    _connection.Open ();

                    var transaction = _connection.BeginTransaction (IsolationLevel.Serializable);

                    var propertyPathString = QueryPropertyPath.BuildPath (propertyPath);

                    var isNumeric = QueryPropertyPath.IsNumeric (propertyPath);

                    var fullIndexName = $"idx_{indexName}_{typeof (TObj).Name}";

                    try
                    {
                        using var createIndexCommand = _connection.CreateCommand ();

                        if (isNumeric)
                        {
                            createIndexCommand.CommandText =
                            @$" CREATE INDEX IF NOT EXISTS {fullIndexName}
                                ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '{propertyPathString}') as NUMERIC));";
                        }
                        else
                        {
                            createIndexCommand.CommandText =
                            @$" CREATE INDEX IF NOT EXISTS {fullIndexName}
                                ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '{propertyPathString}'));";
                        }

                        createIndexCommand.ExecuteNonQuery ();
                    }
                    catch (Exception ex)
                    {
                        throw new TychoDbException ($"Failed to Create Index: {fullIndexName}", ex);
                    }
                    finally
                    {
                        transaction.Commit ();
                    }

                }
                finally
                {
                    _connection.Close();
                }
            }

            return this;
        }

        public ValueTask<bool> CreateIndexAsync<TObj>(Expression<Func<TObj, object>> propertyPath, string indexName, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    async conn =>
                    {
                        var transaction = await conn.BeginTransactionAsync (IsolationLevel.Serializable, cancellationToken).ConfigureAwait (false);

                        var result = false;

                        var propertyPathString = QueryPropertyPath.BuildPath (propertyPath);

                        var fullIndexName = $"idx_{indexName}_{typeof(TObj).Name}";

                        var isNumeric = QueryPropertyPath.IsNumeric (propertyPath);

                        try
                        {
                            using var createIndexCommand = conn.CreateCommand ();

                            if (isNumeric)
                            {
                                createIndexCommand.CommandText =
                                    @$" CREATE INDEX IF NOT EXISTS {fullIndexName}
                                        ON JsonValue(FullTypeName, CAST(JSON_EXTRACT(Data, '{propertyPathString}') as NUMERIC));";
                            }
                            else
                            {
                                createIndexCommand.CommandText =
                                    @$" CREATE INDEX IF NOT EXISTS {fullIndexName}
                                        ON JsonValue(FullTypeName, JSON_EXTRACT(Data, '{propertyPathString}'));";
                            }

                            await createIndexCommand.ExecuteNonQueryAsync ().ConfigureAwait(false);
                            result = true;
                        }
                        catch (Exception ex)
                        {
                            throw new TychoDbException ($"Failed to Create Index: {fullIndexName}", ex);
                        }
                        finally
                        {
                            await transaction.CommitAsync ().ConfigureAwait (false);
                        }

                        return result;
                    });
        }

        protected virtual void Dispose (bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _connection?.Dispose ();
                }

                _isDisposed = true;
            }
        }

        public void Dispose ()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose (disposing: true);
            GC.SuppressFinalize (this);
        }

        private SqliteConnection BuildConnection ()
        {
            lock(_connectionLock)
            {
                _connection = new SqliteConnection (_dbConnectionString);

                try
                {
                    _connection.Open ();

                    var supportsJson = false;

                    // Enable write-ahead logging
                    using var hasJsonCommand = _connection.CreateCommand ();
                    hasJsonCommand.CommandText = @" PRAGMA compile_options; ";
                    using var reader = hasJsonCommand.ExecuteReader ();

                    while (reader.Read ())
                    {
                        if (reader.GetString (0)?.Equals ("ENABLE_JSON1") ?? false)
                        {
                            supportsJson = true;
                            break;
                        }
                    }

                    if (!supportsJson)
                    {
                        throw new TychoDbException ("JSON support is not available for this platform");
                    }


                    using var command = _connection.CreateCommand ();

                    // Enable write-ahead logging and normal synchronous mode
                    command.CommandText =
                        @"
                        PRAGMA journal_mode = WAL;
                        PRAGMA synchronous = normal;

                        CREATE TABLE IF NOT EXISTS JsonValue
                        (
                            Key             TEXT PRIMARY KEY,
                            FullTypeName    TEXT NOT NULL,
                            Data            JSON NOT NULL,
                            Partition       TEXT
                        );

                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename 
                        ON JsonValue (FullTypeName);

                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename_partition 
                        ON JsonValue (FullTypeName, Partition);

                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename 
                        ON JsonValue (Key, FullTypeName);

                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename_partition 
                        ON JsonValue (Key, FullTypeName, Partition);";

                    command.ExecuteNonQuery ();
                }
                finally
                {
                    _connection.Close ();
                }

                return _connection;
            }
        }

        private ValueTask<SqliteConnection> BuildConnectionAsync (CancellationToken cancellationToken = default)
        {
            return
                _processingQueue
                    .Queue (
                        async () =>
                        {
                            _connection = new SqliteConnection (_dbConnectionString);

                            try
                            {
                                await _connection.OpenAsync (cancellationToken).ConfigureAwait (false);

                                var supportsJson = false;

                                // Enable write-ahead logging
                                using var hasJsonCommand = _connection.CreateCommand ();
                                hasJsonCommand.CommandText = @" PRAGMA compile_options; ";
                                using var reader = await hasJsonCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                                while (await reader.ReadAsync (cancellationToken).ConfigureAwait(false))
                                {
                                    if (reader.GetString (0)?.Equals ("ENABLE_JSON1") ?? false)
                                    {
                                        supportsJson = true;
                                        break;
                                    }
                                }

                                if (!supportsJson)
                                {
                                    throw new TychoDbException ("JSON support is not available for this platform");
                                }

                                using var command = _connection.CreateCommand ();

                                // Enable write-ahead logging and normal synchronous mode
                                command.CommandText =
                                    @"
                                        PRAGMA journal_mode = WAL;
                                        PRAGMA synchronous = normal;

                                        CREATE TABLE IF NOT EXISTS JsonValue
                                        (
                                            Key             TEXT PRIMARY KEY,
                                            FullTypeName    TEXT NOT NULL,
                                            Data            JSON NOT NULL,
                                            Partition       TEXT
                                        );

                                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename 
                                        ON JsonValue (FullTypeName);

                                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_fulltypename_partition 
                                        ON JsonValue (FullTypeName, Partition);

                                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename 
                                        ON JsonValue (Key, FullTypeName);

                                        CREATE INDEX IF NOT EXISTS idx_jsonvalue_key_fulltypename_partition 
                                        ON JsonValue (Key, FullTypeName, Partition);";

                                await command.ExecuteNonQueryAsync (cancellationToken).ConfigureAwait (false);
                            }
                            finally
                            {
                                await _connection.CloseAsync ().ConfigureAwait(false);
                            }

                            return _connection;
                        });
        }

        private Func<T, object> GetIdFor<T>()
        {
            var type = typeof (T);
            if (!_registeredTypeInformation.ContainsKey (type))
            {
                throw new TychoDbException ($"Registration missing for type: {type}");
            }

            return _registeredTypeInformation[type].GetId<T> ();
        }
    }

    internal static class SqliteExtensions
    {
        public static ValueTask<T> WithConnectionBlock<T> (this SqliteConnection connection, ProcessingQueue processingQueue, Func<SqliteConnection, T> func)
        {
            if (connection == null)
            {
                throw new TychoDbException ("Please call 'Connect' before performing an operation");
            }

            return processingQueue
                .Queue (
                    async () =>
                    {
                        try
                        {
                            await connection.OpenAsync ().ConfigureAwait (false);
                            return func.Invoke (connection);
                        }
                        finally
                        {
                            await connection.CloseAsync ().ConfigureAwait (false);
                        }
                    });
        }

        public static ValueTask<T> WithConnectionBlock<T> (this SqliteConnection connection, ProcessingQueue processingQueue, Func<SqliteConnection, ValueTask<T>> func)
        {
            if (connection == null)
            {
                throw new TychoDbException ("Please call 'Connect' before performing an operation");
            }

            return processingQueue
                .Queue (
                    async () =>
                    {
                        try
                        {
                            await connection.OpenAsync ().ConfigureAwait (false);
                            return await func.Invoke (connection).ConfigureAwait (false);
                        }
                        finally
                        {
                            await connection.CloseAsync ().ConfigureAwait (false);
                        }
                    });
        }

        public static object AsValueOrDbNull<T>(this T value)
            where T : class
        {
            return value ?? (object)DBNull.Value;
        }
    }
}
