using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Tycho
{
    public class TychoDb : IDisposable
    {
        private const string
            ParameterFullTypeName = "$fullTypeName",
            ParameterPartition = "$partition",
            ParameterKey = "$key",
            ParameterJson = "$json",
            ParameterBlob = "$blob",
            ParameterBlobLength = "$blobLength",
            TableStreamValue = "StreamValue",
            TableStreamValueDataColumn = "Data";

        private readonly object _connectionLock = new object ();

        private readonly string _dbConnectionString;

        private readonly IJsonSerializer _jsonSerializer;

        private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypeInformation = new Dictionary<Type, RegisteredTypeInformation>();

        private readonly ProcessingQueue _processingQueue = new ProcessingQueue();

        private readonly StringBuilder _commandBuilder = new StringBuilder();

        private SqliteConnection _connection;

        private bool _isDisposed;


        private StringBuilder ReusableStringBuilder
        {
            get
            {
                _commandBuilder.Clear();
                return _commandBuilder;
            }
        }

        public TychoDb (string dbPath, IJsonSerializer jsonSerializer, string dbName = "tycho_cache.db", string password = null, bool rebuildCache = false)
        {
            SQLitePCL.Batteries_V2.Init ();

            _jsonSerializer = jsonSerializer;

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

                foreach (var registeredType in _registeredTypeInformation)
                {
                    var value = registeredType.Value;
                    CreateIndex(value.IdPropertyPath, value.IsNumeric, value.TypeName, $"ID_{value.TypeName}_{value.IdProperty}");
                }
            }

            return this;
        }

        public async ValueTask<TychoDb> ConnectAsync ()
        {

            if (_connection == null)
            {
                _connection = await BuildConnectionAsync ().ConfigureAwait(false);

                foreach (var registeredType in _registeredTypeInformation)
                {
                    var value = registeredType.Value;
                    await CreateIndexAsync(value.IdPropertyPath, value.IsNumeric, value.TypeName, $"ID_{value.TypeName}_{value.IdProperty}").ConfigureAwait(false);
                }
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

                        using var transaction = await conn.BeginTransactionAsync (IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

                        try
                        {

                            foreach (var obj in objs)
                            {
                                var rowId = 0L;

                                using var insertCommand = conn.CreateCommand ();
                                insertCommand.CommandText = Queries.InsertOrReplace;

                                insertCommand.Parameters.Add (ParameterKey, SqliteType.Text).Value = keySelector (obj);
                                insertCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull();
                                insertCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;                                
                                insertCommand.Parameters.Add (ParameterJson, SqliteType.Text).Value = _jsonSerializer.Serialize (obj);

                                rowId = (long)await insertCommand.ExecuteScalarAsync (cancellationToken).ConfigureAwait(false);

                                writeCount += rowId > 0 ? 1 : 0;
                            }

                            await transaction.CommitAsync (cancellationToken).ConfigureAwait (false);
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync (cancellationToken).ConfigureAwait (false);
                            throw new TychoDbException ($"Failed Writing Objects", ex);
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
                        using var transaction = await conn.BeginTransactionAsync (IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait (false);

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append (Queries.SelectDataFromJsonValueWithKeyAndFullTypeName);

                            selectCommand.Parameters.Add (ParameterKey, SqliteType.Text).Value = key;
                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;

                            if (!string.IsNullOrEmpty(partition))
                            {
                                commandBuilder.Append (Queries.AndPartitionHasValue);
                                selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }
                            else
                            {
                                commandBuilder.Append (Queries.AndPartitionIsNull);
                            }

                            selectCommand.CommandText = commandBuilder.ToString ();

                            using var reader = await selectCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                            T returnValue = default(T);
                            while (await reader.ReadAsync (cancellationToken).ConfigureAwait (false))
                            {
                                using var stream = reader.GetStream (0);
                                returnValue = await _jsonSerializer.DeserializeAsync<T>(stream, cancellationToken).ConfigureAwait (false);
                            }

                            await transaction.CommitAsync (cancellationToken).ConfigureAwait (false);

                            return returnValue;
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync (cancellationToken).ConfigureAwait (false);
                            throw new TychoDbException ($"Failed Reading Object with key \"{key}\"", ex);
                        }
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
                .WithConnectionBlock<IEnumerable<T>> (
                    _processingQueue,
                    async conn =>
                    {
                        using var transaction = await conn.BeginTransactionAsync (IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait (false);

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.SelectDataFromJsonValueWithFullTypeName);

                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;

                            if (!string.IsNullOrEmpty (partition))
                            {
                                commandBuilder.Append (Queries.AndPartitionHasValue);
                                selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }
                            else
                            {
                                commandBuilder.Append (Queries.AndPartitionIsNull);
                            }

                            if (filter != null)
                            {
                                filter.Build (commandBuilder);
                            }

                            selectCommand.CommandText = commandBuilder.ToString ();

                            using var reader = await selectCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                            var objects = new List<T> ();

                            while (await reader.ReadAsync (cancellationToken).ConfigureAwait (false))
                            {
                                using var stream = reader.GetStream (0);
                                objects.Add(await _jsonSerializer.DeserializeAsync<T> (stream, cancellationToken).ConfigureAwait (false));
                            }

                            await transaction.CommitAsync (cancellationToken).ConfigureAwait (false);

                            return objects;
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync (cancellationToken).ConfigureAwait (false);
                            throw new TychoDbException ($"Failed Reading Objects", ex);
                        }
                    });
        }

        public ValueTask<IEnumerable<TOut>> ReadObjectsAsync<TIn, TOut> (Expression<Func<TIn,TOut>> innerObjectSelection, string partition = null, FilterBuilder<TIn> filter = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock<IEnumerable<TOut>> (
                    _processingQueue,
                    async conn =>
                    {
                        using var transaction = await conn.BeginTransactionAsync (IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait (false);

                        var objects = new List<TOut> ();

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            var selectionPath = QueryPropertyPath.BuildPath (innerObjectSelection);

                            commandBuilder.Append (Queries.ExtractDataFromJsonValueWithFullTypeName (selectionPath));

                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (TIn).FullName;

                            if (!string.IsNullOrEmpty (partition))
                            {
                                commandBuilder.Append (Queries.AndPartitionHasValue);
                                selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }
                            else
                            {
                                commandBuilder.Append (Queries.AndPartitionIsNull);
                            }

                            if (filter != null)
                            {
                                filter.Build (commandBuilder);
                            }

                            selectCommand.CommandText = commandBuilder.ToString ();

                            using var reader = await selectCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                            while (await reader.ReadAsync (cancellationToken).ConfigureAwait (false))
                            {
                                using var stream = reader.GetStream (0);
                                objects.Add (await _jsonSerializer.DeserializeAsync<TOut> (stream, cancellationToken).ConfigureAwait (false));
                            }

                            await transaction.CommitAsync (cancellationToken).ConfigureAwait (false);
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync (cancellationToken).ConfigureAwait (false);
                            throw new TychoDbException ("Failed Reading Objects", ex);
                        }

                        return objects;
                    });
        }

        public ValueTask<bool> DeleteObjectAsync<T> (object key, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    async conn =>
                    {
                        using var transaction = await conn.BeginTransactionAsync (IsolationLevel.Serializable, cancellationToken).ConfigureAwait (false);

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append (Queries.DeleteDataFromJsonValueWithKeyAndFullTypeName);

                            selectCommand.Parameters.Add (ParameterKey, SqliteType.Text).Value = key;
                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;

                            if (!string.IsNullOrEmpty (partition))
                            {
                                commandBuilder.Append (Queries.AndPartitionHasValue);
                                selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }
                            else
                            {
                                commandBuilder.Append (Queries.AndPartitionIsNull);
                            }

                            selectCommand.CommandText = commandBuilder.ToString ();

                            var deletionCount = await selectCommand.ExecuteNonQueryAsync (cancellationToken).ConfigureAwait (false);

                            await transaction.CommitAsync ().ConfigureAwait (false);

                            return deletionCount == 1;
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync (cancellationToken).ConfigureAwait (false);
                            throw new TychoDbException ($"Failed to delete object with key \"{key}\"", ex);
                        }
                    });
        }

        public ValueTask<(bool Successful, int Count)> DeleteObjectsAsync<T> (string partition = null, FilterBuilder<T> filter = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    async conn =>
                    {
                        using var transaction = await conn.BeginTransactionAsync (IsolationLevel.Serializable, cancellationToken).ConfigureAwait (false);

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append (Queries.DeleteDataFromJsonValueWithFullTypeName);

                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;

                            if (!string.IsNullOrEmpty (partition))
                            {
                                commandBuilder.Append (Queries.AndPartitionHasValue);
                                selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull ();
                            }
                            else
                            {
                                commandBuilder.Append (Queries.AndPartitionIsNull);
                            }

                            if (filter != null)
                            {
                                filter.Build (commandBuilder);
                            }

                            selectCommand.CommandText = commandBuilder.ToString ();

                            var deletionCount = await selectCommand.ExecuteNonQueryAsync (cancellationToken).ConfigureAwait (false);

                            await transaction.CommitAsync ().ConfigureAwait (false);

                            return (deletionCount > 0, deletionCount);
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync (cancellationToken).ConfigureAwait (false);
                            throw new TychoDbException ("Failed to delete objects", ex);
                        }
                    });
        }

        public ValueTask<bool> WriteBlobAsync(Stream stream, string key, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    async conn =>
                    {
                        var writeCount = 0;

                        using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

                        try
                        {
                            var rowId = 0L;

                            using var insertCommand = conn.CreateCommand();
                            insertCommand.CommandText = Queries.InsertOrReplaceBlob;

                            insertCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                            insertCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull();
                            insertCommand.Parameters.AddWithValue(ParameterBlobLength, stream.Length);

                            rowId = (long)await insertCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                            writeCount += rowId > 0 ? 1 : 0;

                            if (writeCount > 0)
                            {
                                using (var writeStream = new SqliteBlob(conn, TableStreamValue, TableStreamValueDataColumn, rowId))
                                {
                                    await stream.CopyToAsync(writeStream).ConfigureAwait(false);
                                }
                            }

                            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                            throw new TychoDbException($"Failed Writing Objects", ex);
                        }

                        return writeCount == 1;
                    });
        }

        public ValueTask<Stream> ReadBlobAsync(object key, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    async conn =>
                    {
                        try
                        {
                            using var selectCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.SelectDataFromStreamValueWithKey);

                            selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;

                            if (!string.IsNullOrEmpty(partition))
                            {
                                commandBuilder.Append(Queries.AndPartitionHasValue);
                                selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull();
                            }
                            else
                            {
                                commandBuilder.Append(Queries.AndPartitionIsNull);
                            }

                            selectCommand.CommandText = commandBuilder.ToString();
                            using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                            Stream returnValue = Stream.Null;
                            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                            {
                                returnValue = reader.GetStream(0);
                            }

                            return returnValue;
                        }
                        catch (Exception ex)
                        {
                            throw new TychoDbException($"Failed Reading Object with key \"{key}\"", ex);
                        }
                    });
        }

        public ValueTask<bool> DeleteBlobAsync(object key, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    async conn =>
                    {
                        using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

                        try
                        {
                            using var selectCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.DeleteDataFromStreamValueWithKey);

                            selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;

                            if (!string.IsNullOrEmpty(partition))
                            {
                                commandBuilder.Append(Queries.AndPartitionHasValue);
                                selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrDbNull();
                            }
                            else
                            {
                                commandBuilder.Append(Queries.AndPartitionIsNull);
                            }

                            selectCommand.CommandText = commandBuilder.ToString();

                            var deletionCount = await selectCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                            await transaction.CommitAsync().ConfigureAwait(false);

                            return deletionCount == 1;
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                            throw new TychoDbException($"Failed to delete object with key \"{key}\"", ex);
                        }
                    });
        }

        public TychoDb CreateIndex<TObj> (Expression<Func<TObj, object>> propertyPath, string indexName)
        {
            return CreateIndex(QueryPropertyPath.BuildPath(propertyPath), QueryPropertyPath.IsNumeric(propertyPath), typeof(TObj).Name, indexName);
        }

        public TychoDb CreateIndex(string propertyPathString, bool isNumeric, string objectTypeName, string indexName)
        {
            lock (_connectionLock)
            {
                try
                {
                    _connection.Open();

                    var transaction = _connection.BeginTransaction(IsolationLevel.Serializable);

                    var fullIndexName = $"idx_{indexName}_{objectTypeName}";

                    try
                    {
                        using var createIndexCommand = _connection.CreateCommand();

                        if (isNumeric)
                        {
                            createIndexCommand.CommandText = Queries.CreateIndexForJsonValueAsNumeric(fullIndexName, propertyPathString);
                        }
                        else
                        {
                            createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, propertyPathString);
                        }

                        createIndexCommand.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw new TychoDbException($"Failed to Create Index: {fullIndexName}", ex);
                    }
                    finally
                    {
                        transaction.Commit();
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
            return CreateIndexAsync(QueryPropertyPath.BuildPath(propertyPath), QueryPropertyPath.IsNumeric(propertyPath), typeof(TObj).Name, indexName);
        }

        public ValueTask<bool> CreateIndexAsync(string propertyPathString, bool isNumeric, string objectTypeName, string indexName, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    async conn =>
                    {
                        using var transaction = await conn.BeginTransactionAsync(IsolationLevel.Serializable, cancellationToken).ConfigureAwait(false);

                        var result = false;

                        var fullIndexName = $"idx_{indexName}_{objectTypeName}";

                        try
                        {
                            using var createIndexCommand = conn.CreateCommand();

                            if (isNumeric)
                            {
                                createIndexCommand.CommandText = Queries.CreateIndexForJsonValueAsNumeric(fullIndexName, propertyPathString);
                            }
                            else
                            {
                                createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, propertyPathString);
                            }

                            await createIndexCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

                            await transaction.CommitAsync().ConfigureAwait(false);

                            result = true;
                        }
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                            throw new TychoDbException($"Failed to Create Index: {fullIndexName}", ex);
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
                    hasJsonCommand.CommandText = Queries.PragmaCompileOptions;
                    using var reader = hasJsonCommand.ExecuteReader ();

                    while (reader.Read ())
                    {
                        if (reader.GetString (0)?.Equals (Queries.EnableJSON1Pragma) ?? false)
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
                    command.CommandText = Queries.CreateDatabaseSchema;

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
                                hasJsonCommand.CommandText = Queries.PragmaCompileOptions;

                                using var reader = await hasJsonCommand.ExecuteReaderAsync (cancellationToken).ConfigureAwait (false);

                                while (await reader.ReadAsync (cancellationToken).ConfigureAwait(false))
                                {
                                    if (reader.GetString (0)?.Equals (Queries.EnableJSON1Pragma) ?? false)
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
                                command.CommandText = Queries.CreateDatabaseSchema;

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
