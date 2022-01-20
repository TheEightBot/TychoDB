using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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

        private readonly object _connectionLock = new ();

        private readonly string _dbConnectionString;

        private readonly IJsonSerializer _jsonSerializer;

        private readonly bool _persistConnection;

        private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypeInformation = new ();

        private readonly ProcessingQueue _processingQueue = new ();

        private readonly StringBuilder _commandBuilder = new ();

        private SqliteConnection _connection;

        private bool _isDisposed;

        private bool _requireTypeRegistration;

        private StringBuilder ReusableStringBuilder
        {
            get
            {
                _commandBuilder.Clear();
                return _commandBuilder;
            }
        }

        public TychoDb(string dbPath, IJsonSerializer jsonSerializer, string dbName = "tycho_cache.db", string password = null, bool persistConnection = true, bool rebuildCache = false, bool requireTypeRegistration = true)
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

            _persistConnection = persistConnection;

            _requireTypeRegistration = requireTypeRegistration;
        }

        public TychoDb AddTypeRegistration<T> (Expression<Func<T, object>> idPropertySelector)
            where T : class
        {
            var rti = RegisteredTypeInformation.Create (idPropertySelector);

            _registeredTypeInformation[rti.ObjectType] = rti;

            return this;
        }

        public TychoDb AddTypeRegistration<T>()
            where T : class
        {
            var rti = RegisteredTypeInformation.Create<T>();

            _registeredTypeInformation[rti.ObjectType] = rti;

            return this;
        }

        public TychoDb AddTypeRegistrationWithCustomKeySelector<T>(Func<T, object> keySelector)
            where T : class
        {
            var rti = RegisteredTypeInformation.CreateFromFunc(keySelector);

            _registeredTypeInformation[rti.ObjectType] = rti;

            return this;
        }

        public TychoDb Connect()
        {
            if (_connection != null)
            {
                return this;
            }

            _connection = BuildConnection ();

            return this;
        }

        public async ValueTask<TychoDb> ConnectAsync ()
        {
            if (_connection != null)
            {
                return this;
            }
            
            _connection = await BuildConnectionAsync ().ConfigureAwait(false);

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
            return _connection?.DisposeAsync () ?? new ValueTask (Task.CompletedTask);
        }

        public ValueTask<bool> WriteObjectAsync<T> (T obj, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            return WriteObjectsAsync (new[] { obj }, GetIdSelectorFor<T>(), partition, withTransaction, cancellationToken);
        }

        public ValueTask<bool> WriteObjectAsync<T> (T obj, Func<T, object> keySelector, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            return WriteObjectsAsync (new[] { obj }, keySelector, partition, withTransaction, cancellationToken);
        }

        public ValueTask<bool> WriteObjectsAsync<T> (IEnumerable<T> objs, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            return WriteObjectsAsync (objs, GetIdSelectorFor<T>(), partition, withTransaction, cancellationToken);
        }

        public ValueTask<bool> WriteObjectsAsync<T>(IEnumerable<T> objs, Func<T, object> keySelector, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            if(objs == null)
            {
                throw new ArgumentNullException(nameof(objs));
            }

            if(keySelector == null)
            {
                throw new ArgumentNullException(nameof(keySelector));
            }

            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    conn =>
                    {
                        var successful = false;
                        var writeCount = 0;
                        var objsArray = objs as T[] ?? objs.ToArray();
                        
                        var totalCount = objsArray.Length;

                        SqliteTransaction transaction = null;

                        if(withTransaction)
                        {
                            transaction = conn.BeginTransaction (IsolationLevel.Serializable);
                        }

                        try
                        {
                            using var insertCommand = conn.CreateCommand ();
                            insertCommand.CommandText = Queries.InsertOrReplace;

                            var keyParameter = insertCommand.Parameters.Add(ParameterKey, SqliteType.Text);
                            var jsonParameter = insertCommand.Parameters.Add(ParameterJson, SqliteType.Blob);

                            insertCommand.Parameters
                                .Add(ParameterFullTypeName, SqliteType.Text)
                                .Value = typeof (T).FullName;

                            insertCommand.Parameters
                                .Add(ParameterPartition, SqliteType.Text)
                                .Value = partition.AsValueOrEmptyString();

                            insertCommand.Prepare();

                            foreach (var obj in objsArray)
                            {
                                keyParameter.Value = keySelector(obj);
                                jsonParameter.Value = _jsonSerializer.Serialize (obj);

                                var rowId = (long)insertCommand.ExecuteScalar ();

                                writeCount += rowId > 0 ? 1 : 0;
                            }

                            successful = writeCount == totalCount;

                            if(successful)
                            {
                                transaction?.Commit ();
                            }
                            else
                            {
                                transaction?.Rollback();
                            }
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback ();
                            throw new TychoDbException ($"Failed Writing Objects", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }

                        return successful;
                    },
                    _persistConnection,
                    cancellationToken);         
        }

        public ValueTask<int> CountObjectsAsync<T>(string partition = null, FilterBuilder<T> filter = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<T>();
            }

            return _connection
                .WithConnectionBlock<int>(
                    _processingQueue,
                    conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                        }

                        try
                        {
                            using var selectCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.SelectCountFromJsonValueWithFullTypeName);

                            selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = typeof(T).FullName;
                            selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            if (filter != null)
                            {
                                filter.Build(commandBuilder, _jsonSerializer);
                            }

                            selectCommand.CommandText = commandBuilder.ToString();

                            selectCommand.Prepare();

                            using var reader = selectCommand.ExecuteReader();

                            var count = 0;

                            while (reader.Read())
                            {
                                ++count;
                            }

                            transaction?.Commit();

                            return count;
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback();
                            throw new TychoDbException($"Failed Reading Objects", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<bool> ObjectExistsAsync<T>(T obj, string partition = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            return ObjectExistsAsync<T>(GetIdFor(obj), partition, withTransaction, cancellationToken);
        }

        public ValueTask<bool> ObjectExistsAsync<T>(object key, string partition = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                        }

                        try
                        {
                            using var selectCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.SelectExistsFromJsonValueWithKeyAndFullTypeName);

                            selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                            selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = typeof(T).FullName;
                            selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            selectCommand.CommandText = commandBuilder.ToString();

                            selectCommand.Prepare();

                            using var reader = selectCommand.ExecuteReader();

                            var returnValue = false;
                            while (reader.Read())
                            {
                                returnValue = true;
                            }

                            transaction?.Commit();

                            return returnValue;
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback();
                            throw new TychoDbException($"Failed Reading Object with key \"{key}\"", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<T> ReadObjectAsync<T>(T obj, string partition = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            return ReadObjectAsync<T>(GetIdFor(obj), partition, withTransaction, cancellationToken);
        }

        public ValueTask<T> ReadObjectAsync<T> (object key, string partition = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            if(key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    async conn =>
                    {
                        SqliteTransaction transaction = null;

                        if(withTransaction)
                        {
                            transaction = conn.BeginTransaction (IsolationLevel.RepeatableRead);
                        }

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append (Queries.SelectDataFromJsonValueWithKeyAndFullTypeName);

                            selectCommand.Parameters.Add (ParameterKey, SqliteType.Text).Value = key;
                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;
                            selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();
                            
                            selectCommand.CommandText = commandBuilder.ToString ();

                            selectCommand.Prepare();

                            using var reader = selectCommand.ExecuteReader ();

                            T returnValue = default(T);
                            while (reader.Read ())
                            {
                                using var stream = reader.GetStream (0);
                                returnValue = await _jsonSerializer.DeserializeAsync<T>(stream, cancellationToken).ConfigureAwait (false);
                            }

                            transaction?.Commit ();

                            return returnValue;
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback ();
                            throw new TychoDbException ($"Failed Reading Object with key \"{key}\"", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public async ValueTask<T> ReadObjectAsync<T> (FilterBuilder<T> filter, string partition = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            var results = 
                await ReadObjectsAsync (partition, filter, withTransaction, cancellationToken).ConfigureAwait(false);

            var resultsArray = results as T[] ?? results.ToArray();
            
            if(resultsArray.Length > 1)
            {
                throw new TychoDbException ("Too many matching values were found, please refine your query to limit it to a single match");
            }

            return resultsArray.FirstOrDefault ();
        }

        public ValueTask<IEnumerable<T>> ReadObjectsAsync<T> (string partition = null, FilterBuilder<T> filter = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<T>();
            }

            return _connection
                .WithConnectionBlock<IEnumerable<T>> (
                    _processingQueue,
                    async conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                        }

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.SelectDataFromJsonValueWithFullTypeName);

                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;
                            selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();
                            
                            if (filter != null)
                            {
                                filter.Build (commandBuilder, _jsonSerializer);
                            }

                            selectCommand.CommandText = commandBuilder.ToString ();

                            selectCommand.Prepare();

                            using var reader = selectCommand.ExecuteReader ();

                            var objects = new List<T> ();

                            while (reader.Read ())
                            {
                                using var stream = reader.GetStream (0);
                                objects.Add(await _jsonSerializer.DeserializeAsync<T> (stream, cancellationToken).ConfigureAwait (false));
                            }

                            transaction?.Commit ();

                            return objects;
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback ();
                            throw new TychoDbException ($"Failed Reading Objects", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<IEnumerable<TOut>> ReadObjectsAsync<TIn, TOut> (Expression<Func<TIn,TOut>> innerObjectSelection, string partition = null, FilterBuilder<TIn> filter = null, bool withTransaction = false, CancellationToken cancellationToken = default)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<TIn>();
            }

            return _connection
                .WithConnectionBlock<IEnumerable<TOut>> (
                    _processingQueue,
                    async conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                        }

                        var objects = new List<TOut> ();

                        try
                        {
                            using var selectCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            var selectionPath = QueryPropertyPath.BuildPath (innerObjectSelection);

                            commandBuilder.Append (Queries.ExtractDataFromJsonValueWithFullTypeName (selectionPath));

                            selectCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (TIn).FullName;
                            selectCommand.Parameters.Add (ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            if (filter != null)
                            {
                                filter.Build (commandBuilder, _jsonSerializer);
                            }

                            selectCommand.CommandText = commandBuilder.ToString ();

                            selectCommand.Prepare();

                            using var reader = selectCommand.ExecuteReader ();

                            while (reader.Read ())
                            {
                                using var stream = reader.GetStream (0);
                                objects.Add (await _jsonSerializer.DeserializeAsync<TOut> (stream, cancellationToken).ConfigureAwait (false));
                            }

                            transaction?.Commit ();
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback ();
                            throw new TychoDbException ("Failed Reading Objects", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }

                        return objects;
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<bool> DeleteObjectAsync<T>(T obj, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            return DeleteObjectAsync(GetIdFor(obj), partition, withTransaction, cancellationToken);
        }

        public ValueTask<bool> DeleteObjectAsync<T> (object key, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<T>();
            }

            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                        }

                        try
                        {
                            using var deleteCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append (Queries.DeleteDataFromJsonValueWithKeyAndFullTypeName);

                            deleteCommand.Parameters.Add (ParameterKey, SqliteType.Text).Value = key;
                            deleteCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;
                            deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            deleteCommand.CommandText = commandBuilder.ToString ();

                            deleteCommand.Prepare();

                            var deletionCount = deleteCommand.ExecuteNonQuery ();

                            transaction?.Commit ();

                            return deletionCount == 1;
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback ();
                            throw new TychoDbException ($"Failed to delete object with key \"{key}\"", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<(bool Successful, int Count)> DeleteObjectsAsync<T> (string partition = null, FilterBuilder<T> filter = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<T>();
            }

            return _connection
                .WithConnectionBlock (
                    _processingQueue,
                    conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                        }

                        try
                        {
                            using var deleteCommand = conn.CreateCommand ();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append (Queries.DeleteDataFromJsonValueWithFullTypeName);

                            deleteCommand.Parameters.Add (ParameterFullTypeName, SqliteType.Text).Value = typeof (T).FullName;
                            deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            if (filter != null)
                            {
                                filter.Build (commandBuilder, _jsonSerializer);
                            }

                            deleteCommand.CommandText = commandBuilder.ToString ();

                            deleteCommand.Prepare();

                            var deletionCount = deleteCommand.ExecuteNonQuery ();

                            transaction?.Commit ();

                            return (deletionCount > 0, deletionCount);
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback ();
                            throw new TychoDbException ("Failed to delete objects", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<bool> WriteBlobAsync(Stream stream, object key, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    async conn =>
                    {
                        var writeCount = 0;

                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                        }

                        try
                        {
                            using var insertCommand = conn.CreateCommand();
                            insertCommand.CommandText = Queries.InsertOrReplaceBlob;

                            insertCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                            insertCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();
                            insertCommand.Parameters.AddWithValue(ParameterBlobLength, stream.Length);

                            insertCommand.Prepare ();

                            var rowId = (long)insertCommand.ExecuteScalar();

                            writeCount += rowId > 0 ? 1 : 0;

                            if (writeCount > 0)
                            {
                                using (var writeStream = new SqliteBlob(conn, TableStreamValue, TableStreamValueDataColumn, rowId))
                                {
                                    await stream.CopyToAsync(writeStream, cancellationToken).ConfigureAwait(false);
                                }
                            }

                            transaction?.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback();
                            throw new TychoDbException($"Failed Writing Objects", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }

                        return writeCount == 1;
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<bool> BlobExistsAsync(object key, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    conn =>
                    {
                        try
                        {
                            using var selectCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.SelectExistsFromStreamValueWithKey);

                            selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;

                            selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            selectCommand.CommandText = commandBuilder.ToString();

                            selectCommand.Prepare();

                            using var reader = selectCommand.ExecuteReader();

                            bool returnValue = false;
                            while (reader.Read())
                            {
                                returnValue = true;
                            }

                            return returnValue;
                        }
                        catch (Exception ex)
                        {
                            throw new TychoDbException($"Failed Reading Object with key \"{key}\"", ex);
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<Stream> ReadBlobAsync(object key, string partition = null, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    conn =>
                    {
                        try
                        {
                            using var selectCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.SelectDataFromStreamValueWithKey);

                            selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;

                            selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            selectCommand.CommandText = commandBuilder.ToString();

                            selectCommand.Prepare();

                            using var reader = selectCommand.ExecuteReader();

                            Stream returnValue = Stream.Null;
                            while (reader.Read())
                            {
                                returnValue = reader.GetStream(0);
                            }

                            return returnValue;
                        }
                        catch (Exception ex)
                        {
                            throw new TychoDbException($"Failed Reading Object with key \"{key}\"", ex);
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<bool> DeleteBlobAsync(object key, string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                        }

                        try
                        {
                            using var deleteCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.DeleteDataFromStreamValueWithKey);

                            deleteCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                            deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            deleteCommand.CommandText = commandBuilder.ToString();

                            deleteCommand.Prepare();

                            var deletionCount = deleteCommand.ExecuteNonQuery();

                            transaction?.Commit();

                            return deletionCount == 1;
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback();
                            throw new TychoDbException($"Failed to delete object with key \"{key}\"", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public ValueTask<(bool Successful, int Count)> DeleteBlobsAsync(string partition, bool withTransaction = true, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    conn =>
                    {
                        SqliteTransaction transaction = null;

                        if (withTransaction)
                        {
                            transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                        }

                        try
                        {
                            using var deleteCommand = conn.CreateCommand();

                            var commandBuilder = ReusableStringBuilder;

                            commandBuilder.Append(Queries.DeleteDataFromStreamValueWithPartition);
                            deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partition.AsValueOrEmptyString();

                            deleteCommand.CommandText = commandBuilder.ToString();

                            deleteCommand.Prepare();

                            var deletionCount = deleteCommand.ExecuteNonQuery();

                            transaction?.Commit();

                            return (deletionCount > 0, deletionCount);
                        }
                        catch (Exception ex)
                        {
                            transaction?.Rollback();
                            throw new TychoDbException("Failed to delete objects", ex);
                        }
                        finally
                        {
                            transaction?.Dispose();
                        }
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public TychoDb CreateIndex<TObj> (Expression<Func<TObj, object>> propertyPath, string indexName)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<TObj>();
            }

            return CreateIndex(QueryPropertyPath.BuildPath(propertyPath), QueryPropertyPath.IsNumeric(propertyPath), GetSafeTypeName<TObj>(), indexName);
        }

        public TychoDb CreateIndex(string propertyPathString, bool isNumeric, string objectTypeName, string indexName)
        {
            lock (_connectionLock)
            {
                var transaction = _connection.BeginTransaction(IsolationLevel.Serializable);

                var fullIndexName = $"idx_{indexName}_{objectTypeName}";
                try
                {
                    if (!_persistConnection)
                    {
                        _connection.Open();
                    }

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

                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw new TychoDbException($"Failed to Create Index: {fullIndexName}", ex);
                }
                finally
                {
                    if (!_persistConnection)
                    {
                        _connection.Close();
                    }
                }
            }

            return this;
        }

        public ValueTask<bool> CreateIndexAsync<TObj>(Expression<Func<TObj, object>> propertyPath, string indexName, CancellationToken cancellationToken = default)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<TObj>();
            }

            return CreateIndexAsync(QueryPropertyPath.BuildPath(propertyPath), QueryPropertyPath.IsNumeric(propertyPath), GetSafeTypeName<TObj>(), indexName, cancellationToken);
        }

        public ValueTask<bool> CreateIndexAsync(string propertyPathString, bool isNumeric, string objectTypeName, string indexName, CancellationToken cancellationToken = default)
        {
            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    conn =>
                    {
                        using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                        
                        var fullIndexName = $"idx_{indexName}_{objectTypeName}";

                        try
                        {
                            using var createIndexCommand = conn.CreateCommand();

                            createIndexCommand.CommandText = 
                                isNumeric 
                                    ? Queries.CreateIndexForJsonValueAsNumeric(fullIndexName, propertyPathString) 
                                    : Queries.CreateIndexForJsonValue(fullIndexName, propertyPathString);

                            createIndexCommand.ExecuteNonQuery();

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            throw new TychoDbException($"Failed to Create Index: {fullIndexName}", ex);
                        }

                        return true;
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public TychoDb CreateIndex<TObj>(Expression<Func<TObj, object>>[] propertyPaths, string indexName)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<TObj>();
            }

            var processedPaths =
                    propertyPaths
                        .Select(x => (QueryPropertyPath.BuildPath(x), QueryPropertyPath.IsNumeric(x)))
                        .ToArray();

            lock (_connectionLock)
            {
                var transaction = _connection.BeginTransaction(IsolationLevel.Serializable);

                var fullIndexName = $"idx_{indexName}_{GetSafeTypeName<TObj>()}";
                try
                {
                    if (!_persistConnection)
                    {
                        _connection.Open();
                    }

                    using var createIndexCommand = _connection.CreateCommand();

                    createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, processedPaths);

                    createIndexCommand.ExecuteNonQuery();

                    transaction.Commit();
                }
                catch (Exception ex)
                {
                    transaction.Rollback();
                    throw new TychoDbException($"Failed to Create Index: {fullIndexName}", ex);
                }
                finally
                {
                    if (!_persistConnection)
                    {
                        _connection.Close();
                    }
                }
            }

            return this;
        }

        public ValueTask<bool> CreateIndexAsync<TObj>(Expression<Func<TObj, object>>[] propertyPaths, string indexName, CancellationToken cancellationToken = default)
        {
            if (_requireTypeRegistration)
            {
                CheckHasRegisteredType<TObj>();
            }

            var processedPaths =
                    propertyPaths
                        .Select(x => (QueryPropertyPath.BuildPath(x), QueryPropertyPath.IsNumeric(x)))
                        .ToArray();

            return _connection
                .WithConnectionBlock(
                    _processingQueue,
                    conn =>
                    {
                        using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                        var fullIndexName = $"idx_{indexName}_{GetSafeTypeName<TObj>()}";

                        try
                        {
                            using var createIndexCommand = conn.CreateCommand();

                            createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, processedPaths);

                            createIndexCommand.ExecuteNonQuery();

                            transaction.Commit();
                        }
                        catch (Exception ex)
                        {
                            transaction.Rollback();
                            throw new TychoDbException($"Failed to Create Index: {fullIndexName}", ex);
                        }

                        return true;
                    },
                    _persistConnection,
                    cancellationToken);
        }

        public Func<T, object> GetIdSelectorFor<T>()
        {
            var type = typeof(T);

            CheckHasRegisteredType(type);

            return _registeredTypeInformation[type].GetIdSelector<T>();
        }

        public object GetIdFor<T>(T obj)
        {
            var type = typeof(T);

            CheckHasRegisteredType(type);

            return _registeredTypeInformation[type].GetIdFor<T>(obj);
        }

        public RegisteredTypeInformation GetRegisteredTypeInformationFor<T>()
        {
            var type = typeof(T);

            CheckHasRegisteredType(type);

            return _registeredTypeInformation[type];
        }

        private string GetSafeTypeName<TObj>()
        {
            var type = typeof(TObj);
            return _registeredTypeInformation.ContainsKey(type)
                ? _registeredTypeInformation[type].SafeTypeName
                : type.GetSafeTypeName();
        }

        protected virtual void Dispose (bool disposing)
        {
            if (_isDisposed)
            {
                return;
            }
                
            if (disposing)
            {
                _connection?.Close();
                _connection?.Dispose ();
            }

            _isDisposed = true;
        }

        public void Dispose ()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose (true);
            GC.SuppressFinalize (this);
        }

        private SqliteConnection BuildConnection ()
        {
            lock(_connectionLock)
            {
                var connection = new SqliteConnection (_dbConnectionString);

                connection.Open ();

                var supportsJson = false;

                // Enable write-ahead logging
                using var hasJsonCommand = connection.CreateCommand ();
                hasJsonCommand.CommandText = Queries.PragmaCompileOptions;
                using var reader = hasJsonCommand.ExecuteReader ();

                while (reader.Read ())
                {
                    if (!(reader.GetString(0)?.Equals(Queries.EnableJSON1Pragma) ?? false))
                    {
                        continue;
                    }
                    
                    supportsJson = true;
                    break;
                }

                if (!supportsJson)
                {
                    connection.Close();
                    throw new TychoDbException ("JSON support is not available for this platform");
                }

                using var command = connection.CreateCommand ();

                // Enable write-ahead logging and normal synchronous mode
                command.CommandText = Queries.CreateDatabaseSchema;

                command.ExecuteNonQuery ();

                return connection;
            }
        }

        private ValueTask<SqliteConnection> BuildConnectionAsync (CancellationToken cancellationToken = default)
        {
            return
                _processingQueue
                    .Queue (
                        () =>
                        {
                            _connection = new SqliteConnection (_dbConnectionString);

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
                                _connection.Close();
                                throw new TychoDbException ("JSON support is not available for this platform");
                            }

                            using var command = _connection.CreateCommand ();

                            // Enable write-ahead logging and normal synchronous mode
                            command.CommandText = Queries.CreateDatabaseSchema;

                            command.ExecuteNonQuery ();

                            return _connection;
                        },
                        cancellationToken);
        }

        private void CheckHasRegisteredType<T>()
        {
            var type = typeof(T);

            CheckHasRegisteredType(type);
        }

        private void CheckHasRegisteredType(Type type)
        {
            if (!_registeredTypeInformation.ContainsKey(type))
            {
                throw new TychoDbException($"Registration missing for type: {type}");
            }
        }
    }

    internal static class SqliteExtensions
    {
        public static ValueTask<T> WithConnectionBlock<T> (this SqliteConnection connection, ProcessingQueue processingQueue, Func<SqliteConnection, T> func, bool persistConnection, CancellationToken cancellationToken = default)
        {
            if (connection == null)
            {
                throw new TychoDbException ("Please call 'Connect' before performing an operation");
            }

            return processingQueue
                .Queue (
                    () =>
                    {
                        try
                        {
                            if(!persistConnection)
                            {
                                connection.Open();
                            }

                            return func.Invoke (connection);
                        }
                        finally
                        {
                            if(!persistConnection)
                            {
                                connection.Close();
                            }
                        }
                    },
                    cancellationToken);
        }

        public static ValueTask<T> WithConnectionBlock<T> (this SqliteConnection connection, ProcessingQueue processingQueue, Func<SqliteConnection, ValueTask<T>> func, bool persistConnection, CancellationToken cancellationToken = default)
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
                            if (!persistConnection)
                            {
                                connection.Open();
                            }

                            return await func.Invoke (connection).ConfigureAwait (false);
                        }
                        finally
                        {
                            if (!persistConnection)
                            {
                                connection.Close();
                            }
                        }
                    },
                    cancellationToken);
        }

        public static object AsValueOrDbNull<T>(this T value)
            where T : class
        {
            return value ?? (object)DBNull.Value;
        }

        public static string AsValueOrEmptyString(this string value)
        {
            return value ?? string.Empty;
        }
    }
}
