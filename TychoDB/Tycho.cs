using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace TychoDB;

public class Tycho : IDisposable
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

    private readonly object _connectionLock = new();

    private readonly string _dbConnectionString;

    private readonly IJsonSerializer _jsonSerializer;

    private readonly bool _persistConnection;
    private readonly bool _requireTypeRegistration;
    private readonly bool _useConnectionPooling;
    private readonly int _commandTimeout;

    private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypeInformation = new();

    // Using a ThreadLocal StringBuilder for better performance with multi-threading
    private readonly ThreadLocal<StringBuilder> _commandBuilder = new(() => new StringBuilder(1024));

    private readonly RateLimiter _rateLimiter =
        new ConcurrencyLimiter(
            new ConcurrencyLimiterOptions
            {
                PermitLimit = 1, QueueLimit = int.MaxValue, QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            });

    private SqliteConnection _connection;

    private bool _isDisposed;

    private StringBuilder ReusableStringBuilder
    {
        get
        {
            var builder = _commandBuilder.Value;
            builder.Clear();
            return builder;
        }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Tycho"/> class.
    /// </summary>
    /// <param name="dbPath">The path to the directory where the database file will be stored.</param>
    /// <param name="jsonSerializer">The JSON serializer used for serializing and deserializing objects.</param>
    /// <param name="dbName">The name of the database file. Default is "tycho_cache.db".</param>
    /// <param name="password">The password for the database file. Default is null.</param>
    /// <param name="persistConnection">Indicates whether the database connection should be persisted. Default is true.</param>
    /// <param name="rebuildCache">Indicates whether to rebuild the cache by deleting the existing database file. Default is false.</param>
    /// <param name="requireTypeRegistration">Indicates whether type registration is required. Default is true.</param>
    /// <param name="useConnectionPooling">Indicates whether to use connection pooling. Default is true.</param>
    /// <param name="commandTimeout">The timeout for commands in seconds. Default is 30 seconds.</param>
    public Tycho(
        string dbPath,
        IJsonSerializer jsonSerializer,
        string dbName = "tycho_cache.db",
        string password = null,
        bool persistConnection = true,
        bool rebuildCache = false,
        bool requireTypeRegistration = true,
        bool useConnectionPooling = true,
        int commandTimeout = 30)
    {
        SQLitePCL.Batteries_V2.Init();

        _jsonSerializer = jsonSerializer ?? throw new ArgumentNullException(nameof(jsonSerializer));
        _useConnectionPooling = useConnectionPooling;
        _commandTimeout = commandTimeout;

        var databasePath = Path.Join(dbPath, dbName);

        if (rebuildCache && File.Exists(databasePath))
        {
            File.Delete(databasePath);
        }

        var connectionStringBuilder =
            new SqliteConnectionStringBuilder
            {
                ConnectionString = $"Filename={databasePath}",
                Cache = SqliteCacheMode.Default,
                Mode = SqliteOpenMode.ReadWriteCreate,
            };

        if (password != null)
        {
            connectionStringBuilder.Password = password;
        }

        // Add pooling configuration
        connectionStringBuilder.Pooling = useConnectionPooling;

        _dbConnectionString = connectionStringBuilder.ToString();

        _persistConnection = persistConnection;

        _requireTypeRegistration = requireTypeRegistration;
    }

    public Tycho AddTypeRegistration<T, TId>(
        Expression<Func<T, object>> idPropertySelector,
        EqualityComparer<TId> idComparer = null)
        where T : class
    {
        var rti = RegisteredTypeInformation.Create(idPropertySelector, idComparer);

        _registeredTypeInformation[rti.ObjectType] = rti;

        return this;
    }

    public Tycho AddTypeRegistration<T>()
        where T : class
    {
        var rti = RegisteredTypeInformation.Create<T>();

        _registeredTypeInformation[rti.ObjectType] = rti;

        return this;
    }

    public Tycho AddTypeRegistrationWithCustomKeySelector<T>(
        Func<T, object> keySelector,
        EqualityComparer<string> idComparer = null)
        where T : class
    {
        var rti = RegisteredTypeInformation.CreateFromFunc(keySelector, idComparer);

        _registeredTypeInformation[rti.ObjectType] = rti;

        return this;
    }

    public Tycho Connect()
    {
        if (_connection != null)
        {
            return this;
        }

        _connection = BuildConnection();

        return this;
    }

    public async ValueTask<Tycho> ConnectAsync()
    {
        if (_connection != null)
        {
            return this;
        }

        _connection = await BuildConnectionAsync().ConfigureAwait(false);

        return this;
    }

    public void Disconnect()
    {
        lock (_connectionLock)
        {
            _connection?.Dispose();
            _connection = null;
        }
    }

    public async ValueTask DisconnectAsync()
    {
        if (_connection == null)
        {
            return;
        }

        await _connection.DisposeAsync().ConfigureAwait(false);

        _connection = null;
    }

    public ValueTask<bool> WriteObjectAsync<T>(T obj, string partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return WriteObjectsAsync(new[] { obj }, GetIdSelectorFor<T>(), partition, withTransaction, cancellationToken);
    }

    public ValueTask<bool> WriteObjectAsync<T>(T obj, Func<T, object> keySelector, string partition = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        return WriteObjectsAsync(new[] { obj }, keySelector, partition, withTransaction, cancellationToken);
    }

    public ValueTask<bool> WriteObjectsAsync<T>(IEnumerable<T> objs, string partition = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        return WriteObjectsAsync(objs, GetIdSelectorFor<T>(), partition, withTransaction, cancellationToken);
    }

    public ValueTask<bool> WriteObjectsAsync<T>(IEnumerable<T> objs, Func<T, object> keySelector,
        string partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        if (objs == null)
        {
            throw new ArgumentNullException(nameof(objs));
        }

        if (keySelector == null)
        {
            throw new ArgumentNullException(nameof(keySelector));
        }

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    var successful = false;
                    var writeCount = 0;
                    var potentialTotalCount = 0;

                    SqliteTransaction transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        // Convert to list to avoid multiple enumeration
                        var objsList = objs as IList<T> ?? objs.ToList();
                        potentialTotalCount = objsList.Count;

                        if (potentialTotalCount == 0)
                        {
                            // Nothing to write
                            transaction?.Commit();
                            return true;
                        }

                        using var insertCommand = conn.CreateCommand();
                        insertCommand.CommandTimeout = _commandTimeout;
                        insertCommand.CommandText = Queries.InsertOrReplace;

                        var keyParameter = insertCommand.Parameters.Add(ParameterKey, SqliteType.Text);
                        var jsonParameter = insertCommand.Parameters.Add(ParameterJson, SqliteType.Blob);

                        insertCommand.Parameters
                            .Add(ParameterFullTypeName, SqliteType.Text)
                            .Value = typeof(T).FullName;

                        insertCommand.Parameters
                            .Add(ParameterPartition, SqliteType.Text)
                            .Value = partition.AsValueOrEmptyString();

                        // Batch processing for large datasets
                        const int batchSize = 100;

                        for (int i = 0; i < potentialTotalCount; i += batchSize)
                        {
                            int currentBatchSize = Math.Min(batchSize, potentialTotalCount - i);

                            for (int j = 0; j < currentBatchSize; j++)
                            {
                                var obj = objsList[i + j];
                                keyParameter.Value = keySelector(obj);
                                jsonParameter.Value = _jsonSerializer.Serialize(obj);

                                var rowId = (long)insertCommand.ExecuteScalar();
                                writeCount += rowId > 0 ? 1 : 0;
                            }

                            // Check for cancellation between batches
                            if (cancellationToken.IsCancellationRequested)
                            {
                                break;
                            }
                        }

                        successful = writeCount == potentialTotalCount;

                        if (successful && !cancellationToken.IsCancellationRequested)
                        {
                            transaction?.Commit();
                        }
                        else
                        {
                            transaction?.Rollback();
                        }
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed Writing Objects", ex);
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

    public ValueTask<int> CountObjectsAsync<T>(string partition = null, FilterBuilder<T> filter = null,
        bool withTransaction = false, CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        return _connection
            .WithConnectionBlockAsync<int>(
                _rateLimiter,
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
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

                        if (filter != null)
                        {
                            filter.Build(commandBuilder, _jsonSerializer);
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

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
                        throw new TychoException($"Failed Reading Objects", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<bool> ObjectExistsAsync<T>(T obj, string partition = null, bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        return ObjectExistsAsync<T>(GetIdFor(obj), partition, withTransaction, cancellationToken);
    }

    public ValueTask<bool> ObjectExistsAsync<T>(object key, string partition = null, bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

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
                        throw new TychoException($"Failed Reading Object with key \"{key}\"", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<T> ReadObjectAsync<T>(T obj, string partition = null, bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        return ReadObjectAsync<T>(GetIdFor(obj), partition, withTransaction, cancellationToken);
    }

    public ValueTask<T> ReadObjectAsync<T>(object key, string partition = null, bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        if (key == null)
        {
            throw new ArgumentNullException(nameof(key));
        }

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                async conn =>
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

                        commandBuilder.Append(Queries.SelectDataFromJsonValueWithKeyAndFullTypeName);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = typeof(T).FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        using var reader = selectCommand.ExecuteReader();

                        T returnValue = default(T);
                        while (reader.Read())
                        {
                            using var stream = reader.GetStream(0);
                            returnValue = await _jsonSerializer.DeserializeAsync<T>(stream, cancellationToken)
                                .ConfigureAwait(false);
                        }

                        transaction?.Commit();

                        return returnValue;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed Reading Object with key \"{key}\"", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public async ValueTask<T> ReadFirstObjectAsync<T>(
        FilterBuilder<T> filter,
        string partition = null,
        bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        var results =
            await ReadObjectsAsync(partition, filter, null, 1, withTransaction, cancellationToken)
                .ConfigureAwait(false);

        return results.FirstOrDefault();
    }

    public async ValueTask<T> ReadObjectAsync<T>(
        FilterBuilder<T> filter,
        string partition = null,
        bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        var matches = await CountObjectsAsync(partition, filter, withTransaction, cancellationToken)
            .ConfigureAwait(false);

        if (matches > 1)
        {
            throw new TychoException(
                "Too many matching values were found, please refine your query to limit it to a single match");
        }

        var results =
            await ReadObjectsAsync(partition, filter, null, 1, withTransaction, cancellationToken)
                .ConfigureAwait(false);

        return results.FirstOrDefault();
    }

    public ValueTask<IEnumerable<T>> ReadObjectsAsync<T>(
        string partition = null,
        FilterBuilder<T> filter = null,
        SortBuilder<T> sort = null,
        int? top = null,
        bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        return _connection
            .WithConnectionBlockAsync<IEnumerable<T>>(
                _rateLimiter,
                async conn =>
                {
                    SqliteTransaction transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    try
                    {
                        using var selectCommand = conn.CreateCommand();
                        selectCommand.CommandTimeout = _commandTimeout;

                        var commandBuilder = ReusableStringBuilder;

                        commandBuilder.Append(Queries.SelectDataFromJsonValueWithFullTypeName);

                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = typeof(T).FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

                        if (filter != null)
                        {
                            filter.Build(commandBuilder, _jsonSerializer);
                        }

                        if (sort != null)
                        {
                            sort.Build(commandBuilder);
                        }

                        if (top != null)
                        {
                            commandBuilder.AppendLine(Queries.Limit(top.Value));
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        // Use CommandBehavior.SequentialAccess for better performance
                        using var reader = selectCommand.ExecuteReader(CommandBehavior.SequentialAccess);

                        // Pre-allocate collection to prevent resizing
                        List<T> objects;

                        // Try to determine list size beforehand for better memory efficiency
                        if (top.HasValue)
                        {
                            objects = new List<T>(top.Value);
                        }
                        else
                        {
                            objects = new List<T>();
                        }

                        // Use buffered reading for optimal performance
                        const int bufferSize = 1024 * 32; // 32 KB buffer
                        byte[] buffer = new byte[bufferSize];

                        while (reader.Read())
                        {
                            if (cancellationToken.IsCancellationRequested)
                            {
                                break;
                            }

                            using var stream = reader.GetStream(0);
                            using var memoryStream = new MemoryStream();

                            int bytesRead;
                            while ((bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)
                                       .ConfigureAwait(false)) > 0)
                            {
                                memoryStream.Write(buffer, 0, bytesRead);
                            }

                            memoryStream.Position = 0;
                            objects.Add(await _jsonSerializer.DeserializeAsync<T>(memoryStream, cancellationToken)
                                .ConfigureAwait(false));
                        }

                        transaction?.Commit();

                        return objects;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed Reading Objects", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public async ValueTask<IEnumerable<TOut>> ReadObjectsAsync<TIn, TOut>(
        Expression<Func<TIn, TOut>> innerObjectSelection,
        string partition = null,
        FilterBuilder<TIn> filter = null,
        bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        var results =
            await ReadObjectsWithKeysAsync(innerObjectSelection, partition, filter, withTransaction, cancellationToken)
                .ConfigureAwait(false);

        return results.Select(x => x.InnerObject);
    }

    public ValueTask<IEnumerable<(string Key, TOut InnerObject)>> ReadObjectsWithKeysAsync<TIn, TOut>(
        Expression<Func<TIn, TOut>> innerObjectSelection,
        string partition = null,
        FilterBuilder<TIn> filter = null,
        bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TIn>();
        }

        return _connection
            .WithConnectionBlockAsync<IEnumerable<(string Key, TOut InnerObject)>>(
                _rateLimiter,
                async conn =>
                {
                    SqliteTransaction transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    var objects = new List<(string, TOut)>();

                    try
                    {
                        using var selectCommand = conn.CreateCommand();

                        var commandBuilder = ReusableStringBuilder;

                        var selectionPath = QueryPropertyPath.BuildPath(innerObjectSelection);

                        commandBuilder.Append(Queries.ExtractDataAndKeyFromJsonValueWithFullTypeName(selectionPath));

                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value =
                            typeof(TIn).FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

                        if (filter != null)
                        {
                            filter.Build(commandBuilder, _jsonSerializer);
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        using var reader = selectCommand.ExecuteReader();

                        while (reader.Read())
                        {
                            var key = reader.GetString(0);

                            using var innerObjectStream = reader.GetStream(1);
                            var innerObject = await _jsonSerializer
                                .DeserializeAsync<TOut>(innerObjectStream, cancellationToken).ConfigureAwait(false);

                            objects.Add((key, innerObject));
                        }

                        transaction?.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException("Failed Reading Objects", ex);
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

    public ValueTask<bool> DeleteObjectAsync<T>(T obj, string partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return DeleteObjectAsync(GetIdFor(obj), partition, withTransaction, cancellationToken);
    }

    public ValueTask<bool> DeleteObjectAsync<T>(object key, string partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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

                        commandBuilder.Append(Queries.DeleteDataFromJsonValueWithKeyAndFullTypeName);

                        deleteCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                        deleteCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = typeof(T).FullName;
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        var deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return deletionCount == 1;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed to delete object with key \"{key}\"", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<int> DeleteObjectsAsync<T>(string partition = null, FilterBuilder<T> filter = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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

                        commandBuilder.Append(Queries.DeleteDataFromJsonValueWithFullTypeName);

                        deleteCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = typeof(T).FullName;
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

                        if (filter != null)
                        {
                            filter.Build(commandBuilder, _jsonSerializer);
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        var deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return deletionCount;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException("Failed to delete objects", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<int> DeleteObjectsAsync(string partition, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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

                        commandBuilder.Append(Queries.DeleteDataFromJsonValueWithPartition);

                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        var deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return deletionCount;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException("Failed to delete objects", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<int> DeleteObjectsAsync(bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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

                        commandBuilder.Append(Queries.DeleteDataFromJsonValue);

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        var deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return deletionCount;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException("Failed to delete objects", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<bool> WriteBlobAsync(Stream stream, object key, string partition = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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
                        insertCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();
                        insertCommand.Parameters.AddWithValue(ParameterBlobLength, stream.Length);

                        var rowId = (long)insertCommand.ExecuteScalar();

                        writeCount += rowId > 0 ? 1 : 0;

                        if (writeCount > 0)
                        {
                            using (var writeStream = new SqliteBlob(conn, TableStreamValue, TableStreamValueDataColumn,
                                       rowId))
                            {
                                await stream.CopyToAsync(writeStream, cancellationToken).ConfigureAwait(false);
                            }
                        }

                        transaction?.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed Writing Objects", ex);
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

    public ValueTask<bool> BlobExistsAsync(object key, string partition = null,
        CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    try
                    {
                        using var selectCommand = conn.CreateCommand();

                        var commandBuilder = ReusableStringBuilder;

                        commandBuilder.Append(Queries.SelectExistsFromStreamValueWithKey);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;

                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

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
                        throw new TychoException($"Failed Reading Object with key \"{key}\"", ex);
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<Stream> ReadBlobAsync(object key, string partition = null,
        CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    try
                    {
                        using var selectCommand = conn.CreateCommand();

                        var commandBuilder = ReusableStringBuilder;

                        commandBuilder.Append(Queries.SelectDataFromStreamValueWithKey);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;

                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

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
                        throw new TychoException($"Failed Reading Object with key \"{key}\"", ex);
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<bool> DeleteBlobAsync(object key, string partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        var deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return deletionCount == 1;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed to delete object with key \"{key}\"", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public ValueTask<(bool Successful, int Count)> DeleteBlobsAsync(string partition, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
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
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        var deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return (deletionCount > 0, deletionCount);
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException("Failed to delete objects", ex);
                    }
                    finally
                    {
                        transaction?.Dispose();
                    }
                },
                _persistConnection,
                cancellationToken);
    }

    public Tycho CreateIndex<TObj>(Expression<Func<TObj, object>> propertyPath, string indexName)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TObj>();
        }

        return CreateIndex(QueryPropertyPath.BuildPath(propertyPath), QueryPropertyPath.IsNumeric(propertyPath),
            GetSafeTypeName<TObj>(), indexName);
    }

    public Tycho CreateIndex(string propertyPathString, bool isNumeric, string objectTypeName, string indexName)
    {
        _connection
            .WithConnectionBlock(
                _rateLimiter,
                conn =>
                {
                    var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    var fullIndexName = $"idx_{indexName}_{objectTypeName}";
                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

                        if (isNumeric)
                        {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                            createIndexCommand.CommandText =
                                Queries.CreateIndexForJsonValueAsNumeric(fullIndexName, propertyPathString);
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                        }
                        else
                        {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                            createIndexCommand.CommandText =
                                Queries.CreateIndexForJsonValue(fullIndexName, propertyPathString);
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                        }

                        createIndexCommand.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw new TychoException($"Failed to Create Index: {fullIndexName}", ex);
                    }
                },
                _persistConnection);

        return this;
    }

    public ValueTask<bool> CreateIndexAsync<TObj>(Expression<Func<TObj, object>> propertyPath, string indexName,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TObj>();
        }

        return CreateIndexAsync(QueryPropertyPath.BuildPath(propertyPath), QueryPropertyPath.IsNumeric(propertyPath),
            GetSafeTypeName<TObj>(), indexName, cancellationToken);
    }

    public ValueTask<bool> CreateIndexAsync(string propertyPathString, bool isNumeric, string objectTypeName,
        string indexName, CancellationToken cancellationToken = default)
    {
        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    var fullIndexName = $"idx_{indexName}_{objectTypeName}";

                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        createIndexCommand.CommandText =
                            isNumeric
                                ? Queries.CreateIndexForJsonValueAsNumeric(fullIndexName, propertyPathString)
                                : Queries.CreateIndexForJsonValue(fullIndexName, propertyPathString);
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        createIndexCommand.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw new TychoException($"Failed to Create Index: {fullIndexName}", ex);
                    }

                    return true;
                },
                _persistConnection,
                cancellationToken);
    }

    public Tycho CreateIndex<TObj>(Expression<Func<TObj, object>>[] propertyPaths, string indexName)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TObj>();
        }

        var processedPaths =
            propertyPaths
                .Select(x => (QueryPropertyPath.BuildPath(x), QueryPropertyPath.IsNumeric(x)))
                .ToArray();

        _connection
            .WithConnectionBlock(
                _rateLimiter,
                conn =>
                {
                    var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    var fullIndexName = $"idx_{indexName}_{GetSafeTypeName<TObj>()}";
                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, processedPaths);
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        createIndexCommand.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw new TychoException($"Failed to Create Index: {fullIndexName}", ex);
                    }
                },
                _persistConnection);

        return this;
    }

    public ValueTask<bool> CreateIndexAsync<TObj>(Expression<Func<TObj, object>>[] propertyPaths, string indexName,
        CancellationToken cancellationToken = default)
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
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    var fullIndexName = $"idx_{indexName}_{GetSafeTypeName<TObj>()}";

                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, processedPaths);
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        createIndexCommand.ExecuteNonQuery();

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw new TychoException($"Failed to Create Index: {fullIndexName}", ex);
                    }

                    return true;
                },
                _persistConnection,
                cancellationToken);
    }

    public void Cleanup(bool shrinkMemory = true, bool vacuum = false)
    {
        _connection
            .WithConnectionBlock(
                _rateLimiter,
                conn =>
                {
                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

                        string command = null;

                        if (shrinkMemory)
                        {
                            command += "PRAGMA shrink_memory; ";
                        }

                        if (vacuum)
                        {
                            command += "PRAGMA incremental_vacuum; ";
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        createIndexCommand.CommandText = command;
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        createIndexCommand.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        throw new TychoException($"Failed to shrink memory", ex);
                    }
                },
                _persistConnection);
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

    public bool CompareIdsFor<T>(object id1, object id2)
    {
        var type = typeof(T);

        CheckHasRegisteredType(type);

        return _registeredTypeInformation[type].CompareIdsFor(id1, id2);
    }

    public bool CompareIdsFor<T>(T obj1, T obj2)
    {
        var type = typeof(T);

        CheckHasRegisteredType(type);

        var rti = _registeredTypeInformation[type];

        return rti.CompareIdsFor(obj1, obj2);
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

    protected virtual void Dispose(bool disposing)
    {
        if (_isDisposed)
        {
            return;
        }

        if (disposing)
        {
            _commandBuilder.Dispose();
            _rateLimiter?.Dispose();
            _connection?.Close();
            _connection?.Dispose();
        }

        _isDisposed = true;
    }

    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private SqliteConnection BuildConnection()
    {
        var connection = new SqliteConnection(_dbConnectionString);

        connection
            .WithConnectionBlock(
                _rateLimiter,
                conn =>
                {
                    conn.Open();

                    var supportsJson = false;

                    // Check version
                    using var getVersionCommand = conn.CreateCommand();
                    getVersionCommand.CommandText = Queries.SqliteVersion;
                    var version = getVersionCommand.ExecuteScalar() as string;
                    var splitVersion = version.Split('.');

                    if (int.TryParse(splitVersion[0], out var major) && int.TryParse(splitVersion[1], out var minor) &&
                        (major > 3 || (major >= 3 && minor >= 38)))
                    {
                        supportsJson = true;
                    }
                    else
                    {
                        // Enable write-ahead logging
                        using var hasJsonCommand = conn.CreateCommand();
                        hasJsonCommand.CommandText = Queries.PragmaCompileOptions;
                        using var jsonReader = hasJsonCommand.ExecuteReader();

                        while (jsonReader.Read())
                        {
                            var json1Available = jsonReader.GetString(0);
                            if (!(json1Available?.Equals(Queries.EnableJSON1Pragma) ?? false))
                            {
                                continue;
                            }

                            supportsJson = true;
                            break;
                        }
                    }

                    if (!supportsJson)
                    {
                        conn.Close();
                        throw new TychoException("JSON support is not available for this platform");
                    }

                    using var command = connection.CreateCommand();

                    // Enable write-ahead logging and normal synchronous mode
                    command.CommandText = Queries.CreateDatabaseSchema;

                    command.ExecuteNonQuery();
                },
                _persistConnection);

        return connection;
    }

    private async ValueTask<SqliteConnection> BuildConnectionAsync(CancellationToken cancellationToken = default)
    {
        using var rla = await _rateLimiter.AcquireAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        _connection = new SqliteConnection(_dbConnectionString);

        _connection.Open();

        var supportsJson = false;

        // Enable write-ahead logging
        using var hasJsonCommand = _connection.CreateCommand();
        hasJsonCommand.CommandText = Queries.PragmaCompileOptions;

        using var reader = hasJsonCommand.ExecuteReader();

        while (reader.Read())
        {
            if (reader.GetString(0)?.Equals(Queries.EnableJSON1Pragma) ?? false)
            {
                supportsJson = true;
                break;
            }
        }

        if (!supportsJson)
        {
            _connection.Close();
            throw new TychoException("JSON support is not available for this platform");
        }

        using var command = _connection.CreateCommand();

        // Enable write-ahead logging and normal synchronous mode
        command.CommandText = Queries.CreateDatabaseSchema;

        command.ExecuteNonQuery();

        return _connection;
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
            throw new TychoException($"Registration missing for type: {type}");
        }
    }
}

internal static class SqliteExtensions
{
    public static T WithConnectionBlock<T>(this SqliteConnection connection, RateLimiter rateLimiter,
        Func<SqliteConnection, T> func, bool persistConnection)
    {
        if (connection == null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        using var rla = rateLimiter.AttemptAcquire();

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            return func.Invoke(connection);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }
        }
    }

    public static void WithConnectionBlock(this SqliteConnection connection, RateLimiter rateLimiter,
        Action<SqliteConnection> action, bool persistConnection)
    {
        if (connection == null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        using var rla = rateLimiter.AttemptAcquire();

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            action.Invoke(connection);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }
        }
    }

    public static async ValueTask<T> WithConnectionBlockAsync<T>(
        this SqliteConnection connection,
        RateLimiter rateLimiter,
        Func<SqliteConnection, T> func,
        bool persistConnection,
        CancellationToken cancellationToken = default)
    {
        if (connection == null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        using var rla = await rateLimiter.AcquireAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            return func.Invoke(connection);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }
        }
    }

    public static async ValueTask<T> WithConnectionBlockAsync<T>(
        this SqliteConnection connection,
        RateLimiter rateLimiter,
        Func<SqliteConnection, ValueTask<T>> func,
        bool persistConnection,
        CancellationToken cancellationToken = default)
    {
        if (connection == null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        using var rla = await rateLimiter.AcquireAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            return await func.Invoke(connection).ConfigureAwait(false);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }
        }
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
