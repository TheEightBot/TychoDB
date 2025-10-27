using System;
using System.Buffers;
using System.Collections.Concurrent;
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
    // Constants for parameter names - using static fields avoids string allocations
    private const string
        ParameterFullTypeName = "$fullTypeName",
        ParameterPartition = "$partition",
        ParameterKey = "$key",
        ParameterJson = "$json",
        ParameterBlob = "$blob",
        ParameterBlobLength = "$blobLength",
        TableStreamValue = "StreamValue",
        TableStreamValueDataColumn = "Data";

    // Parameter cache - reuse parameter objects to reduce allocations
    private readonly ConcurrentDictionary<string, SqliteParameter> _parameterCache = new();

    private readonly Lock _connectionLock = new();
    private readonly string _dbConnectionString;
    private readonly IJsonSerializer _jsonSerializer;
    private readonly bool _persistConnection;
    private readonly bool _requireTypeRegistration;
    private readonly int _commandTimeout;
    private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypeInformation = new();

    // Using a ThreadLocal StringBuilder for better performance with multi-threading
    private readonly ThreadLocal<StringBuilder> _commandBuilder = new(() => new StringBuilder(1024));

    // Use ObjectPool for MemoryStream instances
    private readonly ObjectPool<MemoryStream> _memoryStreamPool = new(
        () => new MemoryStream(4096),
        stream =>
        {
            stream.SetLength(0);
            stream.Position = 0;
            return stream;
        });

    private readonly RateLimiter _rateLimiter =
        new ConcurrencyLimiter(
            new ConcurrencyLimiterOptions
            {
                PermitLimit = 1,
                QueueLimit = int.MaxValue,
                QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            });

    private SqliteConnection? _connection;
    private bool _isDisposed;

    private StringBuilder ReusableStringBuilder
    {
        get
        {
            StringBuilder builder = _commandBuilder.Value;
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
        string? password = null,
        bool persistConnection = true,
        bool rebuildCache = false,
        bool requireTypeRegistration = true,
        bool useConnectionPooling = true,
        int commandTimeout = 30)
    {
        SQLitePCL.Batteries_V2.Init();

        _jsonSerializer = jsonSerializer ?? throw new ArgumentNullException(nameof(jsonSerializer));
        _commandTimeout = commandTimeout;

        string databasePath = Path.Join(dbPath, dbName);

        if (rebuildCache && File.Exists(databasePath))
        {
            File.Delete(databasePath);
        }

        var connectionStringBuilder =
            new SqliteConnectionStringBuilder
            {
                ConnectionString = $"Filename={databasePath}",
                Cache = SqliteCacheMode.Shared, // Use shared cache for better performance
                Mode = SqliteOpenMode.ReadWriteCreate,
            };

        if (password is not null)
        {
            connectionStringBuilder.Password = password;
        }

        // Add pooling configuration
        connectionStringBuilder.Pooling = useConnectionPooling;

        _dbConnectionString = connectionStringBuilder.ToString();
        _persistConnection = persistConnection;
        _requireTypeRegistration = requireTypeRegistration;
    }

    /// <summary>
    /// Adds type registration with a custom ID property selector.
    /// </summary>
    /// <typeparam name="T">The type of objects to be registered.</typeparam>
    /// <typeparam name="TId">The type of the ID property.</typeparam>
    /// <param name="idPropertySelector">An expression that selects the ID property from the object.</param>
    /// <param name="idComparer">Optional custom equality comparer for the ID type.</param>
    /// <returns>The current Tycho instance for method chaining.</returns>
    public Tycho AddTypeRegistration<T, TId>(
        Expression<Func<T, object>> idPropertySelector,
        EqualityComparer<TId>? idComparer = null)
        where T : class
    {
        var rti = RegisteredTypeInformation.Create(idPropertySelector, idComparer);

        _registeredTypeInformation[rti.ObjectType] = rti;

        return this;
    }

    /// <summary>
    /// Adds type registration using convention-based ID property detection.
    /// </summary>
    /// <typeparam name="T">The type of objects to be registered.</typeparam>
    /// <returns>The current Tycho instance for method chaining.</returns>
    /// <remarks>This method attempts to find an ID property based on naming conventions.</remarks>
    public Tycho AddTypeRegistration<T>()
        where T : class
    {
        var rti = RegisteredTypeInformation.Create<T>();

        _registeredTypeInformation[rti.ObjectType] = rti;

        return this;
    }

    /// <summary>
    /// Adds type registration with a custom key selector function.
    /// </summary>
    /// <typeparam name="T">The type of objects to be registered.</typeparam>
    /// <param name="keySelector">A function that extracts the key from an object instance.</param>
    /// <param name="idComparer">Optional custom equality comparer for string IDs.</param>
    /// <returns>The current Tycho instance for method chaining.</returns>
    public Tycho AddTypeRegistrationWithCustomKeySelector<T>(
        Func<T, object> keySelector,
        EqualityComparer<string>? idComparer = null)
        where T : class
    {
        var rti = RegisteredTypeInformation.CreateFromFunc(keySelector, idComparer);

        _registeredTypeInformation[rti.ObjectType] = rti;

        return this;
    }

    /// <summary>
    /// Opens a connection to the database.
    /// </summary>
    /// <returns>The current Tycho instance for method chaining.</returns>
    public Tycho Connect()
    {
        if (_connection is not null)
        {
            return this;
        }

        _connection = BuildConnection();

        return this;
    }

    /// <summary>
    /// Asynchronously opens a connection to the database.
    /// </summary>
    /// <returns>A ValueTask containing the current Tycho instance for method chaining.</returns>
    public async ValueTask<Tycho> ConnectAsync()
    {
        if (_connection is not null)
        {
            return this;
        }

        _connection = await BuildConnectionAsync().ConfigureAwait(false);

        return this;
    }

    /// <summary>
    /// Closes the current database connection.
    /// </summary>
    public void Disconnect()
    {
        lock (_connectionLock)
        {
            _connection?.Close();
            _connection?.Dispose();
            _connection = null;
        }
    }

    /// <summary>
    /// Asynchronously closes the current database connection.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous operation.</returns>
    public async ValueTask DisconnectAsync()
    {
        if (_connection is null)
        {
            return;
        }

        await _connection.CloseAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);

        _connection = null;
    }

    public void Backup(SqliteConnection backupDatabaseConnection)
    {
        _connection?.BackupDatabase(backupDatabaseConnection);
    }

    /// <summary>
    /// Writes a single object to the database using registered type information to determine the ID.
    /// </summary>
    /// <typeparam name="T">The type of the object to write.</typeparam>
    /// <param name="obj">The object to write.</param>
    /// <param name="partition">Optional partition key to organize objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> WriteObjectAsync<T>(T obj, string? partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return WriteObjectsAsync([obj,], GetIdSelectorFor<T>(), partition, withTransaction, cancellationToken);
    }

    /// <summary>
    /// Writes a single object to the database using a custom key selector.
    /// </summary>
    /// <typeparam name="T">The type of the object to write.</typeparam>
    /// <param name="obj">The object to write.</param>
    /// <param name="keySelector">A function that extracts the key from the object.</param>
    /// <param name="partition">Optional partition key to organize objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> WriteObjectAsync<T>(T obj, Func<T, object> keySelector, string? partition = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        return WriteObjectsAsync([obj,], keySelector, partition, withTransaction, cancellationToken);
    }

    /// <summary>
    /// Writes multiple objects to the database using registered type information to determine the IDs.
    /// </summary>
    /// <typeparam name="T">The type of the objects to write.</typeparam>
    /// <param name="objs">The collection of objects to write.</param>
    /// <param name="partition">Optional partition key to organize objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> WriteObjectsAsync<T>(IEnumerable<T> objs, string? partition = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        return WriteObjectsAsync(objs, GetIdSelectorFor<T>(), partition, withTransaction, cancellationToken);
    }

    /// <summary>
    /// Writes multiple objects to the database using a custom key selector.
    /// </summary>
    /// <typeparam name="T">The type of the objects to write.</typeparam>
    /// <param name="objs">The collection of objects to write.</param>
    /// <param name="keySelector">A function that extracts the key from each object.</param>
    /// <param name="partition">Optional partition key to organize objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> WriteObjectsAsync<T>(IEnumerable<T> objs, Func<T, object> keySelector,
        string? partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(objs);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    int writeCount = 0;

                    SqliteTransaction? transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    // GetPooledCommand(conn, Queries.InsertOrReplace);
                    var command = conn.CreateCommand();
#pragma warning disable CA2100

                    // TODO: Review for vulnerabilities
                    command.CommandText = Queries.InsertOrReplace;
#pragma warning restore CA2100
                    command.CommandTimeout = _commandTimeout;

                    try
                    {
                        // Convert to list to avoid multiple enumeration
                        var objsList = objs as List<T> ?? [..objs,];
                        int potentialTotalCount = objsList.Count;

                        if (potentialTotalCount == 0)
                        {
                            // Nothing to write
                            transaction?.Commit();
                            return true;
                        }

                        // Use cached parameters to reduce allocations
                        command.Parameters.Add(GetCachedParameter(ParameterFullTypeName, SqliteType.Text,
                            typeof(T).FullName ?? string.Empty));
                        command.Parameters.Add(GetCachedParameter(ParameterPartition, SqliteType.Text,
                            partition.AsValueOrEmptyString()));

                        var keyParameter = command.Parameters.Add(ParameterKey, SqliteType.Text);
                        var jsonParameter = command.Parameters.Add(ParameterJson, SqliteType.Blob);

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

                                var result = command.ExecuteScalar();
                                long rowId = result is not null ? (long)result : 0;
                                writeCount += rowId > 0 ? 1 : 0;
                            }

                            // Check for cancellation between batches
                            if (cancellationToken.IsCancellationRequested)
                            {
                                break;
                            }
                        }

                        bool successful = writeCount == potentialTotalCount;

                        if (successful && !cancellationToken.IsCancellationRequested)
                        {
                            transaction?.Commit();
                        }
                        else
                        {
                            transaction?.Rollback();
                        }

                        return successful;
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
                },
                _persistConnection,
                cancellationToken);
    }

    /// <summary>
    /// Counts objects matching the optional filter criteria.
    /// </summary>
    /// <typeparam name="T">The type of objects to count.</typeparam>
    /// <param name="partition">Optional partition to restrict the count to.</param>
    /// <param name="filter">Optional filter to apply to the objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the count of matching objects.</returns>
    public ValueTask<int> CountObjectsAsync<T>(string? partition = null, FilterBuilder<T>? filter = null,
        bool withTransaction = false, CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        if (filter is not null)
                        {
                            filter.Build(commandBuilder, _jsonSerializer);
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        using var reader = selectCommand.ExecuteReader();

                        int count = 0;

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

    /// <summary>
    /// Checks if an object exists in the database by using the object instance to determine the ID.
    /// </summary>
    /// <typeparam name="T">The type of the object to check.</typeparam>
    /// <param name="obj">The object to check for existence.</param>
    /// <param name="partition">Optional partition to check within.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating if the object exists.</returns>
    public ValueTask<bool> ObjectExistsAsync<T>(T obj, string? partition = null, bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        return ObjectExistsAsync<T>(GetIdFor(obj), partition, withTransaction, cancellationToken);
    }

    /// <summary>
    /// Checks if an object exists in the database by its key.
    /// </summary>
    /// <typeparam name="T">The type of the object to check.</typeparam>
    /// <param name="key">The key of the object to check for existence.</param>
    /// <param name="partition">Optional partition to check within.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating if the object exists.</returns>
    public ValueTask<bool> ObjectExistsAsync<T>(object key, string? partition = null, bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        bool returnValue = false;
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

    /// <summary>
    /// Reads an object from the database by using the object instance to determine the ID.
    /// </summary>
    /// <typeparam name="T">The type of the object to read.</typeparam>
    /// <param name="obj">An object with the same ID as the one to read.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// /// <param name="progress">Optional progress reporter for deserialization. Reports a value between 0.0 and 1.0 as the object is read.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the retrieved object or default value if not found.</returns>
    public ValueTask<T> ReadObjectAsync<T>(T obj, string? partition = null, bool withTransaction = false,
        IProgress<double>? progress = null, CancellationToken cancellationToken = default)
    {
        return ReadObjectAsync<T>(GetIdFor(obj), partition, withTransaction, progress, cancellationToken);
    }

    /// <summary>
    /// Reads an object from the database by its key.
    /// </summary>
    /// <typeparam name="T">The type of the object to read.</typeparam>
    /// <param name="key">The key of the object to read.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// /// <param name="progress">Optional progress reporter for deserialization. Reports a value between 0.0 and 1.0 as the object is read.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the retrieved object or default value if not found.</returns>
    public ValueTask<T> ReadObjectAsync<T>(object key, string? partition = null, bool withTransaction = false,
        IProgress<double>? progress = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                async conn =>
                {
                    SqliteTransaction? transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    try
                    {
                        await using var selectCommand = conn.CreateCommand();

                        var commandBuilder = ReusableStringBuilder;

                        commandBuilder.Append(Queries.SelectDataFromJsonValueWithKeyAndFullTypeName);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = typeof(T).FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                        T returnValue = default(T);
                        while (reader.Read())
                        {
                            await using var stream = reader.GetStream(reader.GetOrdinal(Queries.DataColumn));

                            if (progress is not null)
                            {
                                await using var progressStream = new ProgressStream(stream, progress);
                                returnValue = await _jsonSerializer.DeserializeAsync<T>(progressStream, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                            else
                            {
                                returnValue = await _jsonSerializer.DeserializeAsync<T>(stream, cancellationToken)
                                    .ConfigureAwait(false);
                            }
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

    /// <summary>
    /// Reads the first object that matches the filter criteria.
    /// </summary>
    /// <typeparam name="T">The type of the object to read.</typeparam>
    /// <param name="filter">The filter to apply to the objects.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="progress">Optional progress reporter for deserialization. Reports a value between 0.0 and 1.0 as the object is read.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the first matching object or default value if none found.</returns>
    public async ValueTask<T> ReadFirstObjectAsync<T>(
        FilterBuilder<T> filter,
        string? partition = null,
        bool withTransaction = false,
        IProgress<double>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var results =
            await ReadObjectsAsync(partition, filter, null, 1, withTransaction, progress, cancellationToken)
                .ConfigureAwait(false);

        return results.FirstOrDefault();
    }

    /// <summary>
    /// Reads a single object that matches the filter criteria. Throws an exception if multiple matches are found.
    /// </summary>
    /// <typeparam name="T">The type of the object to read.</typeparam>
    /// <param name="filter">The filter to apply to the objects.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// /// <param name="progress">Optional progress reporter for deserialization. Reports a value between 0.0 and 1.0 as the object is read.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the matching object or default value if none found.</returns>
    /// <exception cref="TychoException">Thrown when multiple matching objects are found.</exception>
    public async ValueTask<T> ReadObjectAsync<T>(
        FilterBuilder<T> filter,
        string? partition = null,
        bool withTransaction = false,
        IProgress<double>? progress = null,
        CancellationToken cancellationToken = default)
    {
        int matches = await CountObjectsAsync(partition, filter, withTransaction, cancellationToken)
            .ConfigureAwait(false);

        if (matches > 1)
        {
            throw new TychoException(
                "Too many matching values were found, please refine your query to limit it to a single match");
        }

        var results =
            await ReadObjectsAsync(partition, filter, null, 1, withTransaction, progress, cancellationToken)
                .ConfigureAwait(false);

        return results.FirstOrDefault();
    }

    /// <summary>
    /// Reads all objects of a specific type matching the optional filter and sort criteria.
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="filter">Optional filter to apply to the objects.</param>
    /// <param name="sort">Optional sorting to apply to the result set.</param>
    /// <param name="top">Optional limit on the number of objects to return.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// /// <param name="progress">Optional progress reporter for deserialization. Reports a value between 0.0 and 1.0 as the object is read.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing an enumerable of the matching objects.</returns>
    public ValueTask<IEnumerable<T>> ReadObjectsAsync<T>(
        string? partition = null,
        FilterBuilder<T>? filter = null,
        SortBuilder<T>? sort = null,
        int? top = null,
        bool withTransaction = false,
        IProgress<double>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync<IEnumerable<T>>(
                _rateLimiter,
                async conn =>
                {
                    SqliteTransaction? transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    var commandBuilder = ReusableStringBuilder;
                    commandBuilder.Append(Queries.SelectDataFromJsonValueWithFullTypeName);

                    // Apply filters and sorting
                    if (filter is not null)
                    {
                        filter.Build(commandBuilder, _jsonSerializer);
                    }

                    if (sort is not null)
                    {
                        sort.Build(commandBuilder);
                    }

                    if (top is not null)
                    {
                        commandBuilder.AppendLine(Queries.Limit(top.Value));
                    }

                    // GetPooledCommand(conn, commandBuilder.ToString());
                    var selectCommand = conn.CreateCommand();

#pragma warning disable CA2100

                    // TODO: Review for vulnerabilities
                    selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100
                    selectCommand.CommandTimeout = _commandTimeout;

                    try
                    {
                        // Use cached parameters
                        selectCommand.Parameters.Add(GetCachedParameter(ParameterFullTypeName, SqliteType.Text,
                            typeof(T).FullName));
                        selectCommand.Parameters.Add(GetCachedParameter(ParameterPartition, SqliteType.Text,
                            partition.AsValueOrEmptyString()));

                        // Use CommandBehavior.SequentialAccess for better performance
                        await using var reader = await selectCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);

                        // Pre-allocate collection to reduce resizing
                        List<T> objects;

                        if (top.HasValue)
                        {
                            objects = new List<T>(top.Value);
                        }
                        else
                        {
                            objects = new List<T>(128); // Default capacity to avoid too many resizes
                        }

                        // Use efficient buffered reading with pooled resources
                        const int bufferSize = 32768; // 32 KB buffer
                        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

                        try
                        {
                            while (reader.Read())
                            {
                                if (cancellationToken.IsCancellationRequested)
                                {
                                    break;
                                }

                                // Use a pooled memory stream to avoid allocations
                                var memoryStream = _memoryStreamPool.Get();
                                try
                                {
                                    int bytesRead;

                                    await using var stream = reader.GetStream(reader.GetOrdinal(Queries.DataColumn));

                                    if (progress is not null)
                                    {
                                        await using Stream progressStream = new ProgressStream(stream, progress);
                                        while ((bytesRead = await progressStream
                                                   .ReadAsync(buffer, 0, buffer.Length, cancellationToken)
                                                   .ConfigureAwait(false)) > 0)
                                        {
                                            memoryStream.Write(buffer, 0, bytesRead);
                                        }
                                    }
                                    else
                                    {
                                        while ((bytesRead = await stream
                                                   .ReadAsync(buffer, 0, buffer.Length, cancellationToken)
                                                   .ConfigureAwait(false)) > 0)
                                        {
                                            memoryStream.Write(buffer, 0, bytesRead);
                                        }
                                    }

                                    memoryStream.Position = 0;
                                    objects.Add(await _jsonSerializer
                                        .DeserializeAsync<T>(memoryStream, cancellationToken).ConfigureAwait(false));
                                }
                                finally
                                {
                                    _memoryStreamPool.Return(memoryStream);
                                }
                            }
                        }
                        finally
                        {
                            // Return the rented buffer
                            ArrayPool<byte>.Shared.Return(buffer);
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

    /// <summary>
    /// Reads a specific property from objects of a given type matching the optional filter criteria.
    /// </summary>
    /// <typeparam name="TIn">The type of the source objects.</typeparam>
    /// <typeparam name="TOut">The type of the property to extract.</typeparam>
    /// <param name="innerObjectSelection">An expression that selects the property to extract.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="filter">Optional filter to apply to the objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing an array of the extracted property values.</returns>
    public async ValueTask<TOut[]> ReadObjectsAsync<TIn, TOut>(
        Expression<Func<TIn, TOut>> innerObjectSelection,
        string? partition = null,
        FilterBuilder<TIn>? filter = null,
        bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        var results =
            await ReadObjectsWithKeysAsync(innerObjectSelection, partition, filter, withTransaction, cancellationToken)
                .ConfigureAwait(false);

        return results.Select(x => x.InnerObject).ToArray();
    }

    /// <summary>
    /// Reads a specific property from objects of a given type along with their keys.
    /// </summary>
    /// <typeparam name="TIn">The type of the source objects.</typeparam>
    /// <typeparam name="TOut">The type of the property to extract.</typeparam>
    /// <param name="innerObjectSelection">An expression that selects the property to extract.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="filter">Optional filter to apply to the objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing an enumerable of tuples with each object's key and the extracted property.</returns>
    public ValueTask<IEnumerable<(string Key, TOut InnerObject)>> ReadObjectsWithKeysAsync<TIn, TOut>(
        Expression<Func<TIn, TOut>> innerObjectSelection,
        string? partition = null,
        FilterBuilder<TIn>? filter = null,
        bool withTransaction = false,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TIn>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync<IEnumerable<(string Key, TOut InnerObject)>>(
                _rateLimiter,
                async conn =>
                {
                    SqliteTransaction? transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    var objects = new List<(string, TOut)>();

                    try
                    {
                        await using var selectCommand = conn.CreateCommand();

                        var commandBuilder = ReusableStringBuilder;

                        string selectionPath = QueryPropertyPath.BuildPath(innerObjectSelection);

                        commandBuilder.Append(Queries.ExtractDataAndKeyFromJsonValueWithFullTypeName(selectionPath));

                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value =
                            typeof(TIn).FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();

                        if (filter is not null)
                        {
                            filter.Build(commandBuilder, _jsonSerializer);
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        await using var reader = await selectCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                        while (reader.Read())
                        {
                            string key = reader.GetString(reader.GetOrdinal(Queries.KeyColumn));

                            await using var innerObjectStream = reader.GetStream(reader.GetOrdinal(Queries.DataColumn));
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

    /// <summary>
    /// Deletes an object from the database by using the object instance to determine the ID.
    /// </summary>
    /// <typeparam name="T">The type of the object to delete.</typeparam>
    /// <param name="obj">The object to delete.</param>
    /// <param name="partition">Optional partition containing the object.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> DeleteObjectAsync<T>(T obj, string? partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return DeleteObjectAsync(GetIdFor(obj), partition, withTransaction, cancellationToken);
    }

    /// <summary>
    /// Deletes an object from the database by its key.
    /// </summary>
    /// <typeparam name="T">The type of the object to delete.</typeparam>
    /// <param name="key">The key of the object to delete.</param>
    /// <param name="partition">Optional partition containing the object.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> DeleteObjectAsync<T>(object key, string? partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        int deletionCount = deleteCommand.ExecuteNonQuery();

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

    /// <summary>
    /// Deletes objects of a specific type matching the optional filter criteria.
    /// </summary>
    /// <typeparam name="T">The type of objects to delete.</typeparam>
    /// <param name="partition">Optional partition containing the objects.</param>
    /// <param name="filter">Optional filter to apply to the objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the count of deleted objects.</returns>
    public ValueTask<int> DeleteObjectsAsync<T>(string? partition = null, FilterBuilder<T>? filter = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        if (filter is not null)
                        {
                            filter.Build(commandBuilder, _jsonSerializer);
                        }

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        int deletionCount = deleteCommand.ExecuteNonQuery();

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

    /// <summary>
    /// Deletes all objects from a specific partition.
    /// </summary>
    /// <param name="partition">The partition to delete all objects from.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the count of deleted objects.</returns>
    public ValueTask<int> DeleteObjectsAsync(string partition, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        int deletionCount = deleteCommand.ExecuteNonQuery();

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

    /// <summary>
    /// Deletes all objects from the database.
    /// </summary>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the count of deleted objects.</returns>
    public ValueTask<int> DeleteObjectsAsync(bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        int deletionCount = deleteCommand.ExecuteNonQuery();

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

    /// <summary>
    /// Writes a binary large object (BLOB) to the database.
    /// </summary>
    /// <param name="stream">The stream containing the BLOB data.</param>
    /// <param name="key">The key to identify the BLOB.</param>
    /// <param name="partition">Optional partition to store the BLOB in.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> WriteBlobAsync(Stream stream, object key, string? partition = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                async conn =>
                {
                    int writeCount = 0;

                    SqliteTransaction? transaction = null;

                    if (withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        await using var insertCommand = conn.CreateCommand();
                        insertCommand.CommandText = Queries.InsertOrReplaceBlob;

                        insertCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = key;
                        insertCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            partition.AsValueOrEmptyString();
                        insertCommand.Parameters.AddWithValue(ParameterBlobLength, stream.Length);

                        long rowId = (long)insertCommand.ExecuteScalar();

                        writeCount += rowId > 0 ? 1 : 0;

                        if (writeCount > 0)
                        {
                            await using (var writeStream = new SqliteBlob(conn, TableStreamValue, TableStreamValueDataColumn,
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

    /// <summary>
    /// Checks if a BLOB exists in the database by its key.
    /// </summary>
    /// <param name="key">The key of the BLOB to check for existence.</param>
    /// <param name="partition">Optional partition to check within.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating if the BLOB exists.</returns>
    public ValueTask<bool> BlobExistsAsync(object key, string? partition = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

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

    /// <summary>
    /// Reads a BLOB from the database by its key.
    /// </summary>
    /// <param name="key">The key of the BLOB to read.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a Stream with the BLOB data, or Stream.Null if not found.</returns>
    public ValueTask<Stream> ReadBlobAsync(object key, string? partition = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

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
                            returnValue = reader.GetStream(reader.GetOrdinal(Queries.DataColumn));
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

    /// <summary>
    /// Deletes a BLOB from the database by its key.
    /// </summary>
    /// <param name="key">The key of the BLOB to delete.</param>
    /// <param name="partition">Optional partition containing the BLOB.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> DeleteBlobAsync(object key, string? partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        int deletionCount = deleteCommand.ExecuteNonQuery();

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

    /// <summary>
    /// Deletes all BLOBs from a specific partition.
    /// </summary>
    /// <param name="partition">The partition to delete all BLOBs from.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a tuple with success flag and count of deleted BLOBs.</returns>
    public ValueTask<(bool Successful, int Count)> DeleteBlobsAsync(string partition, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    SqliteTransaction? transaction = null;

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

                        int deletionCount = deleteCommand.ExecuteNonQuery();

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

    /// <summary>
    /// Creates an index for a specific property of a registered type.
    /// </summary>
    /// <typeparam name="TObj">The type of objects to index.</typeparam>
    /// <param name="propertyPath">An expression that defines the property path to index.</param>
    /// <param name="indexName">The name to give to the index.</param>
    /// <returns>The current Tycho instance for method chaining.</returns>
    public Tycho CreateIndex<TObj>(Expression<Func<TObj, object>> propertyPath, string indexName)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TObj>();
        }

        return CreateIndex(QueryPropertyPath.BuildPath(propertyPath), QueryPropertyPath.IsNumeric(propertyPath),
            GetSafeTypeName<TObj>(), indexName);
    }

    /// <summary>
    /// Creates an index for a specific property using manual configuration.
    /// </summary>
    /// <param name="propertyPathString">The JSON path to the property to index.</param>
    /// <param name="isNumeric">Whether the property is numeric (affects index performance).</param>
    /// <param name="objectTypeName">The name of the object type.</param>
    /// <param name="indexName">The name to give to the index.</param>
    /// <returns>The current Tycho instance for method chaining.</returns>
    public Tycho CreateIndex(string propertyPathString, bool isNumeric, string objectTypeName, string indexName)
    {
        ArgumentNullException.ThrowIfNull(this._connection);

        _connection
            .WithConnectionBlock(
                _rateLimiter,
                conn =>
                {
                    var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    string fullIndexName = $"idx_{indexName}_{objectTypeName}";
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

    /// <summary>
    /// Asynchronously creates an index for a specific property of a registered type.
    /// </summary>
    /// <typeparam name="TObj">The type of objects to index.</typeparam>
    /// <param name="propertyPath">An expression that defines the property path to index.</param>
    /// <param name="indexName">The name to give to the index.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
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

    /// <summary>
    /// Asynchronously creates an index for a specific property using manual configuration.
    /// </summary>
    /// <param name="propertyPathString">The JSON path to the property to index.</param>
    /// <param name="isNumeric">Whether the property is numeric (affects index performance).</param>
    /// <param name="objectTypeName">The name of the object type.</param>
    /// <param name="indexName">The name to give to the index.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> CreateIndexAsync(string propertyPathString, bool isNumeric, string objectTypeName,
        string indexName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _rateLimiter,
                conn =>
                {
                    using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    string fullIndexName = $"idx_{indexName}_{objectTypeName}";

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

    /// <summary>
    /// Creates a composite index on multiple properties of a registered type.
    /// </summary>
    /// <typeparam name="TObj">The type of objects to index.</typeparam>
    /// <param name="propertyPaths">An array of expressions that define the property paths to index.</param>
    /// <param name="indexName">The name to give to the index.</param>
    /// <returns>The current Tycho instance for method chaining.</returns>
    public Tycho CreateIndex<TObj>(Expression<Func<TObj, object>>[] propertyPaths, string indexName)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TObj>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

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

                    string fullIndexName = $"idx_{indexName}_{GetSafeTypeName<TObj>()}";
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

    /// <summary>
    /// Asynchronously creates a composite index on multiple properties of a registered type.
    /// </summary>
    /// <typeparam name="TObj">The type of objects to index.</typeparam>
    /// <param name="propertyPaths">An array of expressions that define the property paths to index.</param>
    /// <param name="indexName">The name to give to the index.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing a boolean indicating success or failure.</returns>
    public ValueTask<bool> CreateIndexAsync<TObj>(Expression<Func<TObj, object>>[] propertyPaths, string indexName,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<TObj>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

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

                    string fullIndexName = $"idx_{indexName}_{GetSafeTypeName<TObj>()}";

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

    /// <summary>
    /// Performs database cleanup operations to optimize performance and reduce size.
    /// </summary>
    /// <param name="shrinkMemory">Whether to shrink the database's memory usage.</param>
    /// <param name="vacuum">Whether to perform an incremental vacuum operation.</param>
    public void Cleanup(bool shrinkMemory = true, bool vacuum = false)
    {
        ArgumentNullException.ThrowIfNull(_connection);

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

    /// <summary>
    /// Gets the key selector function for a registered type.
    /// </summary>
    /// <typeparam name="T">The registered type.</typeparam>
    /// <returns>A function that extracts the key from objects of type T.</returns>
    /// <exception cref="TychoException">Thrown if the type is not registered.</exception>
    public Func<T, object> GetIdSelectorFor<T>()
    {
        var type = typeof(T);
        CheckHasRegisteredType(type);
        if (!_registeredTypeInformation.TryGetValue(type, out var rti) || rti is null)
        {
            throw new TychoException($"Registration missing for type: {type}");
        }

        return rti.GetIdSelector<T>();
    }

    /// <summary>
    /// Gets the ID value for an object instance.
    /// </summary>
    /// <typeparam name="T">The type of the object.</typeparam>
    /// <param name="obj">The object to get the ID for.</param>
    /// <returns>The ID value for the object.</returns>
    /// <exception cref="TychoException">Thrown if the type is not registered.</exception>
    public object GetIdFor<T>(T obj)
    {
        var type = typeof(T);
        CheckHasRegisteredType(type);
        if (!_registeredTypeInformation.TryGetValue(type, out var rti) || rti is null)
        {
            throw new TychoException($"Registration missing for type: {type}");
        }

        return rti.GetIdFor(obj);
    }

    /// <summary>
    /// Compares two ID values for a registered type.
    /// </summary>
    /// <typeparam name="T">The registered type.</typeparam>
    /// <param name="id1">The first ID to compare.</param>
    /// <param name="id2">The second ID to compare.</param>
    /// <returns>True if the IDs are equal according to the type's registered comparer, false otherwise.</returns>
    /// <exception cref="TychoException">Thrown if the type is not registered.</exception>
    public bool CompareIdsFor<T>(object id1, object id2)
    {
        var type = typeof(T);
        CheckHasRegisteredType(type);
        if (!_registeredTypeInformation.TryGetValue(type, out var rti) || rti is null)
        {
            throw new TychoException($"Registration missing for type: {type}");
        }

        return rti.CompareIdsFor(id1, id2);
    }

    /// <summary>
    /// Compares two objects of the same type by their IDs.
    /// </summary>
    /// <typeparam name="T">The type of the objects.</typeparam>
    /// <param name="obj1">The first object to compare.</param>
    /// <param name="obj2">The second object to compare.</param>
    /// <returns>True if the objects have the same ID, false otherwise.</returns>
    /// <exception cref="TychoException">Thrown if the type is not registered.</exception>
    public bool CompareIdsFor<T>(T obj1, T obj2)
    {
        var type = typeof(T);
        CheckHasRegisteredType(type);
        if (!_registeredTypeInformation.TryGetValue(type, out var rti) || rti is null)
        {
            throw new TychoException($"Registration missing for type: {type}");
        }

        return rti.CompareIdsFor(obj1, obj2);
    }

    /// <summary>
    /// Gets the registered type information for a type.
    /// </summary>
    /// <typeparam name="T">The type to get information for.</typeparam>
    /// <returns>The registered type information.</returns>
    /// <exception cref="TychoException">Thrown if the type is not registered.</exception>
    public RegisteredTypeInformation GetRegisteredTypeInformationFor<T>()
    {
        var type = typeof(T);

        CheckHasRegisteredType(type);

        return _registeredTypeInformation[type];
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

    /// <summary>
    /// Releases all resources used by the Tycho instance.
    /// </summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private string GetSafeTypeName<TObj>()
    {
        var type = typeof(TObj);

        return _registeredTypeInformation.ContainsKey(type)
            ? _registeredTypeInformation[type].SafeTypeName
            : type.GetSafeTypeName();
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

                    bool supportsJson = false;

                    // Check version
                    using var getVersionCommand = conn.CreateCommand();
                    getVersionCommand.CommandText = Queries.SqliteVersion;
                    string? version = getVersionCommand.ExecuteScalar() as string;
                    string[] splitVersion = version.Split('.');

                    if (int.TryParse(splitVersion[0], out int major) && int.TryParse(splitVersion[1], out int minor) &&
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
                            string? json1Available = jsonReader.GetString(0);
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

        var connection = new SqliteConnection(_dbConnectionString);

        connection.Open();

        bool supportsJson = false;

        // Check version
        await using var getVersionCommand = connection.CreateCommand();
        getVersionCommand.CommandText = Queries.SqliteVersion;
        string? version = getVersionCommand.ExecuteScalar() as string;
        string[] splitVersion = version.Split('.');

        if (int.TryParse(splitVersion[0], out int major) && int.TryParse(splitVersion[1], out int minor) &&
            (major > 3 || (major >= 3 && minor >= 38)))
        {
            supportsJson = true;
        }
        else
        {
            // Enable write-ahead logging
            await using var hasJsonCommand = connection.CreateCommand();
            hasJsonCommand.CommandText = Queries.PragmaCompileOptions;
            await using var jsonReader = await hasJsonCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            while (jsonReader.Read())
            {
                string? json1Available = jsonReader.GetString(0);
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
            connection.Close();
            throw new TychoException("JSON support is not available for this platform");
        }

        await using var command = connection.CreateCommand();

        // Enable write-ahead logging and normal synchronous mode
        command.CommandText = Queries.CreateDatabaseSchema;

        command.ExecuteNonQuery();

        return connection;
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

    /// <summary>
    /// Gets or creates a SqliteParameter from the parameter cache to reduce allocations.
    /// </summary>
    private SqliteParameter GetCachedParameter(string name, SqliteType type, object value)
    {
        string key = $"{name}_{type}";
        if (!_parameterCache.TryGetValue(key, out var parameter))
        {
            parameter = new SqliteParameter(name, type);
            _parameterCache[key] = parameter;
        }

        parameter.Value = value;
        return parameter;
    }
}

internal static class SqliteExtensions
{
    public static T WithConnectionBlock<T>(this SqliteConnection connection, RateLimiter rateLimiter,
        Func<SqliteConnection, T> func, bool persistConnection)
    {
        if (connection is null)
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
        if (connection is null)
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
        if (connection is null)
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
        if (connection is null)
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

    public static object AsValueOrDbNull<T>(this T? value)
        where T : class
    {
        return value ?? (object)DBNull.Value;
    }

    public static string AsValueOrEmptyString(this string? value)
    {
        return value ?? string.Empty;
    }
}
