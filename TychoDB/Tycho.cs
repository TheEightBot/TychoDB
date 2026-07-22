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
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.IO;

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

    // Per-connection setup script (profile PRAGMAs + schema DDL), built once.
    private readonly string _connectionScript;
    private readonly IJsonSerializer _jsonSerializer;
    private readonly bool _persistConnection;
    private readonly bool _requireTypeRegistration;
    private readonly int _commandTimeout;
    private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypeInformation = new();

    // Using a ThreadLocal StringBuilder for better performance with multi-threading
    private readonly ThreadLocal<StringBuilder> _commandBuilder = new(() => new StringBuilder(1024));

    // RecyclableMemoryStream for efficient memory management - optimized for mobile
    private static readonly RecyclableMemoryStreamManager _memoryStreamManager = new(
        new RecyclableMemoryStreamManager.Options
        {
            BlockSize = 4096,                          // 4KB blocks (mobile-friendly)
            LargeBufferMultiple = 1024 * 1024,         // 1MB large buffer multiple
            MaximumBufferSize = 16 * 1024 * 1024,      // 16MB max buffer
            MaximumSmallPoolFreeBytes = 256 * 1024,    // 256KB max small pool (mobile-friendly)
            MaximumLargePoolFreeBytes = 4 * 1024 * 1024, // 4MB max large pool (mobile-friendly)
            AggressiveBufferReturn = true,             // Return buffers immediately for mobile
        });

    // Serializes all access to the single SQLite connection. A SemaphoreSlim is
    // lighter than a rate limiter for this "one operation at a time" gate and,
    // unlike the previous AttemptAcquire path (which ignored whether a permit was
    // actually obtained), its synchronous Wait genuinely serializes sync callers.
    private readonly SemaphoreSlim _connectionGate = new(1, 1);

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
    /// <param name="performanceProfile">Selects device-appropriate SQLite PRAGMA tuning. Default is <see cref="TychoPerformanceProfile.Mobile"/>.</param>
    /// <param name="cacheSizeKb">Optional override for the SQLite page cache size, in KiB. Overrides the profile default.</param>
    /// <param name="mmapSizeBytes">Optional override for the SQLite memory-map size, in bytes (0 disables mmap). Overrides the profile default.</param>
    public Tycho(
        string dbPath,
        IJsonSerializer jsonSerializer,
        string dbName = "tycho_cache.db",
        string? password = null,
        bool persistConnection = true,
        bool rebuildCache = false,
        bool requireTypeRegistration = true,
        bool useConnectionPooling = true,
        int commandTimeout = 30,
        TychoPerformanceProfile performanceProfile = TychoPerformanceProfile.Mobile,
        int? cacheSizeKb = null,
        long? mmapSizeBytes = null)
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

                // A single persistent connection with locking_mode=EXCLUSIVE owns the
                // database, so a shared cache (which is for coordinating multiple
                // connections) would contradict that; use a private cache.
                Cache = SqliteCacheMode.Private,
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
        _connectionScript = Queries.BuildConnectionScript(performanceProfile, cacheSizeKb, mmapSizeBytes);
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
                _connectionGate,
                (objs, keySelector, partition, withTransaction, _commandTimeout, _jsonSerializer, cancellationToken),
                static (conn, state) =>
                {
                    int writeCount = 0;

                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        // Avoid multiple enumeration. Arrays and lists (the common inputs,
                        // including the single-element array from WriteObjectAsync) already
                        // implement IList<T>, so this avoids a per-call copy for them.
                        var objsList = state.objs as IList<T> ?? state.objs.ToList();
                        int potentialTotalCount = objsList.Count;

                        if (potentialTotalCount == 0)
                        {
                            // Nothing to write
                            transaction?.Commit();
                            return true;
                        }

                        // Rows are written in multi-row INSERT batches (one execution per
                        // batch instead of per row) with FullTypeName/Partition shared and
                        // each row binding its own $key{n}/$json{n}. 100 rows * 2 params + 2
                        // shared = 202 parameters, well under SQLite's 999 variable limit.
                        // (Empirically 100 beats 200: larger batches pay more SQL-prepare cost
                        // than they save in round trips on this workload.)
                        const int batchSize = 100;
                        int fullBatchCount = potentialTotalCount / batchSize;

                        var fullTypeNameValue = TypeCache<T>.FullName;
                        var partitionValue = state.partition.AsValueOrEmptyString();

                        // Prepared command for full-size batches, reused across all of them.
                        SqliteCommand? fullBatchCommand = null;
                        SqliteParameter[]? fullKeyParams = null;
                        SqliteParameter[]? fullJsonParams = null;

                        if (fullBatchCount > 0)
                        {
                            (fullBatchCommand, fullKeyParams, fullJsonParams) =
                                BuildBatchCommand(conn, transaction, batchSize, fullTypeNameValue, partitionValue, state._commandTimeout);
                        }

                        // Use RecyclableMemoryStream for efficient serialization.
                        using var serializationStream = _memoryStreamManager.GetStream("TychoDB.WriteObjects");

                        int index = 0;
                        while (index < potentialTotalCount)
                        {
                            if (state.cancellationToken.IsCancellationRequested)
                            {
                                break;
                            }

                            int currentBatchSize = Math.Min(batchSize, potentialTotalCount - index);

                            SqliteCommand batchCommand;
                            SqliteParameter[] keyParams;
                            SqliteParameter[] jsonParams;

                            if (currentBatchSize == batchSize)
                            {
                                batchCommand = fullBatchCommand!;
                                keyParams = fullKeyParams!;
                                jsonParams = fullJsonParams!;
                            }
                            else
                            {
                                // Final partial batch: build a right-sized command once.
                                (batchCommand, keyParams, jsonParams) =
                                    BuildBatchCommand(conn, transaction, currentBatchSize, fullTypeNameValue, partitionValue, state._commandTimeout);
                            }

                            for (int j = 0; j < currentBatchSize; j++)
                            {
                                var obj = objsList[index + j];
                                keyParams[j].Value = state.keySelector(obj);

                                serializationStream.SetLength(0);
                                state._jsonSerializer.Serialize(obj, serializationStream);

                                // Each row's blob must stay alive until the batch executes,
                                // so an exact-size array per row is required here.
                                jsonParams[j].Value = serializationStream.ToArray();
                            }

                            writeCount += batchCommand.ExecuteNonQuery();

                            if (currentBatchSize != batchSize)
                            {
                                batchCommand.Dispose();
                            }

                            index += currentBatchSize;
                        }

                        fullBatchCommand?.Dispose();

                        bool successful = writeCount == potentialTotalCount;

                        if (successful && !state.cancellationToken.IsCancellationRequested)
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
                _connectionGate,
                (partition, filter, withTransaction, commandBuilder: ReusableStringBuilder, _jsonSerializer),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    try
                    {
                        using var selectCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.SelectCountFromJsonValueWithFullTypeName);

                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = TypeCache<T>.FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

                        var filterParameters = new FilterParameters();
                        if (state.filter is not null)
                        {
                            state.filter.Build(state.commandBuilder, state._jsonSerializer, filterParameters);
                        }

#pragma warning disable CA2100 // Comparison values are parameterized (AddFilterParameters); only validated JSON paths/identifiers are concatenated.
                        selectCommand.CommandText = state.commandBuilder.ToString();
#pragma warning restore CA2100
                        selectCommand.AddFilterParameters(filterParameters);

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
                _connectionGate,
                (key, partition, withTransaction, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    try
                    {
                        using var selectCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.SelectExistsFromJsonValueWithKeyAndFullTypeName);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = state.key;
                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = TypeCache<T>.FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = state.commandBuilder.ToString();
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
                        throw new TychoException($"Failed Reading Object with key \"{state.key}\"", ex);
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
                _connectionGate,
                (key, partition, withTransaction, progress, commandBuilder: ReusableStringBuilder, _jsonSerializer, cancellationToken),
                static async (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    try
                    {
                        await using var selectCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.SelectDataFromJsonValueWithKeyAndFullTypeName);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = state.key;
                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = TypeCache<T>.FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = state.commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        await using var reader = await selectCommand.ExecuteReaderAsync(state.cancellationToken).ConfigureAwait(false);

                        T returnValue = default(T);
                        while (reader.Read())
                        {
                            await using var stream = reader.GetStream(reader.GetOrdinal(Queries.DataColumn));

                            if (state.progress is not null)
                            {
                                await using var progressStream = new ProgressStream(stream, state.progress);
                                returnValue = await state._jsonSerializer.DeserializeAsync<T>(progressStream, state.cancellationToken)
                                    .ConfigureAwait(false);
                            }
                            else
                            {
                                returnValue = await state._jsonSerializer.DeserializeAsync<T>(stream, state.cancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }

                        transaction?.Commit();

                        return returnValue;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed Reading Object with key \"{state.key}\"", ex);
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
            .WithConnectionBlockAsync<IEnumerable<T>, (string? partition, FilterBuilder<T>? filter, SortBuilder<T>? sort, int? top, bool withTransaction, IProgress<double>? progress, StringBuilder commandBuilder, int commandTimeout, IJsonSerializer jsonSerializer, CancellationToken cancellationToken)>(
                _connectionGate,
                (partition, filter, sort, top, withTransaction, progress, ReusableStringBuilder, _commandTimeout, _jsonSerializer, cancellationToken),
                static async (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    var commandBuilder = state.commandBuilder;
                    commandBuilder.Clear().Append(Queries.SelectDataFromJsonValueWithFullTypeName);

                    // Apply filters and sorting
                    var filterParameters = new FilterParameters();
                    if (state.filter is not null)
                    {
                        state.filter.Build(commandBuilder, state.jsonSerializer, filterParameters);
                    }

                    if (state.sort is not null)
                    {
                        state.sort.Build(commandBuilder);
                    }

                    if (state.top is not null)
                    {
                        commandBuilder.AppendLine(Queries.Limit(state.top.Value));
                    }

                    var selectCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Comparison values are parameterized (AddFilterParameters); only validated JSON paths/identifiers are concatenated.
                    selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100
                    selectCommand.CommandTimeout = state.commandTimeout;

                    try
                    {
                        // Use cached parameters
                        selectCommand.Parameters.Add(new SqliteParameter(ParameterFullTypeName, SqliteType.Text) { Value = TypeCache<T>.FullName });
                        selectCommand.Parameters.Add(new SqliteParameter(ParameterPartition, SqliteType.Text) { Value = state.partition.AsValueOrEmptyString() });
                        selectCommand.AddFilterParameters(filterParameters);

                        // Use CommandBehavior.SequentialAccess for better performance
                        await using var reader = await selectCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, state.cancellationToken).ConfigureAwait(false);

                        // Pre-allocate collection to reduce resizing
                        List<T> objects;

                        if (state.top.HasValue)
                        {
                            objects = new List<T>(state.top.Value);
                        }
                        else
                        {
                            objects = new List<T>(128); // Default capacity to avoid too many resizes
                        }

                        // Read each row's bytes into a reused in-memory stream, then
                        // deserialize from it. Deserializing from the in-memory stream is
                        // materially cheaper than deserializing directly from the SqliteBlob
                        // reader stream (measured), because the serializer's async path over an
                        // in-memory stream completes synchronously without per-read allocations.
                        const int bufferSize = 32768; // 32 KB buffer
                        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

                        // Reuse single RecyclableMemoryStream across all rows to avoid per-row allocation
                        using var memoryStream = _memoryStreamManager.GetStream("TychoDB.ReadObjects");

                        try
                        {
                            int dataOrdinal = reader.GetOrdinal(Queries.DataColumn);

                            while (reader.Read())
                            {
                                if (state.cancellationToken.IsCancellationRequested)
                                {
                                    break;
                                }

                                // Reset stream for reuse
                                memoryStream.SetLength(0);

                                int bytesRead;

                                await using var stream = reader.GetStream(dataOrdinal);

                                if (state.progress is not null)
                                {
                                    await using Stream progressStream = new ProgressStream(stream, state.progress);
                                    while ((bytesRead = await progressStream
                                               .ReadAsync(buffer, 0, buffer.Length, state.cancellationToken)
                                               .ConfigureAwait(false)) > 0)
                                    {
                                        memoryStream.Write(buffer, 0, bytesRead);
                                    }
                                }
                                else
                                {
                                    while ((bytesRead = await stream
                                               .ReadAsync(buffer, 0, buffer.Length, state.cancellationToken)
                                               .ConfigureAwait(false)) > 0)
                                    {
                                        memoryStream.Write(buffer, 0, bytesRead);
                                    }
                                }

                                memoryStream.Position = 0;
                                objects.Add(await state.jsonSerializer
                                    .DeserializeAsync<T>(memoryStream, state.cancellationToken).ConfigureAwait(false));
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

        string selectionPath = QueryPropertyPath.BuildPath(innerObjectSelection);

        return _connection
            .WithConnectionBlockAsync<IEnumerable<(string Key, TOut InnerObject)>, (string selectionPath, string? partition, FilterBuilder<TIn>? filter, bool withTransaction, StringBuilder commandBuilder, IJsonSerializer jsonSerializer, CancellationToken cancellationToken)>(
                _connectionGate,
                (selectionPath, partition, filter, withTransaction, ReusableStringBuilder, _jsonSerializer, cancellationToken),
                static async (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    var objects = new List<(string, TOut)>();

                    try
                    {
                        await using var selectCommand = conn.CreateCommand();

                        var commandBuilder = state.commandBuilder;
                        commandBuilder.Clear().Append(Queries.ExtractDataAndKeyFromJsonValueWithFullTypeName(state.selectionPath));

                        selectCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value =
                            typeof(TIn).FullName;
                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

                        var filterParameters = new FilterParameters();
                        if (state.filter is not null)
                        {
                            state.filter.Build(commandBuilder, state.jsonSerializer, filterParameters);
                        }

#pragma warning disable CA2100 // Comparison values are parameterized (AddFilterParameters); only validated JSON paths/identifiers are concatenated.
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100
                        selectCommand.AddFilterParameters(filterParameters);

                        await using var reader = await selectCommand.ExecuteReaderAsync(state.cancellationToken).ConfigureAwait(false);

                        while (reader.Read())
                        {
                            string key = reader.GetString(reader.GetOrdinal(Queries.KeyColumn));

                            await using var innerObjectStream = reader.GetStream(reader.GetOrdinal(Queries.DataColumn));
                            var innerObject = await state.jsonSerializer
                                .DeserializeAsync<TOut>(innerObjectStream, state.cancellationToken).ConfigureAwait(false);

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
        return DeleteObjectWithKeyAsync<T>(GetIdFor(obj), partition, withTransaction, cancellationToken);
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
    public ValueTask<bool> DeleteObjectWithKeyAsync<T>(object key, string? partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync(
                _connectionGate,
                (key, partition, withTransaction, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        using var deleteCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.DeleteDataFromJsonValueWithKeyAndFullTypeName);

                        deleteCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = state.key;
                        deleteCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = TypeCache<T>.FullName;
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = state.commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        int deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return deletionCount == 1;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed to delete object with key \"{state.key}\"", ex);
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
                _connectionGate,
                (partition, filter, withTransaction, commandBuilder: ReusableStringBuilder, _jsonSerializer),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        using var deleteCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.DeleteDataFromJsonValueWithFullTypeName);

                        deleteCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = TypeCache<T>.FullName;
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

                        var filterParameters = new FilterParameters();
                        if (state.filter is not null)
                        {
                            state.filter.Build(state.commandBuilder, state._jsonSerializer, filterParameters);
                        }

#pragma warning disable CA2100 // Comparison values are parameterized (AddFilterParameters); only validated JSON paths/identifiers are concatenated.
                        deleteCommand.CommandText = state.commandBuilder.ToString();
#pragma warning restore CA2100
                        deleteCommand.AddFilterParameters(filterParameters);

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
                _connectionGate,
                (partition, withTransaction, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        using var deleteCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.DeleteDataFromJsonValueWithPartition);

                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = state.commandBuilder.ToString();
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
                _connectionGate,
                (withTransaction, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        using var deleteCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.DeleteDataFromJsonValue);

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = state.commandBuilder.ToString();
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
                _connectionGate,
                (stream, key, partition, withTransaction, cancellationToken),
                static async (conn, state) =>
                {
                    int writeCount = 0;

                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        await using var insertCommand = conn.CreateCommand();
                        insertCommand.CommandText = Queries.InsertOrReplaceBlob;

                        insertCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = state.key;
                        insertCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();
                        insertCommand.Parameters.AddWithValue(ParameterBlobLength, state.stream.Length);

                        long rowId = insertCommand.ExecuteScalar() is long id ? id : 0;

                        writeCount += rowId > 0 ? 1 : 0;

                        if (writeCount > 0)
                        {
                            await using (var writeStream = new SqliteBlob(conn, TableStreamValue, TableStreamValueDataColumn,
                                             rowId))
                            {
                                await state.stream.CopyToAsync(writeStream, state.cancellationToken).ConfigureAwait(false);
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
                _connectionGate,
                (key, partition, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    try
                    {
                        using var selectCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.SelectExistsFromStreamValueWithKey);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = state.key;

                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = state.commandBuilder.ToString();
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
                        throw new TychoException($"Failed Reading Object with key \"{state.key}\"", ex);
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
                _connectionGate,
                (key, partition, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    try
                    {
                        using var selectCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.SelectDataFromStreamValueWithKey);

                        selectCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = state.key;

                        selectCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        selectCommand.CommandText = state.commandBuilder.ToString();
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
                        throw new TychoException($"Failed Reading Object with key \"{state.key}\"", ex);
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
                _connectionGate,
                (key, partition, withTransaction, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        using var deleteCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.DeleteDataFromStreamValueWithKey);

                        deleteCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = state.key;
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = state.commandBuilder.ToString();
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities

                        int deletionCount = deleteCommand.ExecuteNonQuery();

                        transaction?.Commit();

                        return deletionCount == 1;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException($"Failed to delete object with key \"{state.key}\"", ex);
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
                _connectionGate,
                (partition, withTransaction, commandBuilder: ReusableStringBuilder),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        using var deleteCommand = conn.CreateCommand();

                        state.commandBuilder.Clear().Append(Queries.DeleteDataFromStreamValueWithPartition);
                        deleteCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value =
                            state.partition.AsValueOrEmptyString();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        deleteCommand.CommandText = state.commandBuilder.ToString();
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

        // These values are concatenated into DDL as a path/identifiers and cannot
        // be parameterized, so validate them against a strict grammar.
        QueryPropertyPath.ValidatePath(propertyPathString, nameof(propertyPathString));
        QueryPropertyPath.ValidateIdentifier(objectTypeName, nameof(objectTypeName));
        QueryPropertyPath.ValidateIdentifier(indexName, nameof(indexName));

        _connection
            .WithConnectionBlock(
                _connectionGate,
                (propertyPathString, isNumeric, objectTypeName, indexName),
                static (conn, state) =>
                {
                    var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    string fullIndexName = $"idx_{state.indexName}_{state.objectTypeName}";
                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

                        if (state.isNumeric)
                        {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                            createIndexCommand.CommandText =
                                Queries.CreateIndexForJsonValueAsNumeric(fullIndexName, state.propertyPathString);
#pragma warning restore CA2100 // Review SQL queries for security vulnerabilities
                        }
                        else
                        {
#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                            createIndexCommand.CommandText =
                                Queries.CreateIndexForJsonValue(fullIndexName, state.propertyPathString);
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

        QueryPropertyPath.ValidatePath(propertyPathString, nameof(propertyPathString));
        QueryPropertyPath.ValidateIdentifier(objectTypeName, nameof(objectTypeName));
        QueryPropertyPath.ValidateIdentifier(indexName, nameof(indexName));

        return _connection
            .WithConnectionBlockAsync(
                _connectionGate,
                (propertyPathString, isNumeric, objectTypeName, indexName),
                static (conn, state) =>
                {
                    using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    string fullIndexName = $"idx_{state.indexName}_{state.objectTypeName}";

                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        createIndexCommand.CommandText =
                            state.isNumeric
                                ? Queries.CreateIndexForJsonValueAsNumeric(fullIndexName, state.propertyPathString)
                                : Queries.CreateIndexForJsonValue(fullIndexName, state.propertyPathString);
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

        QueryPropertyPath.ValidateIdentifier(indexName, nameof(indexName));

        var processedPaths =
            propertyPaths
                .Select(x => (QueryPropertyPath.BuildPath(x), QueryPropertyPath.IsNumeric(x)))
                .ToArray();

        string safeTypeName = GetSafeTypeName<TObj>();

        _connection
            .WithConnectionBlock(
                _connectionGate,
                (processedPaths, indexName, safeTypeName),
                static (conn, state) =>
                {
                    var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    string fullIndexName = $"idx_{state.indexName}_{state.safeTypeName}";
                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, state.processedPaths);
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

        QueryPropertyPath.ValidateIdentifier(indexName, nameof(indexName));

        var processedPaths =
            propertyPaths
                .Select(x => (QueryPropertyPath.BuildPath(x), QueryPropertyPath.IsNumeric(x)))
                .ToArray();

        string safeTypeName = GetSafeTypeName<TObj>();

        return _connection
            .WithConnectionBlockAsync(
                _connectionGate,
                (processedPaths, indexName, safeTypeName),
                static (conn, state) =>
                {
                    using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

                    string fullIndexName = $"idx_{state.indexName}_{state.safeTypeName}";

                    try
                    {
                        using var createIndexCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Review SQL queries for security vulnerabilities
                        createIndexCommand.CommandText = Queries.CreateIndexForJsonValue(fullIndexName, state.processedPaths);
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
    /// <param name="shrinkMemory">Whether to release heap memory held by SQLite.</param>
    /// <param name="vacuum">
    /// Whether to reclaim free space to disk. On a database already in incremental
    /// auto-vacuum mode this runs a cheap in-place <c>incremental_vacuum</c>. On a
    /// legacy database that was created without incremental auto-vacuum (e.g. by an
    /// older version, or before the auto_vacuum ordering fix), <c>incremental_vacuum</c>
    /// is a no-op, so this instead runs a one-time full <c>VACUUM</c> that both reclaims
    /// the space and converts the database to incremental auto-vacuum for the future.
    /// </param>
    public void Cleanup(bool shrinkMemory = true, bool vacuum = false)
    {
        ArgumentNullException.ThrowIfNull(_connection);

        _connection
            .WithConnectionBlock(
                _connectionGate,
                (shrinkMemory, vacuum),
                static (conn, state) =>
                {
                    try
                    {
                        if (state.shrinkMemory)
                        {
                            using var shrinkCommand = conn.CreateCommand();
                            shrinkCommand.CommandText = "PRAGMA shrink_memory;";
                            shrinkCommand.ExecuteNonQuery();
                        }

                        if (!state.vacuum)
                        {
                            return;
                        }

                        // incremental_vacuum only reclaims space when the database is in
                        // INCREMENTAL (2) auto-vacuum mode.
                        long autoVacuumMode;
                        using (var modeCommand = conn.CreateCommand())
                        {
                            modeCommand.CommandText = "PRAGMA auto_vacuum;";
                            autoVacuumMode = modeCommand.ExecuteScalar() is long mode ? mode : 0L;
                        }

                        using var vacuumCommand = conn.CreateCommand();

                        if (autoVacuumMode == 2)
                        {
                            vacuumCommand.CommandText = "PRAGMA incremental_vacuum;";
                        }
                        else
                        {
                            // Legacy/NONE database: a full VACUUM reclaims free space and
                            // converts to incremental auto-vacuum. Use a file-backed temp
                            // store for the rebuild so a large database does not spike
                            // memory (VACUUM would otherwise honor temp_store = MEMORY and
                            // build the whole copy in RAM), then restore the in-memory
                            // temp store for normal operation.
                            vacuumCommand.CommandText =
                                "PRAGMA temp_store = FILE; PRAGMA auto_vacuum = INCREMENTAL; VACUUM; PRAGMA temp_store = MEMORY;";
                        }

                        vacuumCommand.ExecuteNonQuery();
                    }
                    catch (Exception ex)
                    {
                        throw new TychoException("Failed to clean up database", ex);
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
            _connectionGate?.Dispose();
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
                _connectionGate,
                _connectionScript,
                static (conn, script) =>
                {
                    conn.Open();

                    // Verified once per process (see EnsureJsonSupport).
                    EnsureJsonSupport(conn);

                    using var command = conn.CreateCommand();

                    // Profile PRAGMAs + idempotent schema/index creation. Composed from
                    // library constants and numeric profile values only (no user input).
#pragma warning disable CA2100
                    command.CommandText = script;
#pragma warning restore CA2100

                    command.ExecuteNonQuery();
                },
                _persistConnection);

        return connection;
    }

    private async ValueTask<SqliteConnection> BuildConnectionAsync(CancellationToken cancellationToken = default)
    {
        await _connectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var connection = new SqliteConnection(_dbConnectionString);

            connection.Open();

            // JSON support depends only on the process-wide native SQLite build, so
            // it is verified once and cached rather than re-queried on every connect.
            EnsureJsonSupport(connection);

            await using var command = connection.CreateCommand();

            // Profile PRAGMAs + idempotent schema/index creation. Composed from
            // library constants and numeric profile values only (no user input).
#pragma warning disable CA2100
            command.CommandText = _connectionScript;
#pragma warning restore CA2100

            command.ExecuteNonQuery();

            return connection;
        }
        finally
        {
            _connectionGate.Release();
        }
    }

    // 0 = not yet verified this process, 1 = verified as supported.
    private static int _jsonSupportVerified;

    /// <summary>
    /// Verifies (once per process) that the native SQLite build provides JSON
    /// support. The SQLite version is constant for the lifetime of the process, so
    /// the version/compile-option query is skipped on all connects after the first.
    /// </summary>
    private static void EnsureJsonSupport(SqliteConnection connection)
    {
        if (Volatile.Read(ref _jsonSupportVerified) == 1)
        {
            return;
        }

        bool supportsJson = false;

        using (var getVersionCommand = connection.CreateCommand())
        {
            getVersionCommand.CommandText = Queries.SqliteVersion;
            string? version = getVersionCommand.ExecuteScalar() as string;
            string[] splitVersion = version?.Split('.') ?? Array.Empty<string>();

            if (splitVersion.Length >= 2 &&
                int.TryParse(splitVersion[0], out int major) && int.TryParse(splitVersion[1], out int minor) &&
                (major > 3 || (major >= 3 && minor >= 38)))
            {
                supportsJson = true;
            }
        }

        if (!supportsJson)
        {
            using var hasJsonCommand = connection.CreateCommand();
            hasJsonCommand.CommandText = Queries.PragmaCompileOptions;
            using var jsonReader = hasJsonCommand.ExecuteReader();

            while (jsonReader.Read())
            {
                if (jsonReader.GetString(0)?.Equals(Queries.EnableJSON1Pragma) ?? false)
                {
                    supportsJson = true;
                    break;
                }
            }
        }

        if (!supportsJson)
        {
            connection.Close();
            throw new TychoException("JSON support is not available for this platform");
        }

        Volatile.Write(ref _jsonSupportVerified, 1);
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
    /// Builds a command for a multi-row INSERT OR REPLACE batch of <paramref name="rowCount"/>
    /// rows, returning the per-row key/json parameter arrays for value binding.
    /// </summary>
    private static (SqliteCommand Command, SqliteParameter[] KeyParams, SqliteParameter[] JsonParams) BuildBatchCommand(
        SqliteConnection conn,
        SqliteTransaction? transaction,
        int rowCount,
        object fullTypeNameValue,
        object partitionValue,
        int commandTimeout)
    {
        var command = conn.CreateCommand();
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        command.CommandTimeout = commandTimeout;
#pragma warning disable CA2100 // Query is composed only from constants and integer row indices; row values are parameterized.
        command.CommandText = Queries.BuildBatchInsertOrReplace(rowCount);
#pragma warning restore CA2100

        command.Parameters.Add(new SqliteParameter(ParameterFullTypeName, SqliteType.Text) { Value = fullTypeNameValue });
        command.Parameters.Add(new SqliteParameter(ParameterPartition, SqliteType.Text) { Value = partitionValue });

        var keyParams = new SqliteParameter[rowCount];
        var jsonParams = new SqliteParameter[rowCount];

        for (int i = 0; i < rowCount; i++)
        {
            var indexText = i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            keyParams[i] = command.Parameters.Add("$key" + indexText, SqliteType.Text);
            jsonParams[i] = command.Parameters.Add("$json" + indexText, SqliteType.Blob);
        }

        return (command, keyParams, jsonParams);
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
    /// <summary>
    /// Binds filter comparison values collected during query building onto the
    /// command as parameters, so they are never concatenated into the SQL text.
    /// </summary>
    public static void AddFilterParameters(this SqliteCommand command, FilterParameters parameters)
    {
        var values = parameters.Values;
        for (int i = 0; i < values.Count; i++)
        {
            command.Parameters.Add(
                new SqliteParameter(
                    FilterParameters.ParameterPrefix + i.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    values[i] ?? DBNull.Value));
        }
    }

    public static T WithConnectionBlock<T>(this SqliteConnection connection, SemaphoreSlim gate,
        Func<SqliteConnection, T> func, bool persistConnection)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        gate.Wait();

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

            gate.Release();
        }
    }

    /// <summary>
    /// State-passing overload to avoid closure allocations.
    /// </summary>
    public static T WithConnectionBlock<T, TState>(
        this SqliteConnection connection,
        SemaphoreSlim gate,
        TState state,
        Func<SqliteConnection, TState, T> func,
        bool persistConnection)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        gate.Wait();

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            return func.Invoke(connection, state);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }

            gate.Release();
        }
    }

    public static void WithConnectionBlock(this SqliteConnection connection, SemaphoreSlim gate,
        Action<SqliteConnection> action, bool persistConnection)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        gate.Wait();

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

            gate.Release();
        }
    }

    /// <summary>
    /// State-passing overload to avoid closure allocations.
    /// </summary>
    public static void WithConnectionBlock<TState>(
        this SqliteConnection connection,
        SemaphoreSlim gate,
        TState state,
        Action<SqliteConnection, TState> action,
        bool persistConnection)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        gate.Wait();

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            action.Invoke(connection, state);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }

            gate.Release();
        }
    }

    public static async ValueTask<T> WithConnectionBlockAsync<T>(
        this SqliteConnection connection,
        SemaphoreSlim gate,
        Func<SqliteConnection, T> func,
        bool persistConnection,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);

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

            gate.Release();
        }
    }

    /// <summary>
    /// State-passing overload to avoid closure allocations.
    /// </summary>
    public static async ValueTask<T> WithConnectionBlockAsync<T, TState>(
        this SqliteConnection connection,
        SemaphoreSlim gate,
        TState state,
        Func<SqliteConnection, TState, T> func,
        bool persistConnection,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            return func.Invoke(connection, state);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }

            gate.Release();
        }
    }

    public static async ValueTask<T> WithConnectionBlockAsync<T>(
        this SqliteConnection connection,
        SemaphoreSlim gate,
        Func<SqliteConnection, ValueTask<T>> func,
        bool persistConnection,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);

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

            gate.Release();
        }
    }

    /// <summary>
    /// State-passing overload to avoid closure allocations.
    /// </summary>
    public static async ValueTask<T> WithConnectionBlockAsync<T, TState>(
        this SqliteConnection connection,
        SemaphoreSlim gate,
        TState state,
        Func<SqliteConnection, TState, ValueTask<T>> func,
        bool persistConnection,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new TychoException("Please call 'Connect' before performing an operation");
        }

        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (!persistConnection)
            {
                connection.Open();
            }

            return await func.Invoke(connection, state).ConfigureAwait(false);
        }
        finally
        {
            if (!persistConnection)
            {
                connection.Close();
            }

            gate.Release();
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
