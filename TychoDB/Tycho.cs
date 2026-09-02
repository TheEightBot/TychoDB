using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.Json;
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
        ParameterKeys = "$keys",
        ParameterJson = "$json",
        ParameterBlob = "$blob",
        ParameterBlobLength = "$blobLength",
        TableStreamValue = "StreamValue",
        TableStreamValueDataColumn = "Data";

    /// <summary>
    /// Version of the generated index shape. Bumping this invalidates every stored
    /// index definition so the next CreateIndex call rebuilds it in the new shape.
    /// v2 = partial indexes led by Partition, scoped by FullTypeName.
    /// </summary>
    private const int IndexShapeVersion = 2;

    // Parameter cache - reuse parameter objects to reduce allocations
    private readonly ConcurrentDictionary<string, SqliteParameter> _parameterCache = new();

    private readonly Lock _connectionLock = new();
    private readonly string _dbConnectionString;

    // Per-connection setup script (profile PRAGMAs + schema DDL), built once.
    private readonly string _connectionScript;
    private readonly IJsonSerializer _jsonSerializer;
    private readonly bool _persistConnection;
    private readonly bool _requireTypeRegistration;

    // One rewrite per type, so its divergence verdict is probed once and reused.
    private readonly ConcurrentDictionary<Type, KeyColumnRewrite> _keyColumnRewrites = new();
    private readonly int _commandTimeout;
    private readonly Dictionary<Type, RegisteredTypeInformation> _registeredTypeInformation = new();

    // One command builder per database, written to only from inside a connection block. The gate
    // admits a single operation at a time, so the builder is never touched concurrently, and
    // capturing it at a call site is a side-effect-free reference read. The previous ThreadLocal
    // version cleared the builder *at capture time* on the calling thread, which raced with an
    // in-flight operation still appending to it on a pool thread; every block clears the builder
    // itself as its first statement, so nothing outside the gate needs to.
    private readonly StringBuilder _commandBuilder = new(1024);

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

    /// <summary>
    /// Gets the serializer's JSON member-name resolver, or <see langword="null"/> when the
    /// configured serializer does not implement <see cref="IJsonPropertyNameResolver"/> — in
    /// which case property paths fall back to CLR property names, as they always did.
    /// </summary>
    private IJsonPropertyNameResolver? NameResolver => _jsonSerializer as IJsonPropertyNameResolver;

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

        // The rewrite caches the id path resolved from the previous registration. Re-registering
        // a type can change that path — or remove the id property altogether — so the cached
        // entry has to go, or filters would be rewritten against a path the stored keys no
        // longer come from.
        _keyColumnRewrites.TryRemove(rti.ObjectType, out _);

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

        // The rewrite caches the id path resolved from the previous registration. Re-registering
        // a type can change that path — or remove the id property altogether — so the cached
        // entry has to go, or filters would be rewritten against a path the stored keys no
        // longer come from.
        _keyColumnRewrites.TryRemove(rti.ObjectType, out _);

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

        // The rewrite caches the id path resolved from the previous registration. Re-registering
        // a type can change that path — or remove the id property altogether — so the cached
        // entry has to go, or filters would be rewritten against a path the stored keys no
        // longer come from.
        _keyColumnRewrites.TryRemove(rti.ObjectType, out _);

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
            if (_connection is not null)
            {
                RunOptimize(_connection);
                _connection.Close();
                _connection.Dispose();
                _connection = null;
            }
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

        RunOptimize(_connection);

        await _connection.CloseAsync().ConfigureAwait(false);
        await _connection.DisposeAsync().ConfigureAwait(false);

        _connection = null;
    }

    /// <summary>
    /// Runs SQLite's recommended <c>PRAGMA optimize</c> (bounded by
    /// <c>analysis_limit</c>), refreshing query-planner statistics so indexes —
    /// including expression indexes over JSON_EXTRACT — keep being chosen.
    /// <para>
    /// Called both when opening and when closing a connection. The open-time call is
    /// what SQLite recommends for long-lived processes: a mobile app that connects
    /// once and never cleanly disconnects would otherwise run its entire lifetime on
    /// default planner heuristics.
    /// </para>
    /// Best-effort: failures never block connect or teardown.
    /// </summary>
    private static void RunOptimize(SqliteConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = "PRAGMA analysis_limit = 400; PRAGMA optimize;";
            command.ExecuteNonQuery();
        }
        catch
        {
            // Advisory only — ignore failures during teardown.
        }
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
    /// <remarks>
    /// The key this selector returns is the key the row is stored under, and it overrides any
    /// key the type's registration would supply. If the two disagree, the by-object overloads
    /// keep using the <em>registered</em> key and stop finding the row:
    /// <see cref="ReadObjectAsync{T}(T, string?, bool, IProgress{double}?, CancellationToken)"/>
    /// returns null and <c>DeleteObjectAsync(obj)</c> returns false without deleting anything,
    /// while the row is still there under the key this selector produced.
    /// <para>
    /// Under <c>requireTypeRegistration</c>, a type registered by id property will not let that
    /// happen: a selector that disagrees with the registration throws <see cref="TychoException"/>
    /// rather than writing a row the by-object overloads cannot reach. Outside strict mode the
    /// override is permitted and unchecked.
    /// </para>
    /// </remarks>
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
    /// <remarks>
    /// The key this selector returns is the key the row is stored under, and it overrides any
    /// key the type's registration would supply. If the two disagree, the by-object overloads
    /// keep using the <em>registered</em> key and stop finding the row:
    /// <see cref="ReadObjectAsync{T}(T, string?, bool, IProgress{double}?, CancellationToken)"/>
    /// returns null and <c>DeleteObjectAsync(obj)</c> returns false without deleting anything,
    /// while the row is still there under the key this selector produced.
    /// <para>
    /// Under <c>requireTypeRegistration</c>, a type registered by id property will not let that
    /// happen: a selector that disagrees with the registration throws <see cref="TychoException"/>
    /// rather than writing a row the by-object overloads cannot reach. Outside strict mode the
    /// override is permitted and unchecked.
    /// </para>
    /// </remarks>
    public ValueTask<bool> WriteObjectsAsync<T>(IEnumerable<T> objs, Func<T, object> keySelector,
        string? partition = null, bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(objs);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(_connection);

        keySelector = GuardAgainstKeyDivergence(keySelector);

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
    /// Writes a single object and reports whether the write created the row or replaced an
    /// existing one, using registered type information to determine the ID.
    /// </summary>
    /// <typeparam name="T">The type of the object to write.</typeparam>
    /// <param name="obj">The object to write.</param>
    /// <param name="partition">Optional partition key to organize objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// <see cref="UpsertResult.Inserted"/> when no row existed for the key in that partition,
    /// <see cref="UpsertResult.Updated"/> when one did and its data was replaced.
    /// </returns>
    /// <remarks>
    /// Stored contents are identical to <see cref="WriteObjectAsync{T}(T, string?, bool, CancellationToken)"/>;
    /// the difference is only the answer. The insert/update decision is made inside the
    /// connection gate and the transaction, so a caller keeping an incremental view of the
    /// store (a queue count, an added/removed signal) can rely on it without a
    /// read-then-write pair and an outer lock of its own.
    /// <para>
    /// There is no failure value: the result is only ever one of the two outcomes, and a call
    /// that returns has written the object. Every failure throws <see cref="TychoException"/>
    /// after rolling the transaction back, so the row is left exactly as it was - that covers
    /// the insert being ignored for a reason other than an existing row (the follow-up update
    /// then affects nothing), the update affecting anything other than one row, and any
    /// serializer or SQLite error on either statement.
    /// </para>
    /// </remarks>
    public ValueTask<UpsertResult> UpsertObjectAsync<T>(T obj, string? partition = null, bool withTransaction = true,
        CancellationToken cancellationToken = default)
    {
        return UpsertObjectAsync(obj, GetIdSelectorFor<T>(), partition, withTransaction, cancellationToken);
    }

    /// <summary>
    /// Writes a single object using a custom key selector and reports whether the write created
    /// the row or replaced an existing one.
    /// </summary>
    /// <typeparam name="T">The type of the object to write.</typeparam>
    /// <param name="obj">The object to write.</param>
    /// <param name="keySelector">A function that extracts the key from the object.</param>
    /// <param name="partition">Optional partition key to organize objects.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>
    /// <see cref="UpsertResult.Inserted"/> when no row existed for the key in that partition,
    /// <see cref="UpsertResult.Updated"/> when one did and its data was replaced.
    /// </returns>
    /// <remarks>
    /// The key selector follows the same rules as
    /// <see cref="WriteObjectAsync{T}(T, Func{T, object}, string?, bool, CancellationToken)"/>,
    /// including the strict-mode divergence guard. Failure semantics are those of
    /// <see cref="UpsertObjectAsync{T}(T, string?, bool, CancellationToken)"/>: never a third
    /// result value, always a <see cref="TychoException"/> with the transaction rolled back.
    /// </remarks>
    public ValueTask<UpsertResult> UpsertObjectAsync<T>(T obj, Func<T, object> keySelector, string? partition = null,
        bool withTransaction = true, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(obj);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(_connection);

        keySelector = GuardAgainstKeyDivergence(keySelector);

        return _connection
            .WithConnectionBlockAsync(
                _connectionGate,
                (obj, keySelector, partition, withTransaction, _commandTimeout, _jsonSerializer),
                static (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.Serializable);
                    }

                    try
                    {
                        var keyValue = state.keySelector(state.obj);
                        var fullTypeNameValue = TypeCache<T>.FullName;
                        var partitionValue = state.partition.AsValueOrEmptyString();

                        using var serializationStream = _memoryStreamManager.GetStream("TychoDB.UpsertObject");
                        state._jsonSerializer.Serialize(state.obj, serializationStream);
                        var json = serializationStream.ToArray();

                        // INSERT OR IGNORE affects one row only when the key was absent, which is
                        // exactly the answer; if it affected nothing the row exists and the data
                        // is replaced in place. Both statements run under the same gate and
                        // transaction, so no other writer can slip between them.
                        using var insertCommand = conn.CreateCommand();
                        if (transaction is not null)
                        {
                            insertCommand.Transaction = transaction;
                        }

                        insertCommand.CommandTimeout = state._commandTimeout;
                        insertCommand.CommandText = Queries.InsertOrIgnore;
                        insertCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = keyValue;
                        insertCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = fullTypeNameValue;
                        insertCommand.Parameters.Add(ParameterJson, SqliteType.Blob).Value = json;
                        insertCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partitionValue;

                        var result = UpsertResult.Inserted;
                        var affected = insertCommand.ExecuteNonQuery();

                        if (affected == 0)
                        {
                            using var updateCommand = conn.CreateCommand();
                            if (transaction is not null)
                            {
                                updateCommand.Transaction = transaction;
                            }

                            updateCommand.CommandTimeout = state._commandTimeout;
                            updateCommand.CommandText = Queries.UpdateDataWithKeyAndFullTypeName;
                            updateCommand.Parameters.Add(ParameterKey, SqliteType.Text).Value = keyValue;
                            updateCommand.Parameters.Add(ParameterFullTypeName, SqliteType.Text).Value = fullTypeNameValue;
                            updateCommand.Parameters.Add(ParameterJson, SqliteType.Blob).Value = json;
                            updateCommand.Parameters.Add(ParameterPartition, SqliteType.Text).Value = partitionValue;

                            affected = updateCommand.ExecuteNonQuery();
                            result = UpsertResult.Updated;
                        }

                        // Two outcomes only. INSERT OR IGNORE can be ignored for a constraint
                        // other than the existing-row case, and then the UPDATE finds nothing;
                        // either way anything but exactly one affected row is a failure, and a
                        // failure is an exception plus rollback - never a quiet "Updated".
                        if (affected != 1)
                        {
                            throw new TychoException($"Upsert affected {affected} rows; expected exactly one ({result})");
                        }

                        transaction?.Commit();

                        return result;
                    }
                    catch (TychoException)
                    {
                        transaction?.Rollback();
                        throw;
                    }
                    catch (Exception ex)
                    {
                        transaction?.Rollback();
                        throw new TychoException("Failed Upserting Object", ex);
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
                (partition, filter, withTransaction, commandBuilder: _commandBuilder, _jsonSerializer, keyRewrite: GetKeyColumnRewriteFor<T>()),
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
                            state.filter.Build(
                                state.commandBuilder,
                                state._jsonSerializer,
                                filterParameters,
                                state.keyRewrite?.VerifiedFor(conn, TypeCache<T>.FullName));
                        }

#pragma warning disable CA2100 // Comparison values are parameterized (AddFilterParameters); only validated JSON paths/identifiers are concatenated.
                        selectCommand.CommandText = state.commandBuilder.ToString();
#pragma warning restore CA2100
                        selectCommand.AddFilterParameters(filterParameters);

                        using var reader = selectCommand.ExecuteReader();

                        // One row holding the count, rather than one row per match.
                        int count = reader.Read() ? checked((int)reader.GetInt64(0)) : 0;

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
                (key, partition, withTransaction, commandBuilder: _commandBuilder),
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
                (key, partition, withTransaction, progress, commandBuilder: _commandBuilder, _jsonSerializer, cancellationToken),
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
    /// <param name="progress">
    ///     Optional progress reporter for the overall read. Reports a value between 0.0 and 1.0 based on rows
    ///     read out of the total matching rows, throttled to whole-percent steps (at most ~100 reports per read).
    ///     The matching rows are counted up front with the same predicate, so supplying a reporter adds one
    ///     count query but does not change how rows are deserialized.
    /// </param>
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
        => ReadObjectsCoreAsync(partition, filter, sort, top, withTransaction, progress, null, cancellationToken);

    /// <summary>
    /// Reads the objects stored under a set of keys, in one round trip.
    /// <para>
    /// Keys lead the primary key, so each is a primary-key probe; a filter on the key
    /// <em>property</em> goes through <c>JSON_EXTRACT</c> instead and scans. Prefer this over a
    /// loop of <see cref="ReadObjectAsync{T}(object, string?, bool, IProgress{double}?, CancellationToken)"/>:
    /// it takes the connection gate once rather than once per key, which matters under
    /// contention, and it measured 2.1–2.7x faster end to end than the loop across batches of
    /// 200 to 24,000 keys on a 250,000-row store. Both include deserialization, which is
    /// identical between them and dominates the remainder; the query alone is roughly 2.5x
    /// faster again.
    /// </para>
    /// <para>
    /// Keys are bound as a single JSON array, so there is no limit on how many may be passed
    /// and no chunking to think about. Keys that are not present are simply absent from the
    /// result, so the result may be shorter than the key set, and its order is the database's,
    /// not the key set's. Duplicate keys yield one object each.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The type of objects to read.</typeparam>
    /// <param name="keys">The keys to read. An empty set returns no objects without querying.</param>
    /// <param name="partition">Optional partition to read from.</param>
    /// <param name="sort">Optional sorting to apply to the result set.</param>
    /// <param name="withTransaction">Whether to use a transaction for the operation.</param>
    /// <param name="progress">Optional progress reporter; see <see cref="ReadObjectsAsync{T}"/>.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>A ValueTask containing the objects found for those keys.</returns>
    public ValueTask<IEnumerable<T>> ReadObjectsByKeysAsync<T>(
        IEnumerable<object> keys,
        string? partition = null,
        SortBuilder<T>? sort = null,
        bool withTransaction = false,
        IProgress<double>? progress = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);

        var keysJson = BuildKeyArrayJson(keys);

        if (keysJson is null)
        {
            return new ValueTask<IEnumerable<T>>(Array.Empty<T>());
        }

        return ReadObjectsCoreAsync<T>(partition, null, sort, null, withTransaction, progress, keysJson, cancellationToken);
    }

    /// <summary>
    /// Renders the key set as a JSON array of strings for JSON_EACH to expand, using the same
    /// ToString() form the single-key overloads bind. Returns null for an empty set, which has
    /// no query to run.
    /// </summary>
    private static string? BuildKeyArrayJson(IEnumerable<object> keys)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();

            var any = false;
            foreach (var key in keys)
            {
                ArgumentNullException.ThrowIfNull(key, nameof(key));

                writer.WriteStringValue(key.ToString());
                any = true;
            }

            if (!any)
            {
                return null;
            }

            writer.WriteEndArray();
        }

        return Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
    }

    private ValueTask<IEnumerable<T>> ReadObjectsCoreAsync<T>(
        string? partition,
        FilterBuilder<T>? filter,
        SortBuilder<T>? sort,
        int? top,
        bool withTransaction,
        IProgress<double>? progress,
        string? keysJson,
        CancellationToken cancellationToken)
    {
        if (_requireTypeRegistration)
        {
            CheckHasRegisteredType<T>();
        }

        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlockAsync<IEnumerable<T>, (string? partition, FilterBuilder<T>? filter, SortBuilder<T>? sort, int? top, bool withTransaction, IProgress<double>? progress, StringBuilder commandBuilder, int commandTimeout, IJsonSerializer jsonSerializer, string? keysJson, KeyColumnRewrite? keyRewrite, CancellationToken cancellationToken)>(
                _connectionGate,
                (partition, filter, sort, top, withTransaction, progress, _commandBuilder, _commandTimeout, _jsonSerializer, keysJson, GetKeyColumnRewriteFor<T>(), cancellationToken),
                static async (conn, state) =>
                {
                    SqliteTransaction? transaction = null;

                    if (state.withTransaction)
                    {
                        transaction = conn.BeginTransaction(IsolationLevel.RepeatableRead);
                    }

                    var commandBuilder = state.commandBuilder;
                    commandBuilder.Clear().Append(
                        state.keysJson is null
                            ? Queries.SelectDataFromJsonValueWithFullTypeName
                            : Queries.SelectDataFromJsonValueWithFullTypeNameAndKeys);

                    // Apply filters and sorting
                    var filterParameters = new FilterParameters();

                    // Resolved once and reused by the progress pre-count below, so the count and
                    // the rows it is measuring can never be built from different predicates.
                    var keyRewrite = state.keyRewrite?.VerifiedFor(conn, TypeCache<T>.FullName);

                    if (state.filter is not null)
                    {
                        state.filter.Build(commandBuilder, state.jsonSerializer, filterParameters, keyRewrite);
                    }

                    if (state.sort is not null)
                    {
                        state.sort.Build(commandBuilder, state.jsonSerializer);
                    }

                    if (state.top is not null)
                    {
                        // The base query and filter/sort fragments do not end with a newline, so
                        // one must be inserted here or LIMIT fuses onto the previous token
                        // (e.g. "$partitionLIMIT 50") and fails to parse.
                        commandBuilder.AppendLine().AppendLine(Queries.Limit(state.top.Value));
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
                        if (state.keysJson is not null)
                        {
                            selectCommand.Parameters.Add(new SqliteParameter(ParameterKeys, SqliteType.Text) { Value = state.keysJson });
                        }

                        selectCommand.AddFilterParameters(filterParameters);

                        // Overall progress needs the result-set size up front. Count with the same
                        // predicate before streaming rows; the select command's text is already
                        // captured, so the shared builder can be reused for the count SQL.
                        long totalRows = 0;
                        var lastReportedStep = -1;

                        if (state.progress is not null)
                        {
                            commandBuilder.Clear().Append(
                                state.keysJson is null
                                    ? Queries.SelectCountFromJsonValueWithFullTypeName
                                    : Queries.SelectCountFromJsonValueWithFullTypeNameAndKeys);

                            var countFilterParameters = new FilterParameters();
                            if (state.filter is not null)
                            {
                                state.filter.Build(commandBuilder, state.jsonSerializer, countFilterParameters, keyRewrite);
                            }

                            using var countCommand = conn.CreateCommand();

#pragma warning disable CA2100 // Comparison values are parameterized (AddFilterParameters); only validated JSON paths/identifiers are concatenated.
                            countCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100
                            countCommand.CommandTimeout = state.commandTimeout;
                            countCommand.Parameters.Add(new SqliteParameter(ParameterFullTypeName, SqliteType.Text) { Value = TypeCache<T>.FullName });
                            countCommand.Parameters.Add(new SqliteParameter(ParameterPartition, SqliteType.Text) { Value = state.partition.AsValueOrEmptyString() });
                            if (state.keysJson is not null)
                            {
                                countCommand.Parameters.Add(new SqliteParameter(ParameterKeys, SqliteType.Text) { Value = state.keysJson });
                            }

                            countCommand.AddFilterParameters(countFilterParameters);

                            await using var countReader = await countCommand.ExecuteReaderAsync(state.cancellationToken).ConfigureAwait(false);
                            if (countReader.Read())
                            {
                                totalRows = countReader.GetInt64(0);
                            }

                            if (state.top is not null && totalRows > state.top.Value)
                            {
                                totalRows = state.top.Value;
                            }

                            state.progress.Report(0.0);
                            lastReportedStep = 0;
                        }

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

                            // Fast path is available when the serializer can deserialize straight
                            // from a UTF-8 span. Resolved once per read, not per row. Progress is
                            // row-count based, so reporting never forces the slow streaming path.
                            var utf8Deserializer = state.jsonSerializer as IUtf8JsonDeserializer;

                            long rowsRead = 0;

                            while (reader.Read())
                            {
                                if (state.cancellationToken.IsCancellationRequested)
                                {
                                    break;
                                }

                                if (utf8Deserializer is not null)
                                {
                                    // The reader has already materialized the row's value, so hand
                                    // its UTF-8 bytes straight to the serializer. Avoids, per row:
                                    // an extra Stream allocation, a second full copy of every byte
                                    // into the scratch stream, and the ~4 async state-machine
                                    // transitions (ReadAsync loop, DeserializeAsync, DisposeAsync)
                                    // that dominated large reads.
                                    objects.Add(
                                        utf8Deserializer.Deserialize<T>(
                                            reader.GetFieldValue<byte[]>(dataOrdinal)));
                                }
                                else
                                {
                                    // Reset stream for reuse
                                    memoryStream.SetLength(0);

                                    int bytesRead;

                                    await using var stream = reader.GetStream(dataOrdinal);

                                    while ((bytesRead = await stream
                                               .ReadAsync(buffer, 0, buffer.Length, state.cancellationToken)
                                               .ConfigureAwait(false)) > 0)
                                    {
                                        memoryStream.Write(buffer, 0, bytesRead);
                                    }

                                    memoryStream.Position = 0;
                                    objects.Add(await state.jsonSerializer
                                        .DeserializeAsync<T>(memoryStream, state.cancellationToken).ConfigureAwait(false));
                                }

                                ++rowsRead;

                                if (state.progress is not null && totalRows > 0)
                                {
                                    // Throttle to whole-percent steps so a large read posts at most
                                    // ~100 reports instead of one per row.
                                    var step = (int)Math.Min(100, rowsRead * 100 / totalRows);
                                    if (step != lastReportedStep)
                                    {
                                        lastReportedStep = step;
                                        state.progress.Report(Math.Min(1.0, (double)rowsRead / totalRows));
                                    }
                                }
                            }

                            if (state.progress is not null
                                && lastReportedStep < 100
                                && !state.cancellationToken.IsCancellationRequested)
                            {
                                state.progress.Report(1.0);
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

        string selectionPath = QueryPropertyPath.BuildPath(innerObjectSelection, NameResolver);

        return _connection
            .WithConnectionBlockAsync<IEnumerable<(string Key, TOut InnerObject)>, (string selectionPath, string? partition, FilterBuilder<TIn>? filter, bool withTransaction, StringBuilder commandBuilder, IJsonSerializer jsonSerializer, KeyColumnRewrite? keyRewrite, CancellationToken cancellationToken)>(
                _connectionGate,
                (selectionPath, partition, filter, withTransaction, _commandBuilder, _jsonSerializer, keyRewrite: GetKeyColumnRewriteFor<TIn>(), cancellationToken),
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
                            state.filter.Build(
                                commandBuilder,
                                state.jsonSerializer,
                                filterParameters,
                                state.keyRewrite?.VerifiedFor(conn, typeof(TIn).FullName!));
                        }

#pragma warning disable CA2100 // Comparison values are parameterized (AddFilterParameters); only validated JSON paths/identifiers are concatenated.
                        selectCommand.CommandText = commandBuilder.ToString();
#pragma warning restore CA2100
                        selectCommand.AddFilterParameters(filterParameters);

                        await using var reader = await selectCommand.ExecuteReaderAsync(state.cancellationToken).ConfigureAwait(false);

                        int keyOrdinal = reader.GetOrdinal(Queries.KeyColumn);
                        int dataOrdinal = reader.GetOrdinal(Queries.DataColumn);

                        while (reader.Read())
                        {
                            string key = reader.GetString(keyOrdinal);

                            // The member is absent from this document (never written, or
                            // stored as JSON null). There is nothing to deserialize, and the
                            // absence is not an error, so yield the default for TOut.
                            if (reader.IsDBNull(dataOrdinal))
                            {
                                objects.Add((key, default!));
                                continue;
                            }

                            await using var innerObjectStream = reader.GetStream(dataOrdinal);
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
                (key, partition, withTransaction, commandBuilder: _commandBuilder),
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
                (partition, filter, withTransaction, commandBuilder: _commandBuilder, _jsonSerializer, keyRewrite: GetKeyColumnRewriteFor<T>()),
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
                            state.filter.Build(
                                state.commandBuilder,
                                state._jsonSerializer,
                                filterParameters,
                                state.keyRewrite?.VerifiedFor(conn, TypeCache<T>.FullName));
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
                (partition, withTransaction, commandBuilder: _commandBuilder),
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
                (withTransaction, commandBuilder: _commandBuilder),
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
                (key, partition, commandBuilder: _commandBuilder),
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
                (key, partition, commandBuilder: _commandBuilder),
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
                (key, partition, withTransaction, commandBuilder: _commandBuilder),
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
                (partition, withTransaction, commandBuilder: _commandBuilder),
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

        return CreateIndexCore(
            indexName,
            ToSafeIdentifier(GetSafeTypeName<TObj>()),
            new[] { (QueryPropertyPath.BuildPath(propertyPath, NameResolver), QueryPropertyPath.IsNumeric(propertyPath)) },
            TypeCache<TObj>.FullName);
    }

    /// <summary>
    /// Describes one logical index: the physical SQLite index name, the DDL, and the
    /// identity under which it is recorded in the TychoIndex metadata table.
    /// </summary>
    private readonly record struct IndexDefinition(
        string IndexName,
        string MetadataTypeName,
        string PhysicalName,
        string LegacyPhysicalName,
        string CommandText);

    /// <summary>
    /// Builds the physical index name and the CREATE INDEX statement for a set of
    /// indexed property paths. Every CreateIndex overload funnels through here, so
    /// the generated DDL has exactly one definition.
    /// <para>
    /// When <paramref name="fullTypeName"/> is known (the generic overloads) the index
    /// is partial and scoped to that type, and its physical name is suffixed with a
    /// stable hash of the full type name so two same-named types in different
    /// namespaces no longer collide on one index.
    /// </para>
    /// </summary>
    private static IndexDefinition BuildIndexDefinition(
        string indexName,
        string objectTypeName,
        (string PropertyPathString, bool IsNumeric)[] propertyPaths,
        string? fullTypeName)
    {
        string legacyName = $"idx_{indexName}_{objectTypeName}";

        string physicalName = fullTypeName is null
            ? legacyName
            : $"{legacyName}_{StableHashSuffix(fullTypeName)}";

        return new IndexDefinition(
            indexName,
            fullTypeName ?? objectTypeName,
            physicalName,
            legacyName,
            Queries.CreateIndexForJsonValue(physicalName, propertyPaths, fullTypeName));
    }

    /// <summary>
    /// Deterministic 32-bit FNV-1a hash rendered as 8 hex chars. String.GetHashCode
    /// is randomized per process and must not be used for a name persisted in the
    /// database schema.
    /// </summary>
    private static string StableHashSuffix(string value)
    {
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;

        uint hash = offsetBasis;
        foreach (char c in value)
        {
            hash = (hash ^ (byte)(c & 0xFF)) * prime;
            hash = (hash ^ (byte)(c >> 8)) * prime;
        }

        return hash.ToString("x8", System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Validates the identifiers and paths that are concatenated into index DDL
    /// (they cannot be parameterized) and returns the path/numeric pairs.
    /// </summary>
    private static (string PropertyPathString, bool IsNumeric)[] ValidateIndexInputs(
        string indexName,
        string objectTypeName,
        (string PropertyPathString, bool IsNumeric)[] propertyPaths,
        string pathParamName)
    {
        QueryPropertyPath.ValidateIdentifier(objectTypeName, nameof(objectTypeName));
        QueryPropertyPath.ValidateIdentifier(indexName, nameof(indexName));

        for (int i = 0; i < propertyPaths.Length; i++)
        {
            // Name the offending element so the exception identifies which path failed
            // rather than only the collection it came from.
            var paramName = propertyPaths.Length == 1
                ? pathParamName
                : $"{pathParamName}[{i.ToString(System.Globalization.CultureInfo.InvariantCulture)}]";

            QueryPropertyPath.ValidatePath(propertyPaths[i].PropertyPathString, paramName);
        }

        return propertyPaths;
    }

    /// <summary>
    /// Validates an index name loaded from the <c>TychoIndex</c> metadata table before
    /// it is concatenated into DDL. Tycho only ever writes validated names there, but
    /// the value is read back out of the database file, so it is re-checked rather than
    /// trusted.
    /// </summary>
    private static string ValidateStoredIndexName(string physicalName)
    {
        QueryPropertyPath.ValidateIdentifier(physicalName, nameof(physicalName));
        return physicalName;
    }

    private static void ExecuteNonQuery(SqliteConnection conn, string commandText)
    {
        using var command = conn.CreateCommand();

#pragma warning disable CA2100 // Only validated identifiers/paths are concatenated (ValidateIndexInputs).
        command.CommandText = commandText;
#pragma warning restore CA2100

        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Creates an index if an identical one is not already recorded, migrating away
    /// from any previous shape or name for the same logical index.
    /// <para>
    /// Mobile apps typically call CreateIndex on every launch, so the already-current
    /// case must be cheap: it costs one metadata lookup and one sqlite_master probe,
    /// with no DDL and no ANALYZE.
    /// </para>
    /// </summary>
    private static bool ExecuteCreateIndex(SqliteConnection conn, IndexDefinition definition)
    {
        using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

        try
        {
            if (IsIndexCurrent(conn, definition))
            {
                transaction.Commit();
                return true;
            }

            // Drop the previously recorded physical index (shape or definition
            // changed) and any pre-metadata index that used the legacy name, so an
            // upgraded database does not keep maintaining a stale b-tree forever.
            foreach (var stalePhysicalName in ReadRecordedPhysicalNames(conn, definition))
            {
                ExecuteNonQuery(conn, Queries.DropIndex(ValidateStoredIndexName(stalePhysicalName)));
            }

            if (!string.Equals(definition.LegacyPhysicalName, definition.PhysicalName, StringComparison.Ordinal))
            {
                ExecuteNonQuery(conn, Queries.DropIndex(definition.LegacyPhysicalName));
            }

            ExecuteNonQuery(conn, definition.CommandText);

            using (var upsert = conn.CreateCommand())
            {
                upsert.CommandText = Queries.UpsertIndexMetadata;
                upsert.Parameters.Add(new SqliteParameter("$indexName", SqliteType.Text) { Value = definition.IndexName });
                upsert.Parameters.Add(new SqliteParameter("$fullTypeName", SqliteType.Text) { Value = definition.MetadataTypeName });
                upsert.Parameters.Add(new SqliteParameter("$physicalName", SqliteType.Text) { Value = definition.PhysicalName });
                upsert.Parameters.Add(new SqliteParameter("$definition", SqliteType.Text) { Value = definition.CommandText });
                upsert.Parameters.Add(new SqliteParameter("$shapeVersion", SqliteType.Integer) { Value = IndexShapeVersion });
                upsert.ExecuteNonQuery();
            }

            transaction.Commit();
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            throw new TychoException($"Failed to Create Index: {definition.PhysicalName}", ex);
        }

        // Refresh planner statistics outside the transaction so the index just
        // created is usable by the very next query rather than only after a
        // Disconnect. Advisory: a failure here must not fail index creation.
        try
        {
            ExecuteNonQuery(conn, Queries.AnalyzeBounded);
        }
        catch
        {
            // Statistics are an optimization, not a correctness requirement.
        }

        return true;
    }

    /// <summary>
    /// True when the recorded definition matches what would be created now and the
    /// physical index is still present in the schema.
    /// </summary>
    private static bool IsIndexCurrent(SqliteConnection conn, IndexDefinition definition)
    {
        using var lookup = conn.CreateCommand();
        lookup.CommandText = Queries.SelectIndexMetadata;
        lookup.Parameters.Add(new SqliteParameter("$indexName", SqliteType.Text) { Value = definition.IndexName });
        lookup.Parameters.Add(new SqliteParameter("$fullTypeName", SqliteType.Text) { Value = definition.MetadataTypeName });

        string? recordedDefinition = null;
        long recordedVersion = -1;

        using (var reader = lookup.ExecuteReader())
        {
            if (reader.Read())
            {
                recordedDefinition = reader.GetString(1);
                recordedVersion = reader.GetInt64(2);
            }
        }

        if (recordedVersion != IndexShapeVersion ||
            !string.Equals(recordedDefinition, definition.CommandText, StringComparison.Ordinal))
        {
            return false;
        }

        using var exists = conn.CreateCommand();
        exists.CommandText = Queries.SelectPhysicalIndexExists;
        exists.Parameters.Add(new SqliteParameter("$physicalName", SqliteType.Text) { Value = definition.PhysicalName });
        return exists.ExecuteScalar() is not null;
    }

    private static List<string> ReadRecordedPhysicalNames(SqliteConnection conn, IndexDefinition definition)
    {
        var names = new List<string>(1);

        using var lookup = conn.CreateCommand();
        lookup.CommandText = Queries.SelectIndexMetadata;
        lookup.Parameters.Add(new SqliteParameter("$indexName", SqliteType.Text) { Value = definition.IndexName });
        lookup.Parameters.Add(new SqliteParameter("$fullTypeName", SqliteType.Text) { Value = definition.MetadataTypeName });

        using var reader = lookup.ExecuteReader();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private Tycho CreateIndexCore(
        string indexName,
        string objectTypeName,
        (string PropertyPathString, bool IsNumeric)[] propertyPaths,
        string? fullTypeName = null,
        string pathParamName = "propertyPaths")
    {
        ArgumentNullException.ThrowIfNull(_connection);

        var definition = BuildIndexDefinition(
            indexName,
            objectTypeName,
            ValidateIndexInputs(indexName, objectTypeName, propertyPaths, pathParamName),
            fullTypeName);

        _connection
            .WithConnectionBlock(
                _connectionGate,
                definition,
                static (conn, state) => ExecuteCreateIndex(conn, state),
                _persistConnection);

        return this;
    }

    private ValueTask<bool> CreateIndexCoreAsync(
        string indexName,
        string objectTypeName,
        (string PropertyPathString, bool IsNumeric)[] propertyPaths,
        CancellationToken cancellationToken,
        string? fullTypeName = null,
        string pathParamName = "propertyPaths")
    {
        ArgumentNullException.ThrowIfNull(_connection);

        var definition = BuildIndexDefinition(
            indexName,
            objectTypeName,
            ValidateIndexInputs(indexName, objectTypeName, propertyPaths, pathParamName),
            fullTypeName);

        return _connection
            .WithConnectionBlockAsync(
                _connectionGate,
                definition,
                static (conn, state) => ExecuteCreateIndex(conn, state),
                _persistConnection,
                cancellationToken);
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
        => CreateIndexCore(
            indexName,
            objectTypeName,
            new[] { (propertyPathString, isNumeric) },
            pathParamName: nameof(propertyPathString));

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

        return CreateIndexCoreAsync(
            indexName,
            ToSafeIdentifier(GetSafeTypeName<TObj>()),
            new[] { (QueryPropertyPath.BuildPath(propertyPath, NameResolver), QueryPropertyPath.IsNumeric(propertyPath)) },
            cancellationToken,
            TypeCache<TObj>.FullName);
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
        => CreateIndexCoreAsync(
            indexName,
            objectTypeName,
            new[] { (propertyPathString, isNumeric) },
            cancellationToken,
            pathParamName: nameof(propertyPathString));

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

        return CreateIndexCore(indexName, ToSafeIdentifier(GetSafeTypeName<TObj>()), ProcessIndexPaths(propertyPaths, NameResolver), TypeCache<TObj>.FullName);
    }

    /// <summary>
    /// Resolves each indexed property expression to its JSON path and numeric
    /// classification. The same <see cref="QueryPropertyPath"/> helpers back the
    /// filter builder, so an index and the filters over it agree on the SQL
    /// expression by construction — including the JSON member names, which both
    /// resolve through <paramref name="nameResolver"/>.
    /// </summary>
    private static (string PropertyPathString, bool IsNumeric)[] ProcessIndexPaths<TObj>(
        Expression<Func<TObj, object>>[] propertyPaths,
        IJsonPropertyNameResolver? nameResolver)
    {
        ArgumentNullException.ThrowIfNull(propertyPaths);

        var processed = new (string PropertyPathString, bool IsNumeric)[propertyPaths.Length];
        for (int i = 0; i < propertyPaths.Length; i++)
        {
            processed[i] = (QueryPropertyPath.BuildPath(propertyPaths[i], nameResolver), QueryPropertyPath.IsNumeric(propertyPaths[i]));
        }

        return processed;
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

        return CreateIndexCoreAsync(indexName, ToSafeIdentifier(GetSafeTypeName<TObj>()), ProcessIndexPaths(propertyPaths, NameResolver), cancellationToken, TypeCache<TObj>.FullName);
    }

    /// <summary>
    /// Drops an index previously created for <typeparamref name="TObj"/>, together
    /// with its metadata record.
    /// </summary>
    /// <typeparam name="TObj">The indexed type.</typeparam>
    /// <param name="indexName">The logical index name passed to CreateIndex.</param>
    /// <returns>True if an index was dropped; false if no such index was recorded.</returns>
    public bool DropIndex<TObj>(string indexName)
    {
        ArgumentNullException.ThrowIfNull(_connection);
        QueryPropertyPath.ValidateIdentifier(indexName, nameof(indexName));

        return _connection
            .WithConnectionBlock(
                _connectionGate,
                (indexName, typeName: TypeCache<TObj>.FullName),
                static (conn, state) => ExecuteDropIndex(conn, state.indexName, state.typeName),
                _persistConnection);
    }

    /// <summary>
    /// Asynchronously drops an index previously created for <typeparamref name="TObj"/>.
    /// </summary>
    /// <typeparam name="TObj">The indexed type.</typeparam>
    /// <param name="indexName">The logical index name passed to CreateIndex.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
    /// <returns>True if an index was dropped; false if no such index was recorded.</returns>
    public ValueTask<bool> DropIndexAsync<TObj>(string indexName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(_connection);
        QueryPropertyPath.ValidateIdentifier(indexName, nameof(indexName));

        return _connection
            .WithConnectionBlockAsync(
                _connectionGate,
                (indexName, typeName: TypeCache<TObj>.FullName),
                static (conn, state) => ExecuteDropIndex(conn, state.indexName, state.typeName),
                _persistConnection,
                cancellationToken);
    }

    private static bool ExecuteDropIndex(SqliteConnection conn, string indexName, string typeName)
    {
        using var transaction = conn.BeginTransaction(IsolationLevel.Serializable);

        try
        {
            string? physicalName = null;

            using (var lookup = conn.CreateCommand())
            {
                lookup.CommandText = Queries.SelectIndexMetadata;
                lookup.Parameters.Add(new SqliteParameter("$indexName", SqliteType.Text) { Value = indexName });
                lookup.Parameters.Add(new SqliteParameter("$fullTypeName", SqliteType.Text) { Value = typeName });

                using var reader = lookup.ExecuteReader();
                if (reader.Read())
                {
                    physicalName = reader.GetString(0);
                }
            }

            if (physicalName is null)
            {
                transaction.Commit();
                return false;
            }

            ExecuteNonQuery(conn, Queries.DropIndex(ValidateStoredIndexName(physicalName)));

            using (var delete = conn.CreateCommand())
            {
                delete.CommandText = Queries.DeleteIndexMetadata;
                delete.Parameters.Add(new SqliteParameter("$indexName", SqliteType.Text) { Value = indexName });
                delete.Parameters.Add(new SqliteParameter("$fullTypeName", SqliteType.Text) { Value = typeName });
                delete.ExecuteNonQuery();
            }

            transaction.Commit();
            return true;
        }
        catch (Exception ex)
        {
            transaction.Rollback();
            throw new TychoException($"Failed to Drop Index: {indexName}", ex);
        }
    }

    /// <summary>
    /// Lists the indexes Tycho has created, as recorded in its metadata table.
    /// Indexes created directly against the database outside Tycho are not listed.
    /// </summary>
    /// <returns>The recorded indexes, ordered by type then index name.</returns>
    public IReadOnlyList<TychoIndexInfo> ListIndexes()
    {
        ArgumentNullException.ThrowIfNull(_connection);

        return _connection
            .WithConnectionBlock<IReadOnlyList<TychoIndexInfo>, object?>(
                _connectionGate,
                null,
                static (conn, _) =>
                {
                    var results = new List<TychoIndexInfo>();

                    using var command = conn.CreateCommand();
                    command.CommandText = Queries.SelectAllIndexMetadata;

                    using var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        results.Add(new TychoIndexInfo(
                            reader.GetString(0),
                            reader.GetString(1),
                            reader.GetString(2),
                            reader.GetString(3)));
                    }

                    return results;
                },
                _persistConnection);
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
                            // In-place free-page reclamation, then truncate the WAL file so
                            // its space is returned to disk too.
                            vacuumCommand.CommandText =
                                "PRAGMA incremental_vacuum; PRAGMA wal_checkpoint(TRUNCATE);";
                        }
                        else
                        {
                            // Legacy/NONE database: a full VACUUM reclaims free space and
                            // converts to incremental auto-vacuum. Use a file-backed temp
                            // store for the rebuild so a large database does not spike
                            // memory (VACUUM would otherwise honor temp_store = MEMORY and
                            // build the whole copy in RAM), then restore the in-memory
                            // temp store for normal operation and truncate the WAL.
                            vacuumCommand.CommandText =
                                "PRAGMA temp_store = FILE; PRAGMA auto_vacuum = INCREMENTAL; VACUUM; PRAGMA temp_store = MEMORY; PRAGMA wal_checkpoint(TRUNCATE);";
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
    /// Builds the key-column rewrite candidate for <typeparamref name="T"/>, or null when the
    /// preconditions do not hold: strict registration must be on (so the write guard forbids a
    /// divergent key) and the type must have been registered by id property (so there is a path
    /// to compare a filter against). The candidate still has to clear its divergence probe
    /// against the data before it is used.
    /// </summary>
    private KeyColumnRewrite? GetKeyColumnRewriteFor<T>()
    {
        if (!_requireTypeRegistration
            || !_registeredTypeInformation.TryGetValue(typeof(T), out var rti)
            || rti is null
            || rti.RequiresIdMapping
            || rti.IdPropertyPathSegments is null)
        {
            return null;
        }

        return _keyColumnRewrites.GetOrAdd(
            typeof(T),
            static (_, state) =>
                new KeyColumnRewrite(
                    QueryPropertyPath.RenderPath(
                        state.Segments,
                        QueryPropertyPath.AsNameResolver(state.Serializer)),
                    state.CommandTimeout),
            (Segments: rti.IdPropertyPathSegments, Serializer: _jsonSerializer, CommandTimeout: _commandTimeout));
    }

    /// <summary>
    /// In strict mode, wraps a caller-supplied key selector so that a key disagreeing with the
    /// type's registered id property is rejected instead of written.
    /// <para>
    /// A row stored under a key the registration would not produce is unreachable by
    /// <c>ReadObjectAsync(obj)</c>, <c>ObjectExistsAsync(obj)</c> and <c>DeleteObjectAsync(obj)</c>,
    /// all of which key off the registration — and the delete failure is silent, returning false
    /// while the row survives. Turning that into an exception at the write is the only point
    /// where the disagreement is still visible.
    /// </para>
    /// <para>
    /// The check is a wrapper rather than a pre-pass so the object sequence is enumerated once:
    /// callers routinely pass a lazy query. It applies only when strict registration is on and
    /// the type was registered by id property — a delegate registration has no property to
    /// compare against, and outside strict mode the override is deliberate and permitted.
    /// </para>
    /// </summary>
    private Func<T, object> GuardAgainstKeyDivergence<T>(Func<T, object> keySelector)
    {
        if (!_requireTypeRegistration
            || !_registeredTypeInformation.TryGetValue(typeof(T), out var rti)
            || rti is null
            || rti.RequiresIdMapping
            || rti.IdPropertyPath is null)
        {
            return keySelector;
        }

        var registeredSelector = rti.GetIdSelector<T>();
        var idProperty = rti.IdProperty;

        return obj =>
        {
            var supplied = keySelector(obj);

            // Compared as text because that is what the Key column stores: both sides go
            // through ToString() on their way into the database.
            var suppliedKey = supplied?.ToString();
            var registeredKey = registeredSelector(obj)?.ToString();

            if (!string.Equals(suppliedKey, registeredKey, StringComparison.Ordinal))
            {
                throw new TychoException(
                    $"The supplied key selector produced \"{suppliedKey}\" for {typeof(T).Name}, but its registered id property {idProperty} gives \"{registeredKey}\". " +
                    $"A row written under \"{suppliedKey}\" could not be read or deleted by object, because those overloads use the registered id. " +
                    "Supply the registered key, register the type with a custom key selector instead, or turn off requireTypeRegistration.");
            }

            // Func<T, object> promises a non-null key; the null-conditional above is defensive
            // against a selector that breaks that contract, not an admission that it may.
            return supplied!;
        };
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
            if (_connection is not null)
            {
                RunOptimize(_connection);
            }

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

    /// <summary>
    /// Makes a library-derived type name usable as a SQL identifier. Type names for
    /// closed generics are rendered with separators that are not identifier
    /// characters (e.g. <c>Dictionary_2__String,Int32__</c>), which the identifier
    /// validator rejects. These names come from <see cref="Type"/>, not from the
    /// caller, so the right treatment is to normalize them rather than to reject the
    /// call; caller-supplied identifiers are still validated strictly.
    /// </summary>
    private static string ToSafeIdentifier(string derivedTypeName)
    {
        if (derivedTypeName.Length == 0)
        {
            return "_";
        }

        Span<char> buffer = derivedTypeName.Length <= 128
            ? stackalloc char[derivedTypeName.Length]
            : new char[derivedTypeName.Length];

        for (int i = 0; i < derivedTypeName.Length; i++)
        {
            char c = derivedTypeName[i];
            buffer[i] = char.IsLetterOrDigit(c) || c == '_' ? c : '_';
        }

        // Identifiers may not start with a digit.
        return char.IsDigit(buffer[0]) ? string.Concat("_", buffer.ToString()) : buffer.ToString();
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

                    RunOptimize(conn);
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

            RunOptimize(connection);

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
