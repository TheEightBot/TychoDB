using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TychoDB;

/// <summary>
/// Provides extension methods for Tycho to support LINQ-style queries.
/// </summary>
public static class TychoQueryableExtensions
{
    /// <summary>
    /// Creates a queryable source for the specified entity type.
    /// </summary>
    /// <typeparam name="T">The type of entity to query.</typeparam>
    /// <param name="db">The Tycho database instance.</param>
    /// <param name="partition">Optional partition name for the query.</param>
    /// <returns>A queryable object for building and executing queries.</returns>
    public static TychoQueryable<T> Query<T>(this Tycho db, string? partition = null)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(db);

        return new TychoQueryable<T>(db, partition!);
    }

    /// <summary>
    /// Inserts or updates an entity in the database.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="db">The Tycho database instance.</param>
    /// <param name="entity">The entity to insert or update.</param>
    /// <param name="partition">Optional partition name.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains true if the operation was successful.</returns>
    public static ValueTask<bool> SaveAsync<T>(this Tycho db, T entity, string? partition = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentNullException.ThrowIfNull(entity);

        return db.WriteObjectAsync(entity, partition, true, cancellationToken);
    }

    /// <summary>
    /// Inserts or updates multiple entities in the database.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="db">The Tycho database instance.</param>
    /// <param name="entities">The entities to insert or update.</param>
    /// <param name="partition">Optional partition name.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains true if the operation was successful.</returns>
    public static ValueTask<bool> SaveAllAsync<T>(this Tycho db, IEnumerable<T> entities, string? partition = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(db);

        ArgumentNullException.ThrowIfNull(entities);

        return db.WriteObjectsAsync(entities, partition, true, cancellationToken);
    }

    /// <summary>
    /// Removes an entity from the database.
    /// </summary>
    /// <typeparam name="T">The entity type.</typeparam>
    /// <param name="db">The Tycho database instance.</param>
    /// <param name="entity">The entity to remove.</param>
    /// <param name="partition">Optional partition name.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains true if the deletion was successful.</returns>
    public static ValueTask<bool> RemoveAsync<T>(this Tycho db, T entity, string? partition = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        ArgumentNullException.ThrowIfNull(db);

        ArgumentNullException.ThrowIfNull(entity);

        return db.DeleteObjectAsync(entity, partition, true, cancellationToken);
    }
}
