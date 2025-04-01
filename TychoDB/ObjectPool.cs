using System;
using System.Collections.Concurrent;

namespace TychoDB;

/// <summary>
/// A generic object pool that reduces memory allocations by reusing objects.
/// </summary>
/// <typeparam name="T">The type of object to pool.</typeparam>
internal sealed class ObjectPool<T>
    where T : class
{
    private readonly ConcurrentBag<T> _objects;
    private readonly Func<T> _objectGenerator;
    private readonly Func<T, T> _objectResetter;
    private readonly int _maxSize;

    /// <summary>
    /// Initializes a new instance of the <see cref="ObjectPool{T}"/> class.
    /// Creates a new object pool with the specified object generator and optional resetter.
    /// </summary>
    /// <param name="objectGenerator">Function to create a new instance when none are available.</param>
    /// <param name="objectResetter">Optional function to reset/clear an object before returning to the pool.</param>
    /// <param name="maxSize">Maximum number of objects to keep in the pool, default is 100.</param>
    public ObjectPool(Func<T> objectGenerator, Func<T, T> objectResetter = null, int maxSize = 100)
    {
        _objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));
        _objectResetter = objectResetter;
        _maxSize = maxSize;
        _objects = new ConcurrentBag<T>();
    }

    /// <summary>
    /// Gets an object from the pool or creates a new one if none are available.
    /// </summary>
    /// <returns>An object of type T.</returns>
    public T Get()
    {
        if (_objects.TryTake(out T item))
        {
            return item;
        }

        return _objectGenerator();
    }

    /// <summary>
    /// Returns an object to the pool for reuse.
    /// </summary>
    /// <param name="item">The object to return to the pool.</param>
    public void Return(T item)
    {
        if (item == null)
        {
            return;
        }

        // Apply the reset function if one was provided
        if (_objectResetter != null)
        {
            item = _objectResetter(item);
        }

        // Only add to pool if we haven't reached max size
        if (_objects.Count < _maxSize)
        {
            _objects.Add(item);
        }
    }
}
