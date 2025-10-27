using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace TychoDB;

/// <summary>
/// Provides a LINQ-like query interface for TychoDB.
/// </summary>
/// <typeparam name="T">The type of entity to query.</typeparam>
public class TychoQueryable<T>
    where T : class
{
    private readonly Tycho _db;
    private readonly string _partition;
    private FilterBuilder<T>? _filter;
    private SortBuilder<T>? _sortBuilder;
    private int? _limit;

    internal TychoQueryable(Tycho db, string? partition = null)
    {
        _db = db ?? throw new ArgumentNullException(nameof(db));
        _partition = partition!;
        _filter = null;
        _sortBuilder = null;
    }

    /// <summary>
    /// Filters entities based on a predicate expression.
    /// </summary>
    /// <param name="predicate">The predicate to filter entities.</param>
    /// <returns>A new TychoQueryable with the filter applied.</returns>
    public TychoQueryable<T> Where(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var result = Clone();
        result._filter = BuildFilterFromPredicate(predicate, result._filter);
        return result;
    }

    /// <summary>
    /// Orders entities in ascending order by the specified property.
    /// </summary>
    /// <typeparam name="TKey">The type of the property to order by.</typeparam>
    /// <param name="keySelector">The property selector expression.</param>
    /// <returns>A new TychoQueryable with the ordering applied.</returns>
    public TychoQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);

        var result = Clone();
        result._sortBuilder = SortBuilder<T>.Create().OrderBy(SortDirection.Ascending, keySelector);
        return result;
    }

    /// <summary>
    /// Orders entities in descending order by the specified property.
    /// </summary>
    /// <typeparam name="TKey">The type of the property to order by.</typeparam>
    /// <param name="keySelector">The property selector expression.</param>
    /// <returns>A new TychoQueryable with the ordering applied.</returns>
    public TychoQueryable<T> OrderByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);

        var result = Clone();
        result._sortBuilder = SortBuilder<T>.Create().OrderBy(SortDirection.Descending, keySelector);
        return result;
    }

    /// <summary>
    /// Adds a secondary ascending ordering by the specified property.
    /// </summary>
    /// <typeparam name="TKey">The type of the property to order by.</typeparam>
    /// <param name="keySelector">The property selector expression.</param>
    /// <returns>A new TychoQueryable with the ordering applied.</returns>
    public TychoQueryable<T> ThenBy<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);

        if (_sortBuilder is null)
        {
            throw new InvalidOperationException("OrderBy must be called before ThenBy");
        }

        var result = Clone();

        if (result._sortBuilder is not null)
        {
            result._sortBuilder = result._sortBuilder.OrderBy(SortDirection.Ascending, keySelector);
        }

        return result;
    }

    /// <summary>
    /// Adds a secondary descending ordering by the specified property.
    /// </summary>
    /// <typeparam name="TKey">The type of the property to order by.</typeparam>
    /// <param name="keySelector">The property selector expression.</param>
    /// <returns>A new TychoQueryable with the ordering applied.</returns>
    public TychoQueryable<T> ThenByDescending<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        ArgumentNullException.ThrowIfNull(keySelector);

        if (_sortBuilder is null)
        {
            throw new InvalidOperationException("OrderBy must be called before ThenByDescending");
        }

        var result = Clone();

        if (result._sortBuilder is not null)
        {
            result._sortBuilder = result._sortBuilder.OrderBy(SortDirection.Descending, keySelector);
        }

        return result;
    }

    /// <summary>
    /// Limits the number of results returned.
    /// </summary>
    /// <param name="count">The maximum number of results to return.</param>
    /// <returns>A new TychoQueryable with the limit applied.</returns>
    public TychoQueryable<T> Take(int count)
    {
        if (count <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count must be positive");
        }

        var result = Clone();
        result._limit = count;
        return result;
    }

    /// <summary>
    /// Returns the first entity that matches the filters, or default(T) if no match is found.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the first matching entity or default(T).</returns>
    public async ValueTask<T> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        var result = Clone();
        result._limit = 1;
        var filter = result._filter ?? FilterBuilder<T>.Create();
        var obj = await _db.ReadFirstObjectAsync(filter, _partition, cancellationToken: cancellationToken);
        return obj ?? default!;
    }

    /// <summary>
    /// Returns the entity with the specified ID.
    /// </summary>
    /// <param name="id">The ID of the entity to retrieve.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the matching entity or default(T).</returns>
    public ValueTask<T> FindAsync(object id, CancellationToken cancellationToken = default)
    {
        return _db.ReadObjectAsync<T>(id, _partition, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Determines whether any entity matches the filters.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains true if any entities match; otherwise, false.</returns>
    public async ValueTask<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        var count = await _db.CountObjectsAsync<T>(_partition, _filter, cancellationToken: cancellationToken);
        return count > 0;
    }

    /// <summary>
    /// Returns the single entity that matches the filters, or throws an exception if zero or multiple matches are found.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single matching entity.</returns>
    public ValueTask<T> SingleAsync(CancellationToken cancellationToken = default)
    {
        var filter = _filter ?? FilterBuilder<T>.Create();
        return _db.ReadObjectAsync(filter, _partition, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Returns the single entity that matches the filters, or default(T) if no match is found.
    /// Throws an exception if multiple matches are found.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the single matching entity or default(T).</returns>
    public async ValueTask<T> SingleOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var filter = _filter ?? FilterBuilder<T>.Create();
            var obj = await _db.ReadObjectAsync(filter, _partition, cancellationToken: cancellationToken);
            return obj ?? default!;
        }
        catch (TychoException ex) when (ex.Message.Contains("Too many matching values"))
        {
            throw;
        }
        catch
        {
            return default!;
        }
    }

    /// <summary>
    /// Returns the number of entities that match the filters.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the number of matching entities.</returns>
    public ValueTask<int> CountAsync(CancellationToken cancellationToken = default)
    {
        return _db.CountObjectsAsync<T>(_partition, _filter, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Executes the query and returns the results as a list.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the query results as a list.</returns>
    public async ValueTask<List<T>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var filter = _filter ?? FilterBuilder<T>.Create();
        var results = await _db.ReadObjectsAsync<T>(_partition, filter, _sortBuilder, _limit,
            cancellationToken: cancellationToken);
        return results?.ToList() ?? new List<T>();
    }

    /// <summary>
    /// Creates a clone of this queryable.
    /// </summary>
    /// <returns>A clone of this queryable.</returns>
    private TychoQueryable<T> Clone()
    {
        return new TychoQueryable<T>(_db, _partition)
        {
            _filter = _filter,
            _sortBuilder = _sortBuilder,
            _limit = _limit,
        };
    }

    /// <summary>
    /// Builds a FilterBuilder from a predicate expression.
    /// </summary>
    /// <param name="predicate">The predicate expression.</param>
    /// <param name="existingFilter">An existing filter to extend, or null.</param>
    /// <returns>A FilterBuilder that represents the predicate.</returns>
    private FilterBuilder<T> BuildFilterFromPredicate(
        Expression<Func<T, bool>> predicate,
        FilterBuilder<T>? existingFilter)
    {
        existingFilter = existingFilter is null ? FilterBuilder<T>.Create() : existingFilter.And();

        return BuildFilterFromExpressionInternal(predicate.Body, existingFilter!);
    }

    private FilterBuilder<T> BuildFilterFromExpressionInternal(Expression expression, FilterBuilder<T>? filterBuilder)
    {
        // Handle binary expressions (most common in where clauses)
        if (expression is BinaryExpression binaryExpression)
        {
            switch (binaryExpression.NodeType)
            {
                case ExpressionType.AndAlso:
                    // x => x.A && x.B
                    filterBuilder = BuildFilterFromExpressionInternal(binaryExpression.Left, filterBuilder);
                    filterBuilder = filterBuilder.And();
                    return BuildFilterFromExpressionInternal(binaryExpression.Right, filterBuilder);

                case ExpressionType.OrElse:
                    // x => x.A || x.B
                    filterBuilder = BuildFilterFromExpressionInternal(binaryExpression.Left, filterBuilder);
                    filterBuilder = filterBuilder.Or();
                    return BuildFilterFromExpressionInternal(binaryExpression.Right, filterBuilder);

                case ExpressionType.Equal:
                    // x => x.Property == value
                    return HandleComparisonExpression(binaryExpression, filterBuilder, FilterType.Equals);

                case ExpressionType.NotEqual:
                    // x => x.Property != value
                    return HandleComparisonExpression(binaryExpression, filterBuilder, FilterType.NotEquals);

                case ExpressionType.GreaterThan:
                    // x => x.Property > value
                    return HandleComparisonExpression(binaryExpression, filterBuilder, FilterType.GreaterThan);

                case ExpressionType.GreaterThanOrEqual:
                    // x => x.Property >= value
                    return HandleComparisonExpression(binaryExpression, filterBuilder, FilterType.GreaterThanOrEqualTo);

                case ExpressionType.LessThan:
                    // x => x.Property < value
                    return HandleComparisonExpression(binaryExpression, filterBuilder, FilterType.LessThan);

                case ExpressionType.LessThanOrEqual:
                    // x => x.Property <= value
                    return HandleComparisonExpression(binaryExpression, filterBuilder, FilterType.LessThanOrEqualTo);
            }
        }

        // Handle method calls (e.g., string.Contains, StartsWith, etc.)
        else if (expression is MethodCallExpression methodCallExpression)
        {
            var methodName = methodCallExpression.Method.Name;
            switch (methodName)
            {
                case "Contains" when methodCallExpression.Object is not null && methodCallExpression.Arguments.Count == 1:
                    // x => x.Property.Contains(value)
                    if (methodCallExpression.Object is MemberExpression memberExpressionContains)
                    {
                        var propertyLambda =
                            Expression.Lambda<Func<T, string>>(memberExpressionContains, Expression.Parameter(typeof(T), "x"));
                        var value = GetConstantValue(methodCallExpression.Arguments[0]);
                        return filterBuilder.Filter(FilterType.Contains, propertyLambda, value);
                    }

                    break;

                case "StartsWith" when methodCallExpression.Object is not null && methodCallExpression.Arguments.Count == 1:
                    // x => x.Property.StartsWith(value)
                    if (methodCallExpression.Object is MemberExpression memberExpressionStartsWith)
                    {
                        var propertyLambda =
                            Expression.Lambda<Func<T, string>>(memberExpressionStartsWith, Expression.Parameter(typeof(T), "x"));
                        var value = GetConstantValue(methodCallExpression.Arguments[0]);
                        return filterBuilder.Filter(FilterType.StartsWith, propertyLambda, value);
                    }

                    break;

                case "EndsWith" when methodCallExpression.Object is not null && methodCallExpression.Arguments.Count == 1:
                    // x => x.Property.EndsWith(value)
                    if (methodCallExpression.Object is MemberExpression memberExpression)
                    {
                        var propertyLambda =
                            Expression.Lambda<Func<T, string>>(memberExpression, Expression.Parameter(typeof(T), "x"));
                        var value = GetConstantValue(methodCallExpression.Arguments[0]);
                        return filterBuilder.Filter(FilterType.EndsWith, propertyLambda, value);
                    }

                    break;
            }
        }

        // Handle member access (e.g., x => x.IsActive where IsActive is a bool)
        else if (expression is MemberExpression memberExpression && memberExpression.Type == typeof(bool))
        {
            // x => x.BoolProperty (implicit == true)
            var propertyLambda =
                Expression.Lambda<Func<T, bool>>(memberExpression, Expression.Parameter(typeof(T), "x"));
            return filterBuilder.Filter(FilterType.Equals, propertyLambda, true);
        }

        // Handle unary expressions (e.g., x => !x.IsActive)
        else if (expression is UnaryExpression unaryExpression &&
                 unaryExpression.NodeType == ExpressionType.Not &&
                 unaryExpression.Operand is MemberExpression notMemberExpression &&
                 notMemberExpression.Type == typeof(bool))
        {
            // x => !x.BoolProperty (implicit == false)
            var propertyLambda =
                Expression.Lambda<Func<T, bool>>(notMemberExpression, Expression.Parameter(typeof(T), "x"));
            return filterBuilder.Filter(FilterType.Equals, propertyLambda, false);
        }

        throw new NotSupportedException($"The expression type {expression.NodeType} is not supported.");
    }

    private FilterBuilder<T> HandleComparisonExpression(
        BinaryExpression binaryExpression,
        FilterBuilder<T> filterBuilder, FilterType filterType)
    {
        // Determine which side is the property and which side is the constant value
        MemberExpression memberExpression;
        Expression valueExpression;

        if (IsPropertyExpression(binaryExpression.Left) && IsConstantOrValueExpression(binaryExpression.Right))
        {
            memberExpression = GetMemberExpression(binaryExpression.Left);
            valueExpression = binaryExpression.Right;
        }
        else if (IsPropertyExpression(binaryExpression.Right) && IsConstantOrValueExpression(binaryExpression.Left))
        {
            memberExpression = GetMemberExpression(binaryExpression.Right);
            valueExpression = binaryExpression.Left;

            // For reversed comparisons, we need to swap the operator
            switch (filterType)
            {
                case FilterType.GreaterThan: filterType = FilterType.LessThan; break;
                case FilterType.GreaterThanOrEqualTo: filterType = FilterType.LessThanOrEqualTo; break;
                case FilterType.LessThan: filterType = FilterType.GreaterThan; break;
                case FilterType.LessThanOrEqualTo: filterType = FilterType.GreaterThanOrEqualTo; break;
            }
        }
        else
        {
            throw new NotSupportedException("The comparison must be between a property and a constant value.");
        }

        // Build lambda for the property access
        var parameter = Expression.Parameter(typeof(T), "x");
        var propertyLambda = Expression.Lambda(memberExpression, parameter);

        // Get the constant value
        var value = GetConstantValue(valueExpression);

        // Create method with generic type parameters
        var methodInfo = GetType().GetMethod(
            nameof(CreateFilterWithType),
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var genericMethod = methodInfo.MakeGenericMethod(memberExpression.Type);

        // Invoke method
        return (FilterBuilder<T>)genericMethod.Invoke(
            this,
            new object[] { filterBuilder, filterType, propertyLambda, value })!;
    }

    private FilterBuilder<T> CreateFilterWithType<TProp>(
        FilterBuilder<T> filterBuilder,
        FilterType filterType,
        Expression<Func<T, TProp>> propertyLambda, object value)
    {
        return filterBuilder.Filter(filterType, propertyLambda, value);
    }

    private bool IsPropertyExpression(Expression expression)
    {
        return expression is MemberExpression ||
               (expression is UnaryExpression unary && unary.Operand is MemberExpression);
    }

    private bool IsConstantOrValueExpression(Expression expression)
    {
        return expression is ConstantExpression ||
               expression is MemberExpression ||
               expression is UnaryExpression ||
               expression is MethodCallExpression;
    }

    private MemberExpression? GetMemberExpression(Expression expression)
    {
        if (expression is MemberExpression memberExpression)
        {
            return memberExpression;
        }

        if (expression is UnaryExpression unaryExpression)
        {
            return unaryExpression.Operand as MemberExpression;
        }

        throw new ArgumentException("Expression is not a member expression");
    }

    private object? GetConstantValue(Expression expression)
    {
        if (expression is ConstantExpression constantExpression)
        {
            return constantExpression.Value;
        }

        // Compile and evaluate the expression to get its value
        var lambda = Expression.Lambda(expression);
        var compiled = lambda.Compile();
        return compiled.DynamicInvoke();
    }
}
