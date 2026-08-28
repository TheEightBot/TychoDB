using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

namespace TychoDB;

internal static class QueryPropertyPath
{
    /// <summary>
    /// Maximum number of path segments we can handle with array pooling.
    /// Most property paths are 1-4 segments deep.
    /// </summary>
    private const int MaxPooledSegments = 8;

    /// <summary>
    /// Strips the boxing conversion the compiler inserts when a value-type
    /// property is used in an expression typed as <c>Func&lt;T, object&gt;</c>
    /// (as the CreateIndex overloads are). Without this the body is a
    /// <see cref="UnaryExpression"/> rather than a <see cref="MemberExpression"/>,
    /// so the path walk collects nothing and falls back to "$" — which indexes
    /// the entire document instead of the property.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Expression UnwrapConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    /// <summary>
    /// Resolves the property a path expression ultimately reads, ignoring any
    /// boxing conversion. Returns null when the expression is not a property access.
    /// </summary>
    private static PropertyInfo? GetLeafProperty(Expression body)
        => UnwrapConvert(body) is MemberExpression { Member: PropertyInfo propInfo } ? propInfo : null;

    public static string BuildPath<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        return BuildPath(expression, resolver: null);
    }

    /// <summary>
    /// Builds a JSON path for <paramref name="expression"/>, mapping each CLR property name
    /// through <paramref name="resolver"/> so the path matches how the document was actually
    /// serialized. Passing <see langword="null"/> uses the CLR property names verbatim.
    /// </summary>
    public static string BuildPath<TPathObj, TProp>(
        Expression<Func<TPathObj, TProp>> expression,
        IJsonPropertyNameResolver? resolver)
    {
        return RenderPath(BuildSegments(expression), resolver);
    }

    /// <summary>
    /// Walks the expression tree into root-to-leaf segments, keeping the declaring type of each
    /// property so the JSON member name can be resolved later — filters and sorts are built
    /// before the serializer is known, so the path cannot be rendered at this point.
    /// </summary>
    public static PropertyPathSegment[] BuildSegments<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        // Rent a pooled array for typical short paths
        var segments = ArrayPool<PropertyPathSegment>.Shared.Rent(MaxPooledSegments);
        int segmentCount = 0;
        bool pooled = true;

        try
        {
            // Walk the expression tree to collect property names
            var current = UnwrapConvert(expression.Body);

            while (current is MemberExpression memberExpr)
            {
                if (memberExpr.Member is not PropertyInfo propertyInfo)
                {
                    throw new ArgumentException("The path can only contain properties", nameof(expression));
                }

                if (segmentCount >= MaxPooledSegments)
                {
                    // Fall back to heap allocation for very deep paths
                    ArrayPool<PropertyPathSegment>.Shared.Return(segments, clearArray: true);
                    pooled = false;
                    return BuildSegmentsFallback(expression);
                }

                segments[segmentCount++] =
                    new PropertyPathSegment(propertyInfo.DeclaringType ?? typeof(TPathObj), propertyInfo.Name);

                current = memberExpr.Expression is { } inner ? UnwrapConvert(inner) : null!;
            }

            if (segmentCount == 0)
            {
                return Array.Empty<PropertyPathSegment>();
            }

            // Segments were collected leaf-to-root; hand them back root-to-leaf.
            var result = new PropertyPathSegment[segmentCount];
            for (int i = 0; i < segmentCount; i++)
            {
                result[i] = segments[segmentCount - 1 - i];
            }

            return result;
        }
        finally
        {
            if (pooled)
            {
                ArrayPool<PropertyPathSegment>.Shared.Return(segments, clearArray: true);
            }
        }
    }

    /// <summary>
    /// Renders segments into a SQLite JSON path, resolving each CLR property name to the JSON
    /// member name the serializer writes. Falls back to the CLR name when the serializer cannot
    /// resolve it (or does not implement <see cref="IJsonPropertyNameResolver"/>), which keeps
    /// the previous behaviour for serializers that do not rename members.
    /// </summary>
    public static string RenderPath(PropertyPathSegment[] segments, IJsonPropertyNameResolver? resolver)
    {
        if (segments is null || segments.Length == 0)
        {
            return "$";
        }

        if (segments.Length == 1)
        {
            return string.Concat("$.", ResolveName(segments[0], resolver));
        }

        var names = new string[segments.Length];
        int totalLength = 2; // "$."
        for (int i = 0; i < segments.Length; i++)
        {
            names[i] = ResolveName(segments[i], resolver);
            totalLength += names[i].Length;
            if (i < segments.Length - 1)
            {
                totalLength++; // "."
            }
        }

        return string.Create(totalLength, names, static (span, state) =>
        {
            span[0] = '$';
            span[1] = '.';
            int pos = 2;

            for (int i = 0; i < state.Length; i++)
            {
                state[i].AsSpan().CopyTo(span[pos..]);
                pos += state[i].Length;
                if (i < state.Length - 1)
                {
                    span[pos++] = '.';
                }
            }
        });
    }

    /// <summary>
    /// Resolves the JSON member name for a single CLR property, used by call sites that already
    /// hold the serializer and do not need to defer.
    /// </summary>
    internal static IJsonPropertyNameResolver? AsNameResolver(IJsonSerializer? jsonSerializer)
        => jsonSerializer as IJsonPropertyNameResolver;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string ResolveName(in PropertyPathSegment segment, IJsonPropertyNameResolver? resolver)
    {
        if (resolver is null)
        {
            return segment.ClrName;
        }

        var resolved = resolver.ResolvePropertyName(segment.DeclaringType, segment.ClrName);

        // A resolver returning null means "no opinion" (unknown type, non-serialized member);
        // the CLR name is the same answer the path builder gave before resolution existed.
        if (string.IsNullOrEmpty(resolved))
        {
            return segment.ClrName;
        }

        ValidateResolvedName(resolved!, segment);
        return resolved!;
    }

    /// <summary>
    /// Guards the injection surface opened by resolving names through the serializer. CLR
    /// property names are constrained to identifier characters, but a resolved name comes from
    /// a naming policy or a <c>[JsonPropertyName]</c>/<c>[JsonProperty]</c> attribute and is an
    /// arbitrary string. It is emitted inside the single-quoted SQL literal that carries the
    /// JSON path, so a quote would terminate that literal and '.', '[' and ']' would silently
    /// change which member the path selects.
    /// </summary>
    private static void ValidateResolvedName(string resolvedName, in PropertyPathSegment segment)
    {
        foreach (var c in resolvedName)
        {
            // '-' is permitted because kebab-case is a first-class naming policy and SQLite
            // resolves '$.my-name' without quoting.
            if (!(char.IsLetterOrDigit(c) || c is '_' or '-'))
            {
                throw new ArgumentException(
                    $"The serializer maps '{segment.DeclaringType?.Name}.{segment.ClrName}' to JSON member name " +
                    $"'{resolvedName}', which contains the character '{c}' and cannot be used in a JSON path. " +
                    "Rename the JSON member, or query this property with the raw-string path overload.",
                    nameof(resolvedName));
            }
        }
    }

    /// <summary>
    /// Fallback for paths deeper than MaxPooledSegments.
    /// </summary>
    private static PropertyPathSegment[] BuildSegmentsFallback<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        var visitor = new PropertyPathVisitor();
        visitor.Visit(expression.Body);
        return visitor.PathBuilder.ToArray();
    }

    /// <summary>
    /// Validates a caller-supplied JSON property path. Paths are emitted into the
    /// SQL text as identifiers inside JSON_EXTRACT/JSON_TREE and cannot be bound as
    /// parameters, so they are restricted to a strict grammar (letters, digits,
    /// '_', '.', '$', '[' and ']') to prevent SQL injection through the path
    /// position. Paths produced from expression trees always satisfy this.
    /// </summary>
    public static void ValidatePath(string path, string paramName)
    {
        if (string.IsNullOrEmpty(path))
        {
            throw new ArgumentException("Property path must not be null or empty.", paramName);
        }

        foreach (var c in path)
        {
            if (!(char.IsLetterOrDigit(c) || c is '$' or '.' or '_' or '[' or ']'))
            {
                throw new ArgumentException(
                    $"Property path contains an invalid character '{c}'. Only letters, digits, '_', '.', '$', '[' and ']' are permitted.",
                    paramName);
            }
        }
    }

    /// <summary>
    /// Validates a caller-supplied SQL identifier (e.g. an index name). Identifiers
    /// are concatenated into DDL and cannot be parameterized, so they are limited
    /// to letters, digits and '_' and must not start with a digit.
    /// </summary>
    public static void ValidateIdentifier(string identifier, string paramName)
    {
        if (string.IsNullOrEmpty(identifier))
        {
            throw new ArgumentException("Identifier must not be null or empty.", paramName);
        }

        for (int i = 0; i < identifier.Length; i++)
        {
            char c = identifier[i];
            bool valid = char.IsLetterOrDigit(c) || c == '_';
            if (!valid || (i == 0 && char.IsDigit(c)))
            {
                throw new ArgumentException(
                    $"Identifier '{identifier}' is invalid. Use only letters, digits and '_', and do not start with a digit.",
                    paramName);
            }
        }
    }

    public static bool IsNumeric<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        if (GetLeafProperty(expression.Body) is { } propInfo)
        {
            var propertyType = propInfo.PropertyType;

            return
                propertyType == typeof(int) || Nullable.GetUnderlyingType(propertyType) == typeof(int) ||
                propertyType == typeof(uint) || Nullable.GetUnderlyingType(propertyType) == typeof(uint) ||
                propertyType == typeof(long) || Nullable.GetUnderlyingType(propertyType) == typeof(long) ||
                propertyType == typeof(ulong) || Nullable.GetUnderlyingType(propertyType) == typeof(ulong) ||
                propertyType == typeof(double) || Nullable.GetUnderlyingType(propertyType) == typeof(double) ||
                propertyType == typeof(float) || Nullable.GetUnderlyingType(propertyType) == typeof(float) ||
                propertyType == typeof(decimal) || Nullable.GetUnderlyingType(propertyType) == typeof(decimal);
        }

        return false;
    }

    public static bool IsBool<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        if (GetLeafProperty(expression.Body) is { } propInfo)
        {
            var propertyType = propInfo.PropertyType;

            return propertyType == typeof(bool) || Nullable.GetUnderlyingType(propertyType) == typeof(bool);
        }

        return false;
    }

    public static bool IsDateTime<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        if (GetLeafProperty(expression.Body) is { } propInfo)
        {
            var propertyType = propInfo.PropertyType;

            return
                propertyType == typeof(DateTime) || Nullable.GetUnderlyingType(propertyType) == typeof(DateTime) ||
                propertyType == typeof(DateTimeOffset) || Nullable.GetUnderlyingType(propertyType) == typeof(DateTimeOffset);
        }

        return false;
    }

    private class PropertyPathVisitor : ExpressionVisitor
    {
        internal readonly List<PropertyPathSegment> PathBuilder = new();

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is not PropertyInfo propertyInfo)
            {
                throw new ArgumentException("The path can only contain properties", nameof(node));
            }

            PathBuilder.Insert(0, new PropertyPathSegment(propertyInfo.DeclaringType ?? node.Member.ReflectedType!, propertyInfo.Name));

            return base.VisitMember(node);
        }
    }
}
