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

    public static string BuildPath<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        // Rent a pooled array for typical short paths
        string[] segments = ArrayPool<string>.Shared.Rent(MaxPooledSegments);
        int segmentCount = 0;

        try
        {
            // Walk the expression tree to collect property names
            var current = expression.Body;

            while (current is MemberExpression memberExpr)
            {
                if (memberExpr.Member is not PropertyInfo)
                {
                    throw new ArgumentException("The path can only contain properties", nameof(expression));
                }

                if (segmentCount >= MaxPooledSegments)
                {
                    // Fall back to heap allocation for very deep paths
                    ArrayPool<string>.Shared.Return(segments);
                    return BuildPathFallback(expression);
                }

                segments[segmentCount++] = memberExpr.Member.Name;
                current = memberExpr.Expression;
            }

            if (segmentCount == 0)
            {
                return "$";
            }

            // Build the path string - segments are in reverse order
            return BuildPathString(segments, segmentCount);
        }
        finally
        {
            ArrayPool<string>.Shared.Return(segments, clearArray: false);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string BuildPathString(string[] segments, int count)
    {
        // Calculate total length needed
        int totalLength = 2; // "$."
        for (int i = 0; i < count; i++)
        {
            totalLength += segments[i].Length;
            if (i < count - 1)
            {
                totalLength++; // "."
            }
        }

        // Build the string using string.Create for zero-allocation string building
        return string.Create(totalLength, (segments, count), static (span, state) =>
        {
            var (segs, cnt) = state;
            span[0] = '$';
            span[1] = '.';
            int pos = 2;

            // Segments are in reverse order, so iterate backwards
            for (int i = cnt - 1; i >= 0; i--)
            {
                segs[i].AsSpan().CopyTo(span[pos..]);
                pos += segs[i].Length;
                if (i > 0)
                {
                    span[pos++] = '.';
                }
            }
        });
    }

    /// <summary>
    /// Fallback for paths deeper than MaxPooledSegments.
    /// </summary>
    private static string BuildPathFallback<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        var visitor = new PropertyPathVisitor();
        visitor.Visit(expression.Body);
        return $"$.{string.Join('.', visitor.PathBuilder)}";
    }

    public static bool IsNumeric<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        if (expression.Body is MemberExpression memEx && memEx.Member is PropertyInfo propInfo)
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
        if (expression.Body is MemberExpression memEx && memEx.Member is PropertyInfo propInfo)
        {
            var propertyType = propInfo.PropertyType;

            return propertyType == typeof(bool) || Nullable.GetUnderlyingType(propertyType) == typeof(bool);
        }

        return false;
    }

    public static bool IsDateTime<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
    {
        if (expression.Body is MemberExpression memEx && memEx.Member is PropertyInfo propInfo)
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
        internal readonly List<string> PathBuilder = new();

        protected override Expression VisitMember(MemberExpression node)
        {
            if (!(node.Member is PropertyInfo))
            {
                throw new ArgumentException("The path can only contain properties", nameof(node));
            }

            PathBuilder.Insert(0, node.Member.Name);

            return base.VisitMember(node);
        }
    }
}
