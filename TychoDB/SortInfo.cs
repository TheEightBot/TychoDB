using System;
using System.Linq.Expressions;

namespace TychoDB;

public enum SortDirection
{
    Ascending = 0,
    Descending = 1,
}

/// <summary>
/// Represents sorting information for a query.
/// This is a readonly struct to minimize heap allocations during query building.
/// </summary>
internal readonly struct SortInfo
{
    /// <summary>
    /// Gets the sort direction (ascending or descending).
    /// </summary>
    public SortDirection SortDirection { get; }

    /// <summary>
    /// Gets the literal JSON property path to sort by, or <see langword="null"/> when the path
    /// is carried as segments and rendered at build time.
    /// </summary>
    public string? PropertyPath { get; }

    /// <summary>
    /// Gets the unresolved path segments for an expression-supplied sort, or
    /// <see langword="null"/> when the caller supplied a literal path string.
    /// </summary>
    public PropertyPathSegment[]? PropertyPathSegments { get; }

    /// <summary>
    /// Gets a value indicating whether the property is numeric, and so must be
    /// ordered by the same CAST(... as NUMERIC) expression its index is built on.
    /// </summary>
    public bool IsNumeric { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SortInfo"/> struct.
    /// </summary>
    /// <param name="sortDirection">The direction to sort.</param>
    /// <param name="propertyPath">The JSON property path to sort by.</param>
    /// <param name="isNumeric">Whether the property is numeric.</param>
    public SortInfo(SortDirection sortDirection, string propertyPath, bool isNumeric = false)
    {
        SortDirection = sortDirection;
        PropertyPath = propertyPath;
        PropertyPathSegments = null;
        IsNumeric = isNumeric;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SortInfo"/> struct whose path is resolved at
    /// build time, once the serializer's JSON member names are known.
    /// </summary>
    /// <param name="sortDirection">The direction to sort.</param>
    /// <param name="propertyPathSegments">The unresolved path segments to sort by.</param>
    /// <param name="isNumeric">Whether the property is numeric.</param>
    public SortInfo(SortDirection sortDirection, PropertyPathSegment[] propertyPathSegments, bool isNumeric = false)
    {
        SortDirection = sortDirection;
        PropertyPath = null;
        PropertyPathSegments = propertyPathSegments;
        IsNumeric = isNumeric;
    }
}
