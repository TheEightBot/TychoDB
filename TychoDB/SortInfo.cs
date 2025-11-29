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
    /// Gets the JSON property path to sort by.
    /// </summary>
    public string PropertyPath { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SortInfo"/> struct.
    /// </summary>
    /// <param name="sortDirection">The direction to sort.</param>
    /// <param name="propertyPath">The JSON property path to sort by.</param>
    public SortInfo(SortDirection sortDirection, string propertyPath)
    {
        SortDirection = sortDirection;
        PropertyPath = propertyPath;
    }
}
