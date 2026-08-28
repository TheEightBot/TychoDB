using System;
using System.Linq.Expressions;

namespace TychoDB;

public enum FilterJoin
{
    And = 0,
    Or = 1,
    StartGroup = 2,
    EndGroup = 3,
}

public enum FilterType
{
    Equals = 0,
    NotEquals = 1,
    StartsWith = 2,
    EndsWith = 3,
    Contains = 4,
    GreaterThan = 5,
    GreaterThanOrEqualTo = 6,
    LessThan = 7,
    LessThanOrEqualTo = 8,
}

/// <summary>
/// Represents a filter condition for querying data.
/// This is a readonly struct to minimize heap allocations during query building.
/// </summary>
internal readonly struct Filter
{
    public FilterJoin? Join { get; }

    public FilterType? FilterType { get; }

    public string? PropertyPath { get; }

    /// <summary>
    /// Gets the unresolved segments for an expression-supplied path, or <see langword="null"/>
    /// when the caller supplied a literal path string. Expression paths are rendered at build
    /// time, once the serializer is known, so the JSON member names match the stored document.
    /// </summary>
    public PropertyPathSegment[]? PropertyPathSegments { get; }

    /// <summary>
    /// Gets the unresolved segments for the value path of a list filter, or
    /// <see langword="null"/> when it came from a literal path string.
    /// </summary>
    public PropertyPathSegment[]? PropertyValuePathSegments { get; }

    public bool IsPropertyPathNumeric { get; }

    public bool IsPropertyPathBool { get; }

    public bool IsPropertyPathDateTime { get; }

    public string? PropertyValuePath { get; }

    public bool IsPropertyValuePathNumeric { get; }

    public bool IsPropertyValuePathBool { get; }

    public bool IsPropertyValuePathDateTime { get; }

    public object? Value { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="Filter"/> struct for a simple property comparison.
    /// </summary>
    /// <param name="filterType">The type of filter comparison.</param>
    /// <param name="propertyPath">The literal JSON path, or <see langword="null"/> when <paramref name="propertyPathSegments"/> is supplied.</param>
    /// <param name="propertyPathSegments">Unresolved path segments rendered at build time, or <see langword="null"/> for a literal path.</param>
    /// <param name="isPropertyPathNumeric">Whether the property is numeric.</param>
    /// <param name="isPropertyPathBool">Whether the property is boolean.</param>
    /// <param name="isPropertyPathDateTime">Whether the property is a date/time.</param>
    /// <param name="value">The value to compare against.</param>
    public Filter(FilterType filterType, string? propertyPath, PropertyPathSegment[]? propertyPathSegments, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime, object? value)
    {
        FilterType = filterType;
        PropertyPath = propertyPath;
        PropertyPathSegments = propertyPathSegments;
        IsPropertyPathNumeric = isPropertyPathNumeric;
        IsPropertyPathBool = isPropertyPathBool;
        IsPropertyPathDateTime = isPropertyPathDateTime;
        Value = value;

        // Initialize remaining fields
        Join = null;
        PropertyValuePath = null;
        PropertyValuePathSegments = null;
        IsPropertyValuePathNumeric = false;
        IsPropertyValuePathBool = false;
        IsPropertyValuePathDateTime = false;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Filter"/> struct for a nested/list property comparison.
    /// </summary>
    /// <param name="filterType">The type of filter comparison.</param>
    /// <param name="listPropertyPath">The literal JSON path to the list property, or <see langword="null"/> when segments are supplied.</param>
    /// <param name="listPropertyPathSegments">Unresolved segments for the list property, or <see langword="null"/> for a literal path.</param>
    /// <param name="propertyValuePath">The literal JSON path within the list items, or <see langword="null"/> when segments are supplied.</param>
    /// <param name="propertyValuePathSegments">Unresolved segments for the value path, or <see langword="null"/> for a literal path.</param>
    /// <param name="isPropertyValuePathNumeric">Whether the value path is numeric.</param>
    /// <param name="isPropertyValuePathBool">Whether the value path is boolean.</param>
    /// <param name="isPropertyValuePathDateTime">Whether the value path is a date/time.</param>
    /// <param name="value">The value to compare against.</param>
    public Filter(FilterType filterType, string? listPropertyPath, PropertyPathSegment[]? listPropertyPathSegments, string? propertyValuePath, PropertyPathSegment[]? propertyValuePathSegments, bool isPropertyValuePathNumeric, bool isPropertyValuePathBool, bool isPropertyValuePathDateTime, object? value)
    {
        FilterType = filterType;
        PropertyPath = listPropertyPath;
        PropertyPathSegments = listPropertyPathSegments;
        PropertyValuePath = propertyValuePath;
        PropertyValuePathSegments = propertyValuePathSegments;
        IsPropertyValuePathNumeric = isPropertyValuePathNumeric;
        IsPropertyValuePathBool = isPropertyValuePathBool;
        IsPropertyValuePathDateTime = isPropertyValuePathDateTime;
        Value = value;

        // Initialize remaining fields
        Join = null;
        IsPropertyPathNumeric = false;
        IsPropertyPathBool = false;
        IsPropertyPathDateTime = false;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="Filter"/> struct for a join filter (AND, OR, StartGroup, EndGroup).
    /// </summary>
    /// <param name="join">The type of join operation.</param>
    public Filter(FilterJoin join)
    {
        Join = join;

        // Initialize remaining fields
        FilterType = null;
        PropertyPath = null;
        PropertyPathSegments = null;
        PropertyValuePathSegments = null;
        IsPropertyPathNumeric = false;
        IsPropertyPathBool = false;
        IsPropertyPathDateTime = false;
        PropertyValuePath = null;
        IsPropertyValuePathNumeric = false;
        IsPropertyValuePathBool = false;
        IsPropertyValuePathDateTime = false;
        Value = null;
    }
}
