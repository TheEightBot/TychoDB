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

internal class Filter
{
    public FilterJoin? Join { get; private set; }

    public FilterType? FilterType { get; private set; }

    public string? PropertyPath { get; private set; }

    public bool IsPropertyPathNumeric { get; private set; }

    public bool IsPropertyPathBool { get; private set; }

    public bool IsPropertyPathDateTime { get; private set; }

    public string? PropertyValuePath { get; set; }

    public bool IsPropertyValuePathNumeric { get; private set; }

    public bool IsPropertyValuePathBool { get; private set; }

    public bool IsPropertyValuePathDateTime { get; private set; }

    public object? Value { get; private set; }

    public Filter(FilterType filterType, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime, object? value)
    {
        FilterType = filterType;
        PropertyPath = propertyPath;

        IsPropertyPathNumeric = isPropertyPathNumeric;
        IsPropertyPathBool = isPropertyPathBool;
        IsPropertyPathDateTime = isPropertyPathDateTime;

        Value = value;
    }

    public Filter(FilterType filterType, string listPropertyPath, string? propertyValuePath, bool isPropertyValuePathNumeric, bool isPropertyValuePathBool, bool isPropertyValuePathDateTime, object? value)
    {
        FilterType = filterType;
        PropertyPath = listPropertyPath;
        PropertyValuePath = propertyValuePath;
        IsPropertyValuePathNumeric = isPropertyValuePathNumeric;
        IsPropertyValuePathBool = isPropertyValuePathBool;
        IsPropertyValuePathDateTime = isPropertyValuePathDateTime;
        Value = value;
    }

    public Filter(FilterJoin join)
    {
        Join = join;
    }
}
