using System;
using System.Linq.Expressions;

namespace Tycho;

public enum SortDirection
{
    Ascending = 0,
    Descending = 1,
}

internal class SortInfo
{
    public SortDirection SortDirection { get; private set; }

    public string PropertyPath { get; private set; }

    public bool IsPropertyPathNumeric { get; private set; }

    public bool IsPropertyPathBool { get; private set; }

    public bool IsPropertyPathDateTime { get; private set; }

    public SortInfo(SortDirection sortDirection, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime)
    {
        SortDirection = sortDirection;
        PropertyPath = propertyPath;

        IsPropertyPathNumeric = isPropertyPathNumeric;
        IsPropertyPathBool = isPropertyPathBool;
        IsPropertyPathDateTime = isPropertyPathDateTime;
    }
}
