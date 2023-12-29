using System;
using System.Linq.Expressions;

namespace TychoDB;

public enum SortDirection
{
    Ascending = 0,
    Descending = 1,
}

internal class SortInfo
{
    public SortDirection SortDirection { get; private set; }

    public string PropertyPath { get; private set; }

    public SortInfo(SortDirection sortDirection, string propertyPath)
    {
        SortDirection = sortDirection;
        PropertyPath = propertyPath;
    }
}
