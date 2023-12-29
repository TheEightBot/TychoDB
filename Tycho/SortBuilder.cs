using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Tycho;

public class SortBuilder<TObj>
{
    private readonly List<SortInfo> _sortInfos = new();

    private SortBuilder()
    {
    }

    public static SortBuilder<TObj> Create()
    {
        return new SortBuilder<TObj>();
    }

    public SortBuilder<TObj> OrderBy<TProp>(SortDirection sortDirection, Expression<Func<TObj, TProp>> propertyPath)
    {
        var propertyPathString = QueryPropertyPath.BuildPath(propertyPath);
        var isPropertyPathNumeric = QueryPropertyPath.IsNumeric(propertyPath);
        var isPropertyPathBool = QueryPropertyPath.IsBool(propertyPath);
        var isPropertyPathDateTime = QueryPropertyPath.IsDateTime(propertyPath);

        _sortInfos.Add(new SortInfo(sortDirection, propertyPathString, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime));

        return this;
    }

    public SortBuilder<TObj> OrderBy(SortDirection sortDirection, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime)
    {
        _sortInfos.Add(new SortInfo(sortDirection, propertyPath, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime));

        return this;
    }

    internal void Build(StringBuilder commandBuilder)
    {
        commandBuilder
            .AppendLine("\nORDER BY")
            .AppendJoin(
                $", ",
                _sortInfos.Select(x => $"Data ->> \'{x.PropertyPath}\' {GetSortDirectionSqlCommand(x.SortDirection)}"))
            .AppendLine();
    }

    private string GetSortDirectionSqlCommand(SortDirection sortDirection)
        => sortDirection == SortDirection.Ascending ? "ASC" : "DESC";
}
