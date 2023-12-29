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

        _sortInfos.Add(new SortInfo(sortDirection, propertyPathString));

        return this;
    }

    public SortBuilder<TObj> OrderBy(SortDirection sortDirection, string propertyPath)
    {
        _sortInfos.Add(new SortInfo(sortDirection, propertyPath));

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
