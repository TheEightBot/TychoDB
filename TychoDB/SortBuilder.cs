using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace TychoDB;

public class SortBuilder<TObj>
{
    private const string OrderByPrefix = "\nORDER BY\n";
    private const string DataPrefix = "Data ->> '";
    private const string DataSuffix = "' ";
    private const string Asc = "ASC";
    private const string Desc = "DESC";
    private const string Separator = ", ";

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
        commandBuilder.Append(OrderByPrefix);

        for (var i = 0; i < _sortInfos.Count; i++)
        {
            if (i > 0)
            {
                commandBuilder.Append(Separator);
            }

            var sortInfo = _sortInfos[i];
            commandBuilder.Append(DataPrefix)
                          .Append(sortInfo.PropertyPath)
                          .Append(DataSuffix)
                          .Append(sortInfo.SortDirection == SortDirection.Ascending ? Asc : Desc);
        }

        commandBuilder.AppendLine();
    }
}
