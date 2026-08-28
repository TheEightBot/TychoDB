using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace TychoDB;

public class SortBuilder<TObj>
{
    private const string OrderByPrefix = "\nORDER BY\n";

    // Must match FilterBuilder's JSON_EXTRACT form exactly. SQLite matches
    // expression indexes by structural comparison, so the `Data ->> '$.x'`
    // operator form used previously could never be satisfied by an index built
    // over JSON_EXTRACT(Data, '$.x') — every sort fell back to a temporary
    // b-tree. See docs/indexing-analysis.md.
    private const string DataPrefix = "JSON_EXTRACT(Data, '";
    private const string DataSuffix = "') ";

    // Numeric properties are indexed as CAST(JSON_EXTRACT(...) as NUMERIC) because
    // that is the form numeric filters use; ordering must use the same expression
    // to be satisfied by that index. JSON numbers extract as numbers, so the CAST
    // does not change the resulting order.
    private const string CastNumericPrefix = "CAST(JSON_EXTRACT(Data, '";
    private const string CastNumericSuffix = "') as NUMERIC) ";
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
        // Captured as segments and rendered in Build, where the serializer — and therefore the
        // JSON member names actually written to the document — is known.
        var propertyPathSegments = QueryPropertyPath.BuildSegments(propertyPath);

        _sortInfos.Add(new SortInfo(sortDirection, propertyPathSegments, QueryPropertyPath.IsNumeric(propertyPath)));

        return this;
    }

    public SortBuilder<TObj> OrderBy(SortDirection sortDirection, string propertyPath)
        => OrderBy(sortDirection, propertyPath, isPropertyPathNumeric: false);

    /// <summary>
    /// Orders by a raw JSON property path.
    /// </summary>
    /// <param name="sortDirection">The direction to sort.</param>
    /// <param name="propertyPath">The JSON path to order by.</param>
    /// <param name="isPropertyPathNumeric">
    /// Whether the property is numeric. Numeric properties are indexed as
    /// <c>CAST(JSON_EXTRACT(…) as NUMERIC)</c>, so this must be true for the ordering
    /// to be satisfied by such an index instead of falling back to a temporary
    /// b-tree. The expression-based overload determines this automatically.
    /// </param>
    /// <returns>The current builder for chaining.</returns>
    public SortBuilder<TObj> OrderBy(SortDirection sortDirection, string propertyPath, bool isPropertyPathNumeric)
    {
        // Raw path from the caller is emitted as an identifier inside
        // JSON_EXTRACT(Data, '...') and cannot be parameterized, so validate it
        // against the strict grammar.
        QueryPropertyPath.ValidatePath(propertyPath, nameof(propertyPath));

        _sortInfos.Add(new SortInfo(sortDirection, propertyPath, isPropertyPathNumeric));

        return this;
    }

    internal void Build(StringBuilder commandBuilder, IJsonSerializer jsonSerializer)
    {
        var nameResolver = QueryPropertyPath.AsNameResolver(jsonSerializer);

        commandBuilder.Append(OrderByPrefix);

        for (var i = 0; i < _sortInfos.Count; i++)
        {
            if (i > 0)
            {
                commandBuilder.Append(Separator);
            }

            var sortInfo = _sortInfos[i];
            var propertyPath =
                sortInfo.PropertyPathSegments is null
                    ? sortInfo.PropertyPath
                    : QueryPropertyPath.RenderPath(sortInfo.PropertyPathSegments, nameResolver);

            // Must keep upstream's JSON_EXTRACT / CAST-as-NUMERIC forms: SQLite matches
            // expression indexes structurally, so the ORDER BY expression has to be spelled
            // exactly as the index was built.
            commandBuilder.Append(sortInfo.IsNumeric ? CastNumericPrefix : DataPrefix)
                          .Append(propertyPath)
                          .Append(sortInfo.IsNumeric ? CastNumericSuffix : DataSuffix)
                          .Append(sortInfo.SortDirection == SortDirection.Ascending ? Asc : Desc);
        }

        commandBuilder.AppendLine();
    }
}
