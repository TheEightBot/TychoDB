using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;

namespace TychoDB;

public class FilterBuilder<TObj>
{
    // Pre-allocated string constants to avoid repeated allocations
    private const string AndKeyword = "AND";
    private const string OrKeyword = "OR";
    private const string OpenParen = "(";
    private const string CloseParen = ")";
    private const string JsonExtractPrefix = "JSON_EXTRACT(Data, '";
    private const string JsonExtractSuffix = "')";
    private const string CastNumericPrefix = "CAST(JSON_EXTRACT(Data, '";
    private const string CastNumericSuffix = "') as NUMERIC)";
    private const string ExistsPrefix = "EXISTS(SELECT 1 FROM JSON_TREE(Data, '";
    private const string ExistsMiddle = "') AS JT, JSON_EACH(JT.Value, '";
    private const string ExistsSuffix = "') AS VAL WHERE ";
    private const string ExistsEnd = ")";
    private const string ValValue = "VAL.value";
    private const string CastValNumeric = "CAST(VAL.value as NUMERIC)";
    private const string IsNull = " IS NULL";
    private const string IsNotNull = " IS NOT NULL";
    private const string LikeOperator = " like ";
    private const string LikeEscapeClause = " ESCAPE '\\'";
    private const string Equals = " = ";
    private const string NotEquals = " <> ";
    private const string GreaterThan = " > ";
    private const string GreaterThanOrEqual = " >= ";
    private const string LessThan = " < ";
    private const string LessThanOrEqual = " <= ";
    private const string InOperator = " IN (";
    private const string NotInOperator = " NOT IN (";
    private const string ValueSeparator = ", ";
    private const string OrJoin = " OR ";
    private const string AndJoin = " AND ";

    // IN () is a syntax error in SQLite, so an empty set has to be rendered as a
    // constant. Dropping the term instead would widen the result set, which is the
    // dangerous direction: the caller asked for "none of these" and would get "all".
    private const string MatchNothing = "0 = 1";
    private const string MatchEverything = "1 = 1";

    // Values bind as parameters unless they are genuine numerics or booleans (those become literals).
    // SQLite's SQLITE_MAX_VARIABLE_NUMBER limit is statement-wide (total bound variables in the SQL).
    // Chunking a large set across multiple IN (...) terms does not reduce the parameter count for
    // parameterized values (e.g., strings / enums / DateTime), but it keeps each IN list reasonably sized.
    private const int MaxValuesPerInClause = 900;

    private readonly List<Filter> _filters = new();

    private FilterBuilder()
    {
    }

    public static FilterBuilder<TObj> Create()
    {
        return new FilterBuilder<TObj>();
    }

    public FilterBuilder<TObj> Filter<TProp>(FilterType filterType, Expression<Func<TObj, TProp>> propertyPath, object value)
    {
        EnsureScalarFilterType(filterType);

        // The path is captured as segments rather than rendered here: the serializer that
        // decides the JSON member names is not known until Build.
        var propertyPathSegments = QueryPropertyPath.BuildSegments(propertyPath);
        var isPropertyPathNumeric = QueryPropertyPath.IsNumeric(propertyPath);
        var isPropertyPathBool = QueryPropertyPath.IsBool(propertyPath);
        var isPropertyPathDateTime = QueryPropertyPath.IsDateTime(propertyPath);

        _filters.Add(new Filter(filterType, null, propertyPathSegments, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime, value));

        return this;
    }

    public FilterBuilder<TObj> Filter<TItem, TItemProp>(FilterType filterType, Expression<Func<TObj, IEnumerable<TItem>>> propertyPath, Expression<Func<TItem, TItemProp>> propertyValuePath, object value)
    {
        EnsureScalarFilterType(filterType);

        var propertyPathSegments = QueryPropertyPath.BuildSegments(propertyPath);
        var propertyValuePathSegments = QueryPropertyPath.BuildSegments(propertyValuePath);
        var isPropertyValuePathNumeric = QueryPropertyPath.IsNumeric(propertyValuePath);
        var isPropertyValuePathBool = QueryPropertyPath.IsBool(propertyValuePath);
        var isPropertyValuePathDateTime = QueryPropertyPath.IsDateTime(propertyValuePath);

        _filters.Add(new Filter(filterType, null, propertyPathSegments, null, propertyValuePathSegments, isPropertyValuePathNumeric, isPropertyValuePathBool, isPropertyValuePathDateTime, value));

        return this;
    }

    public FilterBuilder<TObj> Filter(FilterType filterType, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime, object value)
    {
        EnsureScalarFilterType(filterType);

        // This overload accepts a raw JSON path string from the caller. Because
        // the path is emitted as an identifier inside JSON_EXTRACT(...) and
        // cannot be parameterized, validate it against a strict grammar to
        // prevent it from being used as an injection vector.
        QueryPropertyPath.ValidatePath(propertyPath, nameof(propertyPath));

        _filters.Add(new Filter(filterType, propertyPath, null, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime, value));

        return this;
    }

    /// <summary>
    /// Adds a set-membership term: <see cref="FilterType.In"/> or <see cref="FilterType.NotIn"/>.
    /// <para>
    /// A single term rather than a chain of <c>Or()</c>s, so it cannot be mis-grouped, and it
    /// renders through the same numeric <c>CAST</c> the scalar comparisons use — which is what
    /// lets an expression index over the property serve the query.
    /// </para>
    /// <para>
    /// Duplicate values are removed. An empty set matches nothing for <see cref="FilterType.In"/>
    /// and everything for <see cref="FilterType.NotIn"/>. If the set contains <see langword="null"/>,
    /// <see cref="FilterType.In"/> adds an <c>IS NULL</c> disjunct; <see cref="FilterType.NotIn"/> excludes
    /// missing/null members (like <see cref="FilterType.NotEquals"/>).
    /// </summary>
    /// <typeparam name="TProp">The property's type.</typeparam>
    /// <param name="filterType">Must be <see cref="FilterType.In"/> or <see cref="FilterType.NotIn"/>.</param>
    /// <param name="propertyPath">An expression selecting the property to test.</param>
    /// <param name="values">The set to test membership against.</param>
    /// <returns>The current builder for chaining.</returns>
    public FilterBuilder<TObj> Filter<TProp>(FilterType filterType, Expression<Func<TObj, TProp>> propertyPath, IEnumerable<TProp>? values)
    {
        // A literal null argument binds here, not to the object overload: IEnumerable<TProp> is
        // the more specific parameter type. Filter(Equals, x => x.Value, null) has always meant
        // "compare against null", so it is routed back to the scalar path rather than being
        // rejected as a malformed set.
        if (values is null)
        {
            return NullValues(filterType, () => Filter(filterType, propertyPath, (object)null!));
        }

        EnsureSetFilterType(filterType);

        var propertyPathSegments = QueryPropertyPath.BuildSegments(propertyPath);
        var isPropertyPathNumeric = QueryPropertyPath.IsNumeric(propertyPath);
        var isPropertyPathBool = QueryPropertyPath.IsBool(propertyPath);
        var isPropertyPathDateTime = QueryPropertyPath.IsDateTime(propertyPath);

        _filters.Add(
            new Filter(
                filterType,
                null,
                propertyPathSegments,
                isPropertyPathNumeric,
                isPropertyPathBool,
                isPropertyPathDateTime,
                Distinct(values)));

        return this;
    }

    /// <summary>
    /// Adds a set-membership term against a raw JSON path. See the expression overload for the
    /// empty-set, duplicate and null semantics.
    /// </summary>
    /// <param name="filterType">Must be <see cref="FilterType.In"/> or <see cref="FilterType.NotIn"/>.</param>
    /// <param name="propertyPath">The JSON path to test.</param>
    /// <param name="isPropertyPathNumeric">Whether the property is numeric.</param>
    /// <param name="isPropertyPathBool">Whether the property is boolean.</param>
    /// <param name="isPropertyPathDateTime">Whether the property is a date/time.</param>
    /// <param name="values">
    /// The set to test membership against. Typed as <see cref="IEnumerable{T}"/> of
    /// <see cref="object"/> rather than a generic parameter on purpose: a generic overload here
    /// would capture an ordinary <see cref="string"/> comparison value, since
    /// <see cref="string"/> is an <see cref="IEnumerable{T}"/> of <see cref="char"/>, and route
    /// it to set membership. A value-type collection such as <c>int[]</c> therefore needs
    /// <c>Cast&lt;object&gt;()</c>; the expression overload infers the element type from the
    /// property and needs no cast.
    /// </param>
    /// <returns>The current builder for chaining.</returns>
    public FilterBuilder<TObj> Filter(FilterType filterType, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime, IEnumerable<object>? values)
    {
        // See the expression overload: a literal null means the scalar null comparison.
        if (values is null)
        {
            return NullValues(
                filterType,
                () => Filter(filterType, propertyPath, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime, (object)null!));
        }

        EnsureSetFilterType(filterType);
        QueryPropertyPath.ValidatePath(propertyPath, nameof(propertyPath));

        _filters.Add(
            new Filter(
                filterType,
                propertyPath,
                null,
                isPropertyPathNumeric,
                isPropertyPathBool,
                isPropertyPathDateTime,
                Distinct(values)));

        return this;
    }

    /// <summary>
    /// Handles a null passed where a value set was expected: a scalar comparison against null
    /// for the ordinary filter types, and an error for In/NotIn, where "no set at all" is not
    /// the same thing as the empty set and is far more likely to be an accident.
    /// </summary>
    private FilterBuilder<TObj> NullValues(FilterType filterType, Func<FilterBuilder<TObj>> asScalarNull)
    {
        if (filterType is FilterType.In or FilterType.NotIn)
        {
            var empty = filterType == FilterType.In ? "nothing" : "everything";
            var message = $"{filterType} needs a collection; pass an empty one to match {empty}.";

            throw new ArgumentNullException("values", message);
        }

        return asScalarNull();
    }

    private static void EnsureSetFilterType(FilterType filterType)
    {
        if (filterType is not (FilterType.In or FilterType.NotIn))
        {
            throw new ArgumentException(
                $"{filterType} compares against a single value; pass that value rather than a collection. Only In and NotIn take a collection.",
                nameof(filterType));
        }
    }

    private static void EnsureScalarFilterType(FilterType filterType)
    {
        if (filterType is FilterType.In or FilterType.NotIn)
        {
            throw new ArgumentException(
                $"{filterType} tests set membership; use the overload that takes an IEnumerable of values. " +
                "For raw JSON paths, the collection overload takes IEnumerable<object>; value-type collections (e.g., int[]) need Cast<object>().",
                nameof(filterType));
        }
    }

    /// <summary>
    /// Materializes the value set once, dropping duplicates while preserving the caller's
    /// order. At most one null survives; the renderer turns it into an IS NULL test.
    /// </summary>
    private static object?[] Distinct<TValue>(IEnumerable<TValue> values)
    {
        var seen = new HashSet<object>();
        var distinct = new List<object?>();
        var sawNull = false;

        foreach (var value in values)
        {
            if (value is null)
            {
                if (!sawNull)
                {
                    sawNull = true;
                    distinct.Add(null);
                }

                continue;
            }

            if (seen.Add(value))
            {
                distinct.Add(value);
            }
        }

        return distinct.ToArray();
    }

    public FilterBuilder<TObj> And()
    {
        _filters.Add(new Filter(FilterJoin.And));
        return this;
    }

    public FilterBuilder<TObj> Or()
    {
        _filters.Add(new Filter(FilterJoin.Or));
        return this;
    }

    public FilterBuilder<TObj> StartGroup()
    {
        _filters.Add(new Filter(FilterJoin.StartGroup));
        return this;
    }

    public FilterBuilder<TObj> EndGroup()
    {
        _filters.Add(new Filter(FilterJoin.EndGroup));
        return this;
    }

    internal void Build(StringBuilder commandBuilder, IJsonSerializer jsonSerializer, FilterParameters parameters)
    {
        if (_filters.Count == 0)
        {
            return;
        }

        // The caller's filter is only the last conjunct of the generated clause: every query
        // this is appended to already reads "WHERE FullTypeName = $fullTypeName AND Partition =
        // $partition". SQL binds AND tighter than OR, so emitting the terms bare would let an
        // ungrouped Or() split the clause —
        //
        //     (FullTypeName = $t AND Partition = $p AND term1) OR (term2)
        //
        // — and every term after the first Or() would then be matched against the whole table,
        // returning (or, through DeleteObjectsAsync, destroying) rows of other partitions and
        // other stored types, which the reader would go on to deserialize as T. The enclosing
        // parentheses bind the caller's terms into a single conjunct.
        commandBuilder.AppendLine("\nAND").AppendLine(OpenParen);

        // Expression-supplied paths were captured as segments; the serializer is only known
        // here, so render them now against the JSON member names it actually writes.
        var nameResolver = QueryPropertyPath.AsNameResolver(jsonSerializer);
        var valueResolver = jsonSerializer as IJsonValueResolver;

        foreach (var unresolvedFilter in _filters)
        {
            var filter = Resolve(unresolvedFilter, nameResolver, valueResolver);

            if (filter.Join.HasValue)
            {
                switch (filter.Join.Value)
                {
                    case FilterJoin.And:
                        commandBuilder.AppendLine(AndKeyword);
                        break;
                    case FilterJoin.Or:
                        commandBuilder.AppendLine(OrKeyword);
                        break;
                    case FilterJoin.StartGroup:
                        commandBuilder.AppendLine(OpenParen);
                        break;
                    case FilterJoin.EndGroup:
                        commandBuilder.AppendLine(CloseParen);
                        break;
                }

                continue;
            }

            if (filter.FilterType.HasValue && !string.IsNullOrEmpty(filter.PropertyValuePath))
            {
                BuildExistsFilter(commandBuilder, filter, jsonSerializer, parameters);
                continue;
            }
            else if (filter.FilterType.HasValue)
            {
                BuildSimpleFilter(commandBuilder, filter, jsonSerializer, parameters);
            }
        }

        commandBuilder.AppendLine(CloseParen);
    }

    /// <summary>
    /// Renders any deferred path segments into literal JSON paths so the emit code below can
    /// stay unaware of how the path was supplied. Filters built from a literal path string are
    /// returned unchanged.
    /// </summary>
    private static Filter Resolve(in Filter filter, IJsonPropertyNameResolver? nameResolver, IJsonValueResolver? valueResolver)
    {
        if (filter.Join.HasValue)
        {
            return filter;
        }

        var value = ResolveSetOrScalarValue(filter, valueResolver);

        if (filter.PropertyPathSegments is null && filter.PropertyValuePathSegments is null)
        {
            return ReferenceEquals(value, filter.Value)
                ? filter
                : Rebuild(filter, filter.PropertyPath, filter.PropertyValuePath, value);
        }

        var propertyPath =
            filter.PropertyPathSegments is null
                ? filter.PropertyPath
                : QueryPropertyPath.RenderPath(filter.PropertyPathSegments, nameResolver);

        if (filter.PropertyValuePathSegments is null && filter.PropertyValuePath is null)
        {
            return Rebuild(filter, propertyPath, null, value);
        }

        var propertyValuePath =
            filter.PropertyValuePathSegments is null
                ? filter.PropertyValuePath
                : QueryPropertyPath.RenderPath(filter.PropertyValuePathSegments, nameResolver);

        return Rebuild(filter, propertyPath, propertyValuePath, value);
    }

    private static Filter Rebuild(in Filter filter, string? propertyPath, string? propertyValuePath, object? value)
    {
        return propertyValuePath is null
            ? new Filter(
                filter.FilterType!.Value,
                propertyPath,
                null,
                filter.IsPropertyPathNumeric,
                filter.IsPropertyPathBool,
                filter.IsPropertyPathDateTime,
                value)
            : new Filter(
                filter.FilterType!.Value,
                propertyPath,
                null,
                propertyValuePath,
                null,
                filter.IsPropertyValuePathNumeric,
                filter.IsPropertyValuePathBool,
                filter.IsPropertyValuePathDateTime,
                value);
    }

    /// <summary>
    /// Resolves a set-membership term's values element by element, so an enum or DateOnly in an
    /// IN list is compared in the same JSON form the serializer wrote — exactly as the scalar
    /// comparisons already are. Any other term resolves its single value.
    /// </summary>
    private static object? ResolveSetOrScalarValue(in Filter filter, IJsonValueResolver? valueResolver)
    {
        if (filter.FilterType is not (FilterType.In or FilterType.NotIn) || filter.Value is not object?[] values)
        {
            return ResolveValue(filter.Value, valueResolver);
        }

        object?[]? resolved = null;

        for (var i = 0; i < values.Length; i++)
        {
            var value = ResolveValue(values[i], valueResolver);

            if (resolved is null && ReferenceEquals(value, values[i]))
            {
                continue;
            }

            resolved ??= (object?[])values.Clone();
            resolved[i] = value;
        }

        return resolved ?? values;
    }

    /// <summary>
    /// Converts a comparison value into the form the serializer writes into the document, so
    /// the comparison is made against what is actually stored. Only values that would
    /// otherwise fall through to <see cref="object.ToString"/> are converted: an enum whose
    /// JSON form is a number (or a name a converter or naming policy rewrote), or a type such
    /// as <c>DateOnly</c>/<c>TimeOnly</c> whose <c>ToString()</c> is culture-dependent while
    /// its JSON form is not.
    /// </summary>
    private static object? ResolveValue(object? value, IJsonValueResolver? valueResolver)
    {
        if (valueResolver is null || value is null)
        {
            return value;
        }

        switch (value)
        {
            // Already emitted in a form that matches the stored JSON: strings bind directly,
            // booleans and genuine numerics become literals below.
            case string:
            case bool:
            case byte or sbyte or short or ushort or int or uint or long or ulong:
            case float or double or decimal:
                return value;

            // Handled ahead of this by the date-time branch, which formats using the
            // serializer's DateTimeSerializationFormat.
            case DateTime:
            case DateTimeOffset:
                return value;
        }

        return valueResolver.TryResolveJsonValue(value, out var jsonValue) ? jsonValue : value;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AppendExistsPrefix(StringBuilder sb, in Filter filter)
    {
        sb.Append(ExistsPrefix)
          .Append(filter.PropertyPath)
          .Append(ExistsMiddle)
          .Append(filter.PropertyValuePath)
          .Append(ExistsSuffix);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendJsonExtract(StringBuilder sb, string propertyPath)
    {
        sb.Append(JsonExtractPrefix).Append(propertyPath).Append(JsonExtractSuffix);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendCastNumeric(StringBuilder sb, string propertyPath)
    {
        sb.Append(CastNumericPrefix).Append(propertyPath).Append(CastNumericSuffix);
    }

    /// <summary>
    /// Emits a comparison value. Genuine numeric and boolean CLR values are
    /// written as safe literals (no user text can reach the SQL); everything
    /// else — including strings, and values whose runtime type does not match
    /// the property's declared type — is bound as a parameter.
    /// </summary>
    private static void AppendValue(StringBuilder sb, FilterParameters parameters, object? value)
    {
        if (TryAppendSafeLiteral(sb, value))
        {
            return;
        }

        // Bind the same textual form that was previously concatenated (JSON stores
        // values such as Guids/enums as their string representation), so behavior
        // is unchanged while the value can no longer break out of the SQL text.
        sb.Append(parameters.Add(value?.ToString()));
    }

    /// <summary>
    /// Emits a value for a numeric comparison (&gt;, &gt;=, &lt;, &lt;=). Genuine
    /// numeric CLR values become literals; anything else is parameterized so a
    /// non-numeric payload can never be injected as raw SQL.
    /// </summary>
    private static void AppendNumericValue(StringBuilder sb, FilterParameters parameters, object? value)
    {
        if (TryAppendNumericLiteral(sb, value))
        {
            return;
        }

        sb.Append(parameters.Add(value?.ToString()));
    }

    private static bool TryAppendSafeLiteral(StringBuilder sb, object? value)
    {
        if (value is bool b)
        {
            sb.Append(b ? '1' : '0');
            return true;
        }

        return TryAppendNumericLiteral(sb, value);
    }

    private static bool TryAppendNumericLiteral(StringBuilder sb, object? value)
    {
        switch (value)
        {
            case byte or sbyte or short or ushort or int or uint or long:
                sb.Append(Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture));
                return true;
            case ulong ul:
                sb.Append(ul.ToString(CultureInfo.InvariantCulture));
                return true;
            case float f:
                sb.Append(f.ToString("R", CultureInfo.InvariantCulture));
                return true;
            case double d:
                sb.Append(d.ToString("R", CultureInfo.InvariantCulture));
                return true;
            case decimal m:
                sb.Append(m.ToString(CultureInfo.InvariantCulture));
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// Escapes LIKE metacharacters (\ % _) so a user-supplied value matches
    /// literally and cannot force full-table scans via leading wildcards. Used
    /// together with an explicit ESCAPE clause.
    /// </summary>
    private static string BuildLikePattern(object? value, bool leadingWildcard, bool trailingWildcard)
    {
        var raw = value?.ToString() ?? string.Empty;
        var sb = new StringBuilder(raw.Length + 4);

        if (leadingWildcard)
        {
            sb.Append('%');
        }

        foreach (var c in raw)
        {
            if (c is '\\' or '%' or '_')
            {
                sb.Append('\\');
            }

            sb.Append(c);
        }

        if (trailingWildcard)
        {
            sb.Append('%');
        }

        return sb.ToString();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendLike(StringBuilder sb, FilterParameters parameters, object? value, bool leadingWildcard, bool trailingWildcard)
    {
        var pattern = BuildLikePattern(value, leadingWildcard, trailingWildcard);
        sb.Append(LikeOperator).Append(parameters.Add(pattern)).Append(LikeEscapeClause);
    }

    private void BuildExistsFilter(StringBuilder commandBuilder, in Filter filter, IJsonSerializer jsonSerializer, FilterParameters parameters)
    {
        switch (filter.FilterType!.Value)
        {
            case FilterType.Contains:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(ValValue);
                AppendLike(commandBuilder, parameters, filter.Value, leadingWildcard: true, trailingWildcard: true);
                commandBuilder.Append(ExistsEnd).AppendLine();
                break;

            case FilterType.EndsWith:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(ValValue);
                AppendLike(commandBuilder, parameters, filter.Value, leadingWildcard: true, trailingWildcard: false);
                commandBuilder.Append(ExistsEnd).AppendLine();
                break;

            case FilterType.Equals:
                AppendExistsPrefix(commandBuilder, filter);
                if (filter.Value is null)
                {
                    commandBuilder.Append(ValValue).Append(IsNull).Append(ExistsEnd).AppendLine();
                }
                else if (filter.IsPropertyValuePathNumeric)
                {
                    commandBuilder.Append(CastValNumeric).Append(Equals);
                    AppendNumericValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.Append(ExistsEnd).AppendLine();
                }
                else if (filter.IsPropertyValuePathDateTime)
                {
                    var dateTimeString = GetDateTimeString(filter.Value, jsonSerializer);
                    commandBuilder.Append(ValValue).Append(Equals).Append(parameters.Add(dateTimeString)).Append(ExistsEnd).AppendLine();
                }
                else
                {
                    commandBuilder.Append(ValValue).Append(Equals);
                    AppendValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.Append(ExistsEnd).AppendLine();
                }

                break;

            case FilterType.GreaterThan:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(GreaterThan);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.Append(ExistsEnd).AppendLine();
                break;

            case FilterType.GreaterThanOrEqualTo:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(GreaterThanOrEqual);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.Append(ExistsEnd).AppendLine();
                break;

            case FilterType.LessThan:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(LessThan);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.Append(ExistsEnd).AppendLine();
                break;

            case FilterType.LessThanOrEqualTo:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(LessThanOrEqual);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.Append(ExistsEnd).AppendLine();
                break;

            case FilterType.NotEquals:
                AppendExistsPrefix(commandBuilder, filter);
                if (filter.Value is null)
                {
                    commandBuilder.Append(ValValue).Append(IsNotNull).Append(ExistsEnd).AppendLine();
                }
                else
                {
                    commandBuilder.Append(ValValue).Append(NotEquals);
                    AppendValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.Append(ExistsEnd).AppendLine();
                }

                break;

            case FilterType.StartsWith:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(ValValue);
                AppendLike(commandBuilder, parameters, filter.Value, leadingWildcard: false, trailingWildcard: true);
                commandBuilder.Append(ExistsEnd).AppendLine();
                break;
        }
    }

    private void BuildSimpleFilter(StringBuilder commandBuilder, in Filter filter, IJsonSerializer jsonSerializer, FilterParameters parameters)
    {
        switch (filter.FilterType!.Value)
        {
            case FilterType.Contains:
                AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                AppendLike(commandBuilder, parameters, filter.Value, leadingWildcard: true, trailingWildcard: true);
                commandBuilder.AppendLine();
                break;

            case FilterType.EndsWith:
                AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                AppendLike(commandBuilder, parameters, filter.Value, leadingWildcard: true, trailingWildcard: false);
                commandBuilder.AppendLine();
                break;

            case FilterType.Equals:
                if (filter.Value is null)
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(IsNull).AppendLine();
                }
                else if (filter.IsPropertyPathBool)
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(Equals);
                    AppendValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.AppendLine();
                }
                else if (filter.IsPropertyPathNumeric)
                {
                    AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(Equals);
                    AppendNumericValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.AppendLine();
                }
                else if (filter.IsPropertyPathDateTime)
                {
                    var dateTimeString = GetDateTimeString(filter.Value, jsonSerializer);
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(Equals).Append(parameters.Add(dateTimeString)).AppendLine();
                }
                else
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(Equals);
                    AppendValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.AppendLine();
                }

                break;

            case FilterType.GreaterThan:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(GreaterThan);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.AppendLine();
                break;

            case FilterType.GreaterThanOrEqualTo:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(GreaterThanOrEqual);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.AppendLine();
                break;

            case FilterType.LessThan:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(LessThan);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.AppendLine();
                break;

            case FilterType.LessThanOrEqualTo:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(LessThanOrEqual);
                AppendNumericValue(commandBuilder, parameters, filter.Value);
                commandBuilder.AppendLine();
                break;

            case FilterType.NotEquals:
                if (filter.IsPropertyPathBool)
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(NotEquals);
                    AppendValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.AppendLine();
                }
                else
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(NotEquals);
                    AppendValue(commandBuilder, parameters, filter.Value);
                    commandBuilder.AppendLine();
                }

                break;

            case FilterType.StartsWith:
                AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                AppendLike(commandBuilder, parameters, filter.Value, leadingWildcard: false, trailingWildcard: true);
                commandBuilder.AppendLine();
                break;

            case FilterType.In:
            case FilterType.NotIn:
                BuildSetFilter(commandBuilder, filter, jsonSerializer, parameters);
                break;
        }
    }

    /// <summary>
    /// Renders <c>path IN (…)</c> / <c>path NOT IN (…)</c>. The path is emitted through the same
    /// helpers the scalar comparisons use, so a numeric property keeps its
    /// <c>CAST(… as NUMERIC)</c> form and stays matchable by an expression index over it.
    /// </summary>
    private static void BuildSetFilter(StringBuilder commandBuilder, in Filter filter, IJsonSerializer jsonSerializer, FilterParameters parameters)
    {
        var negated = filter.FilterType!.Value == FilterType.NotIn;
        var values = filter.Value as object?[] ?? Array.Empty<object?>();

        // SQL's IN never matches NULL against a NULL in the list, so a null the caller put in
        // the set is pulled out and tested separately. Without this it would silently be a
        // value that can never match.
        var hasNull = false;
        var present = new List<object?>(values.Length);

        foreach (var value in values)
        {
            if (value is null)
            {
                hasNull = true;
            }
            else
            {
                present.Add(value);
            }
        }

        var join = negated ? AndJoin : OrJoin;

        if (present.Count == 0)
        {
            if (hasNull)
            {
                AppendSetPath(commandBuilder, filter);
                commandBuilder.Append(negated ? IsNotNull : IsNull).AppendLine();
            }
            else
            {
                commandBuilder.AppendLine(negated ? MatchEverything : MatchNothing);
            }

            return;
        }

        var chunks = ((present.Count - 1) / MaxValuesPerInClause) + 1;
        var wrap = hasNull || chunks > 1;

        if (wrap)
        {
            commandBuilder.Append(OpenParen);
        }

        for (var chunk = 0; chunk < chunks; chunk++)
        {
            if (chunk > 0)
            {
                commandBuilder.Append(join);
            }

            var start = chunk * MaxValuesPerInClause;
            var end = Math.Min(start + MaxValuesPerInClause, present.Count);

            AppendSetPath(commandBuilder, filter);
            commandBuilder.Append(negated ? NotInOperator : InOperator);

            for (var i = start; i < end; i++)
            {
                if (i > start)
                {
                    commandBuilder.Append(ValueSeparator);
                }

                AppendSetValue(commandBuilder, filter, parameters, present[i], jsonSerializer);
            }

            commandBuilder.Append(CloseParen);
        }

        if (hasNull)
        {
            commandBuilder.Append(join);
            AppendSetPath(commandBuilder, filter);
            commandBuilder.Append(negated ? IsNotNull : IsNull);
        }

        if (wrap)
        {
            commandBuilder.Append(CloseParen);
        }

        commandBuilder.AppendLine();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendSetPath(StringBuilder commandBuilder, in Filter filter)
    {
        if (filter.IsPropertyPathNumeric)
        {
            AppendCastNumeric(commandBuilder, filter.PropertyPath!);
        }
        else
        {
            AppendJsonExtract(commandBuilder, filter.PropertyPath!);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendSetValue(StringBuilder commandBuilder, in Filter filter, FilterParameters parameters, object? value, IJsonSerializer jsonSerializer)
    {
        if (filter.IsPropertyPathNumeric)
        {
            AppendNumericValue(commandBuilder, parameters, value);
        }
        else if (filter.IsPropertyPathDateTime)
        {
            commandBuilder.Append(parameters.Add(GetDateTimeString(value, jsonSerializer)));
        }
        else
        {
            AppendValue(commandBuilder, parameters, value);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string GetDateTimeString(object? value, IJsonSerializer jsonSerializer)
    {
        return value switch
        {
            DateTime dt => dt.ToString(jsonSerializer.DateTimeSerializationFormat),
            DateTimeOffset dto => dto.ToString(jsonSerializer.DateTimeSerializationFormat),
            _ => string.Empty,
        };
    }
}
