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
        var propertyPathString = QueryPropertyPath.BuildPath(propertyPath);
        var isPropertyPathNumeric = QueryPropertyPath.IsNumeric(propertyPath);
        var isPropertyPathBool = QueryPropertyPath.IsBool(propertyPath);
        var isPropertyPathDateTime = QueryPropertyPath.IsDateTime(propertyPath);

        _filters.Add(new Filter(filterType, propertyPathString, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime, value));

        return this;
    }

    public FilterBuilder<TObj> Filter<TItem, TItemProp>(FilterType filterType, Expression<Func<TObj, IEnumerable<TItem>>> propertyPath, Expression<Func<TItem, TItemProp>> propertyValuePath, object value)
    {
        var propertyPathString = QueryPropertyPath.BuildPath(propertyPath);
        var propertyValuePathString = QueryPropertyPath.BuildPath(propertyValuePath);
        var isPropertyValuePathNumeric = QueryPropertyPath.IsNumeric(propertyValuePath);
        var isPropertyValuePathBool = QueryPropertyPath.IsBool(propertyValuePath);
        var isPropertyValuePathDateTime = QueryPropertyPath.IsDateTime(propertyValuePath);

        _filters.Add(new Filter(filterType, propertyPathString, propertyValuePathString, isPropertyValuePathNumeric, isPropertyValuePathBool, isPropertyValuePathDateTime, value));

        return this;
    }

    public FilterBuilder<TObj> Filter(FilterType filterType, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime, object value)
    {
        // This overload accepts a raw JSON path string from the caller. Because
        // the path is emitted as an identifier inside JSON_EXTRACT(...) and
        // cannot be parameterized, validate it against a strict grammar to
        // prevent it from being used as an injection vector.
        QueryPropertyPath.ValidatePath(propertyPath, nameof(propertyPath));

        _filters.Add(new Filter(filterType, propertyPath, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime, value));

        return this;
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
        if (_filters.Count > 0)
        {
            commandBuilder.AppendLine("\nAND");
        }

        foreach (var filter in _filters)
        {
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
