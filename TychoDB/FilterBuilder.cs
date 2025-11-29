using System;
using System.Collections.Generic;
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
    private const string LikePrefix = " like '";
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

    internal void Build(StringBuilder commandBuilder, IJsonSerializer jsonSerializer)
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
                BuildExistsFilter(commandBuilder, filter, jsonSerializer);
                continue;
            }
            else if (filter.FilterType.HasValue)
            {
                BuildSimpleFilter(commandBuilder, filter, jsonSerializer);
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

    private void BuildExistsFilter(StringBuilder commandBuilder, in Filter filter, IJsonSerializer jsonSerializer)
    {
        switch (filter.FilterType!.Value)
        {
            case FilterType.Contains:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(ValValue).Append(LikePrefix).Append('%').Append(filter.Value).Append("%')").AppendLine();
                break;

            case FilterType.EndsWith:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(ValValue).Append(LikePrefix).Append('%').Append(filter.Value).Append("')").AppendLine();
                break;

            case FilterType.Equals:
                AppendExistsPrefix(commandBuilder, filter);
                if (filter.Value is null)
                {
                    commandBuilder.Append(ValValue).Append(IsNull).Append(ExistsEnd).AppendLine();
                }
                else if (filter.IsPropertyValuePathBool)
                {
                    commandBuilder.Append(ValValue).Append(Equals).Append(filter.Value).Append(ExistsEnd).AppendLine();
                }
                else if (filter.IsPropertyValuePathNumeric)
                {
                    commandBuilder.Append(CastValNumeric).Append(Equals).Append('\'').Append(filter.Value).Append("')").AppendLine();
                }
                else if (filter.IsPropertyValuePathDateTime)
                {
                    var dateTimeString = GetDateTimeString(filter.Value, jsonSerializer);
                    commandBuilder.Append(ValValue).Append(Equals).Append('\'').Append(dateTimeString).Append("')").AppendLine();
                }
                else
                {
                    commandBuilder.Append(ValValue).Append(Equals).Append('\'').Append(filter.Value).Append("')").AppendLine();
                }
                break;

            case FilterType.GreaterThan:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(GreaterThan).Append(filter.Value).Append(ExistsEnd).AppendLine();
                break;

            case FilterType.GreaterThanOrEqualTo:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(GreaterThanOrEqual).Append(filter.Value).Append(ExistsEnd).AppendLine();
                break;

            case FilterType.LessThan:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(LessThan).Append(filter.Value).Append(ExistsEnd).AppendLine();
                break;

            case FilterType.LessThanOrEqualTo:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(CastValNumeric).Append(LessThanOrEqual).Append(filter.Value).Append(ExistsEnd).AppendLine();
                break;

            case FilterType.NotEquals:
                AppendExistsPrefix(commandBuilder, filter);
                if (filter.Value is null)
                {
                    commandBuilder.Append(ValValue).Append(IsNotNull).Append(ExistsEnd).AppendLine();
                }
                else if (filter.IsPropertyValuePathBool)
                {
                    commandBuilder.Append(ValValue).Append(NotEquals).Append(filter.Value).Append(ExistsEnd).AppendLine();
                }
                else
                {
                    commandBuilder.Append(ValValue).Append(NotEquals).Append('\'').Append(filter.Value).Append("')").AppendLine();
                }
                break;

            case FilterType.StartsWith:
                AppendExistsPrefix(commandBuilder, filter);
                commandBuilder.Append(ValValue).Append(LikePrefix).Append(filter.Value).Append("%')").AppendLine();
                break;
        }
    }

    private void BuildSimpleFilter(StringBuilder commandBuilder, in Filter filter, IJsonSerializer jsonSerializer)
    {
        switch (filter.FilterType!.Value)
        {
            case FilterType.Contains:
                AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(LikePrefix).Append('%').Append(filter.Value).Append("%'").AppendLine();
                break;

            case FilterType.EndsWith:
                AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(LikePrefix).Append('%').Append(filter.Value).Append('\'').AppendLine();
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
                    commandBuilder.Append(Equals).Append(filter.Value).AppendLine();
                }
                else if (filter.IsPropertyPathNumeric)
                {
                    AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(Equals).Append('\'').Append(filter.Value).Append('\'').AppendLine();
                }
                else if (filter.IsPropertyPathDateTime)
                {
                    var dateTimeString = GetDateTimeString(filter.Value, jsonSerializer);
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(Equals).Append('\'').Append(dateTimeString).Append('\'').AppendLine();
                }
                else
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(Equals).Append('\'').Append(filter.Value).Append('\'').AppendLine();
                }
                break;

            case FilterType.GreaterThan:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(GreaterThan).Append(filter.Value).AppendLine();
                break;

            case FilterType.GreaterThanOrEqualTo:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(GreaterThanOrEqual).Append(filter.Value).AppendLine();
                break;

            case FilterType.LessThan:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(LessThan).Append(filter.Value).AppendLine();
                break;

            case FilterType.LessThanOrEqualTo:
                AppendCastNumeric(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(LessThanOrEqual).Append(filter.Value).AppendLine();
                break;

            case FilterType.NotEquals:
                if (filter.IsPropertyPathBool)
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(NotEquals).Append(filter.Value).AppendLine();
                }
                else
                {
                    AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                    commandBuilder.Append(NotEquals).Append('\'').Append(filter.Value).Append('\'').AppendLine();
                }
                break;

            case FilterType.StartsWith:
                AppendJsonExtract(commandBuilder, filter.PropertyPath!);
                commandBuilder.Append(LikePrefix).Append(filter.Value).Append("%'").AppendLine();
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
            _ => string.Empty
        };
    }
}
