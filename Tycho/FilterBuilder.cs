using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Tycho
{
    public class FilterBuilder<TObj>
    {
        private readonly List<Filter> _filters = new();

        private FilterBuilder()
        {
        }

        public static FilterBuilder<TObj> Create()
        {
            return new FilterBuilder<TObj>();
        }

        public FilterBuilder<TObj> Filter<TProp> (FilterType filterType, Expression<Func<TObj, TProp>> propertyPath, object value)
        {
            var propertyPathString = QueryPropertyPath.BuildPath (propertyPath);
            var isPropertyPathNumeric = QueryPropertyPath.IsNumeric (propertyPath);
            var isPropertyPathBool = QueryPropertyPath.IsBool(propertyPath);
            var isPropertyPathDateTime = QueryPropertyPath.IsDateTime(propertyPath);

            _filters.Add (new Filter (filterType, propertyPathString, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime, value));

            return this;
        }

        public FilterBuilder<TObj> Filter<TItem, TItemProp> (FilterType filterType, Expression<Func<TObj, IEnumerable<TItem>>> propertyPath, Expression<Func<TItem, TItemProp>> propertyValuePath, object value)
        {
            var propertyPathString = QueryPropertyPath.BuildPath (propertyPath);
            var propertyValuePathString = QueryPropertyPath.BuildPath (propertyValuePath);
            var isPropertyValuePathNumeric = QueryPropertyPath.IsNumeric (propertyValuePath);
            var isPropertyValuePathBool = QueryPropertyPath.IsBool(propertyValuePath);
            var isPropertyValuePathDateTime = QueryPropertyPath.IsDateTime(propertyValuePath);

            _filters.Add (new Filter (filterType, propertyPathString, propertyValuePathString, isPropertyValuePathNumeric, isPropertyValuePathBool, isPropertyValuePathDateTime, value));

            return this;
        }

        public FilterBuilder<TObj> Filter (FilterType filterType, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, bool isPropertyPathDateTime, object value)
        {
            _filters.Add (new Filter (filterType, propertyPath, isPropertyPathNumeric, isPropertyPathBool, isPropertyPathDateTime, value));

            return this;
        }

        public FilterBuilder<TObj> And ()
        {
            _filters.Add (new Filter (FilterJoin.And));
            return this;
        }

        public FilterBuilder<TObj> Or ()
        {
            _filters.Add (new Filter (FilterJoin.Or));
            return this;
        }

        public FilterBuilder<TObj> StartGroup ()
        {
            _filters.Add (new Filter (FilterJoin.StartGroup));
            return this;
        }

        public FilterBuilder<TObj> EndGroup ()
        {
            _filters.Add (new Filter (FilterJoin.EndGroup));
            return this;
        }

        internal void Build (StringBuilder commandBuilder, IJsonSerializer jsonSerializer)
        {
            if (_filters.Any())
            {
                commandBuilder.AppendLine ("\nAND");
            }

            foreach (var filter in _filters)
            {
                if (filter.Join.HasValue)
                {
                    switch (filter.Join.Value)
                    {
                        case FilterJoin.And:
                            commandBuilder.AppendLine ($"AND");
                            break;
                        case FilterJoin.Or:
                            commandBuilder.AppendLine ($"OR");
                            break;
                        case FilterJoin.StartGroup:
                            commandBuilder.AppendLine ($"(");
                            break;
                        case FilterJoin.EndGroup:
                            commandBuilder.AppendLine ($")");
                            break;
                    }
                    continue;
                }

                if(filter.FilterType.HasValue && !string.IsNullOrEmpty(filter.PropertyValuePath))
                {
                    switch (filter.FilterType.Value)
                    {
                        case FilterType.Contains:
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value like \'%{filter.Value}%\')");
                            break;
                        case FilterType.EndsWith:
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value like \'%{filter.Value}\')");
                            break;
                        case FilterType.Equals:
                            if (filter.IsPropertyValuePathBool)
                            {
                                commandBuilder.AppendLine($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value = {filter.Value})");
                                break;
                            }

                            if(filter.IsPropertyValuePathNumeric)
                            {
                                commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE CAST(VAL.value as NUMERIC) = \'{filter.Value}\')");
                                break;
                            }

                            if (filter.IsPropertyValuePathDateTime)
                            {
                                var dateTimeString = string.Empty;

                                if(filter.Value is DateTime dt)
                                {
                                    dateTimeString = dt.ToString(jsonSerializer.DateTimeSerializationFormat);
                                }
                                else if (filter.Value is DateTimeOffset dto)
                                {
                                    dateTimeString = dto.ToString(jsonSerializer.DateTimeSerializationFormat);
                                }

                                commandBuilder.AppendLine($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value = \'{dateTimeString}\')");
                                break;
                            }

                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value = \'{filter.Value}\')");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.GreaterThan:
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE CAST(VAL.value as NUMERIC) > {filter.Value})");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.GreaterThanOrEqualTo:
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE CAST(VAL.value as NUMERIC) >= {filter.Value})");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.LessThan:
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE CAST(VAL.value as NUMERIC) < {filter.Value})");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.LessThanOrEqualTo:
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE CAST(VAL.value as NUMERIC) <= {filter.Value})");
                            break;
                        case FilterType.NotEquals:
                            if (filter.IsPropertyValuePathBool)
                            {
                                commandBuilder.AppendLine($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value <> {filter.Value})");
                                break;
                            }
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value <> \'{filter.Value}\')");
                            break;
                        case FilterType.StartsWith:
                            commandBuilder.AppendLine ($"EXISTS(SELECT 1 FROM JSON_TREE(Data, \'{filter.PropertyPath}\') AS JT, JSON_EACH(JT.Value, \'{filter.PropertyValuePath}\') AS VAL WHERE VAL.value like \'{filter.Value}%\')");
                            break;
                    }

                    continue;
                }

                if (filter.FilterType.HasValue)
                {
                    switch (filter.FilterType.Value)
                    {
                        case FilterType.Contains:
                            commandBuilder.AppendLine ($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') like \'%{filter.Value}%\'");
                            break;
                        case FilterType.EndsWith:
                            commandBuilder.AppendLine ($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') like \'%{filter.Value}\'");
                            break;
                        case FilterType.Equals:
                            if(filter.IsPropertyPathBool)
                            {
                                commandBuilder.AppendLine($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') = {filter.Value}");
                                break;
                            }

                            if(filter.IsPropertyPathNumeric)
                            {
                                commandBuilder.AppendLine ($"CAST(JSON_EXTRACT(Data, \'{filter.PropertyPath}\') as NUMERIC) = \'{filter.Value}\'");
                                break;
                            }

                            if (filter.IsPropertyPathDateTime)
                            {
                                var dateTimeString = string.Empty;

                                if (filter.Value is DateTime dt)
                                {
                                    dateTimeString = dt.ToString(jsonSerializer.DateTimeSerializationFormat);
                                }
                                else if (filter.Value is DateTimeOffset dto)
                                {
                                    dateTimeString = dto.ToString(jsonSerializer.DateTimeSerializationFormat);
                                }

                                commandBuilder.AppendLine($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') = \'{dateTimeString}\'");
                                break;
                            }

                            commandBuilder.AppendLine ($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') = \'{filter.Value}\'");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.GreaterThan:
                            commandBuilder.AppendLine ($"CAST(JSON_EXTRACT(Data, \'{filter.PropertyPath}\') as NUMERIC) > {filter.Value}");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.GreaterThanOrEqualTo:
                            commandBuilder.AppendLine ($"CAST(JSON_EXTRACT(Data, \'{filter.PropertyPath}\') as NUMERIC) >= {filter.Value}");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.LessThan:
                            commandBuilder.AppendLine ($"CAST(JSON_EXTRACT(Data, \'{filter.PropertyPath}\') as NUMERIC) < {filter.Value}");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.LessThanOrEqualTo:
                            commandBuilder.AppendLine ($"CAST(JSON_EXTRACT(Data, \'{filter.PropertyPath}\') as NUMERIC) <= {filter.Value}");
                            break;
                        case FilterType.NotEquals:
                            if (filter.IsPropertyPathBool)
                            {
                                commandBuilder.AppendLine($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') <> {filter.Value}");
                                break;
                            }

                            commandBuilder.AppendLine ($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') <> \'{filter.Value}\'");
                            break;
                        case FilterType.StartsWith:
                            commandBuilder.AppendLine ($"JSON_EXTRACT(Data, \'{filter.PropertyPath}\') like \'{filter.Value}%\'");
                            break;
                    }
                }
            }
        }
    }
}
