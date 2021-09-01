using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Microsoft.Data.Sqlite;

namespace Tycho
{
    public class FilterBuilder<TObj>
    {
        private readonly List<Filter> _filters = new List<Filter> ();

        public FilterBuilder ()
        {
        }

        public FilterBuilder<TObj> Filter<TProp> (FilterType filterType, Expression<Func<TObj, TProp>> propertyPath, object value)
        {
            var propertyPathString = QueryPropertyPath.BuildPath (propertyPath);
            var isPropertyPathNumeric = QueryPropertyPath.IsNumeric (propertyPath);
            var isPropertyPathBool = QueryPropertyPath.IsBool(propertyPath);

            _filters.Add (new Filter (filterType, propertyPathString, isPropertyPathNumeric, isPropertyPathBool, value));

            return this;
        }

        public FilterBuilder<TObj> Filter<TItem, TItemProp> (FilterType filterType, Expression<Func<TObj, IEnumerable<TItem>>> propertyPath, Expression<Func<TItem, TItemProp>> propertyValuePath, object value)
        {
            var propertyPathString = QueryPropertyPath.BuildPath (propertyPath);
            var propertyValuePathString = QueryPropertyPath.BuildPath (propertyValuePath);
            var isPropertyValuePathNumeric = QueryPropertyPath.IsNumeric (propertyValuePath);
            var isPropertyValuePathBool = QueryPropertyPath.IsBool(propertyValuePath);

            _filters.Add (new Filter (filterType, propertyPathString, propertyValuePathString, isPropertyValuePathNumeric, isPropertyValuePathBool, value));

            return this;
        }

        public FilterBuilder<TObj> Filter (FilterType filterType, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, object value)
        {
            _filters.Add (new Filter (filterType, propertyPath, isPropertyPathNumeric, isPropertyPathBool, value));

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

        public StringBuilder Build (StringBuilder commandBuilder)
        {
            if (_filters.Any())
            {
                commandBuilder.AppendLine ("AND");
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

                    continue;
                }
            }

            return commandBuilder;
        }
    }
}
