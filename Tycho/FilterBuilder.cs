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

            _filters.Add (new Filter (filterType, propertyPathString, value));

            return this;
        }

        public FilterBuilder<TObj> Filter (FilterType filterType, string propertyPath, object value)
        {
            _filters.Add (new Filter (filterType, propertyPath, value));

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

        public SqliteCommand Build (SqliteCommand command)
        {
            var queryBuilder = new StringBuilder ();

            if (_filters.Any())
            {
                queryBuilder.AppendLine ("AND");
            }

            foreach (var filter in _filters)
            {
                if(filter.FilterType.HasValue)
                {
                    switch (filter.FilterType.Value)
                    {
                        case FilterType.Contains:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') like \'%{filter.Value}%\'");
                            break;
                        case FilterType.EndsWith:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') like \'%{filter.Value}\'");
                            break;
                        case FilterType.Equals:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') = \'{filter.Value}\'");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.GreaterThan:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') + 0 > {filter.Value}");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.GreaterThanOrEqualTo:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') + 0 >= {filter.Value}");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.LessThan:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') + 0 < {filter.Value}");
                            break;
                        //TODO: This is an attack vector and should be parameterized
                        case FilterType.LessThanOrEqualTo:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') + 0 <= {filter.Value}");
                            break;
                        case FilterType.NotEquals:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') <> \'{filter.Value}\'");
                            break;
                        case FilterType.StartsWith:
                            queryBuilder.AppendLine ($"JSON_EXTRACT(DATA, \'{filter.PropertyPath}\') like \'{filter.Value}%\'");
                            break;
                    }

                    continue;
                }

                if(filter.Join.HasValue)
                {
                    switch (filter.Join.Value)
                    {
                        case FilterJoin.And:
                            queryBuilder.AppendLine ($"AND");
                            break;
                        case FilterJoin.Or:
                            queryBuilder.AppendLine ($"OR");
                            break;
                        case FilterJoin.StartGroup:
                            queryBuilder.AppendLine ($"(");
                            break;
                        case FilterJoin.EndGroup:
                            queryBuilder.AppendLine ($")");
                            break;
                    }
                    continue;
                }
            }

            command.CommandText += queryBuilder.ToString ();

            return command;
        }
    }
}
