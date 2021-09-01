using System;
using System.Linq.Expressions;

namespace Tycho
{
    public enum FilterJoin
    {
        And,
        Or,
        StartGroup,
        EndGroup,
    }

    public enum FilterType
    {
        Equals,
        NotEquals,
        StartsWith,
        EndsWith,
        Contains,
        GreaterThan,
        GreaterThanOrEqualTo,
        LessThan,
        LessThanOrEqualTo,
    }

    public class Filter
    {
        public FilterJoin? Join { get; private set; }

        public FilterType? FilterType { get; private set; }

        public string PropertyPath { get; private set; }

        public bool IsPropertyPathNumeric { get; private set; }

        public bool IsPropertyPathBool { get; private set; }

        public string PropertyValuePath { get; set; }

        public bool IsPropertyValuePathNumeric { get; private set; }

        public bool IsPropertyValuePathBool { get; private set; }

        public object Value { get; private set; }

        public Filter (FilterType filterType, string propertyPath, bool isPropertyPathNumeric, bool isPropertyPathBool, object value)
        {
            FilterType = filterType;
            PropertyPath = propertyPath;
            IsPropertyPathNumeric = isPropertyPathNumeric;
            IsPropertyPathBool = isPropertyPathBool;
            Value = value;
        }

        public Filter (FilterType filterType, string listPropertyPath, string propertyValuePath, bool isPropertyValuePathNumeric, bool isPropertyValuePathBool, object value)
        {
            FilterType = filterType;
            PropertyPath = listPropertyPath;
            PropertyValuePath = propertyValuePath;
            IsPropertyValuePathNumeric = isPropertyValuePathNumeric;
            IsPropertyValuePathBool = isPropertyValuePathBool;
            Value = value;
        }

        public Filter(FilterJoin join)
        {
            Join = join;
        }
    }
}
