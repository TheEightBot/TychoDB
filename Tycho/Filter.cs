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

        public object Value { get; private set; }

        public Filter (FilterType filterType, string propertyPath, object value)
        {
            FilterType = filterType;
            PropertyPath = propertyPath;
            Value = value;
        }

        public Filter(FilterJoin join)
        {
            Join = join;
        }
    }
}
