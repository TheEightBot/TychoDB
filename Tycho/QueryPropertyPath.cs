using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace Tycho
{
    internal static class QueryPropertyPath
    {
        public static string BuildPath<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
        {
            var visitor = new PropertyPathVisitor();

            visitor.Visit(expression.Body);

            return $"$.{string.Join('.', visitor.PathBuilder)}";
        }

        public static bool IsNumeric<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
        {
            if (expression.Body is MemberExpression memEx && memEx.Member is PropertyInfo propInfo)
            {
                var propertyType = propInfo.PropertyType;

                return
                    propertyType == typeof(int) || Nullable.GetUnderlyingType(propertyType) == typeof(int) ||
                    propertyType == typeof(uint) || Nullable.GetUnderlyingType(propertyType) == typeof(uint) ||
                    propertyType == typeof(long) || Nullable.GetUnderlyingType(propertyType) == typeof(long) ||
                    propertyType == typeof(ulong) || Nullable.GetUnderlyingType(propertyType) == typeof(ulong) ||
                    propertyType == typeof(double) || Nullable.GetUnderlyingType(propertyType) == typeof(double) ||
                    propertyType == typeof(float) || Nullable.GetUnderlyingType(propertyType) == typeof(float) ||
                    propertyType == typeof(decimal) || Nullable.GetUnderlyingType(propertyType) == typeof(decimal);
            }

            return false;
        }

        public static bool IsBool<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
        {
            if (expression.Body is MemberExpression memEx && memEx.Member is PropertyInfo propInfo)
            {
                var propertyType = propInfo.PropertyType;

                return propertyType == typeof(bool) || Nullable.GetUnderlyingType(propertyType) == typeof(bool);
            }

            return false;
        }

        public static bool IsDateTime<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
        {
            if (expression.Body is MemberExpression memEx && memEx.Member is PropertyInfo propInfo)
            {
                var propertyType = propInfo.PropertyType;

                return
                    propertyType == typeof(DateTime) || Nullable.GetUnderlyingType(propertyType) == typeof(DateTime) ||
                    propertyType == typeof(DateTimeOffset) || Nullable.GetUnderlyingType(propertyType) == typeof(DateTimeOffset);
            }

            return false;
        }

        private class PropertyPathVisitor : ExpressionVisitor
        {
            internal readonly List<string> PathBuilder = new List<string>();

            protected override Expression VisitMember(MemberExpression node)
            {
                if (!(node.Member is PropertyInfo))
                {
                    throw new ArgumentException("The path can only contain properties", nameof(node));
                }

                PathBuilder.Insert(0, node.Member.Name);

                return base.VisitMember(node);
            }
        }
    }
}
