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
        public static string BuildPath<TPathObj, TProp> (Expression<Func<TPathObj, TProp>> expression)
        {
            var visitor = new PropertyPathVisitor ();

            visitor.Visit (expression.Body);

            return $"$.{string.Join('.', visitor.PathBuilder)}";
        }

        public static bool IsNumeric<TPathObj, TProp> (Expression<Func<TPathObj, TProp>> expression)
        {
            var visitor = new PropertyIsNumericVisitor ();

            visitor.Visit (expression.Body);

            return visitor.IsNumeric ?? false;
        }

        public static bool IsBool<TPathObj, TProp>(Expression<Func<TPathObj, TProp>> expression)
        {
            var visitor = new PropertyIsBoolVisitor();

            visitor.Visit(expression.Body);

            return visitor.IsBool ?? false;
        }

        private class PropertyPathVisitor : ExpressionVisitor
        {
            internal readonly List<string> PathBuilder = new List<string>();

            protected override Expression VisitMember (MemberExpression node)
            {
                if (!(node.Member is PropertyInfo))
                {
                    throw new ArgumentException ("The path can only contain properties", nameof (node));
                }

                PathBuilder.Insert(0, node.Member.Name);

                return base.VisitMember (node);
            }
        }

        private class PropertyIsNumericVisitor : ExpressionVisitor
        {
            internal bool? IsNumeric;

            protected override Expression VisitMember (MemberExpression node)
            {
                if (!(node.Member is PropertyInfo))
                {
                    throw new ArgumentException ("The path can only contain properties", nameof (node));
                }

                var propertyType = ((PropertyInfo)node.Member).PropertyType;

                if(!IsNumeric.HasValue)
                {
                    IsNumeric =
                        propertyType == typeof (int) || propertyType == typeof (uint) || propertyType == typeof (long) || propertyType == typeof (ulong) ||
                        propertyType == typeof (double) || propertyType == typeof (float) || propertyType == typeof (decimal) ||
                        propertyType == typeof (Single);
                }

                return base.VisitMember (node);
            }
        }

        private class PropertyIsBoolVisitor : ExpressionVisitor
        {
            internal bool? IsBool;

            protected override Expression VisitMember(MemberExpression node)
            {
                if (!(node.Member is PropertyInfo))
                {
                    throw new ArgumentException("The path can only contain properties", nameof(node));
                }

                var propertyType = ((PropertyInfo)node.Member).PropertyType;

                if (!IsBool.HasValue)
                {
                    //TODO: Should we add comparisons for numerics of 0/1???
                    IsBool = propertyType == typeof(bool);
                }

                return base.VisitMember(node);
            }
        }
    }
}
