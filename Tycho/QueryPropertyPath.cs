using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Tycho
{
    internal static class QueryPropertyPath
    {
        public static string BuildPath<TPathObj, TProp> (Expression<Func<TPathObj, TProp>> expression)
        {
            var visitor = new PropertyVisitor ();

            visitor.Visit (expression.Body);

            visitor.PathBuilder.Reverse ();

            return $"$.{string.Join('.', visitor.PathBuilder)}";
        }

        private class PropertyVisitor : ExpressionVisitor
        {
            internal readonly List<string> PathBuilder = new List<string>();

            protected override Expression VisitMember (MemberExpression node)
            {
                if (!(node.Member is PropertyInfo))
                {
                    throw new ArgumentException ("The path can only contain properties", nameof (node));
                }

                PathBuilder.Add (node.Member.Name);

                return base.VisitMember (node);
            }
        }
    }
}
