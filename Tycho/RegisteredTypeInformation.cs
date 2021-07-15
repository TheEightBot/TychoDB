using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Tycho
{
    internal struct RegisteredTypeInformation
    {
        public Delegate FuncIdSelector { get; set; }

        public bool RequiresIdMapping { get; set; }

        public string IdProperty { get; set; }

        public string TypeFullName { get; set; }

        public string TypeName { get; set; }

        public string TypeNamespace { get; set; }

        public Type ObjectType { get; set; }

        public static RegisteredTypeInformation Create<T, TId> (Expression<Func<T, TId>> idProperty)
        {
            if (idProperty is LambdaExpression lex)
            {
                var compiledExpression = lex.Compile ();
                var type = typeof (T);

                var rti =
                    new RegisteredTypeInformation
                    {
                        FuncIdSelector = compiledExpression,
                        IdProperty = GetMemberInfo(idProperty).Member.Name,
                        TypeFullName = type.FullName,
                        TypeName = type.Name,
                        TypeNamespace = type.Namespace,
                        ObjectType = type
                    };

                return rti;
            }

            throw new ArgumentException ($"The expression provided is not a lambda expression for {typeof (T).Name}", nameof (idProperty));
        }

        public Func<T, object> GetId<T> ()
        {
            return (Func<T, object>)FuncIdSelector;
        }

        private static MemberExpression GetMemberInfo (Expression method)
        {
            LambdaExpression lambda = method as LambdaExpression;
            if (lambda == null)
                throw new ArgumentNullException ("method");

            MemberExpression memberExpr = null;

            if (lambda.Body.NodeType == ExpressionType.Convert)
            {
                memberExpr =
                    ((UnaryExpression)lambda.Body).Operand as MemberExpression;
            }
            else if (lambda.Body.NodeType == ExpressionType.MemberAccess)
            {
                memberExpr = lambda.Body as MemberExpression;
            }

            if (memberExpr == null)
                throw new ArgumentException ("method");

            return memberExpr;
        }
    }
}
