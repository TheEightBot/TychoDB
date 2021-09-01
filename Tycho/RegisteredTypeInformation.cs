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

        public string IdPropertyPath { get; set; }

        public bool IsNumeric { get; set; }

        public bool IsBool { get; set; }

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
                        IdProperty = GetExpressionMemberName (idProperty),
                        IdPropertyPath = QueryPropertyPath.BuildPath(idProperty),
                        IsNumeric = QueryPropertyPath.IsNumeric(idProperty),
                        IsBool = QueryPropertyPath.IsBool(idProperty),
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

        private static string GetExpressionMemberName (Expression method)
        {
            if(method is LambdaExpression lex)
            {
                if (lex.Body.NodeType == ExpressionType.Convert)
                {
                    return (((UnaryExpression)lex.Body).Operand as MemberExpression).Member.Name;
                }

                if (lex.Body.NodeType == ExpressionType.MemberAccess)
                {
                    return (lex.Body as MemberExpression).Member.Name;
                }
            }

            throw new TychoDbException ("The provided expression is not valid member expression");
        }
    }
}
