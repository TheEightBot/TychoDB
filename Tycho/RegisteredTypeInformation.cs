using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Tycho
{
    internal struct RegisteredTypeInformation
    {
        public Delegate FuncIdSelector { get; private set; }

        public bool RequiresIdMapping { get; private set; }

        public string IdProperty { get; private set; }

        public string IdPropertyPath { get; private set; }

        public bool IsNumeric { get; private set; }

        public bool IsBool { get; private set; }

        public string TypeFullName { get; private set; }

        public string TypeName { get; private set; }

        public string TypeNamespace { get; private set; }

        public Type ObjectType { get; private set; }

        public static RegisteredTypeInformation Create<T, TId> (Expression<Func<T, TId>> idProperty)
        {
            if (idProperty is LambdaExpression lex)
            {
                var compiledExpression = lex.Compile ();
                var type = typeof (T);

                var rti =
                    new RegisteredTypeInformation
                    {
                        RequiresIdMapping = false,
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

        public static RegisteredTypeInformation CreateFromFunc<T>(Func<T, object> keySelector)
        {
            var type = typeof(T);

            var rti =
                new RegisteredTypeInformation
                {
                    RequiresIdMapping = false,
                    FuncIdSelector = keySelector,
                    TypeFullName = type.FullName,
                    TypeName = type.Name,
                    TypeNamespace = type.Namespace,
                    ObjectType = type
                };

            return rti;
        }

        public static RegisteredTypeInformation Create<T>()
        {
            var type = typeof(T);

            var rti =
                new RegisteredTypeInformation
                {
                    RequiresIdMapping = true,
                    TypeFullName = type.FullName,
                    TypeName = type.Name,
                    TypeNamespace = type.Namespace,
                    ObjectType = type
                };

            return rti;
        }

        public Func<T, object> GetId<T> ()
        {
            if(RequiresIdMapping)
            {
                throw new TychoDbException($"An id mapping has not been provided for {TypeName}");
            }

            return  (Func<T, object>)FuncIdSelector;
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
