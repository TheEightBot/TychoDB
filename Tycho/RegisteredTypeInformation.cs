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

        public static RegisteredTypeInformation Create<T> (Expression<Func<T, object>> idProperty)
        {
            if (idProperty is LambdaExpression lex && idProperty.Body is MemberExpression mex && mex.Member is PropertyInfo pi)
            {
                var compiledExpression = lex.Compile ();
                var type = typeof (T);

                var rti =
                    new RegisteredTypeInformation
                    {
                        FuncIdSelector = compiledExpression,
                        IdProperty = pi.Name,
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
    }
}
