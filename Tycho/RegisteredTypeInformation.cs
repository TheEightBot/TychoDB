using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Tycho
{
    public class RegisteredTypeInformation
    {
        private Delegate IdSelector { get; set; }

        private Delegate IdComparer { get; set; }

        public bool RequiresIdMapping { get; private set; }

        public string IdProperty { get; private set; }

        public string IdPropertyPath { get; private set; }

        public bool IsNumeric { get; private set; }

        public bool IsBool { get; private set; }

        public string TypeFullName { get; private set; }

        public string TypeName { get; private set; }

        public string SafeTypeName { get; private set; }

        public string TypeNamespace { get; private set; }

        public Type ObjectType { get; private set; }

        public Func<T, object> GetIdSelector<T>()
        {
            if (RequiresIdMapping)
            {
                throw new TychoDbException($"An id mapping has not been provided for {TypeName}");
            }

            return (Func<T, object>)IdSelector;
        }

        public object GetIdFor<T>(T obj)
        {
            return GetIdSelector<T>().Invoke(obj);
        }

        public bool CompareIdsFor<T>(T obj1, T obj2)
        {
            if (RequiresIdMapping)
            {
                throw new TychoDbException($"An id mapping has not been provided for {TypeName}");
            }

            var id1 = GetIdFor(obj1);
            var id2 = GetIdFor(obj2);

            return ((Func<object, object, bool>)IdComparer).Invoke(id1, id2);
        }

        public static RegisteredTypeInformation Create<T, TId>(
            Expression<Func<T, object>> idProperty,
            EqualityComparer<TId> idComparer = null)
        {
            if (idProperty is LambdaExpression lex)
            {
                var type = typeof(T);

                var compiledExpression = lex.Compile();

                if (idComparer == null)
                {
                    idComparer = EqualityComparer<TId>.Default;
                }

                var idComparerFunc =
                    new Func<object, object, bool>(
                        (x1, x2) =>
                            x1 is TId id1 && x2 is TId id2 &&
                            idComparer.Equals(id1, id2));

                var rti =
                    new RegisteredTypeInformation
                    {
                        RequiresIdMapping = false,
                        IdSelector = compiledExpression,
                        IdComparer = idComparerFunc,
                        IdProperty = idProperty.GetExpressionMemberName(),
                        IdPropertyPath = QueryPropertyPath.BuildPath(idProperty),
                        IsNumeric = QueryPropertyPath.IsNumeric(idProperty),
                        IsBool = QueryPropertyPath.IsBool(idProperty),
                        TypeFullName = type.FullName,
                        TypeName = type.Name,
                        SafeTypeName = type.GetSafeTypeName(),
                        TypeNamespace = type.Namespace,
                        ObjectType = type,
                    };

                return rti;
            }

            throw new ArgumentException($"The expression provided is not a lambda expression for {typeof(T).Name}", nameof(idProperty));
        }

        public static RegisteredTypeInformation CreateFromFunc<T>(
            Func<T, object> keySelector,
            EqualityComparer<string> idComparer = null)
        {
            var type = typeof(T);

            if (idComparer == null)
            {
                idComparer = EqualityComparer<string>.Default;
            }

            var idComparerFunc =
                new Func<object, object, bool>(
                    (x1, x2) =>
                        x1 is string id1 && x2 is string id2 &&
                        idComparer.Equals(id1, id2));

            return
                new RegisteredTypeInformation
                {
                    RequiresIdMapping = false,
                    IdSelector = keySelector,
                    IdComparer = idComparerFunc,
                    TypeFullName = type.FullName,
                    TypeName = type.Name,
                    SafeTypeName = type.GetSafeTypeName(),
                    TypeNamespace = type.Namespace,
                    ObjectType = type,
                };
        }

        public static RegisteredTypeInformation Create<T>()
        {
            var type = typeof(T);

            return
                new RegisteredTypeInformation
                {
                    RequiresIdMapping = true,
                    TypeFullName = type.FullName,
                    TypeName = type.Name,
                    SafeTypeName = type.GetSafeTypeName(),
                    TypeNamespace = type.Namespace,
                    ObjectType = type,
                };
        }
    }
}
