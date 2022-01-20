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

        public bool CompareIdsFor(object id1, object id2)
        {
            if (RequiresIdMapping)
            {
                throw new TychoDbException($"An id mapping has not been provided for {TypeName}");
            }

            return ((Func<object, object, bool>)IdComparer).Invoke(id1, id2);
        }

        private RegisteredTypeInformation()
        {

        }

        public static RegisteredTypeInformation Create<T, TId>(
            Expression<Func<T, TId>> idProperty,
            Func<TId, TId, bool> idComparer = null)
        {
            if (idProperty is LambdaExpression lex)
            {
                var type = typeof(T);

                var compiledExpression = lex.Compile();

                if (idComparer == null)
                {
                    idComparer =
                        new Func<TId, TId, bool>((x1, x2) => EqualityComparer<TId>.Default.Equals(x1, x2));
                }

                var rti =
                    new RegisteredTypeInformation
                    {
                        RequiresIdMapping = false,
                        IdSelector = compiledExpression,
                        IdComparer = idComparer,
                        IdProperty = idProperty.GetExpressionMemberName(),
                        IdPropertyPath = QueryPropertyPath.BuildPath(idProperty),
                        IsNumeric = QueryPropertyPath.IsNumeric(idProperty),
                        IsBool = QueryPropertyPath.IsBool(idProperty),
                        TypeFullName = type.FullName,
                        TypeName = type.Name,
                        SafeTypeName = type.GetSafeTypeName(),
                        TypeNamespace = type.Namespace,
                        ObjectType = type
                    };

                return rti;
            }

            throw new ArgumentException($"The expression provided is not a lambda expression for {typeof(T).Name}", nameof(idProperty));
        }

        public static RegisteredTypeInformation CreateFromFunc<T, TId>(
            Func<T, TId> keySelector,
            Func<TId, TId, bool> idComparer = null)
        {
            var type = typeof(T);

            if (idComparer == null)
            {
                idComparer =
                    new Func<TId, TId, bool>((x1, x2) => EqualityComparer<TId>.Default.Equals(x1, x2));
            }

            var rti =
                new RegisteredTypeInformation
                {
                    RequiresIdMapping = false,
                    IdSelector = keySelector,
                    IdComparer = idComparer,
                    TypeFullName = type.FullName,
                    TypeName = type.Name,
                    SafeTypeName = type.GetSafeTypeName(),
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
                    SafeTypeName = type.GetSafeTypeName(),
                    TypeNamespace = type.Namespace,
                    ObjectType = type
                };

            return rti;
        }

    }
}
