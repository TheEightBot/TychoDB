using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace TychoDB;

public record RegisteredTypeInformation
{
    public RegisteredTypeInformation(bool requiresIdMapping, Delegate? idSelector, Delegate? idComparer, string? idProperty, string? idPropertyPath, bool isNumeric, bool isBool, string? typeFullName, string? typeName, string? safeTypeName, string? typeNamespace, Type objectType)
    {
        this.RequiresIdMapping = requiresIdMapping;
        this.IdSelector = idSelector;
        this.IdComparer = idComparer;
        this.IdProperty = idProperty;
        this.IdPropertyPath = idPropertyPath;
        this.IsNumeric = isNumeric;
        this.IsBool = isBool;
        this.TypeFullName = typeFullName;
        this.TypeName = typeName;
        this.SafeTypeName = safeTypeName;
        this.TypeNamespace = typeNamespace;
        this.ObjectType = objectType;
    }

    public RegisteredTypeInformation(bool requiresIdMapping, Delegate? idSelector, Delegate? idComparer, string? typeFullName, string? typeName, string? safeTypeName, string? typeNamespace, Type objectType)
    {
        this.RequiresIdMapping = requiresIdMapping;
        this.IdSelector = idSelector;
        this.IdComparer = idComparer;
        this.TypeFullName = typeFullName;
        this.TypeName = typeName;
        this.SafeTypeName = safeTypeName;
        this.TypeNamespace = typeNamespace;
        this.ObjectType = objectType;
    }

    public RegisteredTypeInformation(bool requiresIdMapping, string? typeFullName, string? typeName, string? safeTypeName, string? typeNamespace, Type objectType)
    {
        this.RequiresIdMapping = requiresIdMapping;
        this.TypeFullName = typeFullName;
        this.TypeName = typeName;
        this.SafeTypeName = safeTypeName;
        this.TypeNamespace = typeNamespace;
        this.ObjectType = objectType;
    }

    private Delegate? IdSelector { get; set; }

    private Delegate? IdComparer { get; set; }

    public bool RequiresIdMapping { get; private set; }

    public string? IdProperty { get; private set; }

    public string? IdPropertyPath { get; private set; }

    public bool IsNumeric { get; private set; }

    public bool IsBool { get; private set; }

    public string? TypeFullName { get; private set; }

    public string? TypeName { get; private set; }

    public string? SafeTypeName { get; private set; }

    public string? TypeNamespace { get; private set; }

    public Type ObjectType { get; private set; }

    public Func<T, object> GetIdSelector<T>()
    {
        if (RequiresIdMapping)
        {
            throw new TychoException($"An id mapping has not been provided for {TypeName}");
        }

        if (IdSelector is null)
        {
            throw new InvalidOperationException("IdSelector is not set.");
        }

        return (Func<T, object>)IdSelector!;
    }

    public object GetIdFor<T>(T obj)
    {
        return GetIdSelector<T>().Invoke(obj);
    }

    public bool CompareIdsFor<T>(T obj1, T obj2)
    {
        if (RequiresIdMapping)
        {
            throw new TychoException($"An id mapping has not been provided for {TypeName}");
        }

        if (IdComparer is null)
        {
            throw new InvalidOperationException("IdComparer is not set.");
        }

        var id1 = GetIdFor(obj1);
        var id2 = GetIdFor(obj2);
        return ((Func<object, object, bool>)IdComparer!).Invoke(id1, id2);
    }

    public static RegisteredTypeInformation Create<T, TId>(
        Expression<Func<T, object>> idProperty,
        EqualityComparer<TId>? idComparer = null)
    {
        if (idProperty is not LambdaExpression lex)
        {
            throw new ArgumentException($"The expression provided is not a lambda expression for {typeof(T).Name}", nameof(idProperty));
        }

        var type = typeof(T);

        var compiledExpression = lex.Compile();

        idComparer ??= EqualityComparer<TId>.Default;

        var idComparerFunc =
            new Func<object, object, bool>(
                (x1, x2) =>
                    x1 is TId id1 && x2 is TId id2 &&
                    idComparer.Equals(id1, id2));

        var rti =
            new RegisteredTypeInformation(requiresIdMapping: false, idSelector: compiledExpression!, idComparer: idComparerFunc!,
                idProperty: idProperty.GetExpressionMemberName(), idPropertyPath: QueryPropertyPath.BuildPath(idProperty),
                isNumeric: QueryPropertyPath.IsNumeric(idProperty), isBool: QueryPropertyPath.IsBool(idProperty), typeFullName: type.FullName,
                typeName: type.Name, safeTypeName: type.GetSafeTypeName(), typeNamespace: type.Namespace, objectType: type!);

        return rti;
    }

    public static RegisteredTypeInformation CreateFromFunc<T>(
        Func<T, object> keySelector,
        EqualityComparer<string>? idComparer = null)
    {
        var type = typeof(T);

        idComparer ??= EqualityComparer<string>.Default;

        var idComparerFunc =
            new Func<object, object, bool>(
                (x1, x2) =>
                    x1 is string id1 && x2 is string id2 &&
                    idComparer.Equals(id1, id2));

        return
            new RegisteredTypeInformation(requiresIdMapping: false, idSelector: keySelector!, idComparer: idComparerFunc!, typeFullName: type.FullName,
                typeName: type.Name, safeTypeName: type.GetSafeTypeName(), typeNamespace: type.Namespace, objectType: type!);
    }

    public static RegisteredTypeInformation Create<T>()
    {
        var type = typeof(T);

        return
            new RegisteredTypeInformation(requiresIdMapping: true, typeFullName: type.FullName, typeName: type.Name, safeTypeName: type.GetSafeTypeName(),
                typeNamespace: type.Namespace, objectType: type!);
    }
}
