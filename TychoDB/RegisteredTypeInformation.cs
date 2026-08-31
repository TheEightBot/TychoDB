using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace TychoDB;

public sealed record RegisteredTypeInformation
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

    /// <summary>
    /// Gets the id property captured as unresolved segments, so it can be rendered against the
    /// serializer's JSON member names at query time exactly as a filter path is. Null unless the
    /// type was registered by property expression.
    /// </summary>
    internal PropertyPathSegment[]? IdPropertyPathSegments { get; private set; }

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
                typeName: type.Name, safeTypeName: type.GetSafeTypeName(), typeNamespace: type.Namespace, objectType: type!)
            {
                IdPropertyPathSegments = QueryPropertyPath.BuildSegments(idProperty),
            };

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

    /// <summary>
    /// Registers <typeparamref name="T"/>, detecting its id property by convention.
    /// <para>
    /// The property is looked for by name, in order: <c>Id</c>, then <c>&lt;TypeName&gt;Id</c>
    /// (for example <c>PersonId</c> on <c>Person</c>), each matched case-insensitively. It must
    /// be a public, readable, non-indexed instance property.
    /// </para>
    /// <para>
    /// When no such property exists the type is still registered — satisfying
    /// <c>requireTypeRegistration</c> — but without an id mapping, so it can only be reached by
    /// an explicitly supplied key, exactly as before. That keeps registering a key-less type
    /// and writing it with a call-site key selector working.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The type to register.</typeparam>
    /// <returns>The registration information for <typeparamref name="T"/>.</returns>
    public static RegisteredTypeInformation Create<T>()
    {
        var type = typeof(T);

        var idProperty = FindConventionalIdProperty(type);

        if (idProperty is not null)
        {
            return Create<T, object>(BuildPropertySelector<T>(idProperty));
        }

        return
            new RegisteredTypeInformation(requiresIdMapping: true, typeFullName: type.FullName, typeName: type.Name, safeTypeName: type.GetSafeTypeName(),
                typeNamespace: type.Namespace, objectType: type!);
    }

    /// <summary>
    /// Finds the property a conventional id would live on: <c>Id</c>, else
    /// <c>&lt;TypeName&gt;Id</c>. Returns null when neither exists.
    /// </summary>
    private static PropertyInfo? FindConventionalIdProperty(Type type)
    {
        var properties =
            type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        // "Id" wins over "<TypeName>Id" when a type declares both, so the more specific name
        // never quietly shadows the obvious one.
        foreach (var candidate in new[] { "Id", type.Name + "Id" })
        {
            foreach (var property in properties)
            {
                if (!property.Name.Equals(candidate, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                // An id has to be readable and addressable as a plain member: an indexer or a
                // write-only property cannot be turned into a property path.
                //
                // CanRead is not enough — it is true for a private getter, and a property with a
                // public setter and a private getter is still returned by a Public lookup. Such
                // a property would compile into a working selector, but neither serializer emits
                // it, so the id would be absent from the stored document: the JSON path would
                // match nothing and every row would look divergent to the key-column rewrite's
                // probe. Requiring a public getter keeps the convention aligned with what is
                // actually written to the document.
                if (property.GetIndexParameters().Length > 0
                    || property.GetMethod is not { IsPublic: true })
                {
                    continue;
                }

                return property;
            }
        }

        return null;
    }

    /// <summary>
    /// Builds the <c>x =&gt; x.Prop</c> expression the property-based registration path expects,
    /// boxing value types through a Convert node exactly as a hand-written
    /// <c>Expression&lt;Func&lt;T, object&gt;&gt;</c> would.
    /// </summary>
    private static Expression<Func<T, object>> BuildPropertySelector<T>(PropertyInfo property)
    {
        var parameter = Expression.Parameter(typeof(T), "x");
        Expression body = Expression.Property(parameter, property);

        if (property.PropertyType.IsValueType)
        {
            body = Expression.Convert(body, typeof(object));
        }

        return Expression.Lambda<Func<T, object>>(body, parameter);
    }
}
