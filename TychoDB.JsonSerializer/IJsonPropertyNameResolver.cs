using System;

namespace TychoDB;

/// <summary>
/// Optional capability for serializers that can report the JSON member name a CLR
/// property is serialized as.
/// </summary>
/// <remarks>
/// <para>
/// Tycho translates property expressions such as <c>x =&gt; x.Description</c> into SQLite JSON
/// paths (<c>$.Description</c>) that are evaluated against the stored document. Without this
/// capability the path can only be built from the CLR property name, so any serializer
/// configuration that renames members — a <c>PropertyNamingPolicy</c>, a
/// <c>[JsonPropertyName]</c>/<c>[JsonProperty]</c> attribute, or a custom contract resolver —
/// produces a path that matches nothing in the stored JSON. Because a JSON path that matches
/// nothing is not an error in SQLite, that mismatch surfaces as silently empty query results,
/// unsorted results, and indexes that never match a row rather than as an exception.
/// </para>
/// <para>
/// Kept separate from <see cref="IJsonSerializer"/> on purpose, mirroring
/// <see cref="IUtf8JsonDeserializer"/>: serializers written against
/// <see cref="IJsonSerializer"/> alone keep working unchanged (Tycho feature-detects this
/// interface and falls back to the CLR property name), so adding it is not a breaking change
/// for third-party serializer implementations.
/// </para>
/// <para>
/// Implementations are called while queries are being built and should be cheap and
/// thread-safe; caching the mapping per type is expected.
/// </para>
/// </remarks>
public interface IJsonPropertyNameResolver
{
    /// <summary>
    /// Returns the JSON member name that <paramref name="clrPropertyName"/> is serialized as
    /// when it is declared on <paramref name="declaringType"/>.
    /// </summary>
    /// <param name="declaringType">The type declaring the property.</param>
    /// <param name="clrPropertyName">The CLR property name.</param>
    /// <returns>
    /// The JSON member name, or <see langword="null"/> if the name cannot be determined — for
    /// example when the property is not serialized, or when the serializer has no metadata for
    /// the type. Callers fall back to <paramref name="clrPropertyName"/> in that case.
    /// </returns>
    string ResolvePropertyName(Type declaringType, string clrPropertyName);
}
