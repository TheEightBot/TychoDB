using System;

namespace TychoDB;

/// <summary>
/// Optional capability for serializers that can report how a CLR value appears in JSON.
/// </summary>
/// <remarks>
/// <para>
/// A filter compares a caller-supplied CLR value against a value inside the stored document,
/// so the comparison is only meaningful in the JSON form the serializer produced. Without this
/// capability the value can only be rendered with <see cref="object.ToString"/>, which differs
/// from the JSON form for any type the serializer treats specially — an enum written as a
/// number (<c>1</c> versus <c>"Green"</c>), an enum renamed by a converter or naming policy,
/// or a <c>DateOnly</c>/<c>TimeOnly</c> written in ISO form while <c>ToString()</c> yields a
/// culture-dependent one. As with property paths, a comparison that matches nothing is not an
/// error in SQLite, so the mismatch surfaces as silently empty results.
/// </para>
/// <para>
/// Kept separate from <see cref="IJsonSerializer"/> on purpose, mirroring
/// <see cref="IUtf8JsonDeserializer"/> and <see cref="IJsonPropertyNameResolver"/>: serializers
/// that do not implement it keep working unchanged, falling back to <c>ToString()</c>.
/// </para>
/// </remarks>
public interface IJsonValueResolver
{
    /// <summary>
    /// Resolves <paramref name="value"/> to the scalar it becomes in JSON.
    /// </summary>
    /// <param name="value">The CLR value to resolve. Never <see langword="null"/>.</param>
    /// <param name="jsonValue">
    /// When this method returns <see langword="true"/>, the scalar JSON form: a
    /// <see cref="string"/>, <see cref="long"/>, <see cref="double"/>, <see cref="bool"/>, or
    /// <see langword="null"/> for JSON null. Strings are returned decoded, not quoted, because
    /// that is the form SQLite's JSON functions yield when reading the stored document.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the value has a scalar JSON form; <see langword="false"/> if
    /// it serializes to an object or array, or cannot be serialized, in which case the caller
    /// falls back to <see cref="object.ToString"/>.
    /// </returns>
    bool TryResolveJsonValue(object value, out object jsonValue);
}
