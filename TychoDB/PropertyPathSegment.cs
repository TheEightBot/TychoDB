using System;

namespace TychoDB;

/// <summary>
/// One property in a JSON path, kept as the CLR name plus the type that declares it.
/// </summary>
/// <remarks>
/// Filters and sorts are constructed before the serializer is known, so a path cannot be
/// rendered when the expression is supplied. Holding the declaring type lets the JSON member
/// name be resolved later, per segment — a nested path crosses several types and each one may
/// be named differently.
/// </remarks>
public readonly struct PropertyPathSegment : IEquatable<PropertyPathSegment>
{
    public PropertyPathSegment(Type declaringType, string clrName)
    {
        DeclaringType = declaringType;
        ClrName = clrName;
    }

    /// <summary>Gets the type declaring the property.</summary>
    public Type DeclaringType { get; }

    /// <summary>Gets the CLR property name.</summary>
    public string ClrName { get; }

    public static bool operator ==(PropertyPathSegment left, PropertyPathSegment right) => left.Equals(right);

    public static bool operator !=(PropertyPathSegment left, PropertyPathSegment right) => !left.Equals(right);

    public bool Equals(PropertyPathSegment other)
        => DeclaringType == other.DeclaringType && string.Equals(ClrName, other.ClrName, StringComparison.Ordinal);

    public override bool Equals(object? obj) => obj is PropertyPathSegment other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(DeclaringType, ClrName);

    public override string ToString() => $"{DeclaringType?.Name}.{ClrName}";
}
