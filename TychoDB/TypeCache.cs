namespace TychoDB;

/// <summary>
/// Static generic type cache for avoiding repeated typeof(T).FullName allocations.
/// Each closed generic type (TypeCache&lt;Person&gt;, TypeCache&lt;Order&gt;, etc.) gets its own static field.
/// </summary>
internal static class TypeCache<T>
{
    /// <summary>
    /// Gets the cached full name of type T.
    /// </summary>
    public static readonly string FullName = typeof(T).FullName ?? string.Empty;
}
