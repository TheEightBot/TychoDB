using System.Collections.Generic;

namespace TychoDB;

/// <summary>
/// Collects filter comparison values so they can be bound to a command as
/// parameters instead of being concatenated into the SQL text. Decoupled from
/// the concrete command type so the filter/sort builders stay free of a direct
/// data-provider dependency; the call site applies the collected values.
/// </summary>
internal sealed class FilterParameters
{
    /// <summary>
    /// Prefix used for auto-generated filter parameter names. Chosen to not
    /// collide with the fixed parameters ($key, $partition, $fullTypeName, ...).
    /// </summary>
    public const string ParameterPrefix = "$fp";

    private readonly List<object?> _values = new();

    public int Count => _values.Count;

    public IReadOnlyList<object?> Values => _values;

    /// <summary>
    /// Registers a value and returns the placeholder name to emit into the SQL text.
    /// </summary>
    public string Add(object? value)
    {
        int index = _values.Count;
        _values.Add(value);
        return ParameterPrefix + index.ToString(System.Globalization.CultureInfo.InvariantCulture);
    }

    public void Clear() => _values.Clear();
}
