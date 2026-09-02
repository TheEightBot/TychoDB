namespace TychoDB;

/// <summary>
/// What <see cref="Tycho.UpsertObjectAsync{T}(T, string?, bool, System.Threading.CancellationToken)"/>
/// did to the row for the object's key.
/// </summary>
public enum UpsertResult
{
    /// <summary>No row existed for the key in that partition; one was created.</summary>
    Inserted,

    /// <summary>A row already existed for the key in that partition; its data was replaced.</summary>
    Updated,
}
