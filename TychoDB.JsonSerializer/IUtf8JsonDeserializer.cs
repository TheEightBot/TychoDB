using System;

namespace TychoDB;

/// <summary>
/// Optional capability for serializers that can deserialize synchronously from a UTF-8 span.
/// </summary>
/// <remarks>
/// Kept separate from <see cref="IJsonSerializer"/> on purpose. The bulk read path deserializes
/// one already-materialized row buffer at a time, where the async path costs a state-machine
/// transition per row for no benefit. Expressing that as its own interface means:
/// <list type="bullet">
/// <item>serializers written against <see cref="IJsonSerializer"/> alone keep working unchanged
/// (Tycho feature-detects and falls back to the streaming path), so this is not a breaking
/// change; and</item>
/// <item>there is no default implementation bridging sync to async — blocking on
/// <see cref="IJsonSerializer.DeserializeAsync{T}"/> would risk deadlocks under a synchronization
/// context and thread-pool starvation under load.</item>
/// </list>
/// </remarks>
public interface IUtf8JsonDeserializer
{
    /// <summary>
    /// Deserializes an object from a UTF-8 encoded JSON span.
    /// </summary>
    T Deserialize<T>(ReadOnlySpan<byte> utf8Json);
}
