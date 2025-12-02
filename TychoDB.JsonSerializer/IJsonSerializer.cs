using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace TychoDB;

public interface IJsonSerializer
{
    string DateTimeSerializationFormat { get; }

    /// <summary>
    /// Serializes an object to a byte array.
    /// </summary>
    object Serialize<T>(T obj);

    /// <summary>
    /// Serializes an object directly to an IBufferWriter to avoid intermediate allocations.
    /// </summary>
    void Serialize<T>(T obj, IBufferWriter<byte> bufferWriter);

    ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken);
}
