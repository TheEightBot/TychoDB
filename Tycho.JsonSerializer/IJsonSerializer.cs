using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Tycho
{
    public interface IJsonSerializer
    {
        string DateTimeSerializationFormat { get; }
        object Serialize<T> (T obj);
        ValueTask<T> DeserializeAsync<T> (Stream stream, CancellationToken cancellationToken);
    }
}
