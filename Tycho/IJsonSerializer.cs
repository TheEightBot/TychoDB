using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Tycho
{
    public interface IJsonSerializer
    {
        object Serialize<T> (T obj);
        ValueTask<T> DeserializeAsync<T> (Stream stream, CancellationToken cancellationToken);
    }
}
