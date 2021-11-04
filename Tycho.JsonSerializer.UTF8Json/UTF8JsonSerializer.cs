using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Utf8Json;

namespace Tycho
{
    public class UTF8JsonSerializer : IJsonSerializer
    {
        private readonly IJsonFormatterResolver _jsonFormatterResolver;

        public string DateTimeSerializationFormat => "O";

        public UTF8JsonSerializer(IJsonFormatterResolver jsonFormatterResolver = null)
        {
            _jsonFormatterResolver =
                jsonFormatterResolver ??
                Utf8Json.JsonSerializer.DefaultResolver;
        }

        public ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken)
        {
            return new ValueTask<T>(Utf8Json.JsonSerializer.DeserializeAsync<T>(stream, _jsonFormatterResolver));
        }

        public object Serialize<T>(T obj)
        {
            return Utf8Json.JsonSerializer.Serialize(obj);
        }

        public override string ToString() => nameof(UTF8JsonSerializer);
    }
}

