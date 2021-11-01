using System;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Tycho
{
    public class SystemTextJsonSerializer : IJsonSerializer
    {
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public string DateTimeSerializationFormat => "O";

        public SystemTextJsonSerializer(JsonSerializerOptions jsonSerializerOptions = null)
        {
            _jsonSerializerOptions =
                jsonSerializerOptions ??
                new JsonSerializerOptions
                {
                    IgnoreReadOnlyProperties = true,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                };
        }

        public ValueTask<T> DeserializeAsync<T> (Stream stream, CancellationToken cancellationToken)
        {
            return JsonSerializer.DeserializeAsync<T> (stream, _jsonSerializerOptions, cancellationToken);
        }

        public object Serialize<T> (T obj)
        {
            return JsonSerializer.SerializeToUtf8Bytes (obj, _jsonSerializerOptions);
        }

        public override string ToString () => nameof (SystemTextJsonSerializer);
    }
}
