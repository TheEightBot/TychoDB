using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
using System.Threading.Tasks;

namespace Tycho
{
    public class SystemTextJsonSerializer : IJsonSerializer
    {
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        private readonly Dictionary<Type, JsonTypeInfo> _jsonTypeSerializers;

        public string DateTimeSerializationFormat { get; }

        public SystemTextJsonSerializer(
            JsonSerializerOptions jsonSerializerOptions = null,
            Dictionary<Type, JsonTypeInfo> jsonTypeSerializers = null,
            string dateTimeSerializationFormat = "yyyy'-'MM'-'dd'T'HH':'mm':'ss.FFFFFFFK")
        {
            DateTimeSerializationFormat = dateTimeSerializationFormat;

            _jsonSerializerOptions =
                jsonSerializerOptions ??
                new JsonSerializerOptions
                {
                    IgnoreReadOnlyProperties = true,
                    NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.WriteAsString,
                    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                };

            _jsonTypeSerializers =
                jsonTypeSerializers
                ?? new Dictionary<Type, JsonTypeInfo>();
        }

        public ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken)
        {
            if (_jsonTypeSerializers.TryGetValue(typeof(T), out var jsonTypeSerializer) && jsonTypeSerializer is JsonTypeInfo<T> jtst)
            {
                return JsonSerializer.DeserializeAsync<T>(stream, jtst, cancellationToken);
            }

            return JsonSerializer.DeserializeAsync<T>(stream, _jsonSerializerOptions, cancellationToken);
        }

        public object Serialize<T>(T obj)
        {
            if (_jsonTypeSerializers.TryGetValue(typeof(T), out var jsonTypeSerializer) && jsonTypeSerializer is JsonTypeInfo<T> jtst)
            {
                return JsonSerializer.SerializeToUtf8Bytes(obj, jtst);
            }

            return JsonSerializer.SerializeToUtf8Bytes(obj, _jsonSerializerOptions);
        }

        public override string ToString() => nameof(SystemTextJsonSerializer);
    }
}
