using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Tycho
{
    public class NewtonsoftJsonSerializer : IJsonSerializer
    {
        private readonly JsonSerializer _jsonSerializer;

        private readonly JsonSerializerSettings _jsonSerializerSettings;

        public string DateTimeSerializationFormat
        {
            get => _jsonSerializerSettings.DateFormatString;
            set => _jsonSerializerSettings.DateFormatString = value;
        }

        public NewtonsoftJsonSerializer(JsonSerializer jsonSerializer = null, JsonSerializerSettings jsonSerializerSettings = null)
        {
            _jsonSerializer =
                jsonSerializer ??
                new JsonSerializer
                {
                    DefaultValueHandling = DefaultValueHandling.Include,
                    NullValueHandling = NullValueHandling.Ignore,
                };

            _jsonSerializerSettings =
                jsonSerializerSettings ??
                new JsonSerializerSettings
                {
                    DefaultValueHandling = DefaultValueHandling.Include,
                    NullValueHandling = NullValueHandling.Include,
                };
        }

        public ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken)
        {
            using var streamReader = new StreamReader(stream);
            using var jsonTextReader = new JsonTextReader(streamReader);
            return new ValueTask<T>(_jsonSerializer.Deserialize<T>(jsonTextReader));
        }

        public object Serialize<T>(T obj)
        {
            return JsonConvert.SerializeObject(obj, _jsonSerializerSettings);
        }

        public override string ToString() => nameof(NewtonsoftJsonSerializer);
    }
}