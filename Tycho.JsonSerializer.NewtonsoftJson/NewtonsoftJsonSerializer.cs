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

        public NewtonsoftJsonSerializer (JsonSerializer jsonSerializer = null)
        {
            _jsonSerializer =
                jsonSerializer ??
                new JsonSerializer
                {
                    DefaultValueHandling = DefaultValueHandling.Include,
                    NullValueHandling = NullValueHandling.Ignore,
                };
        }


        public ValueTask<T> DeserializeAsync<T> (Stream stream, CancellationToken cancellationToken)
        {
            using var streamReader = new StreamReader (stream);
            using var jsonTextReader = new JsonTextReader (streamReader);
            return new ValueTask<T> (_jsonSerializer.Deserialize<T> (jsonTextReader));
        }      

        public object Serialize<T> (T obj)
        {
            return JsonConvert.SerializeObject(obj);
        }

        public override string ToString () => nameof (NewtonsoftJsonSerializer);
    }
}