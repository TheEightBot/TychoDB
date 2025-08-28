using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace TychoDB;

public class NewtonsoftJsonSerializer : IJsonSerializer
{
    // Buffer pool for serialization work
    private static readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;

    private readonly JsonSerializer _jsonSerializer;
    private readonly JsonSerializerSettings _jsonSerializerSettings;

    public string DateTimeSerializationFormat { get; }

    public NewtonsoftJsonSerializer(
        JsonSerializer jsonSerializer = null,
        JsonSerializerSettings jsonSerializerSettings = null,
        string dateTimeSerializationFormat = "O")
    {
        DateTimeSerializationFormat = dateTimeSerializationFormat;

        _jsonSerializer =
            jsonSerializer ??
            new JsonSerializer
            {
                DefaultValueHandling = DefaultValueHandling.Include,
                NullValueHandling = NullValueHandling.Ignore,
                DateFormatString = dateTimeSerializationFormat,

                // Performance optimizations
                MaxDepth = 64, // Higher than default for complex objects
                CheckAdditionalContent = false, // Faster but less strict
                TypeNameHandling = TypeNameHandling.None, // Faster without type information
                MetadataPropertyHandling = MetadataPropertyHandling.Ignore, // Faster without metadata
            };

        _jsonSerializerSettings =
            jsonSerializerSettings ??
            new JsonSerializerSettings
            {
                DefaultValueHandling = DefaultValueHandling.Include,
                NullValueHandling = NullValueHandling.Include,
                DateFormatString = dateTimeSerializationFormat,

                // Performance optimizations
                MaxDepth = 64,
                CheckAdditionalContent = false,
                TypeNameHandling = TypeNameHandling.None,
                MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
                Formatting = Formatting.None, // No indentation for better performance
            };
    }

    public ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken)
    {
        using var streamReader = new StreamReader(stream, Encoding.UTF8, true, 1024, true);
        using var jsonTextReader = new JsonTextReader(streamReader)
        {
            DateFormatString = DateTimeSerializationFormat,
            CloseInput = false,
        };

        try
        {
            var result = _jsonSerializer.Deserialize<T>(jsonTextReader);
            return new ValueTask<T>(result);
        }
        finally
        {
            jsonTextReader.Close();
        }
    }

    public object Serialize<T>(T obj)
    {
        // Estimate size based on object complexity
        int estimatedSize = EstimateSerializedSize(obj);
        byte[] byteBuffer = _bytePool.Rent(estimatedSize);

        try
        {
            using (var ms = new MemoryStream(byteBuffer, 0, estimatedSize, true))
            {
                using (var sw = new StreamWriter(ms, Encoding.UTF8, 1024, true))
                {
                    var jsonTextWriter = new JsonTextWriter(sw)
                    {
                        DateFormatString = DateTimeSerializationFormat,
                        Formatting = Formatting.None,
                    };

                    // Serialize using pooled writer
                    _jsonSerializer.Serialize(jsonTextWriter, obj);
                    jsonTextWriter.Flush();
                    sw.Flush();

                    // Copy to right-sized array
                    byte[] result = new byte[ms.Position];
                    Buffer.BlockCopy(byteBuffer, 0, result, 0, (int)ms.Position);
                    return result;
                }
            }
        }
        catch
        {
            // If pooled serialization fails, fall back to standard method
            byte[] jsonBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj, _jsonSerializerSettings));
            return jsonBytes;
        }
        finally
        {
            // Always return the buffer to the pool
            _bytePool.Return(byteBuffer);
        }
    }

    private int EstimateSerializedSize<T>(T obj)
    {
        // Estimate the size based on object type
        if (obj == null)
        {
            return 16; // "null" plus some overhead
        }

        Type type = typeof(T);

        // For strings, we can make a good guess
        if (type == typeof(string))
        {
            return ((string)(object)obj).Length * 2; // Allow for escaping
        }

        // For collections, try to estimate based on count
        if (obj is System.Collections.ICollection collection)
        {
            return Math.Max(32, collection.Count * 32); // Rough estimate
        }

        // For simple value types like int, bool, etc.
        if (type.IsPrimitive || type.IsEnum)
        {
            return 16;
        }

        // Default buffer size for complex objects - adjust based on your typical object size
        return 4096;
    }

    public override string ToString() => nameof(NewtonsoftJsonSerializer);
}
