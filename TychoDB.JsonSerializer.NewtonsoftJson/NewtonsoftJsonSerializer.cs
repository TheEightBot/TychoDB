using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace TychoDB;

public sealed class NewtonsoftJsonSerializer : IJsonSerializer, IUtf8JsonDeserializer, IJsonPropertyNameResolver
{
    private const int DefaultBufferSize = 4096;
    private const int StreamWriterBufferSize = 1024;

    // UTF-8 without a byte-order mark. Encoding.UTF8 emits a BOM (EF BB BF) as its
    // preamble, which StreamWriter writes ahead of the JSON. That BOM ends up in the
    // stored blob and is passed to SQLite's json($json) as a BLOB argument, where a
    // leading BOM is not valid JSON/JSONB and is rejected as "malformed JSON" on
    // stricter SQLite builds. Serialize clean UTF-8 bytes instead.
    private static readonly Encoding Utf8NoBom = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: false);

    // Maps CLR property name -> JSON member name, per type. ResolveContract walks the
    // contract resolver, which query building would otherwise repeat per filter clause.
    private readonly ConcurrentDictionary<Type, Dictionary<string, string>> _jsonPropertyNames = new();

    private readonly JsonSerializer _jsonSerializer;

    public string DateTimeSerializationFormat { get; }

    public NewtonsoftJsonSerializer(
        JsonSerializer jsonSerializer = null,
        string dateTimeSerializationFormat = "O")
    {
        DateTimeSerializationFormat = dateTimeSerializationFormat;

        _jsonSerializer = jsonSerializer ?? CreateDefaultSerializer(dateTimeSerializationFormat);
    }

    private static JsonSerializer CreateDefaultSerializer(string dateTimeFormat) =>
        new()
        {
            DefaultValueHandling = DefaultValueHandling.Include,
            NullValueHandling = NullValueHandling.Ignore,
            DateFormatString = dateTimeFormat,
            MaxDepth = 64,
            CheckAdditionalContent = false,
            TypeNameHandling = TypeNameHandling.None,
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            Formatting = Formatting.None,
        };

    public ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken)
    {
        using var streamReader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, StreamWriterBufferSize, leaveOpen: true);
        using var jsonTextReader = new JsonTextReader(streamReader)
        {
            DateFormatString = DateTimeSerializationFormat,
            CloseInput = false,
        };

        var result = _jsonSerializer.Deserialize<T>(jsonTextReader);
        return new ValueTask<T>(result);
    }

    public T Deserialize<T>(ReadOnlySpan<byte> utf8Json)
    {
        // Newtonsoft has no UTF-8 span reader, so transcode once. Still avoids the
        // per-row stream + async state machine the bulk read path used to pay.
        var json = Utf8NoBom.GetString(utf8Json);

        using var stringReader = new StringReader(json);
        using var jsonTextReader = new JsonTextReader(stringReader)
        {
            DateFormatString = DateTimeSerializationFormat,
            CloseInput = false,
        };

        return _jsonSerializer.Deserialize<T>(jsonTextReader);
    }

    /// <inheritdoc />
    public string ResolvePropertyName(Type declaringType, string clrPropertyName)
    {
        if (declaringType is null || string.IsNullOrEmpty(clrPropertyName))
        {
            return null;
        }

        var names =
            _jsonPropertyNames.GetOrAdd(
                declaringType,
                static (type, serializer) => serializer.BuildPropertyNameMap(type),
                this);

        return names.TryGetValue(clrPropertyName, out var jsonName) ? jsonName : null;
    }

    private Dictionary<string, string> BuildPropertyNameMap(Type type)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);

        try
        {
            // The contract resolver is the single source of truth for member naming: it applies
            // the naming strategy and [JsonProperty(PropertyName)] together.
            if (_jsonSerializer.ContractResolver?.ResolveContract(type) is JsonObjectContract contract)
            {
                foreach (var property in contract.Properties)
                {
                    if (property.UnderlyingName is { } underlyingName && property.PropertyName is { } propertyName)
                    {
                        map[underlyingName] = propertyName;
                    }
                }
            }
        }
        catch (JsonException)
        {
            // Never let name resolution break query building; callers fall back to the CLR name.
        }

        return map;
    }

    public object Serialize<T>(T obj)
    {
        using var ms = new MemoryStream(DefaultBufferSize);
        SerializeToStream(obj, ms);
        return ms.ToArray();
    }

    /// <summary>
    /// Serializes an object to the provided buffer writer.
    /// </summary>
    /// <remarks>
    /// Note: Newtonsoft.Json does not natively support IBufferWriter, so this implementation
    /// uses an intermediate MemoryStream. For zero-allocation serialization, consider using
    /// System.Text.Json with a JsonSerializerContext instead.
    /// </remarks>
    public void Serialize<T>(T obj, IBufferWriter<byte> bufferWriter)
    {
        using var ms = new MemoryStream(DefaultBufferSize);
        SerializeToStream(obj, ms);

        int bytesWritten = (int)ms.Position;
        var span = bufferWriter.GetSpan(bytesWritten);
        ms.GetBuffer().AsSpan(0, bytesWritten).CopyTo(span);
        bufferWriter.Advance(bytesWritten);
    }

    private void SerializeToStream<T>(T obj, MemoryStream stream)
    {
        using var sw = new StreamWriter(stream, Utf8NoBom, StreamWriterBufferSize, leaveOpen: true);
        using var jsonTextWriter = new JsonTextWriter(sw)
        {
            DateFormatString = DateTimeSerializationFormat,
            Formatting = Formatting.None,
        };

        _jsonSerializer.Serialize(jsonTextWriter, obj);
        jsonTextWriter.Flush();
        sw.Flush();
    }

    public override string ToString() => nameof(NewtonsoftJsonSerializer);
}
