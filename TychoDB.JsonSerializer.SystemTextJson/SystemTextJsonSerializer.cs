using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
using System.Threading.Tasks;

namespace TychoDB;

public sealed class SystemTextJsonSerializer : IJsonSerializer, IUtf8JsonDeserializer, IJsonPropertyNameResolver, IJsonValueResolver
{
    private readonly JsonSerializerOptions _jsonSerializerOptions;
    private readonly Dictionary<Type, JsonTypeInfo> _jsonTypeSerializers;

    // Maps CLR property name -> JSON member name, per type. Resolving goes through
    // JsonTypeInfo, which is not free, and query building asks for the same handful of
    // properties repeatedly.
    private readonly ConcurrentDictionary<Type, Dictionary<string, string>> _jsonPropertyNames = new();

    // GetTypeInfo and reflection-based serialization both throw when the options carry no
    // TypeInfoResolver, which is the case for options that have never been used to
    // (de)serialize. Options also become read-only after first use, so the resolver cannot be
    // assigned onto the caller's instance; work against a copy instead. Built lazily because
    // the common case never needs it.
    private JsonSerializerOptions _metadataOptions;

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
                NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                WriteIndented = false, // Use WriteIndented = false for better performance
                DefaultBufferSize = 16384, // 16KB buffer for better performance with medium-sized objects// Enable the fastest possible serialization
            };

        _jsonTypeSerializers =
            jsonTypeSerializers
            ?? new Dictionary<Type, JsonTypeInfo>();
    }

    public async ValueTask<T> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken)
    {
        if (_jsonTypeSerializers.TryGetValue(typeof(T), out var jsonTypeSerializer) && jsonTypeSerializer is JsonTypeInfo<T> jtst)
        {
            return await JsonSerializer.DeserializeAsync<T>(stream, jtst, cancellationToken).ConfigureAwait(false);
        }

        return await JsonSerializer.DeserializeAsync<T>(stream, _jsonSerializerOptions, cancellationToken).ConfigureAwait(false);
    }

    public T Deserialize<T>(ReadOnlySpan<byte> utf8Json)
    {
        if (_jsonTypeSerializers.TryGetValue(typeof(T), out var jsonTypeSerializer) && jsonTypeSerializer is JsonTypeInfo<T> jtst)
        {
            return JsonSerializer.Deserialize(utf8Json, jtst);
        }

        return JsonSerializer.Deserialize<T>(utf8Json, _jsonSerializerOptions);
    }

    public object Serialize<T>(T obj)
    {
        if (_jsonTypeSerializers.TryGetValue(typeof(T), out var jsonTypeSerializer) && jsonTypeSerializer is JsonTypeInfo<T> jtst)
        {
            return JsonSerializer.SerializeToUtf8Bytes(obj, jtst);
        }

        return JsonSerializer.SerializeToUtf8Bytes(obj, _jsonSerializerOptions);
    }

    public void Serialize<T>(T obj, IBufferWriter<byte> bufferWriter)
    {
        using var writer = new Utf8JsonWriter(bufferWriter);

        if (_jsonTypeSerializers.TryGetValue(typeof(T), out var jsonTypeSerializer) && jsonTypeSerializer is JsonTypeInfo<T> jtst)
        {
            JsonSerializer.Serialize(writer, obj, jtst);
        }
        else
        {
            JsonSerializer.Serialize(writer, obj, _jsonSerializerOptions);
        }
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

    /// <inheritdoc />
    public bool TryResolveJsonValue(object value, out object jsonValue)
    {
        jsonValue = null;

        if (value is null)
        {
            return false;
        }

        byte[] utf8;
        try
        {
            // Serialize against the runtime type so converters registered for it apply -
            // this is exactly the form the value takes inside a stored document.
            utf8 = JsonSerializer.SerializeToUtf8Bytes(value, value.GetType(), GetMetadataOptions());
        }
        catch (NotSupportedException)
        {
            return false;
        }
        catch (JsonException)
        {
            return false;
        }

        var reader = new Utf8JsonReader(utf8);
        if (!reader.Read())
        {
            return false;
        }

        switch (reader.TokenType)
        {
            case JsonTokenType.String:
                jsonValue = reader.GetString();
                return true;
            case JsonTokenType.Number:
                // Integers bind as INTEGER and reals as REAL; SQLite compares across the two
                // numeric storage classes, but not between a number and its text form.
                if (reader.TryGetInt64(out var l))
                {
                    jsonValue = l;
                    return true;
                }

                jsonValue = reader.GetDouble();
                return true;
            case JsonTokenType.True:
                jsonValue = true;
                return true;
            case JsonTokenType.False:
                jsonValue = false;
                return true;
            case JsonTokenType.Null:
                jsonValue = null;
                return true;
            default:
                // Objects and arrays have no scalar form to compare against.
                return false;
        }
    }

    private Dictionary<string, string> BuildPropertyNameMap(Type type)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);

        try
        {
            var typeInfo =
                _jsonTypeSerializers.TryGetValue(type, out var registered)
                    ? registered
                    : GetMetadataOptions().GetTypeInfo(type);

            foreach (var property in typeInfo.Properties)
            {
                // AttributeProvider is the reflected PropertyInfo for both reflection-based and
                // source-generated metadata, and is the only link back to the CLR name once the
                // naming policy has been applied to JsonPropertyInfo.Name.
                if (property.AttributeProvider is MemberInfo member)
                {
                    map[member.Name] = property.Name;
                }
            }
        }
        catch (NotSupportedException)
        {
            // No metadata for this type (e.g. a source-generation-only context that does not
            // include it). Leave the map empty so callers fall back to the CLR name.
        }
        catch (InvalidOperationException)
        {
            // Same intent: never let name resolution break query building.
        }

        return map;
    }

    private JsonSerializerOptions GetMetadataOptions()
    {
        if (_jsonSerializerOptions.TypeInfoResolver is not null)
        {
            return _jsonSerializerOptions;
        }

        return _metadataOptions ??=
            new JsonSerializerOptions(_jsonSerializerOptions)
            {
                TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
            };
    }

    public override string ToString() => nameof(SystemTextJsonSerializer);
}
