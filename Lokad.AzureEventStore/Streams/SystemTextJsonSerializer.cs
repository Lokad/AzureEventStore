using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Lokad.AzureEventStore.Exceptions;

namespace Lokad.AzureEventStore.Streams;

/// <summary> Serializes events as JSON, carrying type information alongside. </summary>
public sealed class SystemTextJsonSerializer<TEvent> : IEventSerializer<TEvent> where TEvent : class
{
    /// <summary> All types in <see cref="_types"/> keyed by name. </summary>
    private readonly IReadOnlyDictionary<string, Type> _typeByName;

    /// <summary> All types that can be serialized by this class. </summary>
    private readonly HashSet<Type> _types = [];

    /// <summary> Used for serialization. </summary>
    private readonly JsonSerializerOptions _options =
        new JsonSerializerOptions(JsonSerializerOptions.Default)
        {
            WriteIndented = false
        };
    
    /// <summary> BOM-less UTF8 encoder. </summary>
    private readonly Encoding _encoding = new UTF8Encoding(false);

    /// <summary>
    /// This byte appears in first position in the serialized event, to indicate
    /// this serializer should be used to deserialize it.
    /// </summary>
    public byte Tag => (byte)'{';

    /// <summary> Construct a new serializer for events of type <see cref="TEvent"/> </summary>
    public SystemTextJsonSerializer()
    {
        var types = new List<Type>();
        var tEvent = typeof(TEvent);

        // JsonObject carries its own JSON structure, so it needs no discriminator.
        if (tEvent != typeof(JsonObject))
        {
            var tEventInfo = tEvent.GetTypeInfo();
            types.AddRange(tEventInfo.Assembly.GetTypes()
                .Where(t =>
                {
                    var tInfo = t.GetTypeInfo();
                    return tInfo is { IsClass: true, IsAbstract: false } &&
                           tEventInfo.IsAssignableFrom(tInfo);
                }));
        }

        var typeByName = new Dictionary<string, Type>();
        foreach (var type in types)
        {
            if (!typeByName.TryAdd(type.Name, type))
                throw new ArgumentException("Two types named '" + type.Name + "' found.");

            _types.Add(type);
        }

        _typeByName = typeByName;
    }

    private string? TryDetectType(ReadOnlyMemory<byte> jsonMem)
    {
        var json = jsonMem.Span;
        string? typename = null;

        var i = json.Length - 1;
        while (i >= 0 && json[i] == ' ') --i;
        if (i >= 1 && json[i--] == '}' && json[i--] == '"')
        {
            var e = i;
            while (i >= 0 && json[i] != '"') --i;
            if (i >= 7 &&
                json[i - 1] == ':' &&
                json[i - 2] == '"' &&
                json[i - 3] == 'e' &&
                json[i - 4] == 'p' &&
                json[i - 5] == 'y' &&
                json[i - 6] == 'T' &&
                json[i - 7] == '"')
            {
                typename =
                    Encoding.UTF8.GetString(json.Slice(i + 1, e - i));
            }
        }

        return typename;
    }
    
    private TEvent DeserializeInner(ReadOnlyMemory<byte> json)
    {
        if (typeof(TEvent) == typeof(JsonObject))
        {
            var jsonObject = JsonSerializer.Deserialize<JsonObject>(json.Span, _options)
                ?? throw new JsonException("Could not deserialize JSON object.");
            return (TEvent)(object)jsonObject;
        }

        // Look for the "Type":"..." property at the end of the JSON.
        var typename = TryDetectType(json);

        if (typename == null)
            throw new Exception("Could not find 'Type' property.");

        if (!_typeByName.TryGetValue(typename, out var type))
            throw new Exception("Unknown 'Type' property: '" + typename + "'");

        var @event = JsonSerializer.Deserialize(json.Span, type, _options)
            ?? throw new JsonException("Could not deserialize event.");
        return (TEvent)@event;
    }

    /// <summary> Deserialize an event, loading its type from its field "Type". </summary>
    public TEvent Deserialize(ReadOnlyMemory<byte> data)
    {
        try { return DeserializeInner(data); }
        catch (Exception ex)
        {
            var json = _encoding.GetString(data.Span);
            throw new EventDeserializationException(json, ex);
        }
    }

    /// <summary> Serialize an event into a buffer, returning the written segment. </summary>
    public Memory<byte> Serialize(TEvent e, byte[] buffer, int position)
    {
        if (buffer == null) throw new ArgumentNullException(nameof(buffer));
        if ((uint)position > buffer.Length) throw new ArgumentOutOfRangeException(nameof(position));

        using var ms = new MemoryStream(buffer);
        ms.Position = position;

        var written = WriteToStream(e, ms);
        return buffer.AsMemory(position, written);
    }

    private void WritePadding(Stream stream, long start)
    {
        var mod = (stream.Position - start) % 8;
        if (mod != 0)
        {
            const byte pad = (byte)' ';
            while (mod++ < 8) stream.WriteByte(pad);
        }
    }

    private int WriteToStream(TEvent? e, Stream stream)
    {
        if (e == null) throw new ArgumentNullException(nameof(e));

        var start = stream.Position;
        if (typeof(TEvent) == typeof(JsonObject))
        {
            stream.Write(JsonSerializer.SerializeToUtf8Bytes(e, _options));
        }
        else
        {
            var type = e.GetType();
            if (!_types.Contains(type))
                throw new ArgumentException("Cannot write unknown type '" + type + "'", nameof(e));

            var json = JsonSerializer.SerializeToUtf8Bytes(e, type, _options);
            stream.Write(json.AsSpan(0, json.Length - 1));
            stream.Write(",\"Type\":"u8);
            stream.Write(JsonSerializer.SerializeToUtf8Bytes(type.Name, _options));
            stream.Write("}"u8);
        }

        WritePadding(stream, start);

        return checked((int)(stream.Position - start));
    }
}
