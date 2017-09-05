using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Lokad.AzureEventStore.Streams
{
    /// <summary> Serializes events as JSON, carrying type information alongside. </summary>
    public sealed class JsonEventSerializer<TEvent> where TEvent : class
    {
        /// <summary> All types in <see cref="_types"/> keyed by name. </summary>
        private readonly IReadOnlyDictionary<string, Type> _typeByName;

        /// <summary> All types that can be serialized by this class. </summary>
        private readonly HashSet<Type> _types = new HashSet<Type>();

        /// <summary> Used for serialization. </summary>
        private readonly JsonSerializerSettings _settings = new JsonSerializerSettings
        {
            DateParseHandling = DateParseHandling.None,
            DateFormatHandling = DateFormatHandling.IsoDateFormat            
        };

        /// <summary> BOM-less UTF8 encoder. </summary>
        private readonly Encoding _encoding = new UTF8Encoding(false);

        /// <summary> Construct a new serializer for events of type <see cref="TEvent"/> </summary>
        public JsonEventSerializer()
        {
            // Enumerate all available types
            var types = new List<Type>();
            var tEvent = typeof (TEvent);

            // Special case: if TEvent == JObject, no additional type serialization 
            // is required. Otherwise, list all instantiable, serializable implementing types.
            if (tEvent != typeof (JObject))
            {
                var tEventInfo = tEvent.GetTypeInfo();
                types.AddRange(tEventInfo.Assembly.GetTypes()
                    .Where(t => {
                        var tInfo = t.GetTypeInfo();
                        return tInfo.IsClass &&
                               !tInfo.IsAbstract &&
                               tEventInfo.IsAssignableFrom(tInfo) &&
                               tInfo.GetCustomAttribute<DataContractAttribute>() != null;
                    }));
            }

            // Generate type linkings
            var typeByName = new Dictionary<string, Type>();
            foreach (var type in types)
            {
                if (typeByName.ContainsKey(type.Name))
                    throw new ArgumentException("Two types named '" + type.Name + "' found.");

                typeByName.Add(type.Name, type);
                _types.Add(type);
            }

            _typeByName = typeByName;
        }

        /// <see cref="Deserialize(byte[])"/>
        private TEvent Deserialize(string json)
        {
            // Special case: no type analysis required
            if (typeof(TEvent) == typeof(JObject))
                return (TEvent) (object) JObject.Parse(json);

            // Look for the "Type":"..." property at the end of the JSON.
            string typename = null;

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
                    typename = json.Substring(i + 1, e - i);
                }
            }
            
            if (typename == null)
                throw new Exception("Could not find 'Type' property.");

            // ...and find the appropriate type.
            Type type;
            if (!_typeByName.TryGetValue(typename, out type))
                throw new Exception("Unknown 'Type' property: '" + typename + "'");
            
            return (TEvent)JsonConvert.DeserializeObject(json, type, _settings);
        }

        /// <summary> Deserialize an event, loading its type from its field "Type". </summary>
        public TEvent Deserialize(byte[] data)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));

            var json = _encoding.GetString(data);
            return Deserialize(json);
        }

        /// <summary> Serialize an event, adding type information to its field "Type". </summary>
        public byte[] Serialize(TEvent e)
        {
            if (e == null) throw new ArgumentNullException(nameof(e));

            var json = JsonConvert.SerializeObject(e, _settings);
            using (var ms = new MemoryStream())
            {
                using (var writer = new StreamWriter(ms, _encoding, 4096, true))
                {
                    if (typeof (TEvent) != typeof (JObject))
                    { 
                        var type = e.GetType();
                        if (!_types.Contains(type))
                            throw new ArgumentException("Cannot write unknown type '{type}'", nameof(e));

                        writer.Write(json.Substring(0, json.Length - 1));
                        writer.Write(",\"Type\":\"");
                        writer.Write(type.Name);
                        writer.Write("\"}");
                    }
                    else
                    {
                        writer.Write(json);
                    }
                }

                var mod = ms.Length%8;
                if (mod != 0)
                {
                    const byte pad = (byte) ' '; // Space character is one-byte ASCII
                    while (mod++ < 8) ms.WriteByte(pad);
                }

                return ms.ToArray();
            }
        }
    }
}
