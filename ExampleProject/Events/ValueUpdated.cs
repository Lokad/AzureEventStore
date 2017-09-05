using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace ExampleProject.Events
{
    /// <summary> An event: the value bound to a key has been updated. </summary>
    /// <remarks> If the key was unbound, a new binding was created. </remarks>
    [DataContract]
    public sealed class ValueUpdated : IEvent
    {
        public ValueUpdated(string key, int value)
        {
            Key = key ?? throw new ArgumentException(nameof(key));
            Value = value;
        }

        /// <summary> The key to which the value is bound. </summary>
        [DataMember]
        public string Key { get; private set; }

        /// <summary> The value bound to the key. </summary>
        [DataMember]
        public int Value { get; private set; }

        [JsonConstructor]
        private ValueUpdated() { }
    }
}
