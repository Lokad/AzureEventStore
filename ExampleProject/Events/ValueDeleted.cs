using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace ExampleProject.Events
{
    /// <summary> An event: the value bound to a key has been deleted. </summary>
    [DataContract]
    public sealed class ValueDeleted : IEvent
    {
        public ValueDeleted(string key)
        {
            Key = key ?? throw new ArgumentException(nameof(key));
        }

        /// <summary> The key to be unbound. </summary>
        [DataMember]
        public string Key { get; private set; }
        
        [JsonConstructor]
        private ValueDeleted() { }
    }
}
