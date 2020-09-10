using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace ExampleProject.Events
{
    /// <summary> An event: a document has been removed from the index. </summary>
    [DataContract]
    public sealed class DocumentRemoved : IEvent
    {
        public DocumentRemoved(int id)
        {
            Id = id;
        }

        /// <summary> The internal identifier of the removed document. </summary>
        [DataMember]
        public int Id { get; private set; }

        [JsonConstructor]
        private DocumentRemoved() { }
    }
}
