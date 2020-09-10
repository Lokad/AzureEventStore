using System;
using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace ExampleProject.Events
{
    /// <summary> An event: the value bound to a key has been updated. </summary>
    /// <remarks> If the key was unbound, a new binding was created. </remarks>
    [DataContract]
    public sealed class DocumentAdded : IEvent
    {
        public DocumentAdded(int id, string path, string contents)
        {
            Id = id;
            Contents = contents ?? throw new ArgumentNullException(nameof(contents));
            Path = path ?? throw new ArgumentNullException(nameof(path));
        }

        /// <summary> The internal identifier of the document. </summary>
        [DataMember]
        public int Id { get; private set; }

        /// <summary> The path where the document was found. </summary>
        [DataMember]
        public string Path { get; private set; }

        /// <summary> The contents of the document. </summary>
        [DataMember]
        public string Contents { get; private set; }

        [JsonConstructor]
        private DocumentAdded() { }
    }
}
