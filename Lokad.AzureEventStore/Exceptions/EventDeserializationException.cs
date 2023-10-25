using System;

namespace Lokad.AzureEventStore.Exceptions
{
    public sealed class EventDeserializationException : Exception 
    {
        public string Json { get; private set; }

        public EventDeserializationException(string json, Exception innerException) : base("Failed to deserialze an event.", innerException)
        {
            Json = json;
        }
    }
}
