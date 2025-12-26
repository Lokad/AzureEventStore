using System;

namespace Lokad.AzureEventStore.Exceptions;

public sealed class EventDeserializationException(string json, Exception innerException)
    : Exception("Failed to deserialze an event.", innerException)
{
    public string Json { get; private set; } = json;
}