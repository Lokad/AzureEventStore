using System;

namespace Lokad.AzureEventStore;

public interface IEventSerializer<TEvent> where TEvent : class
{
    /// <summary>
    ///     The tag identifying this serializer, written as the first
    ///     byte of every event.
    /// </summary>
    byte Tag { get; }

    /// <summary>
    ///     Deserialize an event from a buffer. The first byte of the
    ///     buffer is the tag.
    /// </summary>
    TEvent Deserialize(ReadOnlyMemory<byte> data);

    /// <summary>
    ///     Serialize an event into a buffer, starting at a specific position,
    ///     returning the written segment. Should write its <see cref="Tag"/>
    ///     as the first byte of the segment.
    /// </summary>
    Memory<byte> Serialize(TEvent e, byte[] buffer, int position);
}
