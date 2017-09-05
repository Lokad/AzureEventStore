using System.Collections.Generic;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> The result of a <see cref="IStorageDriver.ReadAsync"/> operation. </summary>
    internal sealed class DriverReadResult
    {
        /// <summary>
        /// The new position of the read cursor, to be used on the next call to
        /// <see cref="IStorageDriver.ReadAsync"/>.
        /// </summary>
        internal readonly long NextPosition;

        /// <summary>
        /// The events read from the stream. Will always contain at least one
        /// event, unless the end of the stream was reached.
        /// </summary>
        internal readonly IReadOnlyList<RawEvent> Events;

        internal DriverReadResult(long nextPosition, IReadOnlyList<RawEvent> events)
        {
            NextPosition = nextPosition;
            Events = events;
        }
    }
}