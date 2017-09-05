using System;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> Raw event data, as read and written through a <see cref="IStorageDriver"/>. </summary>
    internal sealed class RawEvent
    {
        /// <summary>
        /// The sequence number of the event: these should be strictly increasing, 
        /// but not necessarily contiguous (e.g. in case of stream rewrites, events may be
        /// merged).
        /// </summary>
        public readonly uint Sequence;

        /// <summary>
        /// The contents of the event. The size should be a multiple of 8, and no greater
        /// than 2^16 * 8 = 512KB.
        /// </summary>
        public readonly byte[] Contents;

        public RawEvent(uint sequence, byte[] contents)
        {
            Sequence = sequence;
            Contents = contents;

            if (contents.Length % 8 != 0)
                throw new ArgumentException("Content size " + contents.Length + " not a multiple of 8");

            if (contents.Length >= 512 * 1024)
                throw new ArgumentException("Content size " + contents.Length + " exceeds 512KB.");
        }
    }
}