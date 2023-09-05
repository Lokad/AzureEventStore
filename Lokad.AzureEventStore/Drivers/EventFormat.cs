using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> Describes the serialization format for the event stream. </summary>
    /// <remarks>
    ///     The same event format is used for both <see cref="AzureStorageDriver"/>
    ///     and <see cref="FileStorageDriver"/>.
    /// </remarks>
    internal static class EventFormat
    {
        /// <summary>
        /// The maximum size of an event, in bytes, including metadata.
        /// </summary>
        public const int MaxEventFootprint =
            2 // Size
            + 4 // Sequence
            + ushort.MaxValue * 8 // maximum event size
            + 4 // Checksum
            + 2; // Size 

        /// <summary> Append an event to a stream. </summary>
        /// <see cref="Read"/>
        public static int Write(Memory<byte> memory, RawEvent e)
        {
            var span = memory.Span;
            var size = checked((ushort)(e.Contents.Length/8));

            MemoryMarshal.Write(span, ref size);
            span = span[sizeof(ushort)..];

            uint seq = e.Sequence;
            MemoryMarshal.Write(span, ref seq);
            span = span[sizeof(uint)..];

            var payload = e.Contents.Span;
            payload.CopyTo(span);
            span = span[e.Contents.Length..];

            uint checksum = Checksum(seq, payload);
            MemoryMarshal.Write(span, ref checksum);
            span = span[sizeof(uint)..];

            MemoryMarshal.Write(span, ref size);

            return 2 // Size
                   + 4 // Sequence
                   + e.Contents.Length
                   + 4 // Checksum
                   + 2; // size
        }

        /// <summary> Get the last sequence number in a stream. </summary>
        public static async Task<uint> GetLastSequenceAsync(Stream stream, CancellationToken cancel)
        {
            if (stream.Length == 0) return 0;

            var buffer = new byte[4];

            // Read the size at the end of the blob
            // ====================================

            stream.Seek(-2, SeekOrigin.End);

            var offset = 0;
            while (offset < 2)
            {
                var read = await stream.ReadAsync(buffer, offset, 2 - offset, cancel);
                if (read == 0) throw new EndOfStreamException();

                offset += read;
            }

            var lastEventSize = (buffer[0] + (buffer[1] << 8)) * 8;
            var keyOffset = lastEventSize
                         + 2  // The size we just read
                         + 4  // The checksum
                         + 4; // The key we want to read

            // Read the key from the blob
            // ==========================

            stream.Seek(-keyOffset, SeekOrigin.End);

            offset = 0;
            while (offset < 4)
            {
                var read = await stream.ReadAsync(buffer, offset, 4 - offset, cancel);
                if (read == 0) throw new EndOfStreamException();

                offset += read;
            }

            return buffer[0]
                   + ((uint)buffer[1] << 8)
                   + ((uint)buffer[2] << 16)
                   + ((uint)buffer[3] << 24);

        }

        /// <summary> Used by <see cref="Checksum"/>, initialized during construction. </summary>
        private static readonly uint[] ChecksumTable;

        static EventFormat()
        {
            // Create the table for the CRC32 checksum
            const uint polynomial = 0xedb88320u;

            var table = new uint[256];
            for (var i = 0U; i < 256; i++)
            {
                var entry = i;
                for (var j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ polynomial;
                    else
                        entry = entry >> 1;
                table[i] = entry;
            }

            ChecksumTable = table;
        }

        /// <summary> Computes a CRC32 checksum. </summary>
        private static uint Checksum(uint seed, ReadOnlySpan<byte> b)
        {
            var table = ChecksumTable;
            var crc32 = seed;
            foreach (var by in b)
                crc32 = (crc32 >> 8) ^ table[by ^ crc32 & 0xff];

            return crc32;
        }

        public static RawEvent? TryParse(ReadOnlyMemory<byte> contents, out int readBytes)
        {
            var span = contents.Span;
            readBytes = 0;

            if (span.Length < sizeof(ushort)) 
                return null; // Too short to fit an entire event

            var payloadSize = MemoryMarshal.Read<ushort>(span) * 8;
            var totalSize   = payloadSize + 2 * sizeof(uint) + 2 * sizeof(ushort);

            if (span.Length < totalSize)
                return null; // Too short to fit _this_ entire envent

            span = span[sizeof(ushort)..];

            var key = MemoryMarshal.Read<uint>(span);
            span = span[sizeof(uint)..];

            var payload = span[..payloadSize];
            span = span[payloadSize..];

            var expected = Checksum(key, payload);

            var checksum = MemoryMarshal.Read<uint>(span);
            span = span[sizeof(uint)..];

            var payloadSize2 = MemoryMarshal.Read<ushort>(span) * 8;

            if (payloadSize != payloadSize2)
                throw new InvalidDataException(
                    $"Corrupted data: size mismatch {payloadSize} != {payloadSize2}");

            if (checksum != expected)
                throw new InvalidDataException(
                    $"Corrupted data: checksum mismatch {checksum:X8} != {expected:X8}");

            readBytes = totalSize;
            return new RawEvent(key, contents.Slice(sizeof(uint) + sizeof(ushort), payloadSize));
        }
    }
}