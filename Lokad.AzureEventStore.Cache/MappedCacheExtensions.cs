using Lokad.AzureEventStore.Streams;
using Lokad.LargeImmutable;
using Lokad.LargeImmutable.Mapping;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Cache
{
    public static class MappedCacheExtensions
    {
        /// <summary>
        ///     Convert a stream to a <see cref="BigMemoryStream"/>.
        /// </summary>
        /// <remarks>
        ///     If the stream is backed by a mapped file, directly 
        ///     returns it as a memory-mapped big-memory stream. Otherwise,
        ///     copies the contents of the stream into an in-memory one.
        ///     
        ///     If the returned stream is NOT the input stream, then the input
        ///     stream's position is moved to the end (to pretend that it has 
        ///     been read entirely).
        /// </remarks>
        public static async Task<BigMemoryStream> AsBigMemoryStream(
            this Stream stream,
            CancellationToken cancel)
        {
            if (stream is BigMemoryStream bms)
                return bms;

            if (stream is MemoryStream ms)
            {
                stream.Seek(0, SeekOrigin.End);
                return new BigMemoryStream(new VolatileMemory(ms.ToArray()));
            }

            if (stream is BoundedStream bs && bs.InnerStream is BigMemoryStream ibms)
            {
                stream.Seek(0, SeekOrigin.End);
                return new BigMemoryStream(ibms.Memory.Slice(bs.Offset, bs.Length));
            }

            // No obvious conversion, need to copy data from the stream to an array.

            byte[] array;
            try
            {
                array = new byte[stream.Length];
            }
            catch (Exception e)
            {
                throw new ArgumentException(
                    $"When allocating buffer for stream of {stream.Length} bytes.",
                    nameof(stream),
                    e);
            }

            stream.Position = 0;
            var offset = 0;
            while (offset < array.Length)
            {
                var read = await stream.ReadAsync(array, offset, array.Length - offset, cancel);
                if (read == 0)
                    throw new InvalidOperationException("Unexpected end-of-stream");

                offset += read;
            }

            return new BigMemoryStream(new VolatileMemory(array));
        }
    }
}
