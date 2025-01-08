using Lokad.AzureEventStore.Streams;
using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
namespace Lokad.AzureEventStore.Cache
{
    internal static class MappedCacheExtensions
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

    /// <summary>
    ///     A fully in-memory implementation of <see cref="IBigMemory"/>
    /// </summary>
    public sealed class VolatileMemory : IBigMemory
    {
        /// <summary> In-memory byte array that pretends to be in a file. </summary>
        private readonly Memory<byte> _backing;

        public VolatileMemory(Memory<byte> backing) =>
            _backing = backing;

        public VolatileMemory(ReadOnlyMemory<byte> backing) => _backing = MemoryMarshal.AsMemory(backing);

        /// <see cref="IBigMemory.Length"/>
        public long Length => _backing.Length;

        /// <see cref="IBigMemory.AsMemory"/>
        public Memory<byte> AsMemory(long offset, int length) =>
            _backing.Slice((int)offset, length);

        public void Dispose() { }

        /// <see cref="IBigMemory.Slice"/>
        public IBigMemory Slice(long offset, long length) =>
            new VolatileMemory(_backing.Slice((int)offset, (int)length));
    }

    /// <summary> 
    ///     A large chunk of memory (usually a memory-mapped file), used for reading 
    ///     backing data for the data structures.
    /// </summary>
    public interface IBigMemory : IDisposable
    {
        /// <summary> A portion of the file, as memory. </summary>
        Memory<byte> AsMemory(long offset, int length);

        /// <summary> A portion of the backing memory. </summary>
        IBigMemory Slice(long offset, long length);

        /// <summary> Total length of the memory. </summary>
        long Length { get; }
    }
}