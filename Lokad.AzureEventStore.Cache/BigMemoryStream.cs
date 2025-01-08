using System;
using System.IO;

namespace Lokad.AzureEventStore.Cache
{
    /// <summary> A stream that reads from <see cref="IBigMemory"/>. </summary>
    internal sealed class BigMemoryStream : Stream
    {
        /// <summary> Backing for <see cref="Position"/> </summary>
        private long _position;

        /// <summary> The underlying memory. </summary>
        public IBigMemory Memory { get; }

        public BigMemoryStream(IBigMemory memory)
        {
            Memory = memory;
            _position = 0;
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => Memory.Length;

        public override long Position
        {
            get => _position;
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value));
                if (value > Memory.Length)
                    throw new EndOfStreamException();
                _position = value;
            }
        }
        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset + count > buffer.Length)
                throw new ArgumentException(nameof(offset) + nameof(count));
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));
            if (_position + count > Memory.Length)
            {
                // truncate if remaining data length is insufficient
                count = (int)(Memory.Length - _position); 
            }
            Memory.AsMemory(_position, count).CopyTo(buffer.AsMemory(offset, count));
            _position += count;
            return count;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            switch (origin)
            {
                case SeekOrigin.Begin:
                    Position = offset;
                    return Position;
                case SeekOrigin.End:
                    Position = Memory.Length - offset;
                    return Position;
                case SeekOrigin.Current:
                    Position += offset;
                    return Position;
                default:
                    throw new NotSupportedException();
            }
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        ///     References a block of memory as a <see cref="IBigMemory"/>, moves
        ///     the stream forward to the end of the block.
        /// </summary>
        public IBigMemory AsBigMemory(long length)
        {
            var offset = Position;
            Position += length;
            return Memory.Slice(offset, length);
        }
    }
}
