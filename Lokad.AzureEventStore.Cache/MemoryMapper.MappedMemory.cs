using System;
using System.Buffers;

namespace Lokad.AzureEventStore.Cache
{
    partial class MemoryMapper
    {
        /// <summary>
        ///     Used by <see cref="MemoryMapper"/> to create a <see cref="Memory{T}"/> from
        ///     the raw byte range of a memory-mapped file.
        /// </summary>
        internal unsafe class MappedMemory : MemoryManager<byte>
        {
            public override Memory<byte> Memory => CreateMemory(_length);

            /// <param name="offset"> In bytes. </param>
            /// <param name="length"> In bytes. </param>
            public MappedMemory(byte* ptr, long offset, int length)
            {
                _length = length;
                _offset = offset;
                _ptr = ptr;
            }

            ///<summary> Length of entire mapped file </summary>
            private readonly int _length;

            private readonly long _offset;

            private readonly byte* _ptr;

            public override Span<byte> GetSpan() =>
                new Span<byte>(_ptr + _offset, _length);

            public override MemoryHandle Pin(int elementIndex = 0)
            {
                return new MemoryHandle(_ptr + _offset);
            }

            public override void Unpin() { }
            protected override void Dispose(bool disposing) { }
        }
    }
}