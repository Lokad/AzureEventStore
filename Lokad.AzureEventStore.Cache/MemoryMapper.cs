using System;
using System.IO.MemoryMappedFiles;
using System.Threading;

#nullable enable

namespace Lokad.AzureEventStore.Cache
{
    /// <summary>
    ///     Wraps around a <see cref="MemoryMappedFile"/> to provide views over its contents
    ///     as <see cref="Memory{T}"/>.
    /// </summary>
    internal sealed unsafe partial class MemoryMapper : IBigMemory
    {
        /// <summary> The memory-mapped file. </summary>
        /// <remarks> 
        ///     Kept for disposal and slicing only, all access goes 
        ///     through <see cref="_mmva"/> and <see cref="_ptr"/> instead. 
        /// </remarks>
        private readonly MemoryMappedFile _mmf;
        /// <summary>
        ///     Number of undisposed <see cref="MemoryMapper"/> instances 
        ///     pointing to <see cref="_mmf"/> (including this one). 
        ///     Irrelevant if <see cref="_parent"/> is non-null.
        /// </summary>
        private int _references = 1;
        /// <summary>
        ///     If not null, the instance from which this instance was created
        ///     (through slicing), and thus may not dispose its <see cref="_mmf"/>.
        /// </summary>
        private readonly MemoryMapper? _parent;
        /// <summary> View accessor covering all of <see cref="_mmf"/>. </summary>
        private readonly MemoryMappedViewAccessor _mmva;
        /// <summary> Pointer to the byte at offset <see cref="_offset"/> in <see cref="_mmf"/> </summary>
        private readonly byte* _ptr;
        /// <summary> Has this already been disposed ? </summary>
        private bool _disposed;
        /// <summary> The offset of <see cref="_ptr"/> within <see cref="_mmf"/>. </summary>
        private readonly long _offset;
        /// <see cref="IBigMemory.Length"/>
        public long Length { get; }
        /// <summary> Ctor </summary>
        /// <param name="mmf"> Backing file. </param>
        /// <param name="offset"> In bytes. </param>
        /// <param name="length"> In bytes. </param>
        public MemoryMapper(MemoryMappedFile mmf, long offset, long length)
        {
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));
            _mmf = mmf ?? throw new ArgumentNullException(nameof(mmf));
            Length = length;
            
            // We want an accessor between 'offset' and 'offset+Length', but...
            //
            // The CreateViewAccessor and AcquirePointer combo is a weird thing. 
            // It would make sense for the difference between the pointers obtained 
            // with CreateViewAccessor(0, ..) and CreateViewAccessor(offset, ..) to
            // be 'offset' bytes, but instead the difference will be the largest 
            // multiple of the page size smaller than 'offset'. Since there is no 
            // clean way of measuring the page size (to adjust the `_ptr +=` below)
            // the most prudent approach is to instead ask for an accessor 
            // between '0' and 'offset+Length', and then add the expected offset
            // to _ptr instead.
            _mmva = _mmf.CreateViewAccessor(0, offset + Length);
            _mmva.SafeMemoryMappedViewHandle.AcquirePointer(ref _ptr);
            _ptr += offset;
            _disposed = false;
            _offset = offset;
        }
        public static MemoryMapper CreateMemoryMapper(MemoryMappedFile mmf, long offset, long length, out byte* ptr)
        {
            var mmp = new MemoryMapper(mmf, offset, length);
            ptr = mmp._ptr;
            return mmp;
        }
        private MemoryMapper(MemoryMapper sliced, long offset, long length) 
            : this(sliced._mmf, sliced._offset + offset, length)
        {
            _parent = sliced;
            var what = sliced;
            while (what._parent != null) what = what._parent;
            Interlocked.Increment(ref what._references);
        }
        /// <see cref="IBigMemory.AsMemory"/>
        public Memory<byte> AsMemory(long offset, int length)
        {
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (length < 0)
                throw new ArgumentOutOfRangeException(nameof(length));
            if (offset + length > Length)
                throw new ArgumentException(nameof(offset) + nameof(length));
            return new MappedMemory(_ptr, offset, length).Memory;
        }
        /// <see cref="IBigMemory.Slice"/>
        public IBigMemory Slice(long offset, long length) =>
            new MemoryMapper(this, offset, length);
        private void ReleaseUnmanagedResources()
        {
            if (_ptr != null)
            {
                _mmva.SafeMemoryMappedViewHandle.ReleasePointer();
            }
        }
        private void Dispose(bool disposing, bool keepFile = false)
        {
            if (_disposed)
                return;
            ReleaseUnmanagedResources();
            if (disposing)
            {
                // We own the _mmva, so always dispose it
                _mmva?.Dispose();
                var what = this;
                while (what._parent != null) what = what._parent;
                if (!keepFile && 0 == Interlocked.Decrement(ref what._references))
                {
                    // Only dispose the _mmf if we are the last reference to it
                    _mmf?.Dispose();
                }
            }
            _disposed = true;
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        ~MemoryMapper()
        {
            Dispose(false);
        }
        public void Flush(long offset, long length)
        {
            using (var fView = _mmf.CreateViewAccessor(offset, length))
            {
                fView.Flush();
            }
        }
        public void DisposeView()
        {
            Dispose(true, true);
        }
    }
}
