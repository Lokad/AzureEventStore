using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary>
    /// A simple, inefficient, non-persistent <see cref="IStorageDriver"/>
    /// that stores events in memory.
    /// </summary>
    /// <remarks>
    /// The position is the event index in the sequence.
    /// </remarks>
    internal sealed class MemoryStorageDriver : IStorageDriver
    {
        private readonly List<RawEvent> _events = new List<RawEvent>();

        private long Position => _events.Count;

        public Task<long> GetPositionAsync(CancellationToken cancel = new CancellationToken())
        {
            lock (this)
                return Task.FromResult(Position);
        }

        internal DriverWriteResult Write(long position, IEnumerable<RawEvent> events)
        {
            if (position != _events.Count) return new DriverWriteResult(_events.Count, false);
            
            foreach (var e in events)
            {
                // We are not allowed to keep a reference to the original byte arrays,
                // so copy them.
                var b = new byte[e.Contents.Length];
                Array.Copy(e.Contents, 0, b, 0, b.Length);
                _events.Add(new RawEvent(e.Sequence, b));
            }

            return new DriverWriteResult(_events.Count, true);
        }

        public Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events, CancellationToken cancel = new CancellationToken())
        {
            lock (this)
                return Task.FromResult(Write(position, events));            
        }

        internal DriverReadResult Read(long position, long maxBytes)
        {            
            var list = new List<RawEvent>();
            var copied = 0;
            for (var i = (int) position; i < _events.Count; ++i)
            {
                var e = _events[i];                
                copied += e.Contents.Length;

                if (list.Count > 0 && copied > maxBytes) break;

                // We do not want to hand out a reference to our mutable contents, 
                // so copy them.
                var b = new byte[e.Contents.Length];
                Array.Copy(e.Contents, 0, b, 0, b.Length);

                list.Add(new RawEvent(e.Sequence, b));
            }

            return new DriverReadResult(position + list.Count, list);
        }

        public Task<DriverReadResult> ReadAsync(long position, long maxBytes, CancellationToken cancel = new CancellationToken())
        {
            lock (this)
                return Task.FromResult(Read(position, maxBytes));            
        }

        private uint LastKey => _events.Count == 0 ? 0U : _events[_events.Count - 1].Sequence;

        public Task<uint> GetLastKeyAsync(CancellationToken cancel = new CancellationToken())
        {
            lock (this)
                return Task.FromResult(LastKey);            
        }

        private long Seek(uint key, long position)
        {
            long a = position, b = _events.Count;
            while (b - a > 1)
            {
                var m = (int)(a + b) / 2;
                if (_events[m].Sequence > key) b = m;
                else a = m;
            }

            return a;
        }

        public Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = new CancellationToken())
        {
            lock (this)
                return Task.FromResult(Seek(key, position));
        }
    }
}
