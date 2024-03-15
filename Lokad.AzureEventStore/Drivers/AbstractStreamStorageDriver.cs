using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> A simple, inefficient, persistent stream-based storage driver. </summary>
    /// <remarks> Intended for use during unit testing, or local development. </remarks>
    internal abstract class AbstractStreamStorageDriver : IStorageDriver, IDisposable
    {
        private readonly Stream _file;

        protected AbstractStreamStorageDriver(Stream stream)
        {
            _file = stream;
        }

        /// <see cref="GetPositionAsync"/>
        public long GetPosition() => _file.Length;

        public Task<long> GetPositionAsync(CancellationToken cancel = new CancellationToken()) =>
            Task.FromResult(GetPosition());

        public Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events,
            CancellationToken cancel = new CancellationToken())
            => Task.FromResult(Write(position, events));

        internal DriverWriteResult Write(long position, IEnumerable<RawEvent> events)
        {
            var written = false;

            if (_file.Length == position)
            {
                written = true;
                _file.Seek(0, SeekOrigin.End);

                var payload = ArrayPool<byte>.Shared.Rent(4 * 1024 * 1024);
                var payloadLength = 0;
                try
                {
                    foreach (var e in events)
                    {
                        payloadLength += EventFormat.Write(payload.AsMemory(payloadLength), e);
                    }

                    _file.Write(payload, 0, payloadLength);
                    _file.Flush();
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(payload);
                }
            }

            return new DriverWriteResult(_file.Length, written);
        }

        public Task<DriverReadResult> ReadAsync(long position, Memory<byte> backing, CancellationToken cancel = default)
            => Task.FromResult(Read(position, backing));

        internal DriverReadResult Read(long position, Memory<byte> backing)
        {
            var events = new List<RawEvent>();

            if (position >= _file.Length)
                return new DriverReadResult(_file.Length, events);

            _file.Seek(position, SeekOrigin.Begin);

            var availableBytes = _file.Length - position;

            // Fill backing from _file
            var backingBytes = (int)Math.Min(backing.Length, availableBytes);
            for (var readToBuffer = 0; readToBuffer < backingBytes; )
            {
                var read = _file.Read(backing.Span[readToBuffer..]);
                if (read == 0) throw new InvalidDataException("Unexpected end-of-file");
                readToBuffer += read;
            }

            // Read as many events as possible from buffer
            backing = backing[..backingBytes];
            var parsedBytes = 0;
            while (EventFormat.TryParse(backing[parsedBytes..], out var evtSize) is RawEvent ev)
            {
                parsedBytes += evtSize;
                events.Add(ev);
            }

            return new DriverReadResult(position + parsedBytes, events);

        }
        
        public async Task<uint> GetLastKeyAsync(CancellationToken cancel = new CancellationToken()) => 
            await EventFormat.GetLastSequenceAsync(_file, cancel);

        public Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = new CancellationToken()) => 
            Task.FromResult(position);

        public void Dispose()
        {
            _file.Dispose();
        }
    }
}
