using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> A simple, inefficient, persistent file-based storage driver. </summary>
    /// <remarks> Intended for use during local development. </remarks>
    internal sealed class FileStorageDriver : IStorageDriver, IDisposable
    {
        private readonly Stream _file;

        internal FileStorageDriver(string path)
        {
            Directory.CreateDirectory(path);

            var file = Path.Combine(path, "stream.bin");

            _file = new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        }

        /// <see cref="GetPositionAsync"/>
        public long GetPosition() => _file.Length;

        public Task<long> GetPositionAsync(CancellationToken cancel = new CancellationToken()) =>
            Task.FromResult(GetPosition());

        public Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events, CancellationToken cancel = new CancellationToken())
        {
            var written = false;

            if (_file.Length == position)
            {
                written = true;
                _file.Seek(0, SeekOrigin.End);

                using (var w = new BinaryWriter(_file, Encoding.UTF8, true))
                foreach (var e in events)
                    EventFormat.Write(w, e);
            }

            return Task.FromResult(new DriverWriteResult(_file.Length, written));
        }

        public Task<DriverReadResult> ReadAsync(long position, long maxBytes, CancellationToken cancel = new CancellationToken())
        {
            var events = new List<RawEvent>();

            if (position >= _file.Length)
                return Task.FromResult(new DriverReadResult(_file.Length, events));

            _file.Seek(position, SeekOrigin.Begin);

            var readBytes = 0L;

            using (var r = new BinaryReader(_file, Encoding.UTF8, true))
            {
                var availableBytes = _file.Length - position;
                while (readBytes < availableBytes)
                {
                    var evt = EventFormat.Read(r);
                    var newReadBytes = _file.Position - position;

                    if (events.Count == 0 || newReadBytes < maxBytes)
                    {
                        events.Add(evt);
                        readBytes = newReadBytes;
                    }

                    if (newReadBytes >= maxBytes)
                        break;
                }
            }

            return Task.FromResult(new DriverReadResult(position + readBytes, events));
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
