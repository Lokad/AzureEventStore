using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> A driver that caches the responses from another driver. </summary>
    /// <remarks>
    ///     Uses a file to store the initial segment of events from that driver.
    ///     This allows higher performance when querying events from position 0
    /// </remarks>
    internal sealed class CacheStorageDriver : IStorageDriver, IDisposable
    {
        /// <summary> The inner driver for which data is cached. </summary>
        private readonly IStorageDriver _source;

        /// <summary> The local file used as cache. </summary>
        private readonly FileStorageDriver _cache;

        /// <summary> The highest key in the cache. </summary>
        private uint _maxCacheKey;

        public CacheStorageDriver(IStorageDriver source, string path)
        {
            _source = source;
            _cache = new FileStorageDriver(path);
        }

        public void Dispose()
        {
            _cache.Dispose(); 
            (_source as IDisposable)?.Dispose();
        }

        /// <see cref="IStorageDriver.GetPositionAsync"/>
        public Task<long> GetPositionAsync(CancellationToken cancel = new CancellationToken()) =>
            _source.GetPositionAsync(cancel);

        /// <see cref="IStorageDriver.GetLastKeyAsync"/>
        public Task<uint> GetLastKeyAsync(CancellationToken cancel = new CancellationToken()) =>
            _source.GetLastKeyAsync(cancel);

        /// <see cref="IStorageDriver.WriteAsync"/>
        public Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events, CancellationToken cancel = new CancellationToken()) =>
            _source.WriteAsync(position, events, cancel);           

        /// <see cref="IStorageDriver.ReadAsync"/>
        public async Task<DriverReadResult> ReadAsync(long position, long maxBytes, CancellationToken cancel = new CancellationToken())
        {
            // Read data up to the requested position (if not already available)
            var cachePos = _cache.GetPosition();
            while (position >= cachePos)
            {
                var r = await _source.ReadAsync(cachePos, maxBytes, cancel);
                var w = await _cache.WriteAsync(cachePos, r.Events, cancel);

                if (r.NextPosition == cachePos)
                    break;

                if (!w.Success)
                    throw new InvalidOperationException("Failed writing to cache stream.");

                if (w.NextPosition != r.NextPosition)
                    throw new InvalidDataException(
                        $"Position mismatch after cache copy: {cachePos} -> ({r.NextPosition}, {w.NextPosition}.");

                cachePos = w.NextPosition;
            }
            
            return await _cache.ReadAsync(position, maxBytes, cancel);            
        }

        /// <see cref="IStorageDriver.SeekAsync"/>
        public async Task<long> SeekAsync(uint key, long position = 0L, CancellationToken cancel = new CancellationToken())
        {
            if (_maxCacheKey == 0)
                _maxCacheKey = await _cache.GetLastKeyAsync(cancel);

            if (key <= _maxCacheKey || position <= _cache.GetPosition())
                // Key is within cached portion, so we'll have to search for it in there.
                return position;
            
            // Key is within non-cached position, so we actually advance the cached stream up to the 
            // source stream, looking for the specified event.
            while (true)
            {
                const int seekBufferSize = 1024*1024;
                var cachePos = _cache.GetPosition();
                var read = await ReadAsync(cachePos, seekBufferSize, cancel);

                if (read.Events.Any(e => e.Sequence >= key) || read.NextPosition == cachePos)
                    // We have found the events we were searching for, OR we have run out of
                    // events. In both cases, we return a lower bound on the position (with
                    // an error margin of 'seekBufferSize' bytes.
                    return Math.Max(cachePos, position);
            }
        }
    }
}
