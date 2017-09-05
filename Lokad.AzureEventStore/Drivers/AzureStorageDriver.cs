using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> Stores events in multiple Azure Append Blobs. </summary>
    /// <remarks> 
    /// See AzureAppendBlob.md for more information. 
    /// 
    /// Obvious reminder: this class is NOT re-entrant.
    /// </remarks>
    internal sealed class AzureStorageDriver : IStorageDriver
    {
        /// <summary> For each append blob, the position at which it starts. </summary>
        private readonly List<long> _firstPosition = new List<long>(); 

        /// <summary> For each append blob, the first key it contains. </summary>
        /// <remarks>
        /// Always has as many elements as, or one element fewer than, 
        /// <see cref="_firstPosition"/>.
        /// </remarks>
        private readonly List<uint> _firstKey = new List<uint>();

        /// <summary> Read-only access to <see cref="_firstKey"/>. </summary>
        internal IReadOnlyList<uint> FirstKey => _firstKey;

        /// <summary> All append blobs, in the appropriate order. </summary>
        /// <remarks> Always as long as <see cref="_firstPosition"/>. </remarks>
        private readonly List<CloudAppendBlob> _blobs = new List<CloudAppendBlob>();

        /// <summary> Read-only access to <see cref="_blobs"/>. </summary>
        internal IReadOnlyList<CloudAppendBlob> Blobs => _blobs;

        /// <summary> The container where blobs are stored. </summary>
        private readonly CloudBlobContainer _container;

        internal AzureStorageDriver(CloudBlobContainer container)
        {
            _container = container;
        }

        /// <summary> The last known write position. Used to reject writes early. </summary>
        private long _lastKnownPosition;

        private readonly byte[] _buffer = new byte[1024 * 1024 * 4];

        /// <summary> 
        /// Refreshes the local cache of positions, keys and blobs from Azure.         
        /// Returns the write position.
        /// </summary>        
        private async Task<long> RefreshCache(CancellationToken cancel)
        {
            // Download the complete list of blobs from the server. It's easier to 
            // perform processing in-memory rather than streaming through the listing.
            var newBlobs = await _container.ListEventBlobsAsync(cancel);
            
            if (newBlobs.Count == 0)
                // This is an empty stream.
                return _lastKnownPosition = 0;

            // STEP 1: _firstPosition and _lastKnownPosition
            // =============================================

            // Take into account any blobs not already present in the cache.
            // This code computes _firstPosition exactly once for each blob (over multiple 
            // calls).
            for (var i = _blobs.Count; i < newBlobs.Count; ++i)
            {
                _blobs.Add(newBlobs[i]);
                _firstPosition.Add(i == 0 ? 0L : _firstPosition[i - 1] + newBlobs[i - 1].Properties.Length);
            }
                        
            // The last known position always increases based on the length of the last blob.
            _lastKnownPosition = newBlobs[newBlobs.Count - 1].Properties.Length
                                 + _firstPosition[_firstPosition.Count - 1];

            // STEP 2: _firstKey
            // =================
            
            // Is the last blob empty or not ? If it is, we don't want to compute
            // its _firstKey yet.
            var nonEmptyBlobCount = _blobs[_blobs.Count - 1].Properties.Length < 6
                ? _blobs.Count - 1
                : _blobs.Count;

            var oldNonEmptyBlobCount = _firstKey.Count;

            if (nonEmptyBlobCount == oldNonEmptyBlobCount)
                // No new _firstKey entries to compute.
                return _lastKnownPosition;

            // We have new '_firstKey' entries to compute, so first extend the list to 
            // the appropriate size with dummy data...
            while (_firstKey.Count < nonEmptyBlobCount)
                _firstKey.Add(0);
            
            // ...then, in parallel, query the first key of each new blob. 
            await Task.WhenAll(
                Enumerable.Range(oldNonEmptyBlobCount, nonEmptyBlobCount - oldNonEmptyBlobCount)
                    .Select(async pos =>
                    {
                        var buffer = new byte[6];
                        await ReadSubRangeAsync(_blobs[pos], buffer, 0, 0, 6, cancel);
                        _firstKey[pos] = buffer[2]
                                         + ((uint)buffer[3] << 8)
                                         + ((uint)buffer[4] << 16)
                                         + ((uint)buffer[5] << 24);
                    }));
            
            return _lastKnownPosition;
        }

        public Task<long> GetPositionAsync(CancellationToken cancel = new CancellationToken())
        {
            // This is a bad idea:
            //   _firstPosition[_firstPosition.Count - 1] + _blobs[_blobs.Count - 1].Properties.Length
            // 
            // Even if we assume that the blob's metadata was refreshed, it still does not take into 
            // account the possibility of a NEW blob appearing and not being referenced in _blobs. 
            // 
            // The only proper solution is to poll the container for all blob files, at which point we
            // might as well refresh the entire cache. 

            return RefreshCache(cancel);
        }

        /// <summary> Creates a new blob (if it does not yet exist). </summary>
        private async Task CreateLastBlob(CancellationToken cancel)
        {
            var nth = _blobs.Count;
            var blob = _container.ReferenceEventBlob(nth);

            await blob.CreateIfNotExistsAsync(cancel);

            await RefreshCache(cancel);
        }

        public async Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events, CancellationToken cancel = new CancellationToken())
        {
            // Caller knows something we don't? Refresh!
            if (position > _lastKnownPosition)
                _lastKnownPosition = await RefreshCache(cancel); 

            // Quick early-out with no Azure request involved.
            if (position < _lastKnownPosition)
                return new DriverWriteResult(_lastKnownPosition, false);

            // This should only happen very rarely, but it still needs to be done.
            if (_blobs.Count == 0) await CreateLastBlob(cancel);

            // Generate appended payload

            byte[] payload;
            using (var stream = new MemoryStream())
            using (var writer = new BinaryWriter(stream))
            {
                foreach (var e in events)
                {
                    EventFormat.Write(writer, e);
                }

                payload = stream.ToArray();
            }

            // Nothing to write, but still check position
            if (payload.Length == 0)
            {
                _lastKnownPosition = await RefreshCache(cancel);
                return new DriverWriteResult(_lastKnownPosition, position == _lastKnownPosition);
            }

            // Attempt write to last blob.
            bool collision;
            try
            {
                var offset = _firstPosition[_blobs.Count - 1];
                await _blobs[_blobs.Count - 1].AppendTransactionalAsync(payload, position - offset, cancel);

                _lastKnownPosition = position + payload.Length;

                return new DriverWriteResult(_lastKnownPosition, true);
            }
            catch (StorageException e)
            {
                if (!e.IsCollision() && !e.IsMaxReached()) throw;

                collision = e.IsCollision();
            }

            // Collision means we do not have the proper _lastKnownPosition, so refresh
            // the cache and return a failure.
            if (collision)
                return new DriverWriteResult(await RefreshCache(cancel), false);

            // Too many appends can be solved by creating a new blob and appending to it. 
            // The append code is similar but subtly different, so we just rewrite it 
            // below.
            await CreateLastBlob(cancel);

            try
            {
                await _blobs[_blobs.Count - 1].AppendTransactionalAsync(payload, 0, cancel);

                _lastKnownPosition = position + payload.Length;

                return new DriverWriteResult(_lastKnownPosition, true);
            }
            catch (StorageException e)
            {
                // Only a collision can stop us here, no max-appends-reached should
                // happen when appending at position 0.
                if (!e.IsCollision()) throw;
            }

            // Collision here means someone else already created the new blob and appended
            // to it, so refresh everything and report failure.
            return new DriverWriteResult(await RefreshCache(cancel), false);
        }

        public async Task<DriverReadResult> ReadAsync(long position, long maxBytes, CancellationToken cancel = new CancellationToken())
        {
            // STEP 1: PRELIMINARY CHECKS
            // ==========================

            // The local cache tells us there is no data available at the provided position,
            // so start by refreshing the cache.
            if (_blobs.Count == 0 || position >= _lastKnownPosition)
            {
                await RefreshCache(cancel);
            }

            // Even with a fresh cache, our position is beyond any available data:
            // return that there is no more data available.
            if (_blobs.Count == 0 || position >= _lastKnownPosition)
                return new DriverReadResult(_lastKnownPosition, new RawEvent[0]);

            // STEP 2: IDENTIFY BLOB
            // =====================

            CloudAppendBlob blob = null;
            long firstPosition = 0;
            long blobSize = 0;

            for (var i = _blobs.Count - 1; i >= 0; --i)
            {
                if (_firstPosition[i] <= position)
                {
                    blob = _blobs[i];
                    firstPosition = _firstPosition[i];
                    blobSize = (i == _blobs.Count - 1 ? _lastKnownPosition : _firstPosition[i+1]) - firstPosition;
                    break;
                }
            }

            if (blob == null)
                // Since _firstPosition[0] == 0, this means the position is negative
                throw new ArgumentOutOfRangeException("Invalid position:" + position, "position");

            // STEP 3: READ RAW DATA
            // =====================

            var startPos = position - firstPosition;
            maxBytes = Math.Min(maxBytes, Math.Min(_buffer.Length, blobSize - startPos));

            if (maxBytes == 0)
                return new DriverReadResult(_lastKnownPosition, new RawEvent[0]);

            var length = await ReadRangeAsync(blob, startPos, maxBytes, cancel);

            // STEP 4: PARSE DATA
            // ==================

            var events = new List<RawEvent>();
            var readBytes = 0L;

            using (var ms = new MemoryStream(_buffer, 0, length))
            using (var reader = new BinaryReader(ms))
            {
                while (true)
                {
                    try
                    {
                        events.Add(EventFormat.Read(reader));

                        // Only update after finishing a full read !
                        readBytes = ms.Position;
                    }
                    catch (EndOfStreamException)
                    {
                        break;
                    }
                    catch (InvalidDataException e)
                    {
                        throw new InvalidDataException($"{e.Message} at {position + readBytes}");
                    }
                }
            }

            return new DriverReadResult(position + readBytes, events);            
        }

        /// <summary>
        ///     Reads a range from the specified <paramref name="blob"/> into <see cref="_buffer"/>.
        /// </summary>
        /// <remarks>
        ///     If there are many bytes remaining in the blob, and they all fit in the
        ///     <paramref name="maxBytes"/>, will perform multiple calls to <see cref="ReadSubRangeAsync"/>
        ///     in parallel in order to hit multiple back-end servers and therefore download the data 
        ///     faster.
        /// 
        ///     Returns the total size read.
        /// </remarks>
        private async Task<int> ReadRangeAsync(CloudAppendBlob blob, long start, long maxBytes, CancellationToken cancel)
        {
            // 512KB has been measured as a good slice size. A typical 4MB request is sliced into 8 pieces,
            // which are then downloaded in parallel.
            const int maxSliceSize = 1024 * 512; 
            
            List<Task<int>> todo = null;
            var bufferStart = 0;
            while (maxBytes > 2 * maxSliceSize)
            {
                todo = todo ?? new List<Task<int>>();
                todo.Add(ReadSubRangeAsync(blob, _buffer, start, bufferStart, maxSliceSize, cancel));
                
                maxBytes -= maxSliceSize;
                start += maxSliceSize;
                bufferStart += maxSliceSize;
            }

            var length = bufferStart + await ReadSubRangeAsync(blob, _buffer, start, bufferStart, maxBytes, cancel).ConfigureAwait(false);

            if (todo != null)
            {
                await Task.WhenAll(todo).ConfigureAwait(false);
                foreach (var t in todo)
                {
                    if (t.Result != maxSliceSize)
                        throw new Exception($"Expected {maxSliceSize} bytes but got {t.Result}");
                }
            }
            
            return length;
        }

        /// <summary>
        ///     Reads a sub-range from the specified <paramref name="blob"/> into <see cref="_buffer"/>.        
        /// </summary>
        /// <returns>
        ///      Total bytes read, may be smaller than requested, but only if there were 
        ///      not enough bytes in the blob.
        /// </returns>
        private static async Task<int> ReadSubRangeAsync(
            CloudAppendBlob blob, 
            byte[] buffer, 
            long start, 
            int bufferStart, 
            long maxBytes, 
            CancellationToken cancel)
        {
            while (true)
            {
                try
                {
                    return await blob.DownloadRangeToByteArrayAsync(
                        buffer,
                        bufferStart,
                        start,
                        maxBytes,
                        new AccessCondition(),
                        new BlobRequestOptions(),
                        new OperationContext(),
                        cancel);
                }
                catch (StorageException e) when (e.Message.StartsWith("Incorrect number of bytes received."))
                {
                    // Due to a miscommunication between the Azure servers for Append Blob and the
                    // .NET client library, some responses are garbled due to a race condition out
                    // of our control. We retry straight away, as the problem is rarely present
                    // too many times in a row.
                    // 
                    // See also: https://github.com/Azure/azure-storage-net/issues/366
                }
            }
        }

        public async Task<uint> GetLastKeyAsync(CancellationToken cancel = new CancellationToken())
        {
            // We need to look into the last blob, so we need to list all blobs first
            await RefreshCache(cancel);

            // Look into the last non-empty blob
            if (_firstKey.Count == 0) return 0;
            var blob = _blobs[_firstKey.Count - 1];

            // Download one event's worth of data to a local buffer.
            // This will likely truncate an event somewhere at the beginning of the
            // buffer, but we don't care, because looking for the *last* sequence only
            // requires that the last event in the buffer isn't truncated.
            var length = Math.Min(EventFormat.MaxEventFootprint, blob.Properties.Length);
            var bytes = new byte[length];
            await ReadSubRangeAsync(blob, bytes, blob.Properties.Length - length, 0, length, cancel);

            // Look for the sequence number of the last event.
            using (var stream = new MemoryStream(bytes))
                return await EventFormat.GetLastSequenceAsync(stream, cancel);
        }

        public async Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = new CancellationToken())
        {
            // This function is far from perfect: it merely finds the blob that contains the key, 
            // which means that there could be tens of thousands of useless events to be discarded
            // before reaching the key. There is room for optimization by using a persistent index.

            if (key == 0) return position;

            // If the first key is earlier than the last blob's first key, then we can find a good 
            // approximation without refreshing. Otherwise, we need to refresh to improve the 
            // seek accuracy. 
            if (_firstKey.Count == 0 || _firstKey[_firstKey.Count - 1] <= key)
                await RefreshCache(cancel);

            if (_firstKey.Count == 0) return position;

            // No need for any clever optimization: the number of blobs is small, and the time spent
            // traversing the list is tiny compared to the Azure request costs.
            for (var i = 1; i < _firstKey.Count; ++i)            
                if (_firstKey[i] > key) 
                    return Math.Max(position, _firstPosition[i - 1]);

            // None of the _firstKey were after the key we are looking for: if it exists, it
            // must be in the last blob.
            return Math.Max(position, _firstPosition[_firstPosition.Count - 1]);
        }
    }
}
