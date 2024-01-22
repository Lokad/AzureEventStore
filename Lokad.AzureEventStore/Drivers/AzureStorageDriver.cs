using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

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

        /// <summary> All blobs, in the appropriate order. </summary>
        /// <remarks>
        ///     Always as long as <see cref="_firstPosition"/>.
        /// </remarks>
        private readonly List<EventBlob> _blobs = new List<EventBlob>();

        /// <summary> Read-only access to <see cref="_blobs"/>. </summary>
        internal IReadOnlyList<EventBlob> Blobs => _blobs;

        /// <summary> The container where blobs are stored. </summary>
        private readonly BlobContainerClient _container;

        internal AzureStorageDriver(BlobContainerClient container)
        {
            _container = container;
        }

        /// <summary> The last known write position. Used to reject writes early. </summary>
        private long _lastKnownPosition;

        /// <summary> 
        /// Refreshes the local cache of positions, keys and blobs from Azure.         
        /// Returns the write position.
        /// </summary>        
        internal async Task<long> RefreshCache(CancellationToken cancel)
        {
            // Download the complete list of blobs from the server. It's easier to 
            // perform processing in-memory rather than streaming through the listing.
            var newBlobs = await AzureHelpers.RetryAsync(cancel, false, _container.ListEventBlobsAsync);

            if (newBlobs.Count == 0)
                // This is an empty stream.
                return _lastKnownPosition = 0;

            var nonCompactedBlobs = 0;
            for (var i = 0; i < _blobs.Count; i++)
            {
                if (!newBlobs[i].IsCompacted)
                {
                    ++nonCompactedBlobs;
                }
                else if (!_blobs[i].IsCompacted)
                {
                    _blobs[i] = newBlobs[i];
                }
            }

            if (nonCompactedBlobs > 1)
                // Can benefit from compaction.
                TriggerCompaction(newBlobs);

            // STEP 1: _firstPosition and _lastKnownPosition
            // =============================================

            // Take into account any blobs not already present in the cache.
            // This code computes _firstPosition exactly once for each blob (over multiple 
            // calls).
            for (var i = _blobs.Count; i < newBlobs.Count; ++i)
            {
                _blobs.Add(newBlobs[i]);
                _firstPosition.Add(i == 0 ? 0L : _firstPosition[i - 1] + newBlobs[i - 1].Bytes);
            }

            // The last known position always increases based on the length of the last blob.
            _lastKnownPosition = newBlobs[^1].Bytes
                                 + _firstPosition[^1];

            // STEP 2: _firstKey
            // =================

            // Is the last blob empty or not ? If it is, we don't want to compute
            // its _firstKey yet.
            var nonEmptyBlobCount = _blobs[^1].Bytes < 6
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
                        _firstKey[pos] = await _blobs[pos].GetFirstKeyAsync(cancel);
                    }));

            return _lastKnownPosition;
        }

        public Task<long> GetPositionAsync(CancellationToken cancel = default)
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
            var nth = _blobs.Count == 0 ? 0 : _blobs[^1].Nth + 1;
            var blob = _container.ReferenceEventBlob(nth);

            await blob.CreateIfNotExistsAsync(cancel);

            await RefreshCache(cancel);
        }

        public async Task<DriverWriteResult> WriteAsync(
            long position,
            IEnumerable<RawEvent> events,
            CancellationToken cancel = default)
        {
            using var act = Logging.Drivers.StartActivity("AzureStorageDriver.WriteAsync");
            act?.SetTag(Logging.PositionWrite, position)
                .SetTag(Logging.PositionStream, _lastKnownPosition);

            // Caller knows something we don't? Refresh!
            if (position > _lastKnownPosition)
                _lastKnownPosition = await RefreshCache(cancel);

            act?.SetTag(Logging.PositionStream, _lastKnownPosition);

            // Quick early-out with no Azure request involved.
            if (position < _lastKnownPosition)
            {
                act?.SetStatus(ActivityStatusCode.Error, "Conflict");
                return new DriverWriteResult(_lastKnownPosition, false);
            }

            // This should only happen very rarely, but it still needs to be done.
            if (_blobs.Count == 0) await CreateLastBlob(cancel);

            // Generate appended payload

            var payload = ArrayPool<byte>.Shared.Rent(4 * 1024 * 1024);
            var payloadLength = 0;
            var eventCount = 0;
            try
            {
                foreach (var e in events)
                {
                    eventCount++;
                    payloadLength += EventFormat.Write(payload.AsMemory(payloadLength), e);
                }

                act?.SetTag(Logging.EventCount, eventCount)
                    .SetTag(Logging.EventSize, payloadLength);

                // Nothing to write, but still check position
                if (payloadLength == 0)
                {
                    act?.SetStatus(ActivityStatusCode.Ok);
                    _lastKnownPosition = await RefreshCache(cancel);
                    return new DriverWriteResult(_lastKnownPosition, position == _lastKnownPosition);
                }

                // Attempt write to last blob.
                bool collision;
                try
                {
                    var offset = _firstPosition[^1];
                    var lastBlob = _blobs[^1].GetAppendBlobClient();
                    await lastBlob.AppendTransactionalAsync(payload, payloadLength, position - offset, cancel);

                    _lastKnownPosition = position + payloadLength;

                    act?.SetStatus(ActivityStatusCode.Ok);
                    return new DriverWriteResult(_lastKnownPosition, true);
                }
                catch (RequestFailedException e)
                {
                    if (!e.IsCollision() && !e.IsMaxReached()) throw;

                    collision = e.IsCollision();
                }

                // Collision means we do not have the proper _lastKnownPosition, so refresh
                // the cache and return a failure.
                if (collision)
                {
                    _lastKnownPosition = await RefreshCache(cancel);
                    
                    act?.SetTag(Logging.PositionStream, _lastKnownPosition)
                        .SetStatus(ActivityStatusCode.Error, "Collision");

                    return new DriverWriteResult(_lastKnownPosition, false);
                }

                // Too many appends can be solved by creating a new blob and appending to it. 
                // The append code is similar but subtly different, so we just rewrite it 
                // below.
                await CreateLastBlob(cancel);

                try
                {
                    var lastBlob = _blobs[^1].GetAppendBlobClient();
                    await lastBlob.AppendTransactionalAsync(payload, payloadLength, 0, cancel);

                    _lastKnownPosition = position + payloadLength;

                    act?.SetStatus(ActivityStatusCode.Ok);
                    return new DriverWriteResult(_lastKnownPosition, true);
                }
                catch (RequestFailedException e)
                {
                    // Only a collision can stop us here, no max-appends-reached should
                    // happen when appending at position 0.
                    if (!e.IsCollision()) throw;
                }

                // Collision here means someone else already created the new blob and appended
                // to it, so refresh everything and report failure.

                _lastKnownPosition = await RefreshCache(cancel);

                act?.SetTag(Logging.PositionStream, _lastKnownPosition)
                    .SetStatus(ActivityStatusCode.Error, "Collision");

                return new DriverWriteResult(_lastKnownPosition, false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(payload);
            }
        }

        public async Task<DriverReadResult> ReadAsync(
            long position,
            Memory<byte> backing,
            CancellationToken cancel = default)
        {
            using var act = Logging.Drivers.StartActivity("AzureStorageDriver.ReadAsync");
            act?.SetTag(Logging.PositionRead, position)
                .SetTag(Logging.PositionStream, _lastKnownPosition);

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

            EventBlob blob = null;
            long firstPosition = 0;
            long blobSize = 0;

            for (var i = _blobs.Count - 1; i >= 0; --i)
            {
                if (_firstPosition[i] <= position)
                {
                    blob = _blobs[i];
                    firstPosition = _firstPosition[i];
                    blobSize = (i == _blobs.Count - 1 ? _lastKnownPosition : _firstPosition[i + 1]) - firstPosition;
                    break;
                }
            }

            if (blob == null)
                // Since _firstPosition[0] == 0, this means the position is negative
                throw new ArgumentOutOfRangeException("Invalid position:" + position, "position");

            // STEP 3: READ RAW DATA
            // =====================

            var startPos = position - firstPosition;
            var maxBytes = (int)Math.Min(backing.Length, blobSize - startPos);
            backing = backing[..maxBytes];

            if (maxBytes == 0)
            {
                act?.SetTag(Logging.EventCount, 0)
                    .SetTag(Logging.EventSize, 0);

                return new DriverReadResult(_lastKnownPosition, Array.Empty<RawEvent>());
            }

            var readBytes = await ReadRangeAsync(blob, startPos, backing, cancel);
            backing = backing[..readBytes];

            // STEP 4: PARSE DATA
            // ==================

            var events = new List<RawEvent>();
            var parsedBytes = 0;

            try
            {
                while (EventFormat.TryParse(backing[parsedBytes..], out var parsed) is RawEvent re)
                {
                    events.Add(re);
                    parsedBytes += parsed;
                }
            }
            catch (InvalidDataException e)
            {
                throw new InvalidDataException($"{e.Message} at {position + parsedBytes}");
            }

            act?.SetTag(Logging.EventCount, events.Count)
                .SetTag(Logging.EventSize, parsedBytes);

            return new DriverReadResult(position + parsedBytes, events);
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
        private async Task<int> ReadRangeAsync(EventBlob blob, long start, Memory<byte> backing, CancellationToken cancel)
        {
            // 512KB has been measured as a good slice size. A typical 4MB request is sliced into 8 pieces,
            // which are then downloaded in parallel.
            const int maxSliceSize = 1024 * 512;
            var maxBytes = backing.Length;

            List<Task<int>> todo = null;
            var bufferStart = 0;
            while (maxBytes > 2 * maxSliceSize)
            {
                todo ??= new List<Task<int>>();
                todo.Add(blob.ReadSubRangeAsync(
                    backing.Slice(bufferStart, maxSliceSize),
                    start,
                    true,
                    cancel));

                maxBytes -= maxSliceSize;
                start += maxSliceSize;
                bufferStart += maxSliceSize;
            }

            var length = bufferStart + await blob.ReadSubRangeAsync(
                backing.Slice(bufferStart, maxBytes),
                start,
                blob.Bytes >= start + maxBytes,
                cancel).ConfigureAwait(false);

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

        public async Task<uint> GetLastKeyAsync(CancellationToken cancel = default)
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
            var length = Math.Min(EventFormat.MaxEventFootprint, blob.Bytes);
            var bytes = new byte[length];
            await blob.ReadSubRangeAsync(
                bytes,
                blob.Bytes - length,
                false,
                cancel);

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

        /// <summary> Becomes non-null if a compaction is currently being performed. </summary>
        internal Task RunningCompaction { get; private set; }

        /// <summary>
        ///     If no compaction is currently running, perform a compaction by compressing all  
        ///     blobs in <paramref name="blobs"/> (except the last one, which is still 
        ///     incomplete and being appended to) into a single block blob with large blocks,
        ///     to improve read performance.
        ///     
        ///     The running task is stored in <see cref="RunningCompaction"/>. 
        /// </summary>
        private void TriggerCompaction(IReadOnlyList<EventBlob> blobs)
        {
            // Another compaction running.
            if (RunningCompaction != null && !RunningCompaction.IsCompleted) return;

            // Create a copy for local use, since the caller may mess it up. Only include
            // finished blocks (the last block is unfinished and shouldn't be included).
            blobs = blobs.Take(blobs.Count - 1).ToArray();

            RunningCompaction = Task.Run(async () =>
            {
                using var act = Logging.Drivers.StartActivity("AzureStorageDriver.RunningCompaction");

                // This function assumes that the container is writable. If it isn't, the
                // write operations will throw and execution stops silently.
                //
                // This function doesn't need to assume that it is not running concurrently.
                // If two invocations of the function attempt to the write the blob, the
                // second one will throw, but the first one will have written a valid block
                // beforehand, which is what really matters here.

                if (blobs[^1].IsCompacted)
                    // Already a compact blob.
                    return;

                var blobName = blobs[^1].AppendBlob.Name + AzureHelpers.CompactSuffix;

                act?.AddTag("blob", blobName);

                var compactBlob = _container.GetBlockBlobClient(blobName);

                // Building a block blob: write out several 4MB blocks which will
                // be committed in the right order at the end.
                var blocks = new List<string>();
                var buffer = new byte[4 * 1024 * 1024];
                var used = 0;
                var totalWritten = 0L;

                Task WriteBlockAsync()
                {
                    var uid = Convert.ToBase64String(Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N")));
                    blocks.Add(uid);
                    return compactBlob.StageBlockAsync(
                        uid,
                        new MemoryStream(buffer, 0, used, writable: false),
                        MD5.Create().ComputeHash(buffer, 0, used));
                }

                foreach (var blob in blobs)
                {
                    var blobOffset = 0L;
                    var blobLength = blob.Bytes;

                    totalWritten += blobLength;

                    while (blobOffset < blobLength)
                    {
                        var read = await blob.ReadSubRangeAsync(
                            buffer.AsMemory(used, (int)Math.Min(buffer.Length - used, blobLength - blobOffset)),
                            blobOffset,
                            true,
                            CancellationToken.None);

                        used += read;
                        blobOffset += read;

                        if (used == buffer.Length)
                        {
                            await WriteBlockAsync();
                            used = 0;
                        }
                    }
                }

                if (used > 0)
                    await WriteBlockAsync();

                // Commit the list of blocks in one call.
                await compactBlob.CommitBlockListAsync(blocks);

                act?.AddTag("blob.blocks", blocks.Count)
                    .AddTag("blob.size", totalWritten);
            });
        }
    }
}
