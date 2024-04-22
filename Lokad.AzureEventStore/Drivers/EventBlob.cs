using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary>
    ///     Represents an individual append blob in the event stream.
    /// </summary>
    public sealed class EventBlob
    {
        public EventBlob(
            BlobContainerClient container,
            BlobItem appendBlob,
            BlobItem dataBlob,
            long offset,
            long bytes)
        {
            _container = container ?? throw new ArgumentNullException(nameof(container));
            AppendBlob = appendBlob ?? throw new ArgumentNullException(nameof(appendBlob));
            DataBlob = dataBlob ?? throw new ArgumentNullException(nameof(dataBlob));
            Offset = offset;
            Bytes = bytes;
        }

        /// <summary> Used to create <see cref="BlobClient"/> instances. </summary>
        private readonly BlobContainerClient _container;

        /// <summary> The append blob itself, including its metadata. </summary>
        public BlobItem AppendBlob { get; }

        /// <summary>
        ///     The blob from which the contents of the blob should be read.
        ///     Because of compaction, most older blobs should no longer be read
        ///     directly, but instead their data will be found in a compacted,
        ///     larger block blob. For more recent blobs, this will be the same
        ///     as <see cref="AppendBlob"/>.
        /// </summary>
        /// <remarks>
        ///     Data starts at <see cref="Offset"/> and contains <see cref="Bytes"/> bytes. 
        /// </remarks>
        public BlobItem DataBlob { get; }

        /// <summary>
        ///     Offset of this blob's data inside <see cref="DataBlob"/>
        /// </summary>
        public long Offset { get; }

        /// <summary>
        ///     Number of bytes in this blob. If this a compacted blob, will be enforced
        ///     on all reads ; for non-compacted blobs, it will be taken as a possibly
        ///     stale value and reads will be allowed to go past the end (just in case more
        ///     data was appended).
        /// </summary>
        public long Bytes { get; }

        /// <summary>
        ///     Used by <see cref="GetFirstKeyAsync(CancellationToken)"/> to cache
        ///     its result in the metadata.
        /// </summary>
        private const string _firstKeyMetadata = "FirstKey";

        private const string _cacheErrorCode = "AuthorizationPermissionMismatch";

        /// <summary>
        ///     The key of the first event in this blob. Will cache the value in
        ///     the metadata of the blob, to improve performance.
        /// </summary>
        public async Task<uint> GetFirstKeyAsync(CancellationToken cancel)
        {
            if (AppendBlob.Metadata.TryGetValue(_firstKeyMetadata, out var str) &&
                uint.TryParse(str, out var key))
            {
                return key;
            }

            var buffer = new byte[4];

            await ReadSubRangeAsync(buffer, 2, false, cancel);

            key = buffer[0]
                  + ((uint)buffer[1] << 8)
                  + ((uint)buffer[2] << 16)
                  + ((uint)buffer[3] << 24);

            // Trying to cache the key so that subsequent reads find it faster
            // Catch when we don't have permission to do so.
            var appendClient = _container.GetBlobClient(AppendBlob.Name);
            try
            {
                await appendClient.SetMetadataAsync(new Dictionary<string, string>
                {
                    { _firstKeyMetadata, key.ToString(CultureInfo.InvariantCulture) }
                });
            }
            catch (RequestFailedException rfe) when (rfe.ErrorCode == _cacheErrorCode) { }            

            return key;
        }

        /// <summary>
        ///     Reads a sub-range from this blob into <see cref="_buffer"/>.
        /// </summary>
        /// <returns>
        ///      Total bytes read, may be smaller than requested, but only if there were 
        ///      not enough bytes in the blob. <paramref name="likelyLong"/> will be true
        ///      if the caller expects the maxBytes to be reached by the read.
        /// </returns>
        public async Task<int> ReadSubRangeAsync(Memory<byte> buffer, long start, bool likelyLong, CancellationToken cancel)
        {
            var dataClient = _container.GetBlobClient(DataBlob.Name);

            start += Offset;

            if (DataBlob != AppendBlob && start + buffer.Length > Offset + Bytes)
                buffer = buffer.Slice(0, (int)(Offset + Bytes - start));

            if (buffer.Length == 0)
                return 0;

            return await dataClient.ReadSubRangeAsync(buffer, start, likelyLong, cancel);
        }

        /// <summary>
        ///     True if this append blob's data was included in a compacted blob.
        /// </summary>
        public bool IsCompacted => 
            DataBlob != AppendBlob;

        /// <summary> Parse the numeric suffix in 'event.NNNNN' blob name. </summary>
        public int Nth => AzureHelpers.ParseNth(AppendBlob.Name);

        /// <summary>
        /// Return an append blob client to append to this blob.
        /// </summary>
        public AppendBlobClient GetAppendBlobClient() =>
            _container.GetAppendBlobClient(AppendBlob.Name);
    }
}
