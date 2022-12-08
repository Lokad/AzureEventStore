using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> Helper functions used by <see cref="AzureStorageDriver"/>. </summary>
    internal static class AzureHelpers
    {
        /// <summary> Used as a prefix for blob names. </summary>
        private const string Prefix = "events.";

        /// <summary> A suffix for blobs that are compacted. </summary>  
        /// <remarks>
        ///     In order to improve retrieval performance, once an append blob
        ///     `events.NNNNN` reaches its maximum write count, all append blobs
        ///     up to that blob (inclusive) are concatenated into a single block
        ///     blob named `events.NNNNN.compact`. This has two advantages: 
        ///     
        ///     1. It is only necessary to read one blob in order to access all 
        ///     events up to the beginning of the last append blob (meaning at most
        ///     50,000 events). 
        ///     
        ///     2. The compacted block uses 4MB pages (instead of one page per
        ///     append) which is orders of magnitude faster to read back. 
        /// </remarks>
        public const string CompactSuffix = ".compact";

        /// <summary> The name of the nth event blob. </summary>
        private static string NthBlobName(int nth, bool compact = false) =>
            Prefix + nth.ToString("D5") + (compact ? CompactSuffix : "");

        public static int ParseNth(string str)
        {
            if (str == "events.00000")
                return 0;

            if (str.Length != "events.NNNNN".Length)
                throw new ArgumentException($"Expected 'events.NNNNN' but found '{str}'", nameof(str));

            var num = str.Substring("events.".Length).TrimStart('0');

            if (!int.TryParse(num, out var nth))
                throw new ArgumentException($"Expected 'events.NNNNN' but found '{num}' in '{str}'", nameof(str));

            return nth;
        }

        /// <summary>
        ///     Retry an operation without side-effects, if it was 1. interrupted by 
        ///     a HTTP 500 error or 2. took longer than 60 seconds (this can happen when 
        ///     Azure Blob Storage 'loses' a request and takes up to several minutes to 
        ///     fail it, in which case an immediate retry usually succeeds). If 
        ///     <paramref name="likelyLong"/> is false, then on the first attempt the 
        ///     limit is decreased to 3 seconds instead.
        /// </summary>
        public static async Task<T> RetryAsync<T>(
            CancellationToken cancel,
            bool likelyLong,
            Func<CancellationToken, Task<T>> retried)
        {
            for (var retry = 5; ; --retry)
            {
                var delayDuration = likelyLong
                    ? TimeSpan.FromSeconds(60)
                    : TimeSpan.FromSeconds(3);

                // If the first request did not succeed, we need to prepare for the possibility
                // of a long request.
                likelyLong = true;

                using (var delay = new CancellationTokenSource(delayDuration))
                {
                    using (var race = CancellationTokenSource.CreateLinkedTokenSource(delay.Token, cancel))
                    {
                        try
                        {
                            return await retried(race.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                            when (!cancel.IsCancellationRequested && retry > 0)
                        {
                            // Cancellation due to 'delay' or internal timeout, 
                            // not the original 'cancel'
                            continue;
                        }
                        catch (RequestFailedException e)
                            when (e.InnerException is OperationCanceledException &&
                                  !cancel.IsCancellationRequested && retry > 0)
                        {
                            // Cancellation due to 'delay' or internal timeout, 
                            // not the original 'cancel' (and was wrapped by the Azure Storage
                            // library in a RequestFailedException). 
                            continue;
                        }
                        catch (RequestFailedException e)
                            when (e.Status >= 500 && retry > 0)
                        {
                            // Cancellation due to HTTP 500
                            continue;
                        }
                    }
                }
            }
        }

        /// <summary> List all event blobs, in the correct order. </summary>
        public static async Task<List<EventBlob>> ListEventBlobsAsync(
            this BlobContainerClient container,
            CancellationToken cancel = default)
        {
            var eventBlobs = new List<EventBlob>();
            var rawBlobs = new List<BlobItem>();

            var result = container.GetBlobsAsync(traits: BlobTraits.Metadata,
                states: BlobStates.None,
                prefix: Prefix,
                cancellationToken: cancel)
                .AsPages(null);

            await foreach (var page in result)
            {
                foreach (var item in page.Values)
                {
                    if (!(item is BlobItem blob)) continue;
                    rawBlobs.Add(blob);
                }
            }

            // Sort the blobs by name (thanks to NthBlobName, this sorts them in 
            // chronological order), and with the compacted blob *after* the corresponding
            // non-compacted blob. 
            rawBlobs.Sort((a, b) => String.Compare(a.Name, b.Name, StringComparison.Ordinal));

            // Find the last compacted blob, as it will be used as the data backing for 
            // all blobs before it.
            var last = rawBlobs.FindLastIndex(bn => bn.Name.EndsWith(CompactSuffix));

            var offset = 0L;
            for (var i = 0; i < rawBlobs.Count; ++i)
            {
                var appendBlob = rawBlobs[i];
                if (appendBlob.Name.EndsWith(CompactSuffix)) continue;

                var dataBlob = appendBlob;
                var dataOffset = 0L;

                if (i < last)
                {
                    dataBlob = rawBlobs[last];
                    dataOffset = offset;
                }

                var eventBlob = new EventBlob(
                    container,
                    appendBlob,
                    dataBlob,
                    dataOffset,
                    appendBlob.Properties.ContentLength.Value);

                offset += eventBlob.Bytes;

                eventBlobs.Add(eventBlob);
            }

            return eventBlobs;
        }

        /// <summary> Return a reference to the N-th event blob in the container. </summary>
        /// <remarks> Blob may or may not exist. </remarks>
        public static AppendBlobClient ReferenceEventBlob(this BlobContainerClient container, int nth)
        {
            return container.GetAppendBlobClient(NthBlobName(nth));
        }

        /// <summary> Creates the blob if it does not exist. Do nothing if it already does.  </summary>
        public static async Task CreateIfNotExistsAsync(
            this AppendBlobClient blob,
            CancellationToken cancel = default)
        {
            try
            {
                await blob.CreateAsync(new AppendBlobCreateOptions(), cancel);
            }
            catch (RequestFailedException e)
            {
                if (e.ErrorCode != "BlobAlreadyExists") throw;
            }
        }

        /// <summary> True if the exception denotes a "append position is too early" situation. </summary>
        public static bool IsCollision(this RequestFailedException e)
        {
            return e.Status == 412;
        }

        /// <summary> True if the exception denotes a "too many appends on blob" situation. </summary>
        public static bool IsMaxReached(this RequestFailedException e)
        {
            return e.ErrorCode == "BlockCountExceedsLimit";
        }

        /// <summary> Append bytes to a blob only if at the provided append position. </summary>
        /// <remarks> This will throw a <see cref="RequestFailedException"/> on conflict. </remarks>
        public static Task AppendTransactionalAsync(
            this AppendBlobClient blob,
            byte[] data,
            long position,
            CancellationToken cancel = default)
        {
            return blob.AppendBlockAsync(
                new MemoryStream(data, 0, data.Length),
                conditions: new AppendBlobRequestConditions { IfAppendPositionEqual = position },
                cancellationToken: cancel);
        }

        /// <summary>
        ///     Reads a sub-range from the specified <paramref name="blob"/> into <see cref="_buffer"/>. 
        /// </summary>
        /// <returns>
        ///      Total bytes read, may be smaller than requested, but only if there were 
        ///      not enough bytes in the blob. <paramref name="likelyLong"/> will be true
        ///      if the caller expects the maxBytes to be reached by the read.
        /// </returns>
        public static async Task<int> ReadSubRangeAsync(
            this BlobClient blob,
            Memory<byte> buffer,
            long start,
            bool likelyLong,
            CancellationToken cancel)
        {
            while (true)
            {
                try
                {
                    return await RetryAsync(cancel, likelyLong, async c =>
                    {
                        var downloaded = await blob.DownloadContentAsync(range: new HttpRange(start, buffer.Length), cancellationToken: c);
                        var content = downloaded.Value.Content.ToMemory();
                        content.Span.CopyTo(buffer.Span);
                        return content.Length;
                    });
                }
                catch (RequestFailedException e) when (e.Message.StartsWith("Incorrect number of bytes received."))
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
    }
}
