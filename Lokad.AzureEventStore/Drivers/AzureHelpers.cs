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

        /// <summary> Parse the numeric suffix in 'event.NNNNN' blob name. </summary>
        public static int ParseNth(this BlobItem blob) =>
            ParseNth(blob.Name);

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
        public static async Task<List<BlobItem>> ListEventBlobsAsync(
            this BlobContainerClient container,
            CancellationToken cancel = default)
        {
            var freshBlobList = new List<BlobItem>();

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
                    freshBlobList.Add(blob);
                }
            }

            // Sort the blobs by name (thanks to NthBlobName, this sorts them in 
            // chronological order), and with the compacted blob *after* the corresponding
            // non-compacted blob. 
            freshBlobList.Sort((a, b) => String.Compare(a.Name, b.Name, StringComparison.Ordinal));

            // Find the last compacted blob, since we don't need any blobs before it.
            for (var i = freshBlobList.Count - 1; i >= 0; --i)
            {
                if (freshBlobList[i].Name.EndsWith(CompactSuffix))
                {
                    freshBlobList.RemoveRange(0, i);
                    break;
                }
            }

            return freshBlobList;
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
                await blob.CreateAsync(new AppendBlobCreateOptions());
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
    }
}
