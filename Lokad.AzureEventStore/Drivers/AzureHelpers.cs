using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

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

        /// <summary> List all event blobs, in the correct order. </summary>
        public static async Task<List<CloudBlob>> ListEventBlobsAsync(
            this CloudBlobContainer container,
            CancellationToken cancel = default(CancellationToken))
        {
            var freshBlobList = new List<CloudBlob>();
            var token = new BlobContinuationToken();
            while (token != null)
            {
                var list = await container.ListBlobsSegmentedAsync(
                    useFlatBlobListing: true,
                    prefix: Prefix,
                    blobListingDetails: BlobListingDetails.Metadata,
                    maxResults: null,
                    currentToken: token,
                    options: new BlobRequestOptions(),
                    operationContext: new OperationContext(),
                    cancellationToken: cancel);

                token = list.ContinuationToken;

                freshBlobList.AddRange(list.Results.OfType<CloudBlob>());
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
        public static CloudAppendBlob ReferenceEventBlob(this CloudBlobContainer container, int nth)
        {
            return container.GetAppendBlobReference(NthBlobName(nth));
        }

        /// <summary> Creates the blob if it does not exist. Do nothing if it already does.  </summary>
        public static async Task CreateIfNotExistsAsync(
            this CloudAppendBlob blob,
            CancellationToken cancel = default(CancellationToken))
        {
            try
            {
                await blob.CreateOrReplaceAsync(
                    operationContext: new OperationContext(),
                    options: new BlobRequestOptions(),
                    accessCondition: new AccessCondition { IfNoneMatchETag = "*" },
                    cancellationToken: cancel);
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.ExtendedErrorInformation.ErrorCode != "BlobAlreadyExists") throw;
            }
        }

        /// <summary> True if the exception denotes a "append position is too early" situation. </summary>
        public static bool IsCollision(this StorageException e)
        {
            return e.RequestInformation.HttpStatusCode == 412;
        }

        /// <summary> True if the exception denotes a "too many appends on blob" situation. </summary>
        public static bool IsMaxReached(this StorageException e)
        {
            return e.RequestInformation?.ExtendedErrorInformation?.ErrorCode == "BlockCountExceedsLimit";
        }

        /// <summary> Append bytes to a blob only if at the provided append position. </summary>
        /// <remarks> This will throw a <see cref="StorageException"/> on conflict. </remarks>
        public static Task AppendTransactionalAsync(
            this CloudAppendBlob blob,
            byte[] data,
            long position,
            CancellationToken cancel = default(CancellationToken))
        {
            return blob.AppendFromByteArrayAsync(
                accessCondition: new AccessCondition {IfAppendPositionEqual = position},
                buffer: data,
                index: 0,
                count: data.Length,
                options: new BlobRequestOptions(),
                operationContext: new OperationContext(),
                cancellationToken: cancel);
        }
    }
}
