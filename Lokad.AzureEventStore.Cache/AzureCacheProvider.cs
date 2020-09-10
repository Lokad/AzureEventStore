using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Lokad.AzureEventStore.Cache
{
    /// <summary> Persists the cache to a blob storage container. </summary>
    /// <remarks>
    ///     The blobs are named as [statename]/[yyyyMMddhhmmss]. Older blobs
    ///     get deleted once enough have been accumulated. 
    /// </remarks>
    public sealed class AzureCacheProvider : IProjectionCacheProvider
    {
        private readonly CloudBlobContainer _container;
        
        /// <summary>
        ///     If true, will read blobs from the cache. If false, calls to 
        ///     <see cref="OpenReadAsync(string)"/> return null.
        /// </summary>
        public bool AllowRead { get; set; }

        /// <summary>
        ///     If true, will write blobs to the cache. If false, calls to 
        ///     <see cref="OpenWriteAsync(string)"/> return null.
        /// </summary>
        public bool AllowWrite { get; set; }

        /// <summary> If this number of blobs is present in the cache, clear the oldest ones. </summary>
        /// <remarks>
        ///     This number should be at least as large as the number of instances that can
        ///     start simultaneously (as instance N may delete the blob still being read from by instance 
        ///     N - Max).
        ///     
        ///     Clearing occurs during calls to <see cref="OpenWriteAsync(string)"/>, if writing 
        ///     is enabled.
        /// </remarks>
        public int MaxCacheBlobsCount { get; set; } = 100;

        public AzureCacheProvider(CloudBlobContainer container)
        {
            _container = container;
        }

        /// <summary> List all blobs for the specified state. </summary>
        /// <remarks>
        ///     Returned blobs are listed by descending name (i.e. most recent
        ///     first).
        /// </remarks>
        private async Task<CloudBlockBlob[]> Blobs(string stateName)
        {
            await _container.CreateIfNotExistsAsync();

            var result = new List<CloudBlockBlob>();

            var ct = default(BlobContinuationToken);
            while (true)
            {
                var list = await _container.ListBlobsSegmentedAsync(
                    stateName,
                    /* useFlatBlobListing: */true,
                    BlobListingDetails.None,
                    /* max results */ null, ct,
                    new BlobRequestOptions(),
                    new OperationContext());

                ct = list.ContinuationToken;

                foreach (var item in list.Results)
                    if (item is CloudBlockBlob blob)
                        result.Add(blob);

                if (ct == null) break;
            }

            return result.OrderByDescending(b => b.Name).ToArray();
        }

        public async Task<IEnumerable<Task<CacheCandidate>>> OpenReadAsync(string stateName)
        {
            if (!AllowRead) return null;

            return Enumerate(await Blobs(stateName));

            IEnumerable<Task<CacheCandidate>> Enumerate(CloudBlockBlob[] orderedBlobs)
            {
                foreach (var blob in orderedBlobs)
                    yield return Task.Run(async () => 
                        new CacheCandidate(
                            "blob:" + blob.Name, 
                            await blob.OpenReadAsync()));
            }
        }

        public async Task TryWriteAsync(string stateName, Func<Stream, Task> write)
        {
            if (!AllowWrite) return;

            var all = await Blobs(stateName);

            var name = stateName + "/" + DateTime.UtcNow.ToString("yyyyMMddHHmmss");

            // Not the latest: don't bother writing.
            if (all.Length > 0 && string.CompareOrdinal(all[0].Name, name) >= 0)
                return;

            // Clean-up the old and deprecated versions of the cache
            for (var i = MaxCacheBlobsCount; i < all.Length; ++i)
            {
                try
                {
                    await _container.GetBlockBlobReference(all[i].Name).DeleteAsync();
                }
                catch
                {
                    // It's not a problem if deletion failed.
                }
            }
            
            var blob = _container.GetBlockBlobReference(name);
            var stream = await blob.OpenWriteAsync(
                AccessCondition.GenerateIfNotExistsCondition(),
                new BlobRequestOptions(),
                new OperationContext());

            try
            {
                await write(stream).ConfigureAwait(false);
                stream.Dispose();
            }
            catch
            {
                // If failed to write, delete the incomplete blob
                stream.Dispose();
                await blob.DeleteIfExistsAsync().ConfigureAwait(false);
                throw;
            }
        }
    }
}
