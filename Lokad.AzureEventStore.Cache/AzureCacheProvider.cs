using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Threading;
using Azure.Storage.Blobs.Specialized;

namespace Lokad.AzureEventStore.Cache
{
    /// <summary> Persists the cache to a blob storage container. </summary>
    /// <remarks>
    ///     The blobs are named as [statename]/[yyyyMMddhhmmss]. Older blobs
    ///     get deleted once enough have been accumulated. 
    /// </remarks>
    public sealed class AzureCacheProvider : IProjectionCacheProvider
    {
        private readonly BlobContainerClient _container;

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

        public AzureCacheProvider(BlobContainerClient container)
        {
            _container = container;
        }

        /// <summary> List all blobs for the specified state. </summary>
        /// <remarks>
        ///     Returned blobs are listed by descending name (i.e. most recent
        ///     first).
        /// </remarks>
        private async Task<BlobItem[]> Blobs(string stateName)
        {
            // Calling CreateIfNotExists with insufficient privilege will
            // throw an exception _even if_ the container _exists_.
            // Attempting to create the container only if it doesn't exist
            // allows to read existing state caches even if the caller does not
            // have sufficient permission to create the container.
            if (AllowWrite && !await _container.ExistsAsync().ConfigureAwait(false))
                await _container.CreateIfNotExistsAsync().ConfigureAwait(false);

            var result = new List<BlobItem>();

            var token = default(string);
            while (true)
            {
                var list = _container.GetBlobsAsync(traits: BlobTraits.Metadata,
                   states: BlobStates.None,
                   prefix: stateName,
                   cancellationToken: default(CancellationToken))
                   .AsPages(token);

                await foreach (var page in list)
                {
                    foreach (var item in page.Values)
                    {
                        if (!(item is BlobItem blob)) continue;
                        result.Add(blob);
                    }

                    token = page.ContinuationToken;
                }
                if (string.IsNullOrEmpty(token)) break;
            }

            return result.OrderByDescending(b => b.Name).ToArray();
        }

        public async Task<IEnumerable<Task<CacheCandidate>>> OpenReadAsync(string stateName)
        {
            if (!AllowRead) return null;

            return Enumerate(await Blobs(stateName));

            IEnumerable<Task<CacheCandidate>> Enumerate(BlobItem[] orderedBlobs)
            {
                foreach (var blob in orderedBlobs)
                    yield return Task.Run(async () =>
                        new CacheCandidate(
                            "blob:" + blob.Name,
                            await _container.GetBlobClient(blob.Name).OpenReadAsync()));
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
                    await _container.GetBlockBlobClient(all[i].Name).DeleteAsync();
                }
                catch
                {
                    // It's not a problem if deletion failed.
                }
            }

            var blob = _container.GetBlockBlobClient(name);

            var stream = await blob.OpenWriteAsync(true);

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
