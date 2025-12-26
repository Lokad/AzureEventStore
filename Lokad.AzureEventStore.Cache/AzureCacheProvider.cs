#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Threading;
using Azure.Storage.Blobs.Specialized;

namespace Lokad.AzureEventStore.Cache;

/// <summary> Persists the cache to a blob storage container. </summary>
/// <remarks>
///     The blobs are named as [statename]/[yyyyMMddhhmmss]. Older blobs
///     get deleted once enough have been accumulated. 
/// </remarks>
public sealed class AzureCacheProvider(BlobContainerClient container) : IProjectionCacheProvider
{
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
        if (AllowWrite && !await container.ExistsAsync().ConfigureAwait(false))
            await container.CreateIfNotExistsAsync().ConfigureAwait(false);

        var result = new List<BlobItem>();

        var list = container.GetBlobsAsync(traits: BlobTraits.Metadata,
                states: BlobStates.None,
                prefix: stateName,
                cancellationToken: CancellationToken.None)
            .AsPages(null);

        await foreach (var page in list)
        {
            foreach (var item in page.Values)
            {
                if (item is null) continue;
                result.Add(item);
            }
        }

        return result.OrderByDescending(b => b.Name).ToArray();
    }

    public async Task<IAsyncEnumerable<CacheCandidate>> OpenReadAsync(ILogAdapter? log, string stateName)
    {
        if (!AllowRead) return AsyncEnumerable.Empty<CacheCandidate>();

        var orderedBlobs = await Blobs(stateName);
        return orderedBlobs
            .ToAsyncEnumerable()
#if NET10_0_OR_GREATER
            .Select(async (BlobItem blob, CancellationToken cancel) =>
#else
            .SelectAwait(async (blob) =>
#endif
                new CacheCandidate(
                    "blob:" + blob.Name,
                    await container.GetBlobClient(blob.Name).OpenReadAsync()));
    }

    public async Task TryWriteAsync(ILogAdapter? log, string stateName, Func<Stream, Task> write)
    {
        if (!AllowWrite)
        {
            log?.Debug($"Unwritable blob container for {stateName}");
            return;
        }

        var all = await Blobs(stateName);

        var name = $"{stateName}/{DateTime.UtcNow:yyyyMMddHHmmss}";

        // Not the latest: don't bother writing.
        if (all.Length > 0 && string.CompareOrdinal(all[0].Name, name) >= 0)
            return;

        log?.Info($"Start saving {stateName} in blob as {name}");
            
        // Clean-up the old and deprecated versions of the cache
        for (var i = MaxCacheBlobsCount; i < all.Length; ++i)
        {
            try
            {
                await container.GetBlockBlobClient(all[i].Name).DeleteAsync();
            }
            catch
            {
                // It's not a problem if deletion failed.
            }
        }

        var blob = container.GetBlockBlobClient(name);

        var stream = await blob.OpenWriteAsync(true);

        try
        {
            await write(stream).ConfigureAwait(false);
            log?.Info($"End saving {stateName} in blob as {name}");
            await stream.DisposeAsync();
        }
        catch
        {
            // If failed to write, delete the incomplete blob
            await stream.DisposeAsync();
            await blob.DeleteIfExistsAsync().ConfigureAwait(false);
            throw;
        }
    }
}