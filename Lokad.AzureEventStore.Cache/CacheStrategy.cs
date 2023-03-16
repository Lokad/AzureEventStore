using Azure.Storage.Blobs;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Streams;
using System;
namespace Lokad.AzureEventStore.Cache
{
    /// <summary>
    /// Allow the user to select the cache strategy for an even stream.
    /// </summary>
    public static class CacheStrategy
    {
        /// <summary>
        /// No cache.
        /// </summary>
        public static Func<IEventStream, IProjectionCacheProvider> None => null;
        /// <summary>
        /// Use a remote cache and no local storage.
        /// </summary>
        public static Func<IEventStream, IProjectionCacheProvider> Remote(BlobContainerClient blobContainerClient = null)
        {
            if (blobContainerClient != null)
                return (IEventStream _) => new AzureCacheProvider(blobContainerClient) { AllowRead = true, AllowWrite = true };
            return (IEventStream eventStream) =>
            {
                if (eventStream.StateCache == null)
                    throw new InvalidOperationException("Event stream state cache is not defined.");
                return new AzureCacheProvider(eventStream.StateCache) { AllowRead = true, AllowWrite = true };
            };
        }
        /// <summary>
        /// Only local storage and no remote cache.
        /// </summary>
        public static Func<IEventStream, IProjectionCacheProvider> Local(string folder)
        {
            return (IEventStream _) => new FileCacheProvider(folder);
        }
        public static Func<IEventStream, IProjectionCacheProvider> SharedLocal(string folder, BlobContainerClient blobContainerClient = null)
        {
            return SharedLocal(folder, false, blobContainerClient);
        }
        /// <summary>
        /// Expect the event stream to have a state cache option, which it will use as the remote cache store.
        /// The folder is used as the local cache. 
        /// After applying all events and the cache is up-to-date, it is first save to the local cache, then to the remote store.
        /// </summary>
        public static Func<IEventStream, IProjectionCacheProvider> SharedLocal(string folder, bool readOnly, BlobContainerClient blobContainerClient = null)
        {
            if (blobContainerClient != null)
                return (IEventStream _) => new MappedCacheProvider(folder, new AzureCacheProvider(blobContainerClient) { AllowRead = true, AllowWrite = !readOnly });
            return (IEventStream eventStream) =>
            {
                if (eventStream.StateCache == null)
                    return new MappedCacheProvider(folder);
                return new MappedCacheProvider(folder, new AzureCacheProvider(eventStream.StateCache) { AllowRead = true, AllowWrite = !readOnly });
            };
        }
    }
}