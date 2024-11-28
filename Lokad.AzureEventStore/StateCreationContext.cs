#nullable enable
using Lokad.AzureEventStore.Projections;
using Lokad.MemoryMapping;

namespace Lokad.AzureEventStore
{
    /// <summary>
    /// Settings used to create the state."/>
    /// </summary>
    public class StateCreationContext
    {
        /// <summary>
        ///     Final memory mapped folder chosen among the candidates provided by the projection folder provider.
        /// </summary>
        public IMemoryMappedFolder? MemoryMappedFolder { get; }

        /// <summary>
        ///     Provider of a persistent cache to read and write the projection states.
        /// </summary>
        public IProjectionCacheProvider? CacheProvider { get; }

        public StateCreationContext(IMemoryMappedFolder? memoryMappedFolder, IProjectionCacheProvider? cacheProvider)
        {
            MemoryMappedFolder = memoryMappedFolder;
            CacheProvider = cacheProvider;
        }
    }
}
