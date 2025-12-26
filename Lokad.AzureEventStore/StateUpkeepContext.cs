#nullable enable
using Lokad.AzureEventStore.Projections;
using Lokad.MemoryMapping;

namespace Lokad.AzureEventStore;

/// <summary>
///     Settings used to perform upkeep operations on the state.
/// </summary>
public class StateUpkeepContext(IMemoryMappedFolder? memoryMappedFolder, IProjectionCacheProvider? cacheProvider)
{
    /// <summary>
    ///     Final memory mapped folder chosen among the candidates provided by the projection folder provider.
    /// </summary>
    public IMemoryMappedFolder? MemoryMappedFolder { get; } = memoryMappedFolder;

    /// <summary>
    ///     Provider of a persistent cache to read and write the projection states.
    /// </summary>
    public IProjectionCacheProvider? CacheProvider { get; } = cacheProvider;
}