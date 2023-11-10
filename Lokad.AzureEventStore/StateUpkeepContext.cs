using Lokad.AzureEventStore.Projections;

namespace Lokad.AzureEventStore
{
    /// <summary>
    ///     Settings used to perform upkeep operations on the state.
    /// </summary>
    public class StateUpkeepContext
    {
        public IProjectionCacheProvider CacheProvider { get; }

        public StateUpkeepContext(IProjectionCacheProvider cacheProvider)
        {
            CacheProvider = cacheProvider;
        }
    }
}
