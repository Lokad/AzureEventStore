#nullable enable
using Lokad.AzureEventStore.Projections;

namespace Lokad.AzureEventStore
{
    /// <summary>
    /// Settings used to create the state."/>
    /// </summary>
    public class StateCreationContext
    {
        /// <summary> External state folder path. </summary>
        public string? ExternalStateFolderPath { get; }

        public IProjectionCacheProvider CacheProvider { get; }

        public StateCreationContext(string? externalStateFolderPath, IProjectionCacheProvider cacheProvider)
        {
            ExternalStateFolderPath = externalStateFolderPath;
            CacheProvider = cacheProvider;
        }
    }
}
