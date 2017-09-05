using System.IO;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Projections
{
    /// <summary>
    /// An interface for reading and writing projection states to a 
    /// persistent cache.
    /// </summary>
    public interface IProjectionCacheProvider
    {
        /// <summary>
        /// Open a reading stream for the projection with the specified
        /// full name, or <c>null</c> if data is not available.
        /// </summary>
        Task<Stream> OpenReadAsync(string fullname);

        /// <summary>
        /// Open an (initially empty) writing stream for the projection 
        /// with the specified full name, or <c>null</c> if caching is
        /// disabled.
        /// </summary>        
        Task<Stream> OpenWriteAsync(string fullname);
    }
}
