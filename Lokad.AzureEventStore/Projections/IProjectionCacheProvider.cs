using System;
using System.Collections.Generic;
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
        ///     Attempts to open a reading stream for the projection with the 
        ///     specified name.
        /// </summary>
        /// <remarks>
        ///     Enumerating the returned enumerable will open reading streams
        ///     one after another, by decreasing priority (i.e. the most recent
        ///     streams are opened first). 
        ///     
        ///     The caller MUST wait for the task to complete and dispose each 
        ///     stream before moving to the next element in the enumeration.
        ///     
        ///     The key associated with each stream is a name that describes the
        ///     type of stream (blob, file, etc.) and its creation date.
        /// </remarks>
        Task<IEnumerable<Task<CacheCandidate>>> OpenReadAsync(string stateName);

        /// <summary>
        ///     Open an (initially empty) writing stream for the projection 
        ///     with the specified full name, and invoke the provided method. 
        ///     Will do nothing if cache is not writable.
        /// </summary>        
        Task TryWriteAsync(string stateName, Func<Stream, Task> write);
    }
}
