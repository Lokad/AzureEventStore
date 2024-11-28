using Lokad.MemoryMapping;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Projections
{
    public interface IProjectionFolderProvider
    {
        /// <summary> 
        ///    Enumerates folders that contain existing data.
        ///    For example, first could be an existing on-disk folder, 
        ///    second could be the same on-disk folder but emptied and then 
        ///    filled by downloading data from blobs.
        /// </summary>
        IAsyncEnumerable<IMemoryMappedFolder> EnumerateCandidates(
            string stateName, 
            CancellationToken cancel);
    
        /// <summary> Creates an empty folder. </summary>
        IMemoryMappedFolder CreateEmpty(
            string stateName);
    
        /// <summary>
        ///     If supported, tries to save a copy of the contents of this folder
        ///     elsewhere, such as to a blob. Caller must guarantee that no one 
        ///     will write to the folder while this function is running.
        /// </summary>
        /// <returns> True if successful </returns>
        Task<bool> Preserve(
            string stateName, 
            IMemoryMappedFolder folder, 
            CancellationToken cancel);
    }
}
