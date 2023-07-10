#nullable enable
using System.IO;

namespace Lokad.AzureEventStore
{
    /// <summary>
    /// Class to handle external storage for state writing.
    /// It can be loaded externally or build internally.
    /// </summary>
    public class StorageProvider
    {
        /// <summary> 
        /// External state folder path.
        /// Will try to load state from this path.
        /// If it is null, the storage will be done in-memory.
        /// </summary>
        private string? _externalStateFolderPath { get; }

        public StorageProvider(string? externalStateFolderPath)
        {
            _externalStateFolderPath = externalStateFolderPath;
        }

        internal StateCreationContext GetStateCreationContext(string? name)
        {
            if (_externalStateFolderPath == null)
                return new StateCreationContext(null);

            return new StateCreationContext(Path.Combine(_externalStateFolderPath, name));
        }
    }
}
