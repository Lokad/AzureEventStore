using Lokad.AzureEventStore.Projections;
using Lokad.MemoryMapping;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace Lokad.AzureEventStore.Cache
{
    /// <summary>
    ///     Provides a <see cref="MemoryMappedFolder"/>.
    /// </summary>
    public class MemoryMappedFolderProvider : IProjectionFolderProvider
    {
        /// <summary>
        ///     Path of the <see cref="MemoryMappedFolder"/>.
        /// </summary>
        private readonly string _path;

        /// <summary>
        ///     Cache provider to save the <see cref="MemoryMappedFolder"/> entries.
        /// </summary>
        private readonly MemoryMapperCacheProvider? _cacheProvider;

        public MemoryMappedFolderProvider(
            string path, 
            MemoryMapperCacheProvider? memoryMapperCacheProvider)
        {
            _path = path;
            _cacheProvider = memoryMapperCacheProvider;
        }

        public IMemoryMappedFolder CreateEmpty(string stateName)
        {
            var fullPath = Path.Combine(_path, stateName);
            Directory.CreateDirectory(fullPath);

            var files = Directory.GetFiles(fullPath);
            foreach (var file in files)
                File.Delete(file);

            return new MemoryMappedFolder(fullPath);
        }

        public async IAsyncEnumerable<IMemoryMappedFolder> EnumerateCandidates(string stateName, [EnumeratorCancellation] CancellationToken cancel)
        {
            var fullPath = Path.Combine(_path, stateName);
            Directory.CreateDirectory(fullPath);

            var candidate = new MemoryMappedFolder(fullPath);
            var files = candidate.EnumerateEntryNames();
            if (files.Any())
            {
                yield return candidate;
            }

            if (_cacheProvider != null)
            {
                await _cacheProvider.LoadAsync(stateName, candidate, cancel);
                yield return candidate;
            }
        }

        public async Task<bool> Preserve(string stateName, IMemoryMappedFolder folder, CancellationToken cancel)
        {
            if (_cacheProvider == null)
                return false;

            await _cacheProvider.UploadToBlobsAsync(stateName, folder, cancel);
            return true;
        }
    }
}
