using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Projections
{
    /// <summary> Saves projection state to files in a directory.  </summary>
    public sealed class FileCacheProvider : IProjectionCacheProvider
    {
        private readonly string _directory;

        /// <summary> Construct a projection cache saving to the specified directory. </summary>
        public FileCacheProvider(string directory)
        {
            _directory = directory;
        }

        /// <see cref="IProjectionCacheProvider.OpenReadAsync"/>
        public Task<IEnumerable<Task<CacheCandidate>>> OpenReadAsync(string stateName)
        {
            var path = Path.Combine(_directory, stateName);

            try
            {
                Directory.CreateDirectory(_directory);
                return Task.FromResult<IEnumerable<Task<CacheCandidate>>>(
                    new[] { Task.FromResult(new CacheCandidate(
                        "file:" + path,
                        File.OpenRead(path))) 
                    });
            }
            catch (IOException)
            {
                // Failure to read the file means no cache data is available
                return Task.FromResult<IEnumerable<Task<CacheCandidate>>>(
                    Array.Empty<Task<CacheCandidate>>());
            }
        }

        /// <see cref="IProjectionCacheProvider.TryWriteAsync"/>
        public async Task TryWriteAsync(string stateName, Func<Stream, Task> write)
        {
            var path = Path.Combine(_directory, stateName);

            Stream stream;
            try
            {
                Directory.CreateDirectory(_directory);
                stream = new FileStream(path, FileMode.Create);
            }
            catch (IOException)
            {
                // Failure to write the file means caching is disabled
                return;
            }

            try
            {
                await write(stream).ConfigureAwait(false);
                stream.Dispose();
            }
            catch
            {
                // If writing failed, delete the (broken) file.
                stream.Dispose();
                try { File.Delete(path); }
                catch { /* Ignored silently */ }
                throw;
            }
        }
    }
}
