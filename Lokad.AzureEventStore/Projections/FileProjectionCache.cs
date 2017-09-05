using System.IO;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Projections
{
    /// <summary> Saves projection state to files in a directory.  </summary>
    public sealed class FileProjectionCache : IProjectionCacheProvider
    {
        private readonly string _directory;

        /// <summary> Construct a projection cache saving to the specified directory. </summary>
        public FileProjectionCache(string directory)
        {
            _directory = directory;
        }

        /// <see cref="IProjectionCacheProvider.OpenReadAsync"/>
        public Task<Stream> OpenReadAsync(string fullname)
        {
            var path = Path.Combine(_directory, fullname);

            try
            {
                Directory.CreateDirectory(_directory);
                return Task.FromResult<Stream>(File.OpenRead(path));
            }
            catch (IOException)
            {
                // Failure to read the file means no cache data is available
                return Task.FromResult<Stream>(null);
            }
        }

        /// <see cref="IProjectionCacheProvider.OpenWriteAsync"/>
        public Task<Stream> OpenWriteAsync(string fullname)
        {
            var path = Path.Combine(_directory, fullname);

            try
            {
                Directory.CreateDirectory(_directory);
                return Task.FromResult<Stream>(new FileStream(path, FileMode.Create));
            }
            catch (IOException)
            {
                // Failure to write the file means caching is disabled
                return Task.FromResult<Stream>(null);
            }
        }
    }
}
