using Lokad.AzureEventStore.Projections;
using Lokad.LargeImmutable;
using Lokad.LargeImmutable.Mapping;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Cache
{
    public sealed class MappedCacheProvider : IProjectionCacheProvider
    {
        /// <summary>
        ///     If non-null, the <see cref="MappedCacheProvider"/> will act as a proxy for
        ///     this other provider, by 1° copying input cache streams from this provider
        ///     to the local directory and 2° copying written streams from the local 
        ///     directory to this provider.
        /// </summary>
        private readonly IProjectionCacheProvider _inner;

        /// <summary>
        ///     Files are written to this dictionary. 
        /// </summary>
        private readonly string _directory;

        /// <summary>
        /// The number of files created so far by <see cref="OpenWriteLocal(string)"/>
        /// </summary>
        private int _created;

        public MappedCacheProvider(string directory, IProjectionCacheProvider inner = null)
        {
            _directory = directory ?? throw new ArgumentNullException(nameof(directory));
            _inner = inner;

            Directory.CreateDirectory(_directory);
        }

        /// <summary> If this number of cache files are present in the directory, clear the oldest ones. </summary>
        /// <remarks>
        ///     Clearing occurs during calls to <see cref="OpenWriteAsync(string)"/>.
        /// </remarks>
        public int MaxCacheFilesCount { get; set; } = 4;

        public async Task<IEnumerable<Task<CacheCandidate>>> OpenReadAsync(string stateName)
        {
            var innerEnumerable = _inner == null 
                ? Array.Empty<Task<CacheCandidate>>()
                : await _inner.OpenReadAsync(stateName).ConfigureAwait(false);

            return Enumerate(innerEnumerable);

            IEnumerable<Task<CacheCandidate>>
                Enumerate(IEnumerable<Task<CacheCandidate>> fromInner)
            {
                // First, attempt to use local blobs
                var inDirectory = Directory.EnumerateFiles(Path.Combine(_directory, stateName))
                    .OrderByDescending(path => path);

                foreach (var fileName in inDirectory)
                {
                    // Wrap in Task.Run so that exceptions do not kill the enumeration
                    yield return Task.Run(() => MemoryMap(fileName, null));

                    // If still enumerating, then the file did not contain a valid cache, and
                    // so we delete it to avoid reading it again.
                    DeleteFile(fileName);
                }

                // If not, fall back to proxy blobs
                foreach (var task in innerEnumerable)
                {
                    var copyToLocal = Task.Run(async () =>
                    {
                        var result = await task.ConfigureAwait(false);
                        using (var remote = result.Contents)
                        {
                            var open = OpenWriteLocal(stateName);
                            using (var local = open.stream)
                            {
                                await remote.CopyToAsync(local).ConfigureAwait(false);
                            }

                            return (open.path, result.Name);
                        }
                    });

                    yield return Task.Run(async () =>
                    {
                        var (localPath, remoteName) = await copyToLocal.ConfigureAwait(false);
                        return MemoryMap(localPath, remoteName);
                    });

                    // If still enumerating, then the file did not contain a valid cache, and
                    // so we delete it to avoid reading it again.
                    Task.Run(async () =>
                    {
                        var (localName, _) = await copyToLocal.ConfigureAwait(false);
                        DeleteFile(localName);
                    });
                }
            }

            /// Memory-maps the file with the specified name
            CacheCandidate MemoryMap(string fileName, string from = null)
            {
                var path = Path.Combine(_directory, stateName, fileName);
                var length = new FileInfo(path).Length;
                return new CacheCandidate(
                    "mmap:" + path + (from == null ? "" : " from " + from),
                    new BigMemoryStream(new MemoryMapper(
                        MemoryMappedFile.CreateFromFile(path, FileMode.Open),
                        0, length)));
            }

            void DeleteFile(string fileName)
            {
                var path = Path.Combine(_directory, stateName, fileName);
                try { File.Delete(path); } 
                catch { /* Ignored silently. */ }
            }
        }

        /// <summary>
        ///     Open a local file for writing. Returns both the path to the file and the 
        ///     opened, writable, empty stream.
        /// </summary>
        private (string path, Stream stream) OpenWriteLocal(string stateName)
        {
            var filename = DateTime.UtcNow.ToString("yyyyMMddHHmmss") 
                + "-" 
                + _created++ 
                + ".bin";

            Directory.CreateDirectory(Path.Combine(_directory, stateName));

            var path = Path.Combine(_directory, stateName, filename);
            return (path, new FileStream(path, FileMode.Create));
        }

        public async Task TryWriteAsync(string stateName, Func<Stream, Task> write)
        {
            var (filepath, stream) = OpenWriteLocal(stateName);

            try
            {
                // Will throw if it does not want the stream to be saved. 
                await write(stream).ConfigureAwait(false);
            }
            catch
            {
                // In case of failure, delete the broken file.
                stream.Dispose();
                try { File.Delete(filepath); }
                catch { /* Ignored silently. */ }
                throw;
            }

            // Copy the stream to the inner cache provider.
            if (_inner != null)
            {
                await _inner.TryWriteAsync(stateName, async remote =>
                {
                    using (remote)
                    {
                        using (var local = File.OpenRead(filepath))
                        {
                            await local.CopyToAsync(remote).ConfigureAwait(false);
                        }
                    }
                }).ConfigureAwait(false);
            }

            // Delete local files over the limit.
            var inDirectory = Directory.EnumerateFiles(Path.Combine(_directory, stateName))
                .OrderByDescending(path => path)
                .ToArray();

            for (var i = MaxCacheFilesCount; i < inDirectory.Length; ++i)
            {
                try { File.Delete(Path.Combine(_directory, stateName, inDirectory[i])); }
                catch { /* Ignored silently. Some of them might still be memory-mapped ! */ }
            }
        }
    }
}
