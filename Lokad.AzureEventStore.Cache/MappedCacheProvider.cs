#nullable enable
using Lokad.AzureEventStore.Projections;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Cache;

public sealed class MappedCacheProvider : IProjectionCacheProvider
{
    /// <summary>
    ///     If non-null, the <see cref="MappedCacheProvider"/> will act as a proxy for
    ///     this other provider, by 1° copying input cache streams from this provider
    ///     to the local directory and 2° copying written streams from the local 
    ///     directory to this provider.
    /// </summary>
    private readonly IProjectionCacheProvider? _inner;
        
    /// <summary>
    ///     Files are written to this dictionary. 
    /// </summary>
    private readonly string _directory;
        
    /// <summary>
    /// The number of files created so far by <see cref="OpenWriteLocal(string)"/>
    /// </summary>
    private int _created;
        
    public MappedCacheProvider(string directory, IProjectionCacheProvider? inner = null)
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
    public async Task<IAsyncEnumerable<CacheCandidate>> OpenReadAsync(ILogAdapter? log, string stateName)
    {
        var innerEnumerable = _inner == null 
            ? AsyncEnumerable.Empty<CacheCandidate>()
            : await _inner.OpenReadAsync(log, stateName).ConfigureAwait(false);
        
        return Enumerate(innerEnumerable, stateName);
    }
    
    private IAsyncEnumerable<CacheCandidate> Enumerate(IAsyncEnumerable<CacheCandidate> fromInner, string stateName)
    {
        // First, attempt to use local blobs
        var inDirectory = Directory
            .EnumerateFiles(Path.Combine(_directory, stateName))
            .OrderByDescending(path => path)
            .ToArray();

        if (inDirectory.Length > 0)
        {
            return inDirectory.ToAsyncEnumerable()
                .SelectAwait(fileName =>
                {
                    var filePath = Path.Combine(_directory, stateName, fileName);
                    var result = Task.Run(() => MemoryMap(filePath, null));
                    // If still enumerating, then the file did not contain a valid cache, and
                    // so we delete it to avoid reading it again.
                    DeleteFile(filePath);
                    return new ValueTask<CacheCandidate>(result);
                });
        }

        // If not, fall back to proxy blobs
        return fromInner
            .SelectAwait(task =>
            {
                var copyToLocal = Task.Run(async () =>
                {
                    using var remote = task.Contents;
                    var open = OpenWriteLocal(stateName);
                    using (var local = open.stream)
                    {
                        await remote.CopyToAsync(local).ConfigureAwait(false);
                    }
                    return (open.path, task.Name);
                });
            
                var result = Task.Run(async () =>
                {
                    var (localPath, remoteName) = await copyToLocal.ConfigureAwait(false);
                    return MemoryMap(localPath, remoteName);
                });
            
                // If still enumerating, then the file did not contain a valid cache, and
                // so we delete it to avoid reading it again.
                Task.Run(async () =>
                {
                    var (localPath, _) = await copyToLocal.ConfigureAwait(false);
                    DeleteFile(localPath);
                });
                
                return new ValueTask<CacheCandidate>(result);
            });
    }
    /// <summary>
    /// Memory-maps the file at the specified path.
    /// </summary>
    /// <param name="filePath">Path to the memory mapped file.</param>
    /// <param name="from"> information about the <see cref="IProjectionCacheProvider"/> which
    /// local path is the proxy.</param>
    /// <returns>the associated <see cref="CacheCandidate"/></returns>
    private CacheCandidate MemoryMap(string filePath, string? from = null)
    {
        var length = new FileInfo(filePath).Length;
        return new CacheCandidate(
            "mmap:" + filePath + (from == null ? "" : " from " + from),
            new BigMemoryStream(new MemoryMapper(
                MemoryMappedFile.CreateFromFile(filePath, FileMode.Open),
                0, length)));
    }
    /// <summary>
    /// Attempts to delete the file located at the given path.
    /// </summary>
    /// <param name="filePath">path of the file that should be deleted.</param>
    /// <remarks>Any exception raised at the deletion attempt will be
    /// silently ignored.</remarks>
    private void DeleteFile(string filePath)
    {
        try { File.Delete(filePath); }
        catch { /* Ignored silently. */ }
    }
        
    /// <summary>
    ///     Open a local file for writing. Returns both the path to the file and the 
    ///     opened, writable, empty stream.
    /// </summary>
    private (string path, Stream stream) OpenWriteLocal(string stateName)
    {
        var filename = $"{DateTime.UtcNow:yyyyMMddHHmmss}-{_created}.bin";
        _created++;
        
        Directory.CreateDirectory(Path.Combine(_directory, stateName));
        var path = Path.Combine(_directory, stateName, filename);
        return (path, new FileStream(path, FileMode.Create));
    }
    
    public async Task TryWriteAsync(ILogAdapter? log, string stateName, Func<Stream, Task> write)
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
            await _inner.TryWriteAsync(log, stateName, async remote =>
            {
                await using (remote)
                await using (var local = File.OpenRead(filepath))
                    await local.CopyToAsync(remote).ConfigureAwait(false);
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