#nullable enable
using Lokad.AzureEventStore.Projections;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Cache;

/// <summary> Saves projection state to files in a directory.  </summary>
public sealed class FileCacheProvider : IProjectionCacheProvider
{
    /// <summary> Directory path where we want to store the state. </summary>
    private readonly string _directory;
        
    /// <summary> Optional cache that can fetch a cache for us. </summary>
    private readonly IProjectionCacheProvider? _foreignProvider;

    public FileCacheProvider(string directory, IProjectionCacheProvider? foreignProvider)
    {
        _directory = directory;
        _foreignProvider = foreignProvider;
    }
        
    /// <summary> Construct a projection cache saving to the specified directory. </summary>
    public FileCacheProvider(string directory) : this(directory, null) {}

    /// <see cref="IProjectionCacheProvider.OpenReadAsync"/>
    public Task<IAsyncEnumerable<CacheCandidate>> OpenReadAsync(ILogAdapter? log, string stateName)
    {
        var path = Path.Combine(_directory, stateName);

        try
        {
            Directory.CreateDirectory(_directory);
            var candidate = new CacheCandidate(
                "file:" + path,
                File.OpenRead(path));
            
            return Task.FromResult(Enumerable.Repeat(candidate, 1).ToAsyncEnumerable());
        }
        catch (IOException)
        {
            if (_foreignProvider != null)
                return _foreignProvider.OpenReadAsync(log, stateName);
                
            // Failure to read the file means no cache data is available
            return Task.FromResult(AsyncEnumerable.Empty<CacheCandidate>());
        }
    }

    /// <see cref="IProjectionCacheProvider.TryWriteAsync"/>
    public async Task TryWriteAsync(ILogAdapter? log, string stateName, Func<Stream, Task> write)
    {
        var path = Path.Combine(_directory, stateName);
        log?.Info($"Start writing {stateName} at {path}");

        Stream stream;
        try
        {
            Directory.CreateDirectory(_directory);
            stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
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