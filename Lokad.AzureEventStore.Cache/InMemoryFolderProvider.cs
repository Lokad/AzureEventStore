#nullable enable

using Lokad.AzureEventStore.Projections;
using Lokad.MemoryMapping;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Cache;

public class InMemoryFolderProvider : IProjectionFolderProvider
{
    public IMemoryMappedFolder CreateEmpty(string stateName)
    {
        return new InMemoryFolder();
    }

    public async IAsyncEnumerable<IMemoryMappedFolder> EnumerateCandidates(
        ILogAdapter? log,
        string stateName,
        [EnumeratorCancellation] CancellationToken cancel)
    {
        log?.Debug("Enumerating in-memory candidates");
        yield return await Task.FromResult(new InMemoryFolder());
    }

    public Task<bool> Preserve(
        ILogAdapter? log,
        string stateName,
        IMemoryMappedFolder folder,
        CancellationToken cancel)
    {
        log?.Debug("No preservation of in-memory folder.");
        return Task.FromResult(false);
    }
}