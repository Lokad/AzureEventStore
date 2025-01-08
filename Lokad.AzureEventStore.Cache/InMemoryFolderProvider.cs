using Lokad.AzureEventStore.Projections;
using Lokad.MemoryMapping;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Cache
{
    public class InMemoryFolderProvider : IProjectionFolderProvider
    {
        public IMemoryMappedFolder CreateEmpty(string stateName)
        {
            return new InMemoryFolder();
        }

        public async IAsyncEnumerable<IMemoryMappedFolder> EnumerateCandidates(string stateName, [EnumeratorCancellation] CancellationToken cancel)
        {
            yield return await Task.FromResult(new InMemoryFolder());
        }

        public Task<bool> Preserve(string stateName, IMemoryMappedFolder folder, CancellationToken cancel)
        {
            return Task.FromResult(false);
        }
    }
}
