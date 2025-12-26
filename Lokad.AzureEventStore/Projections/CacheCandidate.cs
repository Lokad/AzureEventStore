using System;
using System.IO;

namespace Lokad.AzureEventStore.Projections;

/// <summary> A candidate for a cache value. </summary>
public struct CacheCandidate(string name, Stream contents)
{
    /// <summary> The name of this candidate (for logging and debugging only). </summary>
    /// <example> file:/mnt/cache/State-18/20200909084613 </example>
    public readonly string Name = name ?? throw new ArgumentNullException(nameof(name));

    /// <summary> The contents of the cache as an opened stream. </summary>
    public readonly Stream Contents = contents ?? throw new ArgumentNullException(nameof(contents));
}