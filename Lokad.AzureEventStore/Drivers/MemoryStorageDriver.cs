using System.IO;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> A simple, inefficient, persistent file-based storage driver. </summary>
    /// <remarks> Intended for use during local development. </remarks>
    internal sealed class MemoryStorageDriver : AbstractStreamStorageDriver
    {
        internal MemoryStorageDriver() 
            : base(new MemoryStream())
        {}
    }
}
