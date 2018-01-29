using System.IO;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> A simple, inefficient, persistent file-based storage driver. </summary>
    /// <remarks> Intended for use during local development. </remarks>
    internal sealed class FileStorageDriver : AbstractStreamStorageDriver
    {
        internal FileStorageDriver(string path)
            : base(CreateFile(path))
        {}

        private static FileStream CreateFile(string path)
        {
            Directory.CreateDirectory(path);

            var file = Path.Combine(path, "stream.bin");

            return new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite);
        }
    }
}
