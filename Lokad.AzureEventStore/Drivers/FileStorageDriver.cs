using System.IO;

namespace Lokad.AzureEventStore.Drivers;

/// <summary> A simple, inefficient, persistent file-based storage driver. </summary>
/// <remarks> Intended for local development, with one writer and multiple read-only readers. </remarks>
internal sealed class FileStorageDriver : AbstractStreamStorageDriver
{
    internal FileStorageDriver(string path, bool readOnly)
        : base(CreateFile(path, readOnly))
    {}

    private static FileStream CreateFile(string path, bool readOnly)
    {
        var file = Path.Combine(path, "stream.bin");
        if (readOnly)
            return new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

        Directory.CreateDirectory(path);
        return new FileStream(file, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
    }
}
