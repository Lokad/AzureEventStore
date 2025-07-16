using System.Runtime.InteropServices;

namespace Lokad.AzureEventStore.Test;

public static class TestPaths
{
    public static readonly bool IsLinux =
        RuntimeInformation.IsOSPlatform(OSPlatform.Linux);
    
    public static readonly string Root =
        IsLinux
            ? "/var/Lokad/AzureEventStore"
            : @"C:\LokadData\AzureEventStore";
}