using System;
namespace Lokad.AzureEventStore.Exceptions;

/// <summary>
///     Thrown by <see cref="Drivers.MonoBlobStorageDriver"/> when attempting to append an event
///     to a blob that has already received 50000 writes and is thus no longer writable.
/// </summary>
public sealed class MonoBlobFullException : Exception
{
    public MonoBlobFullException(string blobName) : base("Blob " + blobName + " is full and no longer writable.")
    {
    }

    public MonoBlobFullException()
    {
    }

    public MonoBlobFullException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
