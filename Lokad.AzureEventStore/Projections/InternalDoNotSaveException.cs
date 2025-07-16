using System;

namespace Lokad.AzureEventStore.Projections;

/// <summary> Exception used to escape async control flow. </summary>
public class InternalDoNotSaveException : Exception
{
    private const string MessageTag = "INTERNAL.DO.NOT.SAVE";
    public InternalDoNotSaveException() : base(MessageTag) {}
    public InternalDoNotSaveException(Exception inner) : base(MessageTag, inner) {}
}