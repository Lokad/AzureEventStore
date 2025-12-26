using System;

namespace Lokad.AzureEventStore.Quarantine;

/// <summary> A quarantined event. </summary>    
/// <remarks> Created as a result of event serialization or application error.</remarks>
public sealed class QuarantinedEvent<TEvent>(TEvent e, Exception exception, uint sequence)
{
    /// <summary> The event that caused trouble. Null if serialization error. </summary>
    public readonly TEvent Event = e;

    /// <summary> The problem. </summary>
    /// <remarks> 
    /// May be an <see cref="AggregateException"/> if multiple projections
    /// failed to process event. 
    /// </remarks>
    public readonly Exception Exception = exception;

    /// <summary> The sequence number of the veent. </summary>
    public readonly uint Sequence = sequence;
}