using System;

namespace Lokad.AzureEventStore.Quarantine
{
    /// <summary> A quarantined event. </summary>    
    /// <remarks> Created as a result of event serialization or application error.</remarks>
    public sealed class QuarantinedEvent<TEvent>
    {
        /// <summary> The event that caused trouble. Null if serialization error. </summary>
        public readonly TEvent Event;

        /// <summary> The problem. </summary>
        /// <remarks> 
        /// May be an <see cref="AggregateException"/> if multiple projections
        /// failed to process event. 
        /// </remarks>
        public readonly Exception Exception;

        /// <summary> The sequence number of the veent. </summary>
        public readonly uint Sequence;

        public QuarantinedEvent(TEvent e, Exception exception, uint sequence)
        {
            Event = e;
            Exception = exception;
            Sequence = sequence;
        }
    }
}
