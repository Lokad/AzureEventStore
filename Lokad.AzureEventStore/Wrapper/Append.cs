using System;
using System.Collections.Generic;
using System.Linq;

namespace Lokad.AzureEventStore.Wrapper
{
    /// <summary> An append request containing events and a side-channel value. </summary>
    public struct Append<TEvent, TResult>
    {
        /// <summary> The events to be appended. </summary>
        public readonly IReadOnlyList<TEvent> Events;

        /// <summary> The additional result, on top of the appended events. </summary>
        public readonly TResult Result;

        /// <summary> Create an append request from a result and some events. </summary>
        public Append(TResult result, params TEvent[] events) : this()
        {
            if (events.Any(e => e == null))
                throw new ArgumentException(@"No null events allowed", nameof(events));

            Events = events;
            Result = result;
        }

        /// <summary> Create an append request from a non-result request. </summary>
        public Append(Append<TEvent> a, TResult r)
        {
            Events = a.Events;
            Result = r;
        } 
    }

    /// <summary> An append request containing events. </summary>
    public struct Append<TEvent>
    {
        /// <summary> The events to be appended. </summary>
        public readonly IReadOnlyList<TEvent> Events;

        /// <summary> Create an append request from a result and some events. </summary>
        public Append(params TEvent[] events)
        {
            if (events.Any(e => e == null))
                throw new ArgumentException(@"No null events allowed", nameof(events));

            Events = events;
        }
    }
}