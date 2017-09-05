using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Lokad.AzureEventStore.Quarantine
{
    /// <summary> A thread-safe store for quarantined events. </summary>
    public sealed class EventQuarantine<TEvent> : IEnumerable<QuarantinedEvent<TEvent>> where TEvent : class
    {
        /// <summary> Events are appended here.  </summary>
        private readonly ConcurrentQueue<QuarantinedEvent<TEvent>> _events = 
            new ConcurrentQueue<QuarantinedEvent<TEvent>>();

        /// <summary>
        /// Add an event to the quarantine. Event itself is not provided (e.g. parsing error).
        /// </summary>
        internal void Add(uint seq, Exception e)
        {
            _events.Enqueue(new QuarantinedEvent<TEvent>(null, e, seq));
        }

        /// <summary>
        /// Add an event to the quarantine.
        /// </summary>
        internal void Add(uint seq, TEvent ev, Exception e)
        {
            _events.Enqueue(new QuarantinedEvent<TEvent>(ev, e, seq));
        }

        /// <summary> Return the current count of quarantined events.  </summary>
        public int Count => _events.Count;

        /// <see cref="IEnumerable{T}.GetEnumerator"/>
        public IEnumerator<QuarantinedEvent<TEvent>> GetEnumerator()
        {
            return _events.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
