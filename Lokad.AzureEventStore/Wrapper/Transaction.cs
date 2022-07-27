using Lokad.AzureEventStore.Projections;
using System;
using System.Collections.Generic;

namespace Lokad.AzureEventStore.Wrapper
{
    /// <summary>
    ///     A transaction supports adding several events in succession, and checking
    ///     the state after each event. The actual events (and corresponding state 
    ///     changes) are only applied if the transaction is not aborted, and no 
    ///     exception is thrown by the function operating the transaction.
    /// </summary>
    /// <see cref="EventStreamWrapper{TEvent, TState}.TransactionAsync{T}(Func{Transaction{TEvent, TState}, T}, System.Threading.CancellationToken)"/>
    /// <see cref="EventStreamWrapper{TEvent, TState}.TransactionAsync(Action{Transaction{TEvent, TState}}, System.Threading.CancellationToken)"/>
    public sealed class Transaction<TEvent, TState>
        where TState : class
        where TEvent : class
    {
        /// <summary>
        ///     Any events added to the transaction are kept here. 
        /// </summary>
        private readonly List<TEvent> _events = new List<TEvent>();

        /// <summary>
        ///     All the events that were added to this transaction. 
        ///     If aborted, will always return an empty array.
        /// </summary>
        internal TEvent[] Events => Aborted || _events.Count == 0 
            ? Array.Empty<TEvent>() 
            : _events.ToArray();

        /// <summary>
        ///     Used to compute the current state after every event is added. 
        /// </summary>
        private readonly IReifiedProjection<TEvent, TState> _projection;

        /// <summary>
        ///     Becomes true when <see cref="Abort"/> is called.
        /// </summary>
        public bool Aborted { get; private set; }

        /// <summary>
        ///     The current state within this transaction. This includes all events
        ///     that were already present in the stream, and all events that were
        ///     added to the transaction. 
        /// </summary>
        public TState State => Aborted
            ? throw new InvalidOperationException("Transaction was aborted")
            : _projection.Current;

        /// <summary>
        ///     The (simulated) sequence number. Starts at the current sequence number,
        ///     incremented every time an event is added to the transaction. 
        /// </summary>
        public uint Sequence => Aborted
            ? throw new InvalidOperationException("Transaction was aborted")
            : _projection.Sequence;

        /// <summary> Add an event to the transaction. </summary>
        public void Add(TEvent evt)
        {
            if (Aborted)
                throw new InvalidOperationException("Transaction was aborted");

            // First apply to projection: it may throw if invalid
            _projection.Apply(_projection.Sequence + 1, evt);
            _events.Add(evt);
        }

        /// <summary>
        ///     Mark the transaction as aborted. No new events can be added, and no 
        ///     events will be written to the stream as a consequence of this 
        ///     transaction. 
        /// </summary>
        public void Abort() => Aborted = true;

        internal Transaction(IReifiedProjection<TEvent, TState> proj)
        {
            _events = new List<TEvent>();
            _projection = proj;
        }
    }
}
