using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;

namespace Lokad.AzureEventStore.Streams
{
    /// <summary> A stream of events of the specified type.  </summary>
    /// <remarks> 
    /// This object does NOT support multi-threaded operations. 
    /// 
    /// To perform a transactional append (i.e. to GUARANTEE that all existing events
    /// have been processed before appending a new one), follow these steps:
    //
    ///   1- generate the new events from the current state
    ///   2- call WriteAsync() with the new events
    ///     -- if successful: you're done !
    ///   3- call FetchAsync() 
    ///     -- if it returned true:
    ///       3.a- call TryGetNext() until it returns null
    ///       3.b- use returned events to update current state
    ///       3.c- repeat step 3
    ///     -- if it returned false: go back to step 1
    /// 
    /// To further optimize this process, you may use <see cref="BackgroundFetchAsync"/>
    /// in parallel with the calls to <see cref="TryGetNext"/>.    
    /// </remarks>
    public sealed class EventStream<TEvent> where TEvent : class
    {
        /// <summary> Used for object-to-byte[] conversions. </summary>
        private readonly JsonEventSerializer<TEvent> _serializer = new JsonEventSerializer<TEvent>();

        /// <summary> Where events are stored. </summary>
        internal readonly IStorageDriver Storage;

        /// <summary> Used for logging. </summary>
        private readonly ILogAdapter _log;
        
        /// <summary> Open an event stream, connecting to the specified store. </summary>
        public EventStream(StorageConfiguration storage, ILogAdapter log = null) :this(storage.Connect(), log)
        {
        }

        /// <summary> Open an event stream on the provided store. </summary>
        internal EventStream(IStorageDriver storage, ILogAdapter log = null)
        {
            Storage = storage;
            _log = log;
        }

        /// <summary> The position up to which *remote* reading has progressed so far. </summary>
        /// <remarks> 
        /// This may or may not be the last position in the stream. 
        /// This position represents *remote* reading, so elements in <see cref="_cache"/> count as
        /// having been read.
        /// </remarks>
        internal long Position { get; private set; }

        /// <summary> Heuristic value: the write position is known to be greater than this. </summary>
        private long _minimumWritePosition;

        /// <summary> The sequence number assigned to the last event returned by <see cref="TryGetNext"/>. </summary>
        public uint Sequence { get; private set; }

        /// <summary>
        /// Equal to the sequence number of the last element in <see cref="_cache"/>, equal to
        /// <see cref="Sequence"/> if no cached elements.
        /// </summary>
        private uint _lastSequence;

        /// <summary>
        /// Events that have been received from the remote position, but not returned by 
        /// <see cref="TryGetNext"/> yet.
        /// </summary>
        private readonly Queue<RawEvent> _cache = new Queue<RawEvent>();

        /// <summary> A listener function. </summary>        
        public delegate void Listener(TEvent e, uint seq);
        
        /// <summary> Append one or more events to the stream. </summary>
        /// <remarks> 
        /// Events are assigned consecutive sequence numbers. The FIRST of these
        /// numbers is returned. 
        /// 
        /// If no events are provided, return null.
        /// 
        /// If this object's state no longer represents the remote stream (because other
        /// events have been written from elsewhere), this method will not write any
        /// events and will return null. The caller should call <see cref="FetchAsync"/> 
        /// until it returns false to have the object catch up with remote state.                
        /// </remarks>
        public async Task<uint?> WriteAsync(IReadOnlyList<TEvent> events, CancellationToken cancel = default(CancellationToken))
        {
            if (events.Count == 0) return null;
            if (Position < _minimumWritePosition) return null;

            if (events.Any(e => e == null))
                throw new ArgumentException(@"No null events allowed", nameof(events));

            var sw = Stopwatch.StartNew();

            var rawEvents = events.Select((e, i) => new RawEvent(_lastSequence + (uint)(i + 1), _serializer.Serialize(e)))
                .ToArray();
            try
            {
                var result = await Storage.WriteAsync(Position, rawEvents, cancel);

                _minimumWritePosition = result.NextPosition;

                if (result.Success)
                {
                    foreach (var e in rawEvents) _cache.Enqueue(e);
                    Position = result.NextPosition;

                    var first = _lastSequence + 1;

                    _lastSequence += (uint) rawEvents.Length;

                    _log?.Debug(
                        $"Wrote {rawEvents.Length} events up to seq {_lastSequence} in {sw.Elapsed.TotalSeconds:F3}s.");

                    return first;
                }

                _log?.Debug($"Collision when writing {rawEvents.Length} events after {sw.Elapsed.TotalSeconds:F3}s.");

                return null;
            }
            catch (Exception e)
            {
                _log?.Error($"When writing {rawEvents.Length} events after seq {_lastSequence}.", e);
                throw;
            }
        }

        /// <summary> High-performance read from the stream. </summary>
        /// <remarks> 
        /// Returns the next event, if any. Returns null if there are no more events
        /// available in the local cache, in which case <see cref="FetchAsync"/> should
        /// be called to fetch more remote data (if available).
        /// 
        /// This function will throw if deserialization fails, but the event will
        /// count as read and the sequence will be updated.
        /// </remarks>
        public TEvent TryGetNext()
        {
            if (_cache.Count == 0)
                return null;

            var next = _cache.Dequeue();
            Sequence = next.Sequence;

            return _serializer.Deserialize(next.Contents);
        }

        /// <summary>
        /// Attempts to fetch events from the remote stream, making them available to
        /// <see cref="TryGetNext"/>.
        /// </summary>
        /// <remarks> 
        /// Will always fetch at least one event, if events are available. The actual number
        /// depends on multiple optimization factors.
        /// 
        /// Calling this function adds events to an internal cache, which may grow out of
        /// control unless you call <see cref="TryGetNext"/> regularly.
        /// 
        /// If no events are available on the remote stream, returns false.
        /// </remarks>
        public async Task<bool> FetchAsync(CancellationToken cancel = default(CancellationToken))
        {
            var commit = await BackgroundFetchAsync(cancel);
            return commit();
        }

        /// <summary> Almost thread-safe version of <see cref="FetchAsync"/>. </summary>
        /// <remarks>
        /// You may call <see cref="TryGetNext"/> while the task is running, but NOT
        /// any other method. The function returned by the task is not thread-safe. 
        /// 
        /// This is intended for use in a "fetch in thread A, process in thread B"
        /// pattern: 
        ///  - call BackgroundFetchAsync, store into T
        ///  - repeatedy call TryGetNext until either T is done or no more events
        ///  - call the result of T, store into M
        ///  - if M is false or the last call to TryGetNext returned an event, repeat
        /// </remarks>
        public async Task<Func<bool>> BackgroundFetchAsync(CancellationToken cancel = default(CancellationToken))
        {
            const int maxBytes = 1024 * 1024 * 4;
            
            var read = await Storage.ReadAsync(Position, maxBytes, cancel);

            if (read.Events.Count == 0) return () => false;

            return () =>
            {
                Position = read.NextPosition;

                if (read.Events.Count == 0) return false;

                foreach (var e in read.Events)
                {
                    _cache.Enqueue(e);
                    _lastSequence = e.Sequence;
                }

                return true;
            };
        }

        /// <summary>
        /// Advance the stream by discarding the events. After returning,
        /// <see cref="Sequence"/> has the value of the sequence number of
        /// the event that takes place just before the one at the requested
        /// <paramref name="sequence" /> number. If there is
        /// no such event, the sequence number of the latest event in the stream
        /// is returned
        /// </summary>
        /// <returns>the new value of <see cref="Sequence"/>.</returns>
        public async Task<uint> DiscardUpTo(uint sequence, CancellationToken cancel = default(CancellationToken))
        {
            // First, try to seek forward to skip over many events at once
            var skip = await Storage.SeekAsync(sequence, 0, cancel);
            if (skip > Position) Position = skip;

            // Drop elements from _cache that are before the requested sequence number
            while (_cache.Count > 0 && _cache.Peek().Sequence < sequence)
            {
                var dequeued = _cache.Dequeue();
                Sequence = dequeued.Sequence;
            }

            // Continues until _cache contains at least one event past the target
            // sequence number (or no events left)
            while (_lastSequence < sequence)
            {
                Sequence = _lastSequence;

                const int maxBytes = 1024*1024*4;
                var read = await Storage.ReadAsync(Position, maxBytes, cancel);

                // Reached end of stream.
                if (read.Events.Count == 0)
                {
	                _cache.Clear();
                    Sequence = _lastSequence;
                    return Sequence;
                }

                Position = read.NextPosition;

                // Moved past target sequence: append events to _cache
                var lastReadEvent = read.Events[read.Events.Count - 1];
                _lastSequence = lastReadEvent.Sequence;

                if (_lastSequence >= sequence)
                {
                    foreach (var e in read.Events)
                    {
                        if (e.Sequence >= sequence)
                        {
                            _cache.Enqueue(e);
                        }
                        else
                        {
                            Sequence = e.Sequence;
                        }
                    }
                }
            }

            return Sequence;
        }

        /// <summary> Resets the stream to the beginning. </summary>
        public void Reset()
        {
            _cache.Clear();
            Sequence = 0;
            _lastSequence = 0;
            Position = 0;
            
            // Do NOT reset ! The value is still correct.
            // _minimumWritePosition = 0;
        }        
    }
}
