using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Quarantine;
using Lokad.AzureEventStore.Streams;
using Lokad.AzureEventStore.Util;

namespace Lokad.AzureEventStore.Wrapper
{
    /// <summary>
    /// Single-threaded wrapper around an event stream and corresponding 
    /// projections.
    /// </summary>
    internal sealed class EventStreamWrapper<TEvent, TState> where TState : class where TEvent : class
    {
        /// <summary> The projections that keep the state.  </summary>
        private readonly ReifiedProjectionGroup<TEvent, TState> _projection;

        /// <summary> The stream from which events are read. </summary>
        internal readonly EventStream<TEvent> Stream;

        /// <summary> Quarantined events. </summary>
        public readonly EventQuarantine<TEvent> Quarantine = new EventQuarantine<TEvent>();

        /// <summary> Logging status messages. </summary>
        private readonly ILogAdapter _log;

        /// <summary> Task to track if the previous commit was completed and if a new one is needed. </summary>
        private Task _commitTask;

        public EventStreamWrapper(
            StorageConfiguration storage, 
            IEnumerable<IProjection<TEvent>> projections,
            Func<EventStream<TEvent>, IProjectionCacheProvider> projectionCacheBuilder,
            StorageProvider storageProvider,
            ILogAdapter log = null)
            : this(storage.Connect(out var blobContainerClient), projections, projectionCacheBuilder, storageProvider, log, blobContainerClient)
        { }

        internal EventStreamWrapper(
            IStorageDriver storage, 
            IEnumerable<IProjection<TEvent>> projections,
            Func<EventStream<TEvent>, IProjectionCacheProvider> projectionCacheBuilder,
            StorageProvider storageProvider,
            ILogAdapter log = null,
            BlobContainerClient blobContainerClient = null)
        {
            _log = log;
            Stream = new EventStream<TEvent>(storage, log, blobContainerClient);
            _projection = new ReifiedProjectionGroup<TEvent, TState>(
                projections,
                storageProvider,
                projectionCacheBuilder != null
                    ? projectionCacheBuilder(Stream) 
                    : null, 
                log);
            _commitTask = Task.CompletedTask;
        }

        /// <summary> A task that will complete when the state is refreshed. </summary>
        /// <remarks> 
        ///     Return value is irrelevant - the state should always be pulled 
        ///     from <see cref="Current"/>.
        ///     
        ///     If null, no one is currently waiting for the state to be refreshed.
        /// </remarks>
        private TaskCompletionSource<bool> _waitForState;

        /// <summary> Is someone waiting for state the state to be refreshed ? </summary>
        /// <remarks> 
        ///     Equivalent to asking if there are calls to <see cref="WaitForState"/>
        ///     that have not completed yet.
        /// </remarks>
        public bool WaitingForState => _waitForState != null;

        /// <summary>
        ///     The number of events to be passed to the projection before
        ///     performing upkeep operations like attempting a save/load cycle of the projection.
        /// </summary>
        /// <remarks>
        ///     Due to performance considerations, this number may be slightly 
        ///     exceeded by a few *tens of thousands* of events.
        ///     
        ///     The save/load cycle is attempted ; if saving fails, failure is
        ///     ignored and stream loading continues. If saving succeeds but 
        ///     loading fails, the stream service enters a fully broken state.
        ///     
        ///     Events are only counted during a continuous catch-up phase. 
        ///     Every time the projection catches up with the stream, the 
        ///     event count is reset to zero. This means that this limit is 
        ///     really only expected to be hit during the initial catch-up.
        /// </remarks>
        public uint EventsBetweenUpkeepOpportunities  { get; set; } = uint.MaxValue;

        /// <summary>
        ///     Waits for the next time that the system catches up to the last event 
        ///     in the state (either because of a successful write, or because of 
        ///     <see cref="CatchUpAsync"/> completing. 
        /// </summary>
        public Task WaitForState()
        {
            // Since WaitForState() can be called from multiple threads, we need to 
            // ensure that we don't overwrite _waitForState if there's already a task
            // there (since that task would then never be overwritten).
            var newTask = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var oldTask = Interlocked.CompareExchange(ref _waitForState, newTask, null);
            return (oldTask ?? newTask).Task;
        }

        /// <summary>
        ///     Causes all uncompleted tasks returned by <see cref="WaitForState"/>
        ///     to complete.
        /// </summary>
        private void NotifyRefresh()
        {
            Interlocked.Exchange(ref _waitForState, null)?.TrySetResult(true);
        }

        /// <summary> The current state. </summary>
        public TState Current => _projection.Current;

        /// <summary> The current sequence number. </summary>
        // Could also be '_projection.Sequence', though this is not true in
        // the case of deserialization errors.
        public uint Sequence => Stream.Sequence;

        /// <summary>
        ///    This task completed when <see cref="InitializeAsync(CancellationToken)"/> finished.
        /// </summary>
        private Task _ready { get; set; }

        /// <summary>
        /// Reads up events up to the last one available. Pre-loads the projection from its cache,
        /// if available, to reduce the necessary work.
        /// </summary>
        public async Task InitializeAsync(CancellationToken cancel = default)
        {
            using var act = Logging.Stream.StartActivity("EventStreamWrapper.Initialize");

            var log = _log?.Timestamped();

            using (var act1 = Logging.Stream.StartActivity("EventStreamWrapper.Initialize.Load"))
            {
                await Catchup(_projection, Stream, cancel, log);
            }

            using (var act2 = Logging.Stream.StartActivity("EventStreamWrapper.Initialize.CatchUp"))
            {
                // Start reading everything
                log?.Info("[ES init] catching up with stream.");
                _ready = Task.Run(() => CatchUpAsync(cancel), cancel);
                await _ready;
                log?.Info("[ES init] DONE !");
            }
        }

        internal static async Task Catchup(
            IReifiedProjection projection,
            IEventStream stream,
            CancellationToken cancel = default,
            ILogAdapter log = null)
        {
            try
            {
                // Load project and discard events before that.
                log?.Info("[ES init] loading projections.");

                await projection.CreateAsync(cancel).ConfigureAwait(false);

                var catchUp = projection.Sequence + 1;

                log?.Info($"[ES init] advancing stream to seq {catchUp}.");
                var streamSequence = await stream.DiscardUpTo(catchUp, cancel).ConfigureAwait(false);

                if (cancel.IsCancellationRequested)
                    return;

                if (streamSequence != projection.Sequence)
                {
                    log?.Warning(
                        $"[ES init] projection seq {projection.Sequence} not found in stream (max seq is {streamSequence}: resetting everything.");

                    // Cache is apparently beyond the available sequence. Could happen in 
                    // development environments with non-persistent events but persistent 
                    // caches. Treat cache as invalid and start from the beginning.
                    stream.Reset();
                    projection.Reset();
                }
            }
            catch (Exception e)
            {
                log?.Warning("[ES init] error while reading cache.", e);

                // Something went wrong when reading the cache. Stop.
                stream.Reset();
                projection.Reset();
            }
        }

        /// <summary> Catch up with locally stored data, without remote fetches. </summary>
        /// <returns>
        ///     The number of events that have been passed to the projection.
        /// </returns>
        private uint CatchUpLocal(CancellationToken cancel)
        {
            var caughtUpWithProjection = false;
            var eventsPassedToProjection = 0u;

            while (true)
            {
                TEvent nextEvent;

                try
                {
                    // This might throw due to serialization error
                    //  (but not for other reasons)
                    nextEvent = Stream.TryGetNext();
                }
                catch (Exception ex)
                {
                    _log?.Warning($"[ES read] unreadable event at seq {Stream.Sequence}.", ex);
                    _projection.SetPossiblyInconsistent();
                    Quarantine.Add(Stream.Sequence, ex);
                    continue;
                }

                // No more local events left
                if (nextEvent == null) break;

                var seq = Stream.Sequence;

                if (_log != null && seq % 1000 == 0)
                    _log.Info($"[ES read] processing event at seq {seq}.");

                if (caughtUpWithProjection || seq > _projection.Sequence)
                {
                    caughtUpWithProjection = true;
                    try
                    {
                        ++eventsPassedToProjection;

                        // This might throw due to event processing issues
                        //  by one or more projection components
                        _projection.Apply(seq, nextEvent);
                        Commit(seq, cancel);
                    }
                    catch (Exception ex)
                    {
                        _log?.Warning($"[ES read] processing error on event at seq {seq}.", ex);
                        _projection.SetPossiblyInconsistent();
                        Quarantine.Add(seq, nextEvent, ex);
                    }
                }
            }

            return eventsPassedToProjection;
        }

        /// <summary>
        /// If the previous commit is finished, fire a new one.
        /// </summary>
        public void Commit(uint seq, CancellationToken cancel = default)
        {
            if (_commitTask.IsCompleted) 
                _commitTask = _projection.CommitAsync(seq, cancel);
        }

        /// <summary>
        /// Catch up with the stream (updating the state) until there are no new 
        /// events available.
        /// </summary>
        public async Task CatchUpAsync(CancellationToken cancel = default)
        {
            Func<bool> finishFetch;

            // Local variable, to avoid reaching the limit when not doing the
            // initial catch-up.
            var eventsSinceLastUpkeep = 0u;
            do
            {
                var fetchTask = Stream.BackgroundFetchAsync(cancel);

                // We have started fetching the next batch of events in 
                // the background, so we might as well start processing
                // those we already have. This pattern is optimized for
                // when fetching events takes longer than processing them,
                // and remains safe (i.e. no runaway memory usage) when 
                // the reverse is true.
                eventsSinceLastUpkeep += CatchUpLocal(cancel);

                // Maybe we have reached the event count limit before our 
                // save/load cycle ?
                if (!_ready.IsCompleted && eventsSinceLastUpkeep >= EventsBetweenUpkeepOpportunities)
                {
                    eventsSinceLastUpkeep = 0;
                    using var act = Logging.Stream.StartActivity("EventStreamWrapper.Upkeep");
                    act?.SetTag(Logging.SeqProjection, _projection.Sequence);
                    await _projection.UpkeepOrSaveLoadAsync(Stream.Sequence, cancel);
                }

                finishFetch = await fetchTask;

            } while (finishFetch());

            // We reach this point if 1° all events cached in the stream have
            // been processed and 2° the fetch operation returned no new events
            if (!_ready.IsCompleted)
            {
                using var act = Logging.Stream.StartActivity("EventStreamWrapper.Upkeep");
                act?.SetTag(Logging.UpkeepKind, "initial")
                    .SetTag(Logging.SeqProjection, _projection.Sequence);

                Stopwatch sw = Stopwatch.StartNew();
                await _projection.UpkeepAsync(cancel);
                _log?.Info($"[ES read] upkeep operations done in {sw.Elapsed} at seq {_projection.Sequence}.");
            }            

            NotifyRefresh();
        }

        /// <summary> Append events, constructed from the state, to the stream. </summary>
        /// <remarks> 
        /// Builder returns array of events to be appended, and additional data
        /// that will be returned by this method. Builder may be called more than
        /// once. 
        /// </remarks>
        public async Task<AppendResult<T>> AppendEventsAsync<T>(
            Func<TState, Append<TEvent, T>> builder,
            CancellationToken cancel = default)
        {
            var thrownByBuilder = false;
            var attempts = 0;

            using var act = Logging.Stream.StartActivity("EventStreamWrapper.AppendEventsAsync");
            
            try
            {
                while (true)
                {
                    ++attempts;
                    act?.SetTag(Logging.Attempts, attempts);

                    thrownByBuilder = true;
                    var tuple = builder(Current);
                    thrownByBuilder = false;

                    // No events to append, just return result
                    if (tuple.Events == null || tuple.Events.Count == 0)
                    {
                        act?.SetTag(Logging.EventCount, 0);
                        return new AppendResult<T>(0, 0, tuple.Result);
                    }

                    act?.SetTag(Logging.EventCount, tuple.Events.Count);

                    // Check for corrupted events. If even one event is corrupted, then events are not appended.
                    CheckEvents(tuple.Events);

                    // Append the events                
                    var done = await Stream.WriteAsync(tuple.Events, cancel).ConfigureAwait(false);

                    if (done == null)
                    {
                        // Append failed. Catch up and try again.
                        await CatchUpAsync(cancel).ConfigureAwait(false);
                    }
                    else
                    {
                        // Append succeeded. Catch up with locally available events (including those
                        // that were just added), then return append information.
                        CatchUpLocal(cancel);
                        NotifyRefresh();
                        return new AppendResult<T>(tuple.Events.Count, (uint)done, tuple.Result);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception e)
            {
                if (!thrownByBuilder)
                    _log?.Error("While appending events", e);
                throw;
            }
        }

        /// <summary>
        ///     After applying the callback to the transaction, try to write the generated
        ///     events to the stream.
        /// </summary>
        /// <returns>
        ///     A <see cref="TransactionResult{T}"/> that contains an <see cref="AppendResult{T}"/>
        ///     and if the append succeeded or not.
        /// </returns>
        public async Task<TransactionResult<T>> TryCommitTransactionAsync<T>(
            Transaction<TEvent, TState> transaction, 
            T result,
            uint sequence,
            CancellationToken cancel = default)
        {
            // Re-start the transaction if the event stream was advanced.
            if (sequence != Sequence)
                return new TransactionResult<T>(null, false);

            var events = transaction.Events;
            // No events to append, just return result
            if (events.Length == 0)
            {
                transaction.HandleCommit();
                return new TransactionResult<T>(new AppendResult<T>(0, 0, result), true);
            }

            // No need to check for corrupted events, since the transaction
            // automatically applies them on the fly.

            // Append the events                
            var done = await Stream.WriteAsync(events, cancel).ConfigureAwait(false);

            if (done == null)
            {
                // Append failed. Catch up and try again.
                await CatchUpAsync(cancel).ConfigureAwait(false);
                return new TransactionResult<T>(null, false);
            }
            else
            {
                // Append succeeded. Catch up with locally available events (including those
                // that were just added), then return append information.
                CatchUpLocal(cancel);
                NotifyRefresh();
                transaction.HandleCommit();
                return new TransactionResult<T>(new AppendResult<T>(events.Length, (uint)done, result), true);
            }
        }

        /// <summary>
        ///     After applying the callback to the transaction, try to write the generated
        ///     events to the stream.
        /// </summary>
        /// <returns>
        ///     A <see cref="TransactionResult"/> that contains an <see cref="AppendResult"/>
        ///     and if the append succeeded or not.
        /// </returns>
        public async Task<TransactionResult> TryCommitTransactionAsync(Transaction<TEvent, TState> transaction, uint sequence, CancellationToken cancel = default)
        {
            // Re-start the transaction if the event stream was advanced.
            if (sequence != Sequence)
                return new TransactionResult(null, false);

            var events = transaction.Events;
            // No events to append, just return result
            if (events.Length == 0)
            {
                transaction.HandleCommit();
                return new TransactionResult(new AppendResult(0, 0), true);
            }

            // No need to check for corrupted events, since the transaction
            // automatically applies them on the fly.

            // Append the events                
            var done = await Stream.WriteAsync(events, cancel).ConfigureAwait(false);

            if (done == null)
            {
                // Append failed. Catch up and try again.
                await CatchUpAsync(cancel).ConfigureAwait(false);
                return new TransactionResult(null, false);
            }
            else
            {
                // Append succeeded. Catch up with locally available events (including those
                // that were just added), then return append information.
                CatchUpLocal(cancel);
                NotifyRefresh();
                transaction.HandleCommit();
                return new TransactionResult(new AppendResult(events.Length, (uint)done), true);
            }
        }

        /// <summary> Check candidate events. </summary>
        /// <remarks> 
        /// An exception is thrown application of projections does not succed.
        /// Validity check does not result in any change of the projection.
        /// </remarks>
        private void CheckEvents(IReadOnlyList<TEvent> events)
        {
            _projection.TryApply(Stream.Sequence, events);
        }

        /// <summary> Append events, constructed from the state, to the stream. </summary>
        /// <remarks> 
        /// Builder returns array of events to be appended. It may be called more than
        /// once. 
        /// </remarks>
        public async Task<AppendResult> AppendEventsAsync(
            Func<TState, Append<TEvent>> builder,
            CancellationToken cancel = default)
        =>
            await AppendEventsAsync(s => new Append<TEvent, bool>(builder(s), false), cancel)
                .ConfigureAwait(false);

        /// <summary> Append events to the stream. </summary>
        /// <remarks> 
        /// This is a dangerous method, because it always adds the event to the stream
        /// regardless of state (this can lead to duplicates in multi-writer scenarios, 
        /// and so on). Make sure you know what you're doing. 
        /// </remarks>
        public async Task<AppendResult> AppendEventsAsync(
            TEvent[] events,
            CancellationToken cancel = default)
        =>
            await AppendEventsAsync(s => new Append<TEvent, bool>(false, events), cancel);

        /// <summary> Attempt to save the projection to the cache. </summary>
        /// <remarks> 
        /// While this returns a task, the save operation itself does not touch the
        /// object (only an immutable copy of the state), so you do not need to 
        /// wait for this task to finish before starting another operation.
        /// </remarks>
        public Task TrySaveAsync(CancellationToken cancel = default) =>
            _projection.TrySaveAsync(cancel);

        /// <summary> Reset the wrapper. Used when it is necessary to try again. </summary>
        public void Reset()
        {
            _projection.Reset();
            Stream.Reset();
        }

        /// <summary> Create an independent clone of this reified projection. </summary>
        /// <remarks> Not thread-safe ! </remarks>
        internal IReifiedProjection<TEvent, TState> GetProjectionClone()
        {
            return _projection.Clone();
        }
    }
}
