using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Quarantine;
using Lokad.AzureEventStore.Util;
using Lokad.AzureEventStore.Wrapper;

namespace Lokad.AzureEventStore
{
    /// <summary> 
    /// Connects to a stream and keeps the projected state up-to-date
    /// automatically. Support appending new events. 
    /// </summary>
    public sealed class EventStreamService<TEvent,TState> : SleepyProcess<Func<Task>> 
        where TEvent : class where TState : class
    {
        /// <summary> Exception thrown during stream initialization. </summary>
        private Exception _initFailure;

        /// <see cref="IsReady"/>
        private bool _isReady;

        /// <summary>
        /// True if the initialization is done and the state can be accessed. 
        /// Accessing the state before this is true results in an error.
        /// </summary>
        /// <remarks>
        /// If initialization failed, will throw (including the exception encountered 
        /// during initialization as an inner exception). Note that since initialization
        /// is retried forever, this may throw, then return true on the next try.
        /// </remarks>
        public bool IsReady 
        {
            get
            {
                if (_isReady) return true;
                if (_initFailure != null) throw new Exception("EventStreamService initiation is currently failing.", _initFailure);
                return false;
            } 
        }

        /// <summary>
        /// This task completed when the initialization is complete. 
        /// </summary>
        public readonly Task Ready;

        /// <summary> The underlying wrapper. </summary>
        internal readonly EventStreamWrapper<TEvent, TState> Wrapper;

        /// <summary> The quarantined events. </summary>
        public readonly EventQuarantine<TEvent> Quarantine;

        private readonly ILogAdapter _log;

        private readonly CancellationToken _cancel;

        /// <summary> Initialize and start the service. </summary>
        /// <remarks> 
        /// This will start a background task that performs all processing
        /// and regularly updates the contents.
        /// </remarks>
        /// <param name="storage"> Identifies where the event stream data is stored. </param>
        /// <param name="projections"> All available <see cref="IProjection{T}"/> instances. </param>
        /// <param name="projectionCache"> Used to save projected state. </param>
        /// <param name="log"> Used for logging. </param>
        /// <param name="cancel"> Stops the background tasks when called. </param>
        public static EventStreamService<TEvent,TState> StartNew(
            StorageConfiguration storage,
            IEnumerable<IProjection<TEvent>> projections,
            IProjectionCacheProvider projectionCache,
            ILogAdapter log,
            CancellationToken cancel)
        =>
            new EventStreamService<TEvent, TState>(
                storage, projections, projectionCache, log, cancel);
        
        /// <remarks>
        ///     This constructor is private so that it is obvious (via <c>StartNew</c>)
        ///     that background tasks are being creatd.
        /// </remarks>
        private EventStreamService(
            StorageConfiguration storage,
            IEnumerable<IProjection<TEvent>> projections,
            IProjectionCacheProvider projectionCache,
            ILogAdapter log,
            CancellationToken cancel) : base(TimeSpan.FromSeconds(30), cancel)
        {
            _log = log;
            _cancel = cancel;
            Wrapper = new EventStreamWrapper<TEvent, TState>(storage, projections, projectionCache, log);
            Quarantine = Wrapper.Quarantine;

            Ready = Task.Run(Initialize, cancel);
        }

        #region Concurrency boilerplate

        /// <summary> Initialize the wrapper and set <see cref="IsReady"/> to true. </summary>
        /// <remarks> 
        /// Upon failure, logs the exception and tries again after a short delay, forever.
        /// </remarks>
        private async Task Initialize()
        {
            while (!_cancel.IsCancellationRequested)
            {
                try
                {
                    await Wrapper.InitializeAsync(_cancel).ConfigureAwait(false);
                    _isReady = true;
                    return;
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception e)
                {
                    _log.Error("Could not initialize EventStreamService", e);
                    _initFailure = e;
                    Wrapper.Reset();                    
                }

                await Task.Delay(TimeSpan.FromSeconds(5), _cancel).ConfigureAwait(false);
            }
        }

        protected override async Task RunAsync(IReadOnlyList<Func<Task>> messages)
        {
            if (!IsReady)
                // Do not do anything until the stream has finished loading.
                return;

            // Every time we wake up, we expect the wrapper to refresh itself, either
            // as a consequence of one of the messages (i.e. a write) or because it
            // detects that a state refresh was requested.
            var _ = Wrapper.WaitForState();

            foreach (var msg in messages)
            {
                try
                {
                    await msg().ConfigureAwait(false);
                }
                catch
                {
                    // TODO: log this
                }
            }

            // If someone asked for a refresh, and it has not been satisfied by the 
            // above messages, perform a refresh automatically.
            if (Wrapper.WaitingForState)
                await Wrapper.CatchUpAsync(_cancel).ConfigureAwait(false);
        }

        /// <summary>
        /// The <see cref="LocalState"/> many not lag behind the <see cref="CurrentState"/>
        /// longer than this many seconds. 
        /// </summary>
        public double RefreshPeriod
        {
            get => Period.TotalSeconds;
            set => Period = TimeSpan.FromSeconds(value);
        }

        /// <summary>
        ///     Enqueues an asynchronous action, which will be performed after all currently
        ///     queued actions.
        /// </summary>
        private Task<T> EnqueueAction<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancel)
        {
            if (!IsReady) throw new StreamNotReadyException();

            // This will store the result of the action
            var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

            async Task Wrapper()
            {
                // Combine service-level cancellation with action-level cancellation
                using (var cts = new CancellationTokenSource())
                using (_cancel.Register(cts.Cancel))
                using (cancel.Register(cts.Cancel))
                {
                    // Handle cancellation, exception and successful evaluation
                    try
                    {
                        var result = await action(cts.Token);
                        tcs.TrySetResult(result);
                    }
                    catch (OperationCanceledException)
                    {
                        tcs.TrySetCanceled();
                    }
                    catch (Exception e)
                    {
                        tcs.TrySetException(e);
                    }
                }
            }

            Post(Wrapper);

            return tcs.Task;
        }

        #endregion

        /// <summary> Utility function for creating an <see cref="Append{TEvent}"/>. </summary>
        /// <remarks> Does not actually perform the append. </remarks>
        [Pure]
        public Append<TEvent> Use(params TEvent[] events) => new Append<TEvent>(events);

        /// <summary> Utility function for creating an <see cref="Append{TEvent,T}"/>. </summary>
        /// <remarks> Does not actually perform the append. </remarks>
        [Pure]
        public Append<TEvent, T> With<T>(T more, params TEvent[] events) => new Append<TEvent, T>(more, events);

        /// <summary> Retrieve the current state. </summary>
        /// <remarks>
        /// The returned state includes all events written to the remote stream
        /// until the moment this function is called.
        /// </remarks>
        public async Task<TState> CurrentState(CancellationToken cancel)
        {
            if (!IsReady) throw new StreamNotReadyException();

            // We wrap the `WaitForState` task in an outer task, because `EnqueueAction`
            // causes the background loop to wait for the result of the function---and
            // the `WaitForState` task relies on the background loop to become completed,
            // so the background loop waiting for it would result in a deadlock !
            var task = await EnqueueAction(c => Task.FromResult(Wrapper.WaitForState()), cancel)
                .ConfigureAwait(false);

            // This waits for the background loop to, somehow, complete a refresh.
            await task.ConfigureAwait(false);

            return Wrapper.Current;
        }

        /// <summary> Retrieve the local state. </summary>
        /// <remarks>
        /// This state takes into account all events until the last
        /// call to <see cref="CurrentState"/> or to a member of the
        /// <see cref="AppendEventsAsync(Func{TState,Append{TEvent}},CancellationToken)"/>
        /// method family. There may be other events in the remote 
        /// stream that are not taken into account.
        /// 
        /// Use <see cref="CurrentState"/> to take into account 
        /// remote events as well.
        /// 
        /// This property should be used if 1° you don't care about very recent
        /// remote events (e.g. in a background processing loop that will learn
        /// about those events in a future iteration, and doesnt care about the
        /// delay) or 2° if you only care about the outcome of a recent local
        /// event (such as right after appending it).
        /// </remarks>
        public TState LocalState
        {
            get
            {
                if (!IsReady) throw new StreamNotReadyException();
                return Wrapper.Current;
            }
        } 

        /// <summary> Force the wrapper to catch up with the stream. </summary>
        public Task CatchUpAsync(CancellationToken cancel) => CurrentState(cancel);

        /// <summary> Append events, constructed from the state, to the stream. </summary>
        /// <remarks> 
        /// Builder returns array of events to be appended. It may be called more than
        /// once. 
        /// </remarks>
        public Task<AppendResult> AppendEventsAsync(
            Func<TState, Append<TEvent>> builder, 
            CancellationToken cancel) 
        =>
            EnqueueAction(c => Wrapper.AppendEventsAsync(builder, c), cancel);

        /// <summary> Append events, constructed from the state, to the stream. </summary>
        /// <remarks> 
        /// Builder returns array of events to be appended, and additional data
        /// that will be returned by this method. Builder may be called more than
        /// once. 
        /// </remarks>
        public Task<AppendResult<T>> AppendEventsAsync<T>(
            Func<TState, Append<TEvent, T>> builder,
            CancellationToken cancel) 
        =>
            EnqueueAction(c => Wrapper.AppendEventsAsync(builder, c), cancel);
        

        /// <summary> Append events to the stream. </summary>
        /// <remarks> 
        /// This is a dangerous method, because it always adds the event to the stream
        /// regardless of state (this can lead to duplicates in multi-writer scenarios, 
        /// and so on). Make sure you know what you're doing. 
        /// </remarks>
        public Task<AppendResult> AppendEventsAsync(
            TEvent[] events,
            CancellationToken cancel = default)
        =>
            EnqueueAction(c => Wrapper.AppendEventsAsync(events, c), cancel);
        
        /// <summary> Attempt to save the projection to the cache. </summary>
        public Task TrySaveAsync(CancellationToken cancel = default) =>
            Wrapper.TrySaveAsync(cancel);

        /// <summary>
        ///     The number of events to be passed to the projection before
        ///     attempting a save/load cycle of the projection.
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
        public uint EventsBetweenCacheSaves
        {
            get => Wrapper.EventsBetweenCacheSaves;
            set => Wrapper.EventsBetweenCacheSaves = value;
        }
    }
}
