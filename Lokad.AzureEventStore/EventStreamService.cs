using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Quarantine;
using Lokad.AzureEventStore.Streams;
using Lokad.AzureEventStore.Util;
using Lokad.AzureEventStore.Wrapper;

namespace Lokad.AzureEventStore
{
    /// <summary> 
    /// Connects to a stream and keeps the projected state up-to-date
    /// automatically. Support appending new events. 
    /// </summary>
    public sealed class EventStreamService<TEvent,TState> where TEvent : class where TState : class
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

        private readonly AsyncQueue<Func<Task>> _pending = new AsyncQueue<Func<Task>>();         
        
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
        {
            return new EventStreamService<TEvent, TState>(
                storage, projections, projectionCache, null, log, cancel);
        }

        /// <summary> Initialize and start the service. </summary>
        /// <remarks> 
        /// This will start a background task that performs all processing
        /// and regularly updates the contents.
        /// </remarks>
        /// <param name="storage"> Identifies where the event stream data is stored. </param>
        /// <param name="projections"> All available <see cref="IProjection{T}"/> instances. </param>
        /// <param name="projectionCache"> Used to save projected state. </param>
        /// <param name="events"> 
        ///     Called on each event committed to the stream (including intial catch-up of 
        ///     already commited events). 
        ///     <see cref="EventStreamWrapper{TEvent,TState}.OnEachCommitted"/>
        /// </param>
        /// <param name="log"> Used for logging. </param>
        /// <param name="cancel"> Stops the background tasks when called. </param>
        public static EventStreamService<TEvent, TState> StartNew(
            StorageConfiguration storage,
            IEnumerable<IProjection<TEvent>> projections,
            IProjectionCacheProvider projectionCache,
            IEnumerable<Tuple<EventStream<TEvent>.Listener, uint>> events,
            ILogAdapter log,
            CancellationToken cancel)
        {
            return new EventStreamService<TEvent, TState>(
                storage, projections, projectionCache, events, log, cancel);
        }
        
        /// <remarks>
        ///     This constructor is private so that it is obvious (via <c>StartNew</c>)
        ///     that background tasks are being creatd.
        /// </remarks>
        private EventStreamService(
            StorageConfiguration storage,
            IEnumerable<IProjection<TEvent>> projections,
            IProjectionCacheProvider projectionCache,
            IEnumerable<Tuple<EventStream<TEvent>.Listener, uint>> events,
            ILogAdapter log,
            CancellationToken cancel)
        {
            _log = log;
            _cancel = cancel;
            Wrapper = new EventStreamWrapper<TEvent, TState>(storage, projections, projectionCache, log);
            Quarantine = Wrapper.Quarantine;

            if (events != null)
                foreach (var pair in events)
                    Wrapper.OnEachCommitted(pair.Item1, pair.Item2);

            Ready = Task.Run(Initialize, cancel);
            Task.Run(Loop, cancel);            
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

        /// <summary> Loop forever (or at least, until it's done). </summary>
        /// <remarks> Any exceptions thrown are passed to the exception callback. </remarks>
        private async Task Loop()
        {
            // Wait for initialization to finish and check success.
            try
            {
                await Ready.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception e)
            {
                _log?.Error("While waiting to catch up with event stream.", e);
                return;
            }

            // The auto-refresh process
            // ========================

            // This is a background task that checks periodically whether
            // a synchronization with the remote stream has occurred. If 
            // it has not, it performs one.

#pragma warning disable CS4014 
            Task.Run(async () =>
            {
                while (!_cancel.IsCancellationRequested)
                {
                    var syncStep = Wrapper.SyncStep;
                    
                    // RefreshPeriod/2 because it's possible to have a 
                    // syncStep increment happen right before this 'await',
                    // in which case the delay will execute twice before
                    // it detects that no sync is happening.
                    await Task.Delay(TimeSpan.FromSeconds(RefreshPeriod/2), _cancel);
                    
                    if (syncStep == Wrapper.SyncStep)
                        await CatchUpAsync(default(CancellationToken));
                }

            }, _cancel);
#pragma warning restore CS4014 

            // The actual loop
            // ===============

            while (!_cancel.IsCancellationRequested)
            {   
                // This sleeps until an action becomes available in the queue
                var nextAction = await _pending.Dequeue(_cancel).ConfigureAwait(false);

                // The action does not throw (is it wrapped properly)
                await nextAction().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// The <see cref="LocalState"/> many not lag behind the <see cref="CurrentState"/>
        /// longer than this many seconds. 
        /// </summary>
        /// <remarks>
        /// In practice, even in the absence of any other activity, the 
        /// </remarks>
        public double RefreshPeriod { get; set; } = 60;

        /// <summary>
        /// Enqueues an asynchronous action, which will be performed after all currently
        /// queued actions.
        /// </summary>
        /// <remarks>
        /// It is assumed that all enqueued actions will query the current stream state, 
        /// either by attempting a write or by performing catch-up.
        /// </remarks>
        private Task<T> EnqueueAction<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancel)
        {
            if (!IsReady) throw new InvalidOperationException("EventStreamService not ready.");

            // This will store the result of the action
            var tcs = new TaskCompletionSource<T>();
            
            Func<Task> func = async () =>
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
            };

            _pending.Enqueue(func);

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
        public Task<TState> CurrentState(CancellationToken cancel)
        {
            if (!IsReady) throw new StreamNotReadyException();

            var syncStep = Wrapper.SyncStep;
            return EnqueueAction(async c =>
            {
                if (syncStep == Wrapper.SyncStep)
                    await Wrapper.CatchUpAsync(c).ConfigureAwait(false);

                return Wrapper.Current;

            }, cancel);
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
            CancellationToken cancel = default(CancellationToken))
        =>
            EnqueueAction(c => Wrapper.AppendEventsAsync(events, c), cancel);
        
        /// <summary> Attempt to save the projection to the cache. </summary>
        public Task TrySaveAsync(CancellationToken cancel = default(CancellationToken)) =>
            Wrapper.TrySaveAsync(cancel);
    }
}
