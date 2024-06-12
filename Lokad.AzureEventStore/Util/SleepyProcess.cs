using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Util
{
    /// <summary>
    ///     Helper class: a long-running background task that calls a given
    ///     function when told to wake up, or after a period of inactivity.
    /// </summary>
    public abstract class SleepyProcess
    {
        /// <summary> 
        ///     If this duration has elapsed since the last time <see cref="RunAsync"/>
        ///     has completed, call it again. Calls may happen more frequently, 
        ///     than the period, if wake-ups are processed.
        /// </summary>
        public TimeSpan Period { get; set; }

        /// <summary> Cancellation token, used to cancel the entire process. </summary>
        private readonly CancellationToken _cancel;

        /// <see cref="Finished"/>
        private readonly TaskCompletionSource<bool> _finished;

        /// <summary> A task that completes (successfully) once the sleepy process is stopped. </summary>
        public Task Finished => _finished.Task;

        /// <summary>
        ///     Used if wake-up requests have come in while <see cref="RunAsync"/> is
        ///     executing. Will cause <see cref="RunAsync"/> to start executing again
        ///     immediately.
        /// </summary>
        private int _status;

        /// <summary> Value of <see cref="_status"/> if currently sleeping. </summary>
        private const int StatusNotExecuting = 0;

        /// <summary> 
        ///     Value of <see cref="_status"/> if currently executing <see cref="RunAsync"/>
        ///     and no wake-up has been received yet.
        /// </summary>
        private const int StatusExecuting = 1;

        /// <summary>
        ///     Value of <see cref="_status"/> if currently executing <see cref="RunAsync"/>
        ///     and a wake-up has been received.
        /// </summary>
        private const int StatusScheduled = 2;

        /// <summary> Ask the process to start executing ASAP. </summary>
        /// <remarks> 
        ///     If process is already executing, it will execute AGAIN when it
        ///     completes (unless <paramref name="again"/> is false).
        /// </remarks>
        public void WakeUp(bool again = true)
        {
            if (_cancel.IsCancellationRequested)
                return;

            // Assume that we're NOT executing, and move to executing state. This
            // is the most likely situation.
            var status = Interlocked.CompareExchange(ref _status, StatusExecuting, StatusNotExecuting);
            if (status == StatusNotExecuting)
            {
                Execute();
                return;
            }

            // It's already executing, and we don't need to schedule another execution
            // after this one, so return immediately.
            if (!again || status == StatusScheduled) return;

            // Schedule another execution after this one.
            status = Interlocked.CompareExchange(ref _status, StatusScheduled, StatusExecuting);

            // Looks like the current execution has finished. Now is a great time to 
            // schedule another execution !
            if (status == StatusNotExecuting)
            {
                // If someone already started another execution, just stop (it counts as our
                // "another execution" since it started after our call was initiated).
                if (Interlocked.CompareExchange(ref _status, StatusExecuting, StatusNotExecuting) == StatusNotExecuting)
                    Execute();
            }
        }

        /// <summary> This function is called when the process wakes up. </summary>
        protected abstract Task RunAsync();

        /// <summary> Execute the process function. </summary>
        private void Execute()
        {
            // Make sure the LocalAsync from our caller doesn't leak in
            using var suppress = ExecutionContext.SuppressFlow();

            Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        await RunAsync().ConfigureAwait(false);
                    }
                    catch
                    {
                        // TODO: log this
                    }

                    // Back from 'executing' to 'not executing'
                    var status = Interlocked.CompareExchange(ref _status, StatusNotExecuting, StatusExecuting);
                    if (status == StatusScheduled)
                    {
                        // Back from 'scheduled' to 'executing', then loop again.
                        Interlocked.CompareExchange(ref _status, StatusExecuting, StatusScheduled);
                        continue;
                    }

                    return;
                }
            });
        }

        /// <summary> Creates and starts a new process. </summary>
        protected SleepyProcess(TimeSpan period, CancellationToken cancel)
        {
            Period = period;
            _cancel = cancel;

            _finished = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            cancel.Register(() => _finished.TrySetCanceled());

            Run(cancel);
        }

        /// <summary> Performs periodic wake-ups until canceled. </summary>
        private void Run(CancellationToken cancel)
        {
            // Make sure the LocalAsync from our caller doesn't leak in
            using var suppress = ExecutionContext.SuppressFlow();

            Task.Run(async () =>
            {
                while (!cancel.IsCancellationRequested)
                {
                    await Task.Delay(Period, cancel).ConfigureAwait(false);
                    WakeUp(again: false);
                }
            }, cancel);
        }
    }

    /// <summary>
    ///     Like <see cref="SleepyProcess"/>, but can receive messages of type 
    ///     <typeparamref name="T"/> that wake up the process as well.
    /// </summary>
    public abstract class SleepyProcess<T> : SleepyProcess
    {
        /// <summary> Incoming messages are stored here. </summary>
        private readonly ConcurrentQueue<T> _inbox = new ConcurrentQueue<T>();

        /// <summary> Used to pass the arguments to <see cref="RunAsync(IReadOnlyList{T})"/> </summary>
        private readonly List<T> _arguments = new List<T>();

        /// <summary> Called when woken up, with all messages since the last wake-up call. </summary>
        protected abstract Task RunAsync(IReadOnlyList<T> messages);

        /// <summary> Dequeues messages and passes them to <see cref="RunAsync(IReadOnlyList{T})"/>. </summary>
        protected override Task RunAsync()
        {
            _arguments.Clear();
            while (_inbox.TryDequeue(out var message))
                _arguments.Add(message);

            return RunAsync(_arguments);
        }

        /// <summary> Send a message to the process. </summary>
        /// <remarks> It will wake up and handle the message ASAP. </remarks>
        public void Post(T message)
        {
            _inbox.Enqueue(message);
            WakeUp();
        }

        protected SleepyProcess(TimeSpan period, CancellationToken cancel) : base(period, cancel)
        {
        }
    }
}
