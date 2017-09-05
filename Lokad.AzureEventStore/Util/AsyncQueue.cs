using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Util
{
    /// <summary>
    /// An asynchronous queue. Adding elements is instantaneous, reading elements
    /// blocks until an element becomes available.
    /// </summary>
    public sealed class AsyncQueue<T>
    {
        /// <summary>
        /// A queue containing all values that have been appended but not read yet
        /// (when the queue is in a more-write-than-read situation).
        /// </summary>
        private readonly Queue<T> _readQueue =
            new Queue<T>();

        /// <summary>
        /// A queue containing all unread values that are waiting to be written
        /// to the queue (when the queue is in a more-read-than-write situation).
        /// </summary>
        /// <remarks>
        /// The function returns true if it accepts the value, false if it
        /// does not (because the corresponding Dequeue() was canceled).
        /// </remarks>
        private readonly Queue<Func<T, bool>> _writeQueue =
            new Queue<Func<T, bool>>();

        /// <summary>
        /// A task that blocks until a value is added to the queue for reading.
        /// </summary>
        public Task<T> Dequeue(CancellationToken cancel)
        {
            lock (this)
            {
                if (_readQueue.Count > 0)
                    return Task.FromResult(_readQueue.Dequeue());

                var tcs = new TaskCompletionSource<T>();
                var reg = cancel.Register(tcs.SetCanceled);

                _writeQueue.Enqueue(value =>
                {
                    if (cancel.IsCancellationRequested)
                        return false;

                    reg.Dispose();
                    tcs.SetResult(value);
                    return true;
                });

                return tcs.Task;
            }
        }

        /// <summary> Adds a task to the queue. </summary>
        public void Enqueue(T value)
        {
            lock (this)
            {
                while (true)
                {
                    if (_writeQueue.Count == 0)
                    {
                        _readQueue.Enqueue(value);
                        return;
                    }

                    var expect = _writeQueue.Dequeue();
                    if (expect(value)) return;
                }
            }
        }
    }
}