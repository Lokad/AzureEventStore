using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Streams
{
    /// <summary>
    /// <see cref="EventStream{TEvent}"/> members that don't depend on the event type. For those that do,
	/// there is <see cref="IEventStream{TEvent}"/>.
    /// </summary>
    public interface IEventStream
    {
        /// <summary> The sequence number assigned to the last event returned by <see cref="IEventStream{TEvent}.TryGetNext"/>. </summary>
        uint Sequence { get; }

        /// <summary> Almost thread-safe version of <see cref="Extensions.FetchAsync"/>. </summary>
        /// <remarks>
        /// You may call <see cref="IEventStream{TEvent}.TryGetNext"/> while the task is running, but NOT
        /// any other method. The function returned by the task is not thread-safe.
        ///
        /// This is intended for use in a "fetch in thread A, process in thread B"
        /// pattern:
        ///  - call BackgroundFetchAsync, store into T
        ///  - repeatedy call TryGetNext until either T is done or no more events
        ///  - call the result of T, store into M
        ///  - if M is false or the last call to TryGetNext returned an event, repeat
        /// </remarks>
        Task<Func<bool>> BackgroundFetchAsync(CancellationToken cancel = default(CancellationToken));

        /// <summary>
        /// Advance the stream by discarding the events. After returning,
        /// <see cref="Sequence"/> has the value of the sequence number of
        /// the event that takes place just before the one at the requested
        /// <paramref name="sequence" /> number.
        /// </summary>
        /// <returns>the new value of <see cref="Sequence"/>.</returns>
        Task<uint> DiscardUpTo(uint sequence, CancellationToken cancel = default(CancellationToken));

        /// <summary> Resets the stream to the beginning. </summary>
        void Reset();
    }

    /// <summary>
    /// <see cref="EventStream{TEvent}"/> members that depend in the event type
    /// </summary>
    public interface IEventStream<TEvent> : IEventStream
    {
        /// <summary>Provides the caller with the next event in the stream</summary>
        /// <remarks>
        /// Returns the next event, if any. Returns null if there are no more events
        /// available in the local cache, in which case <see cref="Extensions.FetchAsync"/> should
        /// be called to fetch more remote data (if available).
        ///
        /// This function will throw if deserialization fails, but the event will
        /// count as read and the sequence will be updated.
        /// </remarks>
        TEvent TryGetNext();

        /// <summary> Append one or more events to the stream. </summary>
        /// <remarks>
        /// Events are assigned consecutive sequence numbers. The FIRST of these
        /// numbers is returned.
        ///
        /// If no events are provided, return null.
        ///
        /// If this object's state no longer represents the remote stream (because other
        /// events have been written from elsewhere), this method will not write any
        /// events and will return null. The caller should call <see cref="Extensions.FetchAsync"/>
        /// until it returns false to have the object catch up with remote state.
        /// </remarks>
        Task<uint?> WriteAsync(IReadOnlyList<TEvent> events, CancellationToken cancel = default(CancellationToken));
    }

    public static class Extensions
    {

#if NETSTANDARD2_1

        /// <summary>
        ///     Enumerates all remaining events in the stream, as an async-enumerable.
        ///     Stops when the end of the stream is reached.
        /// </summary>
        public static async IAsyncEnumerable<(uint seq, TEvent ev)> Events<TEvent>(
            this IEventStream<TEvent> es,
            [EnumeratorCancellation] CancellationToken cancel)
        {
            Func<bool> more;
            do
            {
                var moreTask = es.BackgroundFetchAsync(cancel);

                while (es.TryGetNext() is TEvent e)
                    yield return (es.Sequence, e);

                more = await moreTask.ConfigureAwait(false);
            }
            while (more());
        }
#endif

        /// <summary>
        /// Attempts to fetch events from the remote stream, making them available to
        /// <see cref="IEventStream{TEvent}.TryGetNext"/>.
        /// </summary>
        /// <remarks>
        /// Will always fetch at least one event, if events are available. The actual number
        /// depends on multiple optimization factors.
        ///
        /// Calling this function adds events to an internal cache, which may grow out of
        /// control unless you call <see cref="IEventStream{TEvent}.TryGetNext"/> regularly.
        ///
        /// If no events are available on the remote stream, returns false.
        /// </remarks>
        public static async Task<bool> FetchAsync(this IEventStream stream, CancellationToken cancel = default(CancellationToken))
        {
            var commit = await stream.BackgroundFetchAsync(cancel);
            return commit();
        }

        /// <summary>
        /// provides the caller with the next event in the stream. It may trigger at most one call to <see cref="FetchAsync"/>.
        /// You may use this to implement active (polling) wait for an event. What this method won't do is block until a new
        /// event has been published.
        /// </summary>
        public static async Task<TEvent> TryGetNextAsync<TEvent>(this IEventStream<TEvent> stream, CancellationToken cancel = default(CancellationToken))
            where TEvent: class
        {
            var next = stream.TryGetNext();
            if (next != null)
            {
                return next;
            }

            if (await stream.FetchAsync(cancel))
            {
                return stream.TryGetNext();
            }

            return null;
        }
    }
}
