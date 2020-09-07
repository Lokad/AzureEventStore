using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;

namespace Lokad.AzureEventStore.Streams
{
    /// <summary> A write-only event stream used for data migrations. </summary>
    public sealed class MigrationStream<TEvent> where TEvent : class
    {
        private readonly IStorageDriver _driver;

        private uint? _sequence;
        private long? _position;

        private readonly JsonEventSerializer<TEvent> _serializer = new JsonEventSerializer<TEvent>(); 

        /// <summary> Construct a migration stream from a configuration. </summary>
        public MigrationStream(StorageConfiguration config) : this(config.Connect())
        {
            if (config.ReadOnly)
                throw new ArgumentException("Expected writable storage.");
        }

        internal MigrationStream(IStorageDriver driver)
        {
            _driver = driver;
        } 

        /// <summary> Return the last written sequence in the output stream. </summary>
        public async Task<uint> LastWrittenAsync(CancellationToken cancel = default(CancellationToken))
        {            
            return (uint)(_sequence ?? 
                (_sequence = await _driver.GetLastKeyAsync(cancel).ConfigureAwait(false)));
        }

        /// <summary> Appends events to the stream. </summary>
        /// <remarks> 
        /// The sequence number may not be before the current one.
        /// You can provide an arbitrarily large number of events, which will then be
        /// split into multiple writes. 
        /// </remarks>
        public async Task WriteAsync(
            IEnumerable<KeyValuePair<uint, TEvent>> events, 
            CancellationToken cancel = default(CancellationToken))
        {
            var sequence = _sequence ?? await LastWrittenAsync(cancel).ConfigureAwait(false);

            var position = (long)(_position ?? 
                (_position = await _driver.GetPositionAsync(cancel).ConfigureAwait(false)));

            // If an exception is thrown before we return properly, it might leave the
            // cached values (_sequence and _position) in an invalid state, so we null 
            // them to cause a re-load on the next call. If returning properly, we'll
            // set them back to the proper values.
            _sequence = null;
            _position = null;

            var list = new List<RawEvent>();
            var otherList = new List<RawEvent>();

            // Assigned the current writing task every time 'write' is called.
            Task writing = Task.FromResult(0);

            // Writes the events in the list to the driver
            Func<IReadOnlyList<RawEvent>, Task> write = async written =>
            {
                if (written.Count == 0) return;

                var result = await _driver.WriteAsync(position, written, cancel)
                    .ConfigureAwait(false);

                if (!result.Success)
                    throw new Exception(
                        $"Error writing events {written[0].Sequence} to {written[written.Count - 1].Sequence}");

                position = result.NextPosition;
            };

            foreach (var kv in events)
            {
                cancel.ThrowIfCancellationRequested();

                var e = kv.Value;
                var seq = kv.Key;

                if (seq <= sequence) 
                    throw new ArgumentException($"Out-of-order sequence #{seq}", nameof(events));

                sequence = seq;
                list.Add(new RawEvent(seq, _serializer.Serialize(e)));

                // Avoid runaway scheduling (having to write increasingly 
                // large sets of events because write is slower than enumeration
                // or serialization)
                //
                // Also, start a new write as soon as the previous one is done.                
                if (writing.IsCompleted || list.Count > 1000)
                {
                    await writing.ConfigureAwait(false);

                    // The created task will ALWAYS be awaited, to guarantee
                    // that exceptions bubble up appropriately.
                    writing = write(list);

                    // Do not overwrite the list straight away, as it will 
                    // be used during the write process. Instead, keep two
                    // lists and swap them (there is only one write process
                    // and one serialization process running at any given 
                    // time, so two lists are enough).
                    var temp = list;
                    list = otherList;
                    otherList = temp;

                    list.Clear();
                }
            }

            await writing.ConfigureAwait(false);
            await write(list).ConfigureAwait(false);

            // Cache the values we reached.
            _sequence = sequence;
            _position = position;
        }

        /// <summary>
        ///     Migrate events from the source stream to this migration stream.
        /// </summary>
        /// <param name="source">
        ///     Events are read from this stream.
        /// </param>
        /// <param name="migrator">
        ///     Function invoked to migrate an event. Receives the event and 
        ///     its sequence number as argument, must return the event to be
        ///     written (or null if the event should be dropped). 
        ///     
        ///     The function will be invoked starting with the very first
        ///     event in the source stream, but its return value will be 
        ///     ignored if the sequence number is smaller than the value 
        ///     returned by <see cref="LastWrittenAsync"/> (the purpose of 
        ///     this is to let the migrator function accumulate any necessary
        ///     internal state).
        ///     
        ///     The events retain their sequence number. Dropped events (those
        ///     for which the migrator returns null) are not migrated at all.
        /// </param>
        /// <param name="refreshDelay">
        ///     No effect until migration has reached the end of the source 
        ///     stream.
        ///     
        ///     If null, migration ends when the end of the source stream is 
        ///     reached. 
        ///     
        ///     If provided, once the end of the source stream is 
        ///     reached, will poll the source stream forever (with this delay)
        ///     looking for new events.
        /// </param>
        /// <param name="cancel">
        ///     Invoke to interrupt the migration.
        /// </param>
        public async Task MigrateFromAsync(
            IEventStream<TEvent> source,
            Func<TEvent, uint, TEvent> migrator,
            TimeSpan? refreshDelay,
            CancellationToken cancel)
        {
            var lastSeq = await LastWrittenAsync(cancel);
            var list = new List<KeyValuePair<uint, TEvent>>();

            while (true)
            {
                cancel.ThrowIfCancellationRequested();

                var fetch = source.BackgroundFetchAsync();

                while (source.TryGetNext() is TEvent next)
                {
                    var seq = source.Sequence;

                    if (!(migrator(next, seq) is TEvent migrated))
                        // Do not migrate this event.
                        continue;

                    if (seq <= lastSeq)
                        // Event already migrated previously
                        continue;

                    list.Add(new KeyValuePair<uint, TEvent>(seq, migrated));
                }

                if (list.Count > 0)
                {
                    await WriteAsync(list, cancel).ConfigureAwait(false);
                    list.Clear();
                }

                var more = await fetch.ConfigureAwait(false);

                if (more()) continue;

                // Reached the end of the stream.
                if (refreshDelay == null) 
                    return;
                else
                    await Task.Delay(refreshDelay.Value, cancel).ConfigureAwait(false);
            } 
        }
    }
}
