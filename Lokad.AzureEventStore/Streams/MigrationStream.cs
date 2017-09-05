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
    }
}
