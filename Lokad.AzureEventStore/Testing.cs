using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Streams;

namespace Lokad.AzureEventStore
{
    /// <summary> Methods for testing code based on this library. </summary>
    public static class Testing
    {
        /// <summary> Retrieve the internal <see cref="EventStream{TEvent}"/> object from a service. </summary>        
        public static EventStream<TEvent> GetStream<TEvent,TState>(EventStreamService<TEvent,TState> ess) 
            where TEvent : class 
            where TState : class
        {
            return ess.Wrapper.Stream;
        }

        /// <summary> Retrieves a list of all events written to a stream so far. </summary>
        public static Task<IReadOnlyList<KeyValuePair<uint, TEvent>>> GetEvents<TEvent, TState>(
            EventStreamService<TEvent, TState> ess,
            CancellationToken cancel = default(CancellationToken))
            where TEvent : class
            where TState : class
        {
            return GetEvents(GetStream(ess), cancel);
        }

        /// <summary> Retrieves a list of all events written to a stream so far. </summary>
        public static async Task<IReadOnlyList<KeyValuePair<uint, TEvent>>> GetEvents<TEvent>(
            EventStream<TEvent> stream,
            CancellationToken cancel = default(CancellationToken))
            where TEvent : class
        {
            // Copy the stream to have it reset to position 0
            stream = new EventStream<TEvent>(stream.Storage);

            var list = new List<KeyValuePair<uint, TEvent>>();

            Func<bool> finishFetch;

            do
            {
                var fetchTask = stream.BackgroundFetchAsync(cancel);

                TEvent nextEvent;
                while ((nextEvent = stream.TryGetNext()) != null)                
                    list.Add(new KeyValuePair<uint, TEvent>(stream.Sequence, nextEvent));

                finishFetch = await fetchTask;

            } while (finishFetch());

            return list;
        }

        /// <summary> Returns a configuration for an in-memory storage driver. </summary>
        public static StorageConfiguration InMemory
        {
            get {  return new StorageConfiguration(new MemoryStorageDriver()); }
        }

        /// <summary> Returns a configuration for an in-memory storage driver with initial data. </summary>
        public static StorageConfiguration Initialize<TEvent>(params TEvent[] events) where TEvent : class
        {
            var msd = new MemoryStorageDriver();
            var ms = new MigrationStream<TEvent>(msd);

            ms.WriteAsync(events.Select((e, i) => new KeyValuePair<uint, TEvent>((uint) (i + 1), e)))
                .Wait();
 
            return new StorageConfiguration(msd);
        }

        /// <summary> Saves projection state to memory.  </summary>
        public sealed class InMemoryCache : IProjectionCacheProvider
        {
            private readonly Dictionary<string, MemoryStream> _streams = new Dictionary<string, MemoryStream>();
            
            /// <see cref="IProjectionCacheProvider.OpenReadAsync"/>
            public Task<Stream> OpenReadAsync(string fullname)
            {
                MemoryStream stream;
                if (!_streams.TryGetValue(fullname, out stream)) return Task.FromResult<Stream>(null);
                
                return Task.FromResult<Stream>(new MemoryStream(stream.ToArray()));
            }

            /// <see cref="IProjectionCacheProvider.OpenWriteAsync"/>
            public Task<Stream> OpenWriteAsync(string fullname)
            {
                var ms = new MemoryStream();
                _streams[fullname] = ms;

                return Task.FromResult<Stream>(ms);
            }
        }
    }
}
