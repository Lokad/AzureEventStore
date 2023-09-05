using System;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Streams;
using Xunit;

namespace Lokad.AzureEventStore.Test.streams
{
    public interface IStreamEvent {}

    [DataContract]
    public sealed class StreamEvent : IStreamEvent
    {
        [DataMember]
        public int X { get; set; }
    }

    [DataContract]
    public sealed class IntegerEvent : IStreamEvent
    {
        [DataMember]
        public int Integer { get; set; }
        public IntegerEvent(int i = 0) { Integer = i; }
    }

    public sealed class event_stream
    {
        [Fact]
        public async Task write()
        {
            var driver = new MemoryStorageDriver();
            var stream = new EventStream<IStreamEvent>(driver);

            var result = await stream.WriteAsync(new IStreamEvent[] {new StreamEvent()});
            Assert.Equal(1u, result);
        }

        [Fact]
        public async Task write_multiple()
        {
            var driver = new MemoryStorageDriver();
            var stream = new EventStream<IStreamEvent>(driver);

            var result = await stream.WriteAsync(new IStreamEvent[] { new StreamEvent(), new StreamEvent() });
            Assert.Equal(1u, result);
            
            result = await stream.WriteAsync(new IStreamEvent[] { new StreamEvent() });
            Assert.Equal(3u,  result);
        }

        [Fact]
        public async Task write_wrong_position()
        {
            var driver = new MemoryStorageDriver();
            await driver.WriteAsync(0, new[] {new RawEvent(1U, new byte[8])});

            var stream = new EventStream<IStreamEvent>(driver);

            var result = await stream.WriteAsync(new IStreamEvent[] { new StreamEvent() });
            Assert.Null(result);
        }

        [Fact]
        public async Task write_restore_position()
        {
            var driver = new MemoryStorageDriver();

            var stream = new EventStream<IStreamEvent>(driver);
            await stream.WriteAsync(new IStreamEvent[] { new StreamEvent() });

            stream = new EventStream<IStreamEvent>(driver);

            var result = await stream.WriteAsync(new IStreamEvent[] { new StreamEvent() });
            Assert.Null(result);

            while (await stream.FetchAsync()) {}

            result = await stream.WriteAsync(new IStreamEvent[] { new StreamEvent() });
            Assert.Equal(2u, result);
        }

        [Fact]
        public async Task write_changing_position()
        {
            var driver = new MemoryStorageDriver();
            
            var streamA = new EventStream<IStreamEvent>(driver);
            var streamB = new EventStream<IStreamEvent>(driver);

            var result = await streamA.WriteAsync(new IStreamEvent[] { new StreamEvent() });
            Assert.Equal(1u, result);

            while (await streamB.FetchAsync()) 
                while (streamB.TryGetNext() is IStreamEvent) { }

            result = await streamB.WriteAsync(new IStreamEvent[] { new StreamEvent() });
            Assert.Equal(2u, result);

            result = await streamA.WriteAsync(new IStreamEvent[] { new StreamEvent() });
            Assert.Null(result);

            while (await streamA.FetchAsync())
                while (streamA.TryGetNext() is IStreamEvent) { }

            result = await streamA.WriteAsync(new IStreamEvent[] {new StreamEvent()});
            Assert.Equal(3u, result);
        }

        [Fact]
        public async Task naive_read()
        {
            var driver = new MemoryStorageDriver();

            var stream = new EventStream<IStreamEvent>(driver);
            await stream.WriteAsync(Enumerable.Range(0, 20)
                .Select(i => (IStreamEvent)new IntegerEvent(i)).ToArray());

            stream = new EventStream<IStreamEvent>(driver);

            while (await stream.FetchAsync()) { }

            var next = 0;

            IStreamEvent e;
            while ((e = stream.TryGetNext()) != null)
            {
                Assert.Equal(next, ((IntegerEvent)e).Integer);
                ++next;
            }

            Assert.Equal(20, next);
        }

        [Fact]
        public async Task interlocked_read()
        {
            var driver = new MemoryStorageDriver();

            var stream = new EventStream<IStreamEvent>(driver);
            await stream.WriteAsync(Enumerable.Range(0, 20)
                .Select(i => (IStreamEvent)new IntegerEvent(i)).ToArray());

            stream = new EventStream<IStreamEvent>(driver);

            var next = 0;
            do
            {
                IStreamEvent e;
                while ((e = stream.TryGetNext()) != null)
                {
                    Assert.Equal(next, ((IntegerEvent) e).Integer);
                    ++next;
                }

            } while (await stream.FetchAsync());

            Assert.Equal(20, next);
        }

        [Fact]
        public async Task background_read()
        {
            var driver = new MemoryStorageDriver();

            var stream = new EventStream<IStreamEvent>(driver);
            await stream.WriteAsync(Enumerable.Range(0, 20)
                .Select(i => (IStreamEvent)new IntegerEvent(i)).ToArray());

            stream = new EventStream<IStreamEvent>(driver);

            Func<bool> shouldContinue;
            var next = 0;
            do
            {
                var task = stream.BackgroundFetchAsync();

                IStreamEvent e;
                while ((e = stream.TryGetNext()) != null)
                {
                    Assert.Equal(next, ((IntegerEvent)e).Integer);
                    ++next;
                }

                shouldContinue = await task;

            } while (shouldContinue());

            Assert.Equal(20, next);
        }


        [Fact]
        public async Task discard()
        {
            var driver = new MemoryStorageDriver();

            var stream = new EventStream<IStreamEvent>(driver);
            await stream.WriteAsync(Enumerable.Range(0, 20)
                .Select(i => (IStreamEvent)new IntegerEvent(i)).ToArray());

            stream = new EventStream<IStreamEvent>(driver);

            await stream.DiscardUpTo(11);

            Func<bool> shouldContinue;
            var next = 10;
            do
            {
                var task = stream.BackgroundFetchAsync();

                IStreamEvent e;
                while ((e = stream.TryGetNext()) != null)
                {
                    Assert.Equal(next, ((IntegerEvent)e).Integer);
                    ++next;
                }

                shouldContinue = await task;

            } while (shouldContinue());

            Assert.Equal(20, next);
        }

        [Fact]
        public async Task discard_after_fetch()
        {
            var driver = new MemoryStorageDriver();

            var stream = new EventStream<IStreamEvent>(driver);
            await stream.WriteAsync(Enumerable.Range(0, 20)
                .Select(i => (IStreamEvent)new IntegerEvent(i)).ToArray());

            stream = new EventStream<IStreamEvent>(driver);

            while (await stream.FetchAsync()) { }
            await stream.DiscardUpTo(11);
            
            Assert.Equal((uint) 10, stream.Sequence);

            var next = 10;

            IStreamEvent e;
            while ((e = stream.TryGetNext()) != null)
            {
                Assert.Equal(next, ((IntegerEvent)e).Integer);
                ++next;
            }

            Assert.Equal(20, next);
        }

        [Fact]
        public async Task discard_above_end()
        {
            var driver = new MemoryStorageDriver();

            var stream = new EventStream<IStreamEvent>(driver);
            await stream.WriteAsync(Enumerable.Range(0, 20)
                .Select(i => (IStreamEvent)new IntegerEvent(i)).ToArray());

            stream = new EventStream<IStreamEvent>(driver);
            while (await stream.FetchAsync()) { }

            await stream.DiscardUpTo(30);

            Assert.Equal((uint) 20, stream.Sequence);
        }
    }
}
