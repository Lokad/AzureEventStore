using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Streams;
using Xunit;

namespace Lokad.AzureEventStore.Test.streams;

public interface IOneEvent {}

[DataContract]
public sealed record OneEvent([property: DataMember] int A) : IOneEvent;

public sealed class OneEventSerializer : IEventSerializer<IOneEvent>
{
    public byte Tag => (byte)'O';

    public IOneEvent Deserialize(ReadOnlyMemory<byte> data)
    {
        var span = data.Span;
        if (span.Length < 1 + sizeof(int))
            throw new ArgumentException("Insufficient data for OneEvent.", nameof(data));
        if (span[0] != Tag)
            throw new InvalidOperationException($"Unexpected tag 0x{span[0]:X2}.");

        var value = MemoryMarshal.Read<int>(span.Slice(1, sizeof(int)));
        return new OneEvent(value);
    }

    public Memory<byte> Serialize(IOneEvent e, byte[] buffer, int position)
    {
        if (buffer == null) throw new ArgumentNullException(nameof(buffer));
        if ((uint)position > buffer.Length) throw new ArgumentOutOfRangeException(nameof(position));
        if (buffer.Length - position < 8) throw new ArgumentException("Not enough space in buffer.", nameof(buffer));
        if (e is not OneEvent one) throw new ArgumentException("Unsupported event type.", nameof(e));

        buffer[position] = Tag;
        var value = one.A;
        MemoryMarshal.Write(buffer.AsSpan(position + 1, sizeof(int)), ref value);
        buffer.AsSpan(position + 1 + sizeof(int), 8 - 1 - sizeof(int)).Clear();

        return buffer.AsMemory(position, 8);
    }
}

public sealed class event_stream_serializers
{
    private static async Task CatchUpAsync(EventStream<IOneEvent> stream)
    {
        while (await stream.FetchAsync())
        while (stream.TryGetNext() != null) { }
    }

    private static async Task<IReadOnlyList<IOneEvent>> ReadAllAsync(EventStream<IOneEvent> stream)
    {
        while (await stream.FetchAsync()) { }

        var list = new List<IOneEvent>();
        IOneEvent next;
        while ((next = stream.TryGetNext()) != null)
            list.Add(next);

        return list;
    }

    [Fact]
    public async Task multiple_events_with_one_serializer_roundtrip()
    {
        var driver = new MemoryStorageDriver();
        var serializer = new OneEventSerializer();

        var writeStream = new EventStream<IOneEvent>(driver, serializers: [serializer]);
        await writeStream.WriteAsync([new OneEvent(1), new OneEvent(2), new OneEvent(3)]);

        var readStream = new EventStream<IOneEvent>(driver, serializers: [serializer]);
        var events = await ReadAllAsync(readStream);

        Assert.Equal([1, 2, 3], events.Cast<OneEvent>().Select(e => e.A).ToArray());
    }

    [Fact]
    public async Task multiple_events_with_different_serializers_roundtrip()
    {
        var driver = new MemoryStorageDriver();

        var jsonStream = new EventStream<IOneEvent>(driver);
        await jsonStream.WriteAsync([new OneEvent(1), new OneEvent(2)]);

        var oneStream = new EventStream<IOneEvent>(driver, serializers: [new OneEventSerializer()]);
        await CatchUpAsync(oneStream);
        await oneStream.WriteAsync([new OneEvent(3), new OneEvent(4)]);

        var readStream = new EventStream<IOneEvent>(driver, serializers: [new OneEventSerializer()]);
        var events = await ReadAllAsync(readStream);

        Assert.Equal([1, 2, 3, 4], events.Cast<OneEvent>().Select(e => e.A).ToArray());
    }

    [Fact]
    public async Task read_without_one_event_serializer_throws()
    {
        var driver = new MemoryStorageDriver();

        var jsonStream = new EventStream<IOneEvent>(driver);
        await jsonStream.WriteAsync([new OneEvent(1), new OneEvent(2)]);

        var oneStream = new EventStream<IOneEvent>(driver, serializers: [new OneEventSerializer()]);
        await CatchUpAsync(oneStream);
        await oneStream.WriteAsync([new OneEvent(3), new OneEvent(4)]);

        var readStream = new EventStream<IOneEvent>(driver);
        while (await readStream.FetchAsync()) { }

        Assert.Equal(1, ((OneEvent)readStream.TryGetNext()).A);
        Assert.Equal(2, ((OneEvent)readStream.TryGetNext()).A);
        Assert.Throws<InvalidOperationException>(() => readStream.TryGetNext());
    }

    [Fact]
    public async Task read_without_explicit_json_serializer_works()
    {
        var driver = new MemoryStorageDriver();

        var jsonStream = new EventStream<IOneEvent>(driver);
        await jsonStream.WriteAsync([new OneEvent(1), new OneEvent(2)]);

        var oneStream = new EventStream<IOneEvent>(driver, serializers: [new OneEventSerializer()]);
        await CatchUpAsync(oneStream);
        await oneStream.WriteAsync([new OneEvent(3), new OneEvent(4)]);

        var readStream = new EventStream<IOneEvent>(driver, serializers: [new OneEventSerializer()]);
        var events = await ReadAllAsync(readStream);

        Assert.Equal([1, 2, 3, 4], events.Cast<OneEvent>().Select(e => e.A).ToArray());
    }

    [Fact]
    public async Task one_event_serializer_tag_is_written()
    {
        var driver = new MemoryStorageDriver();
        var serializer = new OneEventSerializer();

        var stream = new EventStream<IOneEvent>(driver, serializers: [serializer]);
        await stream.WriteAsync([new OneEvent(42)]);

        var read = await driver.ReadAsync(0, new byte[1024]);
        Assert.Single(read.Events);
        var tag = read.Events[0].Contents.Span[0];

        Assert.Equal(serializer.Tag, tag);
        Assert.NotEqual((byte)'{', tag);
    }
}
