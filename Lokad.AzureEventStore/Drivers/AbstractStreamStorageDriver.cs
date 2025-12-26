using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers;

/// <summary> A simple, inefficient, persistent stream-based storage driver. </summary>
/// <remarks> Intended for use during unit testing, or local development. </remarks>
internal abstract class AbstractStreamStorageDriver(Stream stream) : IStorageDriver, IDisposable
{
    /// <see cref="GetPositionAsync"/>
    public long GetPosition() => stream.Length;

    public Task<long> GetPositionAsync(CancellationToken cancel = new()) =>
        Task.FromResult(GetPosition());

    public Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events,
        CancellationToken cancel = new())
        => Task.FromResult(Write(position, events));

    public DriverWriteResult Write(long position, IEnumerable<RawEvent> events)
    {
        var written = false;

        if (stream.Length == position)
        {
            written = true;
            stream.Seek(0, SeekOrigin.End);

            var payload = ArrayPool<byte>.Shared.Rent(4 * 1024 * 1024);
            var payloadLength = 0;
            try
            {
                foreach (var e in events)
                {
                    payloadLength += EventFormat.Write(payload.AsMemory(payloadLength), e);
                }

                stream.Write(payload, 0, payloadLength);
                stream.Flush();
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(payload);
            }
        }

        return new DriverWriteResult(stream.Length, written);
    }

    public Task<DriverReadResult> ReadAsync(long position, Memory<byte> backing, CancellationToken cancel = default)
        => Task.FromResult(Read(position, backing));

    public DriverReadResult Read(long position, Memory<byte> backing)
    {
        var events = new List<RawEvent>();

        if (position >= stream.Length)
            return new DriverReadResult(stream.Length, events);

        stream.Seek(position, SeekOrigin.Begin);

        var availableBytes = stream.Length - position;

        // Fill backing from _file
        var backingBytes = (int)Math.Min(backing.Length, availableBytes);
        for (var readToBuffer = 0; readToBuffer < backingBytes; )
        {
            var read = stream.Read(backing.Span[readToBuffer..]);
            if (read == 0) throw new InvalidDataException("Unexpected end-of-file");
            readToBuffer += read;
        }

        // Read as many events as possible from buffer
        backing = backing[..backingBytes];
        var parsedBytes = 0;
        while (EventFormat.TryParse(backing[parsedBytes..], out var evtSize) is { } ev)
        {
            parsedBytes += evtSize;
            events.Add(ev);
        }

        return new DriverReadResult(position + parsedBytes, events);

    }
        
    public async Task<uint> GetLastKeyAsync(CancellationToken cancel = new()) => 
        await EventFormat.GetLastSequenceAsync(stream, cancel);

    public Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = new()) => 
        Task.FromResult(position);

    public void Dispose()
    {
        stream.Dispose();
    }
}