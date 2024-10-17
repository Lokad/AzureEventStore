using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Lokad.AzureEventStore.Exceptions;

namespace Lokad.AzureEventStore.Drivers;

/// <summary> A storage driver that writes to a single append blob. </summary>
/// <remarks>
///     After 50000 writes, the blob becomes full and will not support any more
///     writes. At that point, additional writes throw <see cref="MonoBlobFullException"/>
/// </remarks>
internal sealed class MonoBlobStorageDriver : IStorageDriver
{
    /// <summary> The append blob itself. </summary>
    private readonly AppendBlobClient _blob;

    /// <summary> The last known position of the blob (i.e. the blob length) </summary>
    private long _lastKnownPosition;

    public MonoBlobStorageDriver(AppendBlobClient blob)
    {
        _blob = blob;
    }

    /// <summary> Reads the blob size into <see cref="_lastKnownPosition"/> and returns it. </summary>
    private async Task<long> RefreshLastKnownPosition(CancellationToken cancel)
    {
        try
        {
            var props = await _blob.GetPropertiesAsync(cancellationToken: cancel);
            return _lastKnownPosition = props.Value.ContentLength;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return _lastKnownPosition = 0;
        }
    }

    /// <inheritdoc/>
    public async Task<uint> GetLastKeyAsync(CancellationToken cancel = default)
    {
        var size = await RefreshLastKnownPosition(cancel);
        if (size == 0) return 0;

        // Download one event's worth of data to a local buffer.
        // This will likely truncate an event somewhere at the beginning of the
        // buffer, but we don't care, because looking for the *last* sequence only
        // requires that the last event in the buffer isn't truncated.
        var length = Math.Min(EventFormat.MaxEventFootprint, size);

        var content = await _blob.DownloadContentAsync(
            range: new HttpRange(size - length, length),
            cancellationToken: cancel);

        var bytes = content.Value.Content.ToArray();
        using (var stream = new MemoryStream(bytes))
            return await EventFormat.GetLastSequenceAsync(stream, cancel);
    }

    /// <inheritdoc/>
    public Task<long> GetPositionAsync(CancellationToken cancel = default) =>
        RefreshLastKnownPosition(cancel);

    /// <inheritdoc/>
    public async Task<DriverReadResult> ReadAsync(
        long position,
        Memory<byte> backing,
        CancellationToken cancel = default)
    {
        using var act = Logging.Drivers.StartActivity("MonoBlobStorage.ReadAsync");
        act?.SetTag(Logging.PositionRead, position)
            .SetTag(Logging.PositionStream, _lastKnownPosition);

        ReadOnlyMemory<byte> memory;
        try
        {
            var content = await _blob.DownloadContentAsync(
                range: new HttpRange(position, backing.Length),
                cancellationToken: cancel);

            memory = content.Value.Content.ToMemory();
        }
        catch (RequestFailedException ex) when (ex.Status == 416 || ex.Status == 404)
        {
            // 404: Blob does not exist yet
            // 416: Position was at or after the end of the blob
            memory = Array.Empty<byte>();
        }

        if (memory.Length == 0)
        {
            act?.SetTag(Logging.EventCount, 0)
                .SetTag(Logging.EventSize, 0);

            return new DriverReadResult(position, Array.Empty<RawEvent>());
        }

        var parsedBytes = 0;
        try
        {
            var events = new List<RawEvent>();

            while (EventFormat.TryParse(memory[parsedBytes..], out var parsed) is RawEvent re)
            {
                events.Add(re);
                parsedBytes += parsed;
            }

            act?.SetTag(Logging.EventCount, events.Count)
                .SetTag(Logging.EventSize, parsedBytes);

            return new DriverReadResult(position + parsedBytes, events);
        }
        catch (InvalidCastException e)
        {
            throw new InvalidDataException($"{e.Message} at {position + parsedBytes}");
        }
    }

    /// <inheritdoc/>
    public Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = default) =>
        // Can't be any more clever than this without loading+parsing the blob, which 
        // is what the EventStream will do anyway if we return 0.
        Task.FromResult(0L);

    public async Task<DriverWriteResult> WriteAsync(
        long position,
        IEnumerable<RawEvent> events,
        CancellationToken cancel = default)
    {
        using var act = Logging.Drivers.StartActivity("MonoBlobStorageDriver.WriteAsync");
        act?.SetTag(Logging.PositionWrite, position)
            .SetTag(Logging.PositionStream, _lastKnownPosition);

        if (position > _lastKnownPosition)
        {
            await RefreshLastKnownPosition(cancel);
            act?.SetTag(Logging.PositionStream, _lastKnownPosition);
        }

        if (position < _lastKnownPosition)
        {
            act?.SetStatus(ActivityStatusCode.Error, "Conflict");
            return new DriverWriteResult(_lastKnownPosition, false);
        }

        if (_lastKnownPosition == 0)
            // Our way of detecting collisions when appending requires the blob to
            // already exist.
            await _blob.CreateIfNotExistsAsync(new AppendBlobCreateOptions(), cancel);


        var payload = ArrayPool<byte>.Shared.Rent(4 * 1024 * 1024);
        var payloadLength = 0;
        var eventCount = 0;
        try
        {
            foreach (var e in events)
            {
                eventCount++;
                payloadLength += EventFormat.Write(payload.AsMemory(payloadLength), e);
            }

            act?.SetTag(Logging.EventCount, eventCount)
                .SetTag(Logging.EventSize, payloadLength);

            // Nothing to write, but still check position
            if (payloadLength == 0)
            {
                act?.SetStatus(ActivityStatusCode.Ok);
                await RefreshLastKnownPosition(cancel);
                return new DriverWriteResult(_lastKnownPosition, position == _lastKnownPosition);
            }

            // Attempt write to last blob.
            try
            {
                await _blob.AppendTransactionalAsync(payload, payloadLength, position, cancel);

                _lastKnownPosition = position + payloadLength;

                act?.SetStatus(ActivityStatusCode.Ok);
                return new DriverWriteResult(_lastKnownPosition, true);
            }
            catch (RequestFailedException e)
            {
                if (e.IsMaxReached())
                {
                    act?.SetStatus(ActivityStatusCode.Error, "MonoBlobFull");
                    throw new MonoBlobFullException(_blob.Name);
                }

                if (!e.IsCollision()) throw;

                // Collision means we do not have the proper _lastKnownPosition, so refresh
                // the cache and return a failure.
                await RefreshLastKnownPosition(cancel);

                act?.SetTag(Logging.PositionStream, _lastKnownPosition)
                    .SetStatus(ActivityStatusCode.Error, "Collision");

                return new DriverWriteResult(_lastKnownPosition, false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(payload);
        }
    }
}
