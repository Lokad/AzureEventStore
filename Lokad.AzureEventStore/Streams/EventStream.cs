using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Lokad.AzureEventStore.Drivers;

namespace Lokad.AzureEventStore.Streams;

/// <summary> A stream of events of the specified type.  </summary>
/// <remarks>
/// <para>
/// This object does NOT support multi-threaded operations. 
/// </para>
/// <para>
/// To perform a transactional append (i.e. to GUARANTEE that all existing events
/// have been processed before appending a new one), follow these steps:
/// </para>
/// <para>
///   1- generate the new events from the current state
///   2- call WriteAsync() with the new events
///     -- if successful: you're done !
///   3- call FetchAsync() 
///     -- if it returned true:
///       3.a- call TryGetNext() until it returns null
///       3.b- use returned events to update current state
///       3.c- repeat step 3
///     -- if it returned false: go back to step 1
/// </para>
/// <para>
/// To further optimize this process, you may use <see cref="BackgroundFetchAsync"/>
/// in parallel with the calls to <see cref="TryGetNext"/>.    
/// </para>
/// </remarks>
public sealed class EventStream<TEvent> : IEventStream<TEvent> where TEvent : class
{
    /// <summary> Used for object-to-byte[] conversions. </summary>
    private readonly IReadOnlyList<IEventSerializer<TEvent>> _serializers;

    /// <summary> Where events are stored. </summary>
    internal readonly IStorageDriver Storage;

    /// <summary> Used for logging. </summary>
    private readonly ILogAdapter _log;

    /// <summary> Open an event stream, connecting to the specified store. </summary>
    public EventStream(
        StorageConfiguration storage,
        ILogAdapter log = null,
        IReadOnlyList<IEventSerializer<TEvent>> serializers = null)
        : this(storage.Connect(out var stateCache), log, stateCache, serializers)
    {
    }

    /// <summary> Open an event stream on the provided store. </summary>
    internal EventStream(
        IStorageDriver storage,
        ILogAdapter log = null,
        BlobContainerClient stateCache = null,
        IReadOnlyList<IEventSerializer<TEvent>> serializers = null)
    {
        Storage = storage;
        _log = log;
        StateCache = stateCache;
        _serializers = EnsureJsonSerializer(serializers);
    }

    /// <summary> Ensures that there's always a JSON serializer registered. </summary>
    private static IReadOnlyList<IEventSerializer<TEvent>> EnsureJsonSerializer(
        IReadOnlyList<IEventSerializer<TEvent>> serializers)
    {
        var list = serializers == null ? new List<IEventSerializer<TEvent>>() : new List<IEventSerializer<TEvent>>(serializers);

        if (list.Count == 0 || list.All(s => s.Tag != (byte)'{'))
            list.Add(new JsonEventSerializer<TEvent>());

        return list;
    }

    /// <summary> The position up to which *remote* reading has progressed so far. </summary>
    /// <remarks> 
    /// This may or may not be the last position in the stream. 
    /// This position represents *remote* reading, so elements in <see cref="_cache"/> count as
    /// having been read.
    /// </remarks>
    internal long Position { get; private set; }

    /// <summary> Heuristic value: the write position is known to be greater than this. </summary>
    private long _minimumWritePosition;

    /// <summary>
    /// <see cref="IEventStream.Sequence"/>
    /// </summary>
    public uint Sequence { get; private set; }

    /// <summary>
    /// Equal to the sequence number of the last element in <see cref="_cache"/>, equal to
    /// <see cref="Sequence"/> if no cached elements.
    /// </summary>
    private uint _lastSequence;

    /// <summary>
    /// Initial remote sequence number fetched during stream initialization.
    /// </summary>
    private uint _initialRemoteSequence;

    /// <summary>
    /// The maximum of the initial remote sequence and the last known sequence.
    /// </summary>
    public uint RemoteSequence => Math.Max(_initialRemoteSequence, _lastSequence);

    internal async Task InitializeInitialRemoteSequenceAsync(CancellationToken cancel = default)
    {
        try
        {
            _initialRemoteSequence = await Storage.GetLastKeyAsync(cancel).ConfigureAwait(false);
        }
        catch
        {
            // Ignore failures; RemoteSequence will fall back to _lastSequence.
        }
    }

    /// <summary>
    /// Events that have been received from the remote position, but not returned by 
    /// <see cref="TryGetNext"/> yet.
    /// </summary>
    private readonly Queue<RawEvent> _cache = new();

    /// <summary>
    ///     The size of the buffers used by the event stream; 4MiB is the expected
    ///     majority case, and individual events are limited to 512KiB each.
    /// </summary>
    private const int MaxBytes = 1024 * 1024 * 4;

    /// <summary>
    ///     The array that contains the data inside the raw events in <see cref="_cache"/>.
    /// </summary>
    private byte[] _activeBacking = new byte[MaxBytes];

    /// <summary>
    ///     The array that is currently used by <see cref="BackgroundFetchAsync(CancellationToken)"/>
    ///     to retrieve event data.
    /// </summary>
    private byte[] _fetchBacking = new byte[MaxBytes];

    /// <summary>
    /// <see cref="IEventStream{TEvent}.WriteAsync"/>
    /// </summary>
    public async Task<uint?> WriteAsync(IReadOnlyList<TEvent> events, CancellationToken cancel = default)
    {
        if (events.Count == 0) return null;
        if (Position < _minimumWritePosition) return null;

        if (events.Any(e => e == null))
            throw new ArgumentException(@"No null events allowed", nameof(events));

        var sw = Stopwatch.StartNew();

        var pooledBuffer = ArrayPool<byte>.Shared.Rent(MaxBytes);
        var buffer = pooledBuffer;
        var position = 0;
        var rawEvents = new RawEvent[events.Count];

        for (var i = 0; i < events.Count; i++)
        {
            // Most of the time, we have few events and they are small enough, so the 
            // pooledBuffer is large enough to hold them all. But for a case where writing
            // multiple megabytes at the same time, use additional non-pooled arrays for
            // storing the event data.
            if (buffer.Length - position < RawEvent.MaxEventSize)
            {
                buffer = new byte[MaxBytes];
                position = 0;
            }

            var data = _serializers[0].Serialize(events[i], buffer, position);
            position += data.Length;

            rawEvents[i] = new RawEvent(_lastSequence + (uint)(i + 1), data);
        }

        try
        {
            var result = await Storage.WriteAsync(Position, rawEvents, cancel);

            _minimumWritePosition = result.NextPosition;

            if (result.Success)
            {
                foreach (var e in rawEvents) _cache.Enqueue(e);
                Position = result.NextPosition;

                var first = _lastSequence + 1;

                _lastSequence += (uint) rawEvents.Length;

                _log?.Debug(
                    $"Wrote {rawEvents.Length} events up to seq {_lastSequence} in {sw.Elapsed.TotalSeconds:F3}s.");

                return first;
            }

            _log?.Debug($"Collision when writing {rawEvents.Length} events after {sw.Elapsed.TotalSeconds:F3}s.");

            return null;
        }
        catch (Exception e)
        {
            _log?.Error($"When writing {rawEvents.Length} events after seq {_lastSequence}.", e);
            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(pooledBuffer);
        }
    }

    /// <summary> High-performance read from the stream. </summary>
    /// <see cref="IEventStream{TEvent}.TryGetNext"/>
    public TEvent TryGetNext()
    {
        if (_cache.Count == 0)
            return null;

        var next = _cache.Dequeue();
        Sequence = next.Sequence;
            
        if (next.Contents.Length == 0)
            throw new InvalidOperationException("Event contents are empty.");

        var tag = next.Contents.Span[0];
        var serializer = _serializers.FirstOrDefault(s => s.Tag == tag);

        if (serializer == null)
            throw new InvalidOperationException($"No serializer registered for tag 0x{tag:X2}.");

        return serializer.Deserialize(next.Contents);
    }

    public async Task<Func<bool>> BackgroundFetchAsync(CancellationToken cancel = default)
    {
        var read = await Storage.ReadAsync(Position, _fetchBacking, cancel);

        if (read.Events.Count == 0)
            return () => false;
            
        return () =>
        {
            if (_cache.Count > 0)
                return true;
                
            Position = read.NextPosition;

            foreach (var e in read.Events)
            {
                _cache.Enqueue(e);
                _lastSequence = e.Sequence;
            }

            (_fetchBacking, _activeBacking) = (_activeBacking, _fetchBacking);

            return true;
        };
    }

    /// <see cref="IEventStream.DiscardUpTo"/>
    public async Task<uint> DiscardUpTo(uint sequence, CancellationToken cancel = default)
    {
        using var act = Logging.Stream.StartActivity("EventStream.DiscardUpTo");
        act?.AddTag("seq.stream", Sequence)
            .AddTag("seq.to", sequence);

        // First, try to seek forward to skip over many events at once
        var skip = await Storage.SeekAsync(sequence, 0, cancel);
        if (skip > Position) Position = skip;

        // Drop elements from _cache that are before the requested sequence number
        while (_cache.Count > 0 && _cache.Peek().Sequence < sequence)
        {
            var dequeued = _cache.Dequeue();
            Sequence = dequeued.Sequence;
        }

        // Continues until _cache contains at least one event past the target
        // sequence number (or no events left)
        while (_lastSequence < sequence)
        {
            Sequence = _lastSequence;

            var read = await Storage.ReadAsync(Position, _fetchBacking, cancel);

            // Reached end of stream.
            if (read.Events.Count == 0)
            {
                _cache.Clear();
                Sequence = _lastSequence;
                return Sequence;
            }

            Position = read.NextPosition;

            // Moved past target sequence: append events to _cache
            var lastReadEvent = read.Events[^1];
            _lastSequence = lastReadEvent.Sequence;

            if (_lastSequence >= sequence)
            {
                foreach (var e in read.Events)
                {
                    if (e.Sequence >= sequence)
                    {
                        _cache.Enqueue(e);
                    }
                    else
                    {
                        Sequence = e.Sequence;
                    }
                }
            }

            (_fetchBacking, _activeBacking) = (_activeBacking, _fetchBacking);
        }

        return Sequence;
    }

    /// <summary><see cref="IEventStream.Reset"/></summary>
    public void Reset()
    {
        _cache.Clear();
        Sequence = 0;
        _lastSequence = 0;
        Position = 0;
            
        // Do NOT reset ! The value is still correct.
        // _minimumWritePosition = 0;
    }

    /// <summary> <see cref="IEventStream.StateCache"/> </summary>
    public BlobContainerClient StateCache { get; }
}
