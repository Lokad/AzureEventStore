using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Projections
{
    /// <summary> Keeps track of the state updated by a <see cref="IProjection{T,T}"/>. </summary>
    /// <remarks> This class does not support multi-threaded access. </remarks>
    internal sealed class ReifiedProjection<TEvent, TState> : IReifiedProjection<TEvent, TState> where TState : class
    {
        private readonly IProjection<TEvent, TState> _projection;
        
        private readonly IProjectionCacheProvider _cacheProvider;

        /// <summary> Becomes true if the state is possibly inconsistent. </summary>
        /// <remarks> Inconsistent state can be due to errors when parsing or applying events. </remarks>
        private bool _possiblyInconsistent;

        /// <summary> The current state of the projection. </summary>
        public TState Current { get; private set; }

        /// <summary> The sequence number of the last event processed by this projection. </summary>
        public uint Sequence { get; private set; }

        /// <see cref="IReifiedProjection{T}.Name"/>
        public string Name { get; }

        /// <summary> For logging. </summary>
        private readonly ILogAdapter _log;

        public ReifiedProjection(IProjection<TEvent, TState> projection, IProjectionCacheProvider cacheProvider = null, ILogAdapter log = null)
        {
            if (projection == null)
                throw new ArgumentNullException(nameof(projection));

            // Cache the projection's full name. This shields us against projection authors
            // writing changing names.
            Name = projection.FullName;

            if (Name == null)
                throw new ArgumentException("Projection must have a name", nameof(projection));
            
            var nameRegexp = new Regex("^[-a-zA-Z0-9_]{1,16}$");
            if (!nameRegexp.IsMatch(Name))    
                throw new ArgumentException("Projection name must match [-a-zA-Z0-9_]{1,16}", nameof(projection));

            _projection = projection;
            
            _cacheProvider = cacheProvider;
            _log = log;

            _log?.Debug("Using projection: " + Name);

            Reset();            
        }

        /// <summary> Reset the projection to its initial state and sequence number <c>0</c>. </summary>
        public void Reset()
        {
            Sequence = 0U;
            Current = _projection.Initial;
            _possiblyInconsistent = false;

            if (Current == null)
                throw new InvalidOperationException("Projection initial state must not be null");
        }

        /// <summary>
        /// Notify the projection that the state may be inconsistent, due to 
        /// an event that could not be read or parsed.
        /// </summary>
        public void SetPossiblyInconsistent()
        {
            _possiblyInconsistent = true;
        }

        /// <summary> Apply the specified event to the state. </summary>
        /// <remarks> The sequence number must be greater than <see cref="Sequence"/>. </remarks>
        public void Apply(uint seq, TEvent e)
        {
            if (seq <= Sequence) 
                throw new ArgumentException($"Event seq {seq} applied after seq {Sequence}", nameof(seq));
            
            // Always update sequence, even if update fails.
            Sequence = seq;

            try
            {
                var newState = _projection.Apply(seq, e, Current);
                if (newState == null)
                    throw new InvalidOperationException("Event generated a null state.");

                Current = newState;
            }
            catch (Exception ex)
            {
                _log?.Warning($"[{Name}] error at seq {seq}", ex);

                _possiblyInconsistent = true;
                throw;
            }
        }

        /// <summary>
        /// Attempt to load this projection from the source, updating its
        /// <see cref="Current"/> and <see cref="Sequence"/>.
        /// </summary>
        /// <remarks> 
        /// Object is unchanged if loading fails. 
        /// 
        /// Obviously, as this object does not support multi-threaded access,
        /// it should NOT be accessed in any way before the task has completed.
        /// </remarks>
        public async Task TryLoadAsync(CancellationToken cancel = default(CancellationToken))
        {
            if (_cacheProvider == null)
            {
                _log?.Warning($"[{Name}] no read cache provider !");
                return;
            }

            Stream source;

            var sw = Stopwatch.StartNew();

            try
            {
                source = await _cacheProvider.OpenReadAsync(Name);
            }
            catch (Exception ex)
            {
                _log?.Warning($"[{Name}] error when opening cache.", ex);
                return;
            }

            if (source == null)
            {
                _log?.Info($"[{Name}] no cached data found.");
                return;
            }

            try
            {
                // Load the sequence number from the input
                uint seq;
                using (var br = new BinaryReader(source, Encoding.UTF8, true))
                    seq = br.ReadUInt32();

                _log?.Debug($"[{Name}] cache is at seq {seq}.");

                // Load the state, which advances the stream
                var state = await _projection.TryLoadAsync(source, cancel).ConfigureAwait(false);

                if (state == null)
                {
                    _log?.Warning($"[{Name}] projection could not parse cache.");
                    return;
                }

                // Sanity check: is the same sequence number found at the end ? 
                uint endseq;
                using (var br = new BinaryReader(source, Encoding.UTF8, true))
                    endseq = br.ReadUInt32();

                if (endseq != seq)
                {
                    _log?.Warning($"[{Name}] sanity-check seq is {endseq}.");
                    return;
                }

                _log?.Info($"[{Name}] loaded from cache in {sw.Elapsed:mm':'ss'.'fff}.");

                Current = state;
                Sequence = seq;

                // Do NOT set _possiblyInconsistent to false here ! 
                // Inconsistency can have external causes, e.g. event read
                // failure, that are not automagically solved by loading from cache.
            }
            catch (EndOfStreamException)
            {
                _log?.Warning($"[{Name}] cache is incomplete.");
                // Incomplete streams are simply treated as missing
            }
            catch (Exception ex)
            {
                _log?.Warning($"[{Name}] could not parse cache.", ex);
                throw;
            }
        }

        /// <summary> Attempt to save this projection to the destination stream. </summary>
        /// <remarks>
        /// The returned task does not access the object in any way, so the object
        /// may be safely accessed before the task has finished executing.
        /// </remarks>
        public async Task TrySaveAsync(CancellationToken cancel = default(CancellationToken))
        {
            if (_possiblyInconsistent)
            {
                _log?.Warning($"[{Name}] state is possibly inconsistent, not saving.");
                return;
            }

            if (_cacheProvider == null)
            {
                _log?.Warning($"[{Name}] no write cache provider !");
                return;
            }

            var sequence = Sequence;
            var current = Current;

            _log?.Debug($"[{Name}] saving to seq {sequence}.");

            Stream destination;
            var sw = Stopwatch.StartNew();

            try
            {
                destination = await _cacheProvider.OpenWriteAsync(Name);
            }
            catch (Exception ex)
            {
                _log?.Warning($"[{Name}] when opening write cache.", ex);
                throw;
            }

            if (destination == null)
            {
                _log?.Warning($"[{Name}] caching is disabled for this projection.");
                return;
            }

            try
            {
                using (destination)
                {
                    using (var wr = new BinaryWriter(destination, Encoding.UTF8, true))
                        wr.Write(sequence);

                    if (!await _projection.TrySaveAsync(destination, current, cancel))
                    {
                        _log?.Warning($"[{Name}] projection failed to save.");
                        return;
                    }

                    if (!destination.CanWrite)
                        throw new Exception("Projection saving closed the stream ! ");

                    using (var wr = new BinaryWriter(destination, Encoding.UTF8, true))
                        wr.Write(sequence);
                }
            }
            catch (Exception e)
            {
                _log?.Warning($"[{Name}] while saving to cache.", e);
                throw;
            }

            _log?.Info($"[{Name}] saved in cache in {sw.Elapsed:mm':'ss'.'fff}.");
        }
    }
}
