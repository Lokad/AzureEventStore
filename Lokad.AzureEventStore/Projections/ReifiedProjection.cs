using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Streams;

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

        /// <summary> Settings used to create the state. </summary>
        private readonly StateCreationContext _stateCreationContext;

        /// <summary> Disposable handling the loaded external state. Null otherwise. </summary>
        private IDisposable _disposable { get; set; }

        public ReifiedProjection(
            IProjection<TEvent, TState> projection, 
            StorageProvider storageProvider, 
            IProjectionCacheProvider cacheProvider = null, 
            ILogAdapter log = null)
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
            _stateCreationContext = storageProvider.GetStateCreationContext(Name);
             _possiblyInconsistent = false;
             Sequence = 0U;
            _disposable = null;
        }

        /// <summary>
        /// Called to initialize the projection state.
        /// Attempt to load an external state by calling 
        /// <see cref="IProjection{TEvent, TState}.TryRestoreAsync(StateCreationContext, CancellationToken)"/>.
        /// If no state was restored, then try to load one 
        /// from a cache provider by calling <see cref="IProjection{TEvent, TState}.TryLoadAsync(Stream, CancellationToken)"/>.
        /// If no state was restored, initialize with the initial state.
        /// </summary>
        public async Task CreateAsync(CancellationToken cancel = default)
        {
            var restoredState = await _projection.TryRestoreAsync(_stateCreationContext, cancel);
            if (restoredState != null)
            {
                Sequence = restoredState.Sequence;
                Current = restoredState.State;
                _disposable = restoredState.Disposable;
                return;
            }

            if (!(await TryLoadAsync(cancel)))
                Reset();
        }

        /// <summary> Reset the projection to its initial state and sequence number <c>0</c>. </summary>
        public void Reset()
        {
            if (_disposable != null)
            {
                _disposable.Dispose();
                _disposable = null;
            }
            
            Sequence = 0U;
            Current = _projection.Initial(_stateCreationContext);
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

        /// <summary> Apply the candidate events without invalidation. </summary>
        /// <remarks> The sequence number must be greater than <see cref="Sequence"/>. 
        /// Projection is not changed. </remarks>
        public void TryApply(uint seq, IReadOnlyList<TEvent> events)
        {
            TState newState = Current;
            try
            {
                foreach (TEvent e in events)
                {
                    seq++;
                    newState = _projection.Apply(seq, e, newState);
                    if (newState == null)
                        throw new InvalidOperationException("Event generated a null state.");
                }
            }
            catch (Exception ex)
            {
                ex.Data.Add("TryApplyMessage", "An error occured while trying to apply a projection.");
                _log?.Warning("[{Name}] error on event candidate", ex);
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
        /// <returns> 
        /// True if loading was successful, false if it failed.
        /// </returns>
        public async Task<bool> TryLoadAsync(CancellationToken cancel = default)
        {
            if (_cacheProvider == null)
            {
                _log?.Warning($"[{Name}] no read cache provider !");
                return false;
            }

            var sw = Stopwatch.StartNew();

            IEnumerable<Task<CacheCandidate>> candidates;
            try
            {
                candidates = await _cacheProvider.OpenReadAsync(Name);
            }
            catch (Exception ex)
            {
                _log?.Warning($"[{Name}] error when opening cache.", ex);
                return false;
            }

            foreach (var candidateTask in candidates)
            {
                CacheCandidate candidate;
                try
                {
                    candidate = await candidateTask;
                }
                catch (Exception ex)
                {
                    _log?.Warning($"[{Name}] error when opening cache.", ex);
                    continue;
                }

                _log?.Info($"[{Name}] reading cache {candidate.Name}");

                var stream = candidate.Contents;
                try
                {

                    // Load the sequence number from the input
                    uint seq;
                    using (var br = new BinaryReader(stream, Encoding.UTF8, true))
                        seq = br.ReadUInt32();

                    _log?.Debug($"[{Name}] cache is at seq {seq}.");

                    // Create a new stream to hide the write of the sequence numbers
                    // (at the top and the bottom of the stream).
                    var boundedStream = new BoundedStream(stream, stream.Length - 8);

                    // Load the state, which advances the stream
                    var state = await _projection.TryLoadAsync(boundedStream, cancel)
                        .ConfigureAwait(false);

                    if (state == null)
                    {
                        _log?.Warning($"[{Name}] projection could not parse cache {candidate.Name}");
                        continue;
                    }

                    // Sanity check: is the same sequence number found at the end ? 
                    uint endseq;
                    using (var br = new BinaryReader(stream, Encoding.UTF8, true))
                        endseq = br.ReadUInt32();

                    if (endseq != seq)
                    {
                        _log?.Warning($"[{Name}] sanity-check seq is {endseq} in cache {candidate.Name}");
                        continue;
                    }

                    _log?.Info($"[{Name}] loaded {stream.Length} bytes in {sw.Elapsed:mm':'ss'.'fff} from cache {candidate.Name}");

                    Current = state;
                    Sequence = seq;
                    return true;

                    // Do NOT set _possiblyInconsistent to false here ! 
                    // Inconsistency can have external causes, e.g. event read
                    // failure, that are not automagically solved by loading from cache.
                }
                catch (EndOfStreamException)
                {
                    _log?.Warning($"[{Name}] incomplete cache {candidate.Name}");
                    // Incomplete streams are simply treated as missing
                }
                catch (Exception ex)
                {
                    _log?.Warning($"[{Name}] could not parse cache {candidate.Name}", ex);
                    // If a cache file cannot be parsed, try the next one
                }
                finally
                {
                    stream.Dispose();
                }
            }
            return false;
        }

        /// <summary> Attempt to save this projection to the destination stream. </summary>
        /// <remarks>
        ///     The returned task does not access the projection in any way, so the 
        ///     projection may be safely accessed before the task has finished executing.
        /// </remarks>
        /// <returns>
        ///     True if saving was successful, false if it failed.
        /// </returns>
        public async Task<bool> TrySaveAsync(CancellationToken cancel = default)
        {
            if (_possiblyInconsistent)
            {
                _log?.Warning($"[{Name}] state is possibly inconsistent, not saving.");
                return false;
            }

            if (_cacheProvider == null)
            {
                _log?.Warning($"[{Name}] no write cache provider !");
                return false;
            }

            var sequence = Sequence;
            var current = Current;

            _log?.Debug($"[{Name}] saving to seq {sequence}.");

            var sw = Stopwatch.StartNew();

            var wrote = 0L;
            try
            {
                await _cacheProvider.TryWriteAsync(Name, async destination =>
                {
                    try
                    {
                        using (destination)
                        {
                            using (var wr = new BinaryWriter(destination, Encoding.UTF8, true))
                                wr.Write(sequence);

                            if (!await _projection.TrySaveAsync(destination, current, cancel))
                            {
                                _log?.Warning($"[{Name}] projection failed to save.");
                                throw new Exception("INTERNAL.DO.NOT.SAVE");
                            }

                            if (!destination.CanWrite)
                                throw new Exception("Projection saving closed the stream ! ");

                            using (var wr = new BinaryWriter(destination, Encoding.UTF8, true))
                                wr.Write(sequence);

                            wrote = destination.Position;
                        }
                    }
                    catch (Exception e) when (e.Message == "INTERNAL.DO.NOT.SAVE") { throw; }
                    catch (Exception e)
                    {
                        _log?.Warning($"[{Name}] while saving to cache.", e);
                        throw new Exception("INTERNAL.DO.NOT.SAVE", e);
                    }

                }).ConfigureAwait(false);

                if (wrote == 0)
                {
                    _log?.Warning($"[{Name}] caching is disabled for this projection.");
                }
                else
                {
                    _log?.Info($"[{Name}] saved {wrote} bytes to cache in {sw.Elapsed:mm':'ss'.'fff}.");
                }

                return true;
            }
            catch (Exception e) when (e.Message == "INTERNAL.DO.NOT.SAVE") 
            {
                // The inner function asked us not to save, and already logged the reason.
                // But if an inner exception is included, throw it (preserving the existing
                // stack trace).
                if (e.InnerException != null)
                    ExceptionDispatchInfo.Capture(e.InnerException).Throw();

                return false;
            }
            catch (Exception ex)
            {
                _log?.Warning($"[{Name}] when opening write cache.", ex);
                return false;
            }
        }

        private ReifiedProjection(ReifiedProjection<TEvent, TState> clone)
        {
            _projection = clone._projection;
            _cacheProvider = clone._cacheProvider;
            Current = clone.Current;
            Sequence = clone.Sequence;
            Name = clone.Name;
            _log = clone._log;
            _possiblyInconsistent = clone._possiblyInconsistent;
            _stateCreationContext = clone._stateCreationContext;
        }

        /// <inheritdoc/>
        IReifiedProjection<TEvent, TState> IReifiedProjection<TEvent, TState>.Clone() =>
            new ReifiedProjection<TEvent, TState>(this);

        /// <inheritdoc/>
        IReifiedProjection<TEvent> IReifiedProjection<TEvent>.Clone() =>
            new ReifiedProjection<TEvent, TState>(this);


        /// <summary>
        /// Marks ‘state’ as being the latest in the sequence of states produced by applying events persisted in the stream 
        /// (as opposed to tentative state instances that are produced by applying tentative events that will not be persisted). 
        /// This gives the projection the liberty to perform any operations related to the persistence of the state, 
        /// such as flushing parts of it to a memory-mapped file.
        /// </summary>
        public async Task CommitAsync(uint sequence, CancellationToken cancel = default)
        {
            await _projection.CommitAsync(Current, sequence, cancel);
        }

        /// <summary>
        ///     Provides the projection with an opportunity to perform upkeep operations on 
        ///     the state (such as compacting the memory representation, or flushing to disk).
        ///     This function is only called during the initial stream catch-up phase, so it
        ///     is guaranteed that no other thread is currently accessing the state, a 
        ///     sub-element of the state, or any sub-element of any ancestor state that has been
        ///     returned by this projection (meaning that it is safe to make that data
        ///     unavailable for the entire duration of the upkeep).
        /// </summary>
        /// <remarks>
        ///     This function is called at least once during the stream catch-up phase, but 
        ///     maybe called several times depending on unspecified factors, such as the
        ///     number of processed events. 
        /// </remarks>
        public async Task UpkeepAsync(CancellationToken cancel = default)
        {
            var candidate = await _projection.UpkeepAsync(Current, cancel);
            if (candidate != null)
                Current = candidate;
        }
    }
}
