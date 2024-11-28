#nullable enable
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Projections
{
    /// <summary> Projects events onto an immutable state. </summary>
    /// <typeparam name="TEvent"> The type of events to be projected. </typeparam>
    /// <see cref="IProjection{T,T}"/>
    // ReSharper disable once UnusedTypeParameter
    public interface IProjection<in TEvent>
    {
        /// <summary> The complete name of this projection. </summary>
        /// <example>main-3</example>
        /// <remarks> 
        /// Human-readable, should include version information. Used to
        /// read/write the state and to access projection data from the
        /// projection host.
        /// 
        /// Regexp: [-a-zA-Z0-9_]{1,16}
        /// 
        /// Obviously, multiple accesses to this property should always return
        /// the same value.
        /// </remarks>
        string FullName { get; }

        /// <summary> The type of the state. </summary>
        Type State { get; }

        /// <summary> Indicates if the projection needs a <see cref="IProjectionFolderProvider"/>. </summary>
        bool NeedsMemoryMappedFolder { get; }
    }

    /// <summary> Projects events onto an immutable state. </summary>
    /// <typeparam name="TEvent"> The type of events to be projected. </typeparam>
    /// <typeparam name="TState"> 
    /// The type of the state. Should be immutable reference type.
    /// <c>null</c> is never a valid state. 
    /// </typeparam>
    public interface IProjection<in TEvent,TState> : IProjection<TEvent>
    {
        /// <summary> The state before any events are applied to it. </summary>
        TState Initial(StateCreationContext stateCreationContext);

        /// <summary>
        /// Applies the specified event (with the provided sequence number) to
        /// the previous state, returning a modified state.
        /// </summary>
        TState Apply(uint sequence, TEvent e, TState previous);

        /// <summary> Attempt to load state from a source stream. </summary>
        /// <returns> null if loading was unsuccessful. </returns>
        Task<TState?> TryLoadAsync(Stream source, CancellationToken cancel);

        /// <summary> Attempt to load an external state with <paramref name="stateCreationContext"/>. </summary>
        /// <returns> null if loading was unsuccessful. </returns>
        Task<RestoredState<TState>?> TryRestoreAsync(StateCreationContext stateCreationContext, CancellationToken cancel = default);

        /// <summary> Attempt to save state to a destination stream. </summary>
        /// <returns> true if saving was successful. </returns>
        Task<bool> TrySaveAsync(Stream destination, TState state, CancellationToken cancel);

        /// <summary>
        /// Marks ‘state’ as being the latest in the sequence of states produced by applying events persisted in the stream 
        /// (as opposed to tentative state instances that are produced by applying tentative events that will not be persisted). 
        /// This gives the projection the liberty to perform any operations related to the persistence of the state, 
        /// such as flushing parts of it to an external state that may be loaded later.
        /// </summary>
        Task CommitAsync(TState state, uint sequence, CancellationToken cancel = default);

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
        Task<TState> UpkeepAsync(StateUpkeepContext stateUpkeepContext, TState state, CancellationToken cancel = default);
    }

    /// <summary>
    /// Loaded external state with <see cref="IProjection{TEvent, TState}.TryRestoreAsync(StateCreationContext, CancellationToken)"/>
    /// </summary>
    public class RestoredState<TState>
    {
        /// <summary> Last saved sequence. </summary>
        public uint Sequence;
        
        /// <summary> Last saved state. </summary>
        public TState State;

        /// <summary> 
        /// Disposable handling the loaded external state.
        /// If provided, this disposable must be disposed before
        /// requesting a new state from either <see cref="IProjection{TEvent, TState}.Initial(StateCreationContext)"/>,
        /// <see cref="IProjection{TEvent, TState}.TryLoadAsync(Stream, CancellationToken)"/>,
        /// and <see cref="IProjection{TEvent, TState}.TryRestoreAsync(StateCreationContext, CancellationToken)"/>.
        /// </summary>
        public IDisposable? Disposable;

        public RestoredState(uint sequence, TState state, IDisposable? disposable = null)
        {
            Sequence = sequence;
            State = state;
            Disposable = disposable;
        }
    }  
}
