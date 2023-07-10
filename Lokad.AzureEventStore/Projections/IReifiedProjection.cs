using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Lokad.AzureEventStore.Projections
{
    /// <summary>
    /// A <see cref="IReifiedProjection{T}"/> with the type of the events
    /// abstracted away
    /// </summary>
    internal interface IReifiedProjection
    {
        /// <summary> Attempt to save this projection to the destination stream. </summary>
        /// <remarks>
        ///     The returned task does not access the projection in any way, so the 
        ///     projection may be safely accessed before the task has finished executing.
        /// </remarks>
        /// <returns>
        ///     True if saving was successful, false if it failed.
        /// </returns>
        Task<bool> TrySaveAsync(CancellationToken cancel = default);

        /// <summary>
        /// Attempt to load this projection from the source, updating its
        /// current state and <see cref="Sequence"/>.
        /// </summary>
        /// <remarks> Projection is unchanged if loading fails. </remarks>
        Task<bool> TryLoadAsync(CancellationToken cancel = default);

        /// <summary>
        /// Called to initialize the projection state.
        /// </summary>
        Task CreateAsync(CancellationToken cancel = default);

        /// <summary>
        /// Notify the projection that the state may be inconsistent, due to 
        /// an event that could not be read or parsed.
        /// </summary>
        void SetPossiblyInconsistent();

        /// <summary> Reset the projection to its initial state and sequence number <c>0</c>. </summary>
        void Reset();

        /// <summary> The sequence number of the last event processed by this projection. </summary>
        uint Sequence { get; }

        /// <summary> The name of the underlying projection. </summary>
        string Name { get; }

        /// <summary> 
        /// Marks ‘state’ as being the latest in the sequence of states produced by applying events persisted in the stream 
        /// (as opposed to tentative state instances that are produced by applying tentative events that will not be persisted). 
        /// This gives the projection the liberty to perform any operations related to the persistence of the state, 
        /// such as flushing parts of it to an external state that may be loaded later.
        /// </summary>
        Task CommitAsync(uint sequence, CancellationToken cancel = default);
    }

    /// <summary>
    /// A <see cref="ReifiedProjection{T,T}"/> with the type of the state
    /// abstracted away
    /// </summary>
    internal interface IReifiedProjection<in TEvent> : IReifiedProjection
    {
        /// <summary> Apply the specified event to the state. </summary>
        /// <remarks> The sequence number must be greater than <see cref="IReifiedProjection.Sequence"/>. </remarks>
        void Apply(uint seq, TEvent e);

        /// <summary> Check applicability the specified event to the state. </summary>
        /// <remarks> The method should throw an exception in case of event invalidity.
        /// Projection must not be changed. </remarks>
        void TryApply(uint seq, IReadOnlyList<TEvent> e);

        /// <summary> Create an independent clone of this reified projection. </summary>
        /// <remarks>
        ///     The <see cref="IReifiedProjection{TEvent, TState}.Current"/> and 
        ///     <see cref="Sequence"/> of the clone are the same,
        ///     but evolve independently from the original projection. 
        /// </remarks>
        IReifiedProjection<TEvent> Clone();
    }

    internal interface IReifiedProjection<in TEvent, out TState> : IReifiedProjection<TEvent>
    {
        /// <summary> The current state of the projection. </summary>
        TState Current { get; }

        /// <summary> Create an independent clone of this reified projection. </summary>
        /// <remarks>
        ///     The <see cref="IReifiedProjection{TEvent, TState}.Current"/> and 
        ///     <see cref="Sequence"/> of the clone are the same,
        ///     but evolve independently from the original projection. 
        /// </remarks>
        new IReifiedProjection<TEvent, TState> Clone();
    }
}