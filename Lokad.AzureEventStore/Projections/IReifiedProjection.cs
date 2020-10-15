using System.Threading;
using System.Threading.Tasks;

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
        Task TryLoadAsync(CancellationToken cancel = default);

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
    }

    internal interface IReifiedProjection<in TEvent, out TState> : IReifiedProjection<TEvent>
    {
        /// <summary> The current state of the projection. </summary>
        TState Current { get; }
    }
}