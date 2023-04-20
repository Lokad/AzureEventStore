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
        Task<TState> TryLoadAsync(Stream source, CancellationToken cancel);

        /// <summary> Attempt to save state to a destination stream. </summary>
        /// <returns> true if saving was successful. </returns>
        Task<bool> TrySaveAsync(Stream destination, TState state, CancellationToken cancel);
    }
}
