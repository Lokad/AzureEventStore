using System;
using System.Collections.Immutable;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using ExampleProject.Events;

namespace ExampleProject
{
    public sealed class Projection : IProjection<IEvent, State>
    {
        public State Initial => new State(ImmutableDictionary<string, int>.Empty);

        /// <summary> Update the state by applying an event. </summary>
        public State Apply(uint sequence, IEvent e, State previous)
        {
            // In a real-life project, this would likely have been implemented using
            // a visitor pattern (so that, when adding event types, the compiler can
            // help us detect all cases where these new events need to be handled).

            if (e is ValueDeleted d)
                return new State(previous.Bindings.Remove(d.Key));

            if (e is ValueUpdated u)
                return new State(previous.Bindings.SetItem(u.Key, u.Value));

            throw new ArgumentOutOfRangeException(nameof(e), "Unknown event type " + e.GetType());
        }

        /// <remarks> We do not support loading the state. </remarks>
        public Task<State> TryLoadAsync(Stream source, CancellationToken cancel) => 
            throw new NotSupportedException();

        /// <remarks> We do not support saving the state. </remarks>
        public Task<bool> TrySaveAsync(Stream destination, State state, CancellationToken cancel) =>
            Task.FromResult(false);

        /// <see cref="IProjection{TEvent}.FullName"/>
        public string FullName => "State";

        /// <see cref="IProjection{TEvent}.State"/>
        public Type State => typeof(State);
    }
}
