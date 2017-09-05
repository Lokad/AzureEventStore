using System.Collections.Immutable;

namespace ExampleProject
{
    /// <summary> The immutable state of the application: binds a number to each key string. </summary>
    public sealed class State
    {
        public State(ImmutableDictionary<string, int> bindings)
        {
            Bindings = bindings;
        }

        /// <summary> The bindings. </summary>
        public ImmutableDictionary<string,int> Bindings { get; }
    }
}
