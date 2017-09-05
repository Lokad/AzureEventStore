namespace ExampleProject.Events
{
    /// <summary> Implemented by all events in this example project. </summary>
    public interface IEvent
    {
        /// <summary> The key to which this event applies. </summary>
        string Key { get; }
    }
}