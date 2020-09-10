namespace ExampleProject.Events
{
    /// <summary> Implemented by all events in this example project. </summary>
    public interface IEvent
    {
        /// <summary> The document to which this event applies. </summary>
        int Id { get; }
    }
}