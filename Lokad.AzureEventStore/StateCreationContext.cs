#nullable enable
namespace Lokad.AzureEventStore
{
    /// <summary>
    /// Settings used to create the state."/>
    /// </summary>
    public class StateCreationContext
    {
        /// <summary> External state folder path. </summary>
        public string? ExternalStateFolderPath { get; set; }

        public StateCreationContext(string? externalStateFolderPath)
        {
            ExternalStateFolderPath = externalStateFolderPath;
        }
    }
}
