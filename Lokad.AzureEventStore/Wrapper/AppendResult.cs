namespace Lokad.AzureEventStore.Wrapper
{
    /// <summary> The result of appending to an event stream. </summary>
    public class AppendResult
    {
        /// <summary> The number of events appended. </summary>
        public readonly int Count;

        /// <summary> The sequence number of the first appended event. </summary>
        public readonly uint First;

        internal AppendResult(int count, uint first)
        {
            Count = count;
            First = first;
        }
    }

    /// <summary> The result of appending to an event stream. </summary>
    public sealed class AppendResult<T> : AppendResult
    {
        /// <summary> Additional data about the appended result. </summary>
        public readonly T More;

        internal AppendResult(int count, uint first, T more) : base(count, first)
        {
            More = more;
        }
    }
}