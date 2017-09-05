namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> The result of a <see cref="IStorageDriver.WriteAsync"/> operation. </summary>
    internal sealed class DriverWriteResult
    {
        /// <summary>
        /// The new position of the write cursor, to be used on the next call
        /// to <see cref="IStorageDriver.WriteAsync"/>. If no other writes occur, 
        /// this will also be returned by <see cref="IStorageDriver.GetPositionAsync"/>.
        /// </summary>
        internal readonly long NextPosition;

        /// <summary>
        /// True if the data was written. False if the provided position was obsolete,
        /// in which case no data was written.
        /// </summary>
        internal readonly bool Success;

        internal DriverWriteResult(long nextPosition, bool success)
        {
            NextPosition = nextPosition;
            Success = success;
        }
    }
}