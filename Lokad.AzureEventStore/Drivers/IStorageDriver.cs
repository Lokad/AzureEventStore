using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary> The interface implemented by low-level drivers. </summary>
    /// <remarks>
    /// The abstraction is that a storage driver points to a sequence of stored events,
    /// each with:
    ///  - an <c>uint</c> key. These keys SHOULD be strictly increasing, 
    ///    but this property will not be verified or enforced by the abstraction, though 
    ///    it will be used for certain optimizations. Key <c>0</c> is reserved and should
    ///    not be used.
    ///  - content, as a sequence of bytes of arbitrary length (but that length can be 
    ///    divided by 8).
    ///  - a <c>long</c> position, which is guaranteed to be strictly increasing, and 
    ///    will be managed internally by the driver. The actual meaning of positions is
    ///    opaque (do not try to understand them), except for position 0, which represents
    ///    the beginning of the sequence.
    /// 
    /// The methods of this interface are NOT expected to be re-entrant, or support any 
    /// kind of concurrent execution.
    /// </remarks>
    internal interface IStorageDriver
    {
        /// <summary> The position of the last event within this driver.  </summary>
        Task<long> GetPositionAsync(CancellationToken cancel = default(CancellationToken));

        /// <summary> 
        /// Attempts to write a sequence of events to the underlying event stream, 
        /// at the specified position. Fails if events have already been added to 
        /// the stream after the specified position (by another thread/process/server).         
        /// </summary>
        Task<DriverWriteResult> WriteAsync(
            long position, 
            IEnumerable<RawEvent> events, 
            CancellationToken cancel = default(CancellationToken));

        /// <summary>
        /// Attempts to read events from the underlying event stream, starting at the 
        /// specified position (which must be valid). Will read up to <paramref name="maxBytes"/>,
        /// but always at least one event (if it is available).  
        /// </summary>
        Task<DriverReadResult> ReadAsync(
            long position, 
            long maxBytes,
            CancellationToken cancel = default(CancellationToken));

        /// <summary> The last available key in the stream. </summary>
        /// <remarks> If no keys are available, returns <c>0</c>. </remarks>
        Task<uint> GetLastKeyAsync(CancellationToken cancel = default(CancellationToken));

        /// <summary> Seek a position before the provided key. </summary>
        /// <remarks> 
        /// The driver should make a reasonable attempt to return a seek position 
        /// as close as possible to the key, but it is entirely acceptable for this
        /// function to always return 0. The only guarantee is that the key (or
        /// keys greater than it) will never be found before the returned position.
        /// 
        /// If <paramref name="position"/> is provided, then the returned value may 
        /// be no larger than this value.
        /// </remarks>
        Task<long> SeekAsync(
            uint key,
            long position = 0,
            CancellationToken cancel = default(CancellationToken));
    }
}
