using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    /// <summary>
    /// Wrap an <see cref="IStorageDriver"/> and forbidding write
    /// operations.
    /// </summary>
    internal sealed class ReadOnlyDriverWrapper : IStorageDriver
    {
        internal readonly IStorageDriver Wrapped;

        public ReadOnlyDriverWrapper(IStorageDriver wrapped)
        {
            Wrapped = wrapped;
        }

        public Task<long> GetPositionAsync(CancellationToken cancel = new CancellationToken()) => 
            Wrapped.GetPositionAsync(cancel);

        public Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events, CancellationToken cancel = new CancellationToken())
        {
            throw new InvalidOperationException("Storage driver is read-only");
        }

        public Task<DriverReadResult> ReadAsync(long position, long maxBytes, CancellationToken cancel = new CancellationToken()) => 
            Wrapped.ReadAsync(position, maxBytes, cancel);

        public Task<uint> GetLastKeyAsync(CancellationToken cancel = new CancellationToken()) => 
            Wrapped.GetLastKeyAsync(cancel);

        public Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = new CancellationToken()) => 
            Wrapped.SeekAsync(key, position, cancel);
    }
}
