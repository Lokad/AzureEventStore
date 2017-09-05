using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers
{
    internal sealed class StatsDriverWrapper : IStorageDriver
    {
        public readonly IStorageDriver Inner;

        public StatsDriverWrapper(IStorageDriver inner)
        {
            Inner = inner;
        }

        public async Task<long> GetPositionAsync(CancellationToken cancel = new CancellationToken())
        {
            var sw = Stopwatch.StartNew();
            try
            {
                return await Inner.GetPositionAsync(cancel);
            }
            finally
            {
                Trace.WriteLine("GetPositionAsync " + sw.ElapsedMilliseconds);
            }
        }

        public async Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events, CancellationToken cancel = new CancellationToken())
        {
            var sw = Stopwatch.StartNew();
            try
            {
                return await Inner.WriteAsync(position, events, cancel);
            }
            finally
            {
                Trace.WriteLine("WriteAsync " + sw.ElapsedMilliseconds);
            }
        }

        public async Task<DriverReadResult> ReadAsync(long position, long maxBytes, CancellationToken cancel = new CancellationToken())
        {
            var sw = Stopwatch.StartNew();
            try
            {
                return await Inner.ReadAsync(position, maxBytes, cancel);
            }
            finally
            {
                Trace.WriteLine("ReadAsync " + sw.ElapsedMilliseconds);
            }
        }

        public async Task<uint> GetLastKeyAsync(CancellationToken cancel = new CancellationToken())
        {
            var sw = Stopwatch.StartNew();
            try
            {
                return await Inner.GetLastKeyAsync(cancel);
            }
            finally
            {
                Trace.WriteLine("GetLastKeyAsync " + sw.ElapsedMilliseconds);
            }
        }

        public async Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = new CancellationToken())
        {
            var sw = Stopwatch.StartNew();
            try
            {
                return await Inner.SeekAsync(key, position, cancel);
            }
            finally
            {
                Trace.WriteLine("SeekAsync " + sw.ElapsedMilliseconds);
            }
        }
    }
}
