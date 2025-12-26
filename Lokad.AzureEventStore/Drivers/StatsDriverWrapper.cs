using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Drivers;

internal sealed class StatsDriverWrapper(IStorageDriver inner) : IStorageDriver
{
    public readonly IStorageDriver Inner = inner;

    public async Task<long> GetPositionAsync(CancellationToken cancel = new())
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

    public async Task<DriverWriteResult> WriteAsync(long position, IEnumerable<RawEvent> events, CancellationToken cancel = new())
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

    public async Task<DriverReadResult> ReadAsync(long position, Memory<byte> backing, CancellationToken cancel = new())
    {
        var sw = Stopwatch.StartNew();
        try
        {
            return await Inner.ReadAsync(position, backing, cancel);
        }
        finally
        {
            Trace.WriteLine("ReadAsync " + sw.ElapsedMilliseconds);
        }
    }

    public async Task<uint> GetLastKeyAsync(CancellationToken cancel = new())
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

    public async Task<long> SeekAsync(uint key, long position = 0, CancellationToken cancel = new())
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