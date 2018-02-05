using System;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Streams;

namespace Lokad.AzureEventStore.Wrapper
{
    internal static class Initialization
    {
        internal static async Task Run(IReifiedProjection projection, IEventStream stream, CancellationToken cancel = default(CancellationToken), ILogAdapter log = null)
        {
            try
            {
                // Load project and discard events before that.
                log?.Info("[ES init] loading projections.");

                await projection.TryLoadAsync(cancel).ConfigureAwait(false);

                var catchUp = projection.Sequence + 1;

                log?.Info($"[ES init] advancing stream to seq {catchUp}.");
                var streamSequence = await stream.DiscardUpTo(catchUp, cancel).ConfigureAwait(false);

                if( cancel.IsCancellationRequested)
                    return;

                if (streamSequence != projection.Sequence)
                {
                    log?.Warning(
                        $"[ES init] projection seq {projection.Sequence} not found in stream (max seq is {streamSequence}: resetting everything.");

                    // Cache is apparently beyond the available sequence. Could happen in
                    // development environments with non-persistent events but persistent
                    // caches. Treat cache as invalid and start from the beginning.
                    stream.Reset();
                    projection.Reset();
                }
            }
            catch (Exception e)
            {
                log?.Warning("[ES init] error while reading cache.", e);

                // Something went wrong when reading the cache. Stop.
                stream.Reset();
                projection.Reset();
            }
        }
    }
}
