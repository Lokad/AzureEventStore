using System;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Wrapper
{
    internal static class Initialization
    {
        internal static async Task Run(IInitFacade facade, ILogAdapter log = null)
        {
            try
            {
                // Load project and discard events before that.
                log?.Info("[ES init] loading projections.");

                var projection = await facade.Projection.ConfigureAwait(false);

                var catchUp = projection.Sequence + 1;

                log?.Info($"[ES init] advancing stream to seq {catchUp}.");
                var streamSequence = await facade.DiscardStreamUpTo(catchUp).ConfigureAwait(false);

                if (streamSequence < catchUp)
                {
                    log?.Warning(
                        $"[ES init] invalid seq {catchUp} > {streamSequence}, resetting everything.");

                    // Cache is apparently beyond the available sequence. Could happen in
                    // development environments with non-persistent events but persistent
                    // caches. Treat cache as invalid and start from the beginning.
                    facade.Reset();
                }
            }
            catch (Exception e)
            {
                log?.Warning("[ES init] error while reading cache.", e);

                // Something went wrong when reading the cache. Stop.
                facade.Reset();
            }
        }
    }
}
