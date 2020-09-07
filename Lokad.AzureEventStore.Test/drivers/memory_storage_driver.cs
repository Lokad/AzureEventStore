using Lokad.AzureEventStore.Drivers;
using Xunit;

namespace Lokad.AzureEventStore.Test.drivers
{
    public sealed class memory_storage_driver : storage_driver
    {
        internal override IStorageDriver GetFreshStorageDriver()
        {
            return new MemoryStorageDriver();
        }

        protected override void DeleteStorageDriver()
        {
            // Nothing to do, it's in-memory
        }
    }
}
