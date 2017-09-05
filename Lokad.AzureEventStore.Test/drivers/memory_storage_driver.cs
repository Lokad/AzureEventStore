using Lokad.AzureEventStore.Drivers;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.drivers
{
    [TestFixture]
    internal sealed class memory_storage_driver : storage_driver
    {
        protected override IStorageDriver GetFreshStorageDriver()
        {
            return new MemoryStorageDriver();
        }

        protected override void DeleteStorageDriver()
        {
            // Nothing to do, it's in-memory
        }
    }
}
