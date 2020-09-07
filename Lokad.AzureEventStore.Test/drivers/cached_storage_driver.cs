using System;
using System.IO;
using Lokad.AzureEventStore.Drivers;
using Xunit;

namespace Lokad.AzureEventStore.Test.drivers
{
    public sealed class cached_storage_driver : azure_storage_driver
    {
        private string _cache;

        internal override IStorageDriver GetFreshStorageDriver()
        {
            _cache = @"C:\LokadData\AzureEventStore\FileStorageTests\" + Guid.NewGuid();
            return new CacheStorageDriver(base.GetFreshStorageDriver(), _cache);
        }

        protected override void DeleteStorageDriver()
        {
            base.DeleteStorageDriver();
            
            try
            {
                Directory.Delete(_cache, true);
            }
            catch
            {
                Console.WriteLine($"Failed to delete {_cache}");
            }
        }
    }
}
