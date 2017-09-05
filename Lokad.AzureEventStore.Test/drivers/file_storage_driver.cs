using System;
using System.IO;
using Lokad.AzureEventStore.Drivers;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.drivers
{
    [TestFixture]
    internal sealed class file_storage_driver : storage_driver
    {
        private string _path;

        protected override IStorageDriver GetFreshStorageDriver()
        {
            _path = @"C:\LokadData\AzureEventStore\FileStorageTests\" + Guid.NewGuid();
            return new FileStorageDriver(_path);
        }

        protected override void DeleteStorageDriver()
        {
            try
            {
                Directory.Delete(_path, true);
            }
            catch
            {
                Console.WriteLine($"Failed to delete {_path}");
            }
        }
    }
}
