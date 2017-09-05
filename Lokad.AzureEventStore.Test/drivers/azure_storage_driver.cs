using System;
using System.IO;
using Lokad.AzureEventStore.Drivers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.drivers
{
    [TestFixture]
    internal class azure_storage_driver : storage_driver
    {
        private CloudBlobContainer _container;
        
        protected override IStorageDriver GetFreshStorageDriver()
        {
            // For CI, read from environment variable
            var str = Environment.GetEnvironmentVariable("AZURE_CONNECTION");

            if (str == null && File.Exists("../../../azure_connection.txt"))
                // For local development, read from on-disk file (should be in
                // directory Lokad.AzureEventStore.Test)
                str = File.ReadAllText("../../../azure_connection.txt").Trim();

            var split = str?.Split(':');
            if (split == null || split.Length != 2)
                throw new Exception("Environment variable AZURE_CONNECTION should be 'account:key'");

            var account = new CloudStorageAccount(new StorageCredentials(split[0], split[1]), true);
            _container = account.CreateCloudBlobClient().GetContainerReference(Guid.NewGuid().ToString());
            _container.CreateIfNotExists();
            return new AzureStorageDriver(_container);
        }

        protected override void DeleteStorageDriver()
        {
            base.DeleteStorageDriver();

            try
            {
                _container.DeleteIfExists();
            }
            catch
            {
                Console.WriteLine($"Failed to delete {_container.Name}");
            }
        }
    }
}
