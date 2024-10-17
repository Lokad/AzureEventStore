using System;
using System.IO;
using Lokad.AzureEventStore.Drivers;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;

namespace Lokad.AzureEventStore.Test.drivers
{
    public class mono_blob_storage_driver : storage_driver
    {
        private BlobContainerClient _container;

        internal override IStorageDriver GetFreshStorageDriver()
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

            var credential = new StorageSharedKeyCredential(split[0], split[1]);
            var serviceUri = new Uri($"https://{split[0]}.blob.core.windows.net");
            var account = new BlobServiceClient(serviceUri, credential);
            _container = account.GetBlobContainerClient(Guid.NewGuid().ToString());
            _container.CreateIfNotExists();

            var blob = _container.GetAppendBlobClient("append");
            return new MonoBlobStorageDriver(blob);
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
