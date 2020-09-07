using System;
using System.IO;
using Lokad.AzureEventStore.Drivers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Lokad.AzureEventStore
{
    /// <summary>
    /// Describes what kind of <see cref="IStorageDriver"/> should be
    /// used. 
    /// </summary>
    public sealed class StorageConfiguration
    {       
        /// <summary> How to connect to the stream ? </summary>
        public string ConnectionString { get; }

        /// <summary> Cache the stream to this on-disk location. </summary>
        public string CachePath { get; set; }

        /// <summary> Should this stream be read-only ? </summary>
        public bool ReadOnly { get; set; }

        /// <summary> Set to <c>true</c> to enable tracing of requests to output. </summary>
        public bool Trace { get; set; }

        private readonly IStorageDriver _storageDriver;

        /// <summary> Create a storage configuration. </summary>
        public StorageConfiguration(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary> Return the provided storage driver, for testing purposes. </summary>        
        internal StorageConfiguration(IStorageDriver driver)
        {
            _storageDriver = driver;
        }

        public bool IsAzureStorage
        {
            get
            {
                var oic = StringComparison.OrdinalIgnoreCase;
                return ConnectionString.StartsWith("DefaultEndpointsProtocol", oic) ||
                       ConnectionString.StartsWith("BlobEndpoint", oic);
            }
        }

        internal IStorageDriver Connect()
        {
            if (_storageDriver != null) return _storageDriver;
            
            // If _storageDriver is null, then ConnectionString is not.

            IStorageDriver driver;

            var cname = "";
            var cs = ConnectionString;

            var containerI = cs.IndexOf(";Container=", StringComparison.OrdinalIgnoreCase);
            if (containerI >= 0)
            {
                cname = cs.Substring(containerI + ";Container=".Length);
                cs = cs.Substring(0, containerI);
            }

            if (IsAzureStorage)
            {
                var account = CloudStorageAccount.Parse(cs);
                var client = account.CreateCloudBlobClient();
                var container = string.IsNullOrWhiteSpace(cname)
                    ? client.GetRootContainerReference()
                    : client.GetContainerReference(cname);

                if (!client.Credentials.IsSAS)
                    container.CreateIfNotExistsAsync().Wait();

                driver = new AzureStorageDriver(container);
            }
            else
            {
                string filePath = string.IsNullOrWhiteSpace(cname) ? cs : Path.Combine(cs, cname);
                driver = new FileStorageDriver(filePath);
            }
            
            if (Trace)
                driver = new StatsDriverWrapper(driver);

            if (ReadOnly)
                driver = new ReadOnlyDriverWrapper(driver);

            if (CachePath != null)
                driver = new CacheStorageDriver(driver, CachePath);

            return driver;
        }
    }
}
