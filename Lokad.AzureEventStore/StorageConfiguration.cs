using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Lokad.AzureEventStore.Drivers;
using System;
using System.IO;

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

        /// <summary>
        /// Can be used to create a storage configuration from a MemoryStream.
        /// </summary>
        public StorageConfiguration(MemoryStream stream)
        {
            _storageDriver = new MemoryStorageDriver(stream);
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

        /// <summary>
        /// Only use for local storage, path where are stocked the events.
        /// </summary>
        internal string FilePath
        {
            get
            {
                if (IsAzureStorage)
                    throw new InvalidOperationException("Connection string redirects towards an Azure blob.");

                (var connectionString, var containerName) = ParseConnectionString(ConnectionString);
                return string.IsNullOrWhiteSpace(containerName) ? connectionString : Path.Combine(connectionString, containerName);
            }
        }

        internal IStorageDriver Connect()
        {
            return Connect(out _);
        }

        internal IStorageDriver Connect(out BlobContainerClient stateCache)
        {
            stateCache = null;
            if (_storageDriver != null) return _storageDriver;

            // If _storageDriver is null, then ConnectionString is not.

            IStorageDriver driver;
            if (IsAzureStorage)
            {
                var container = GetBlobContainerClient();
                var config = EventStreamConfig.GetEventStreamConfigFromAzureBlob(container);
                stateCache = config != null ? GetBlobContainerClient(ConnectionString, config.StateCache) : null;
                driver = new AzureStorageDriver(container);
            }
            else
                driver = new FileStorageDriver(FilePath);

            if (Trace)
                driver = new StatsDriverWrapper(driver);

            if (ReadOnly)
                driver = new ReadOnlyDriverWrapper(driver);

            if (CachePath != null)
                driver = new CacheStorageDriver(driver, CachePath);

            return driver;
        }

        public BlobContainerClient GetBlobContainerClient()
        {
            (var connectionString, var containerName) = ParseConnectionString(ConnectionString);
            return GetBlobContainerClient(connectionString, containerName);
        }

        private static BlobContainerClient GetBlobContainerClient(string connectionString, string name)
        {
            var account = new BlobServiceClient(connectionString);
            // HINT: Root container name must be "$root".
            // docs: https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-container-create#create-the-root-container
            var container = string.IsNullOrWhiteSpace(name)
                ? account.GetBlobContainerClient("$root")
                : account.GetBlobContainerClient(name);
            if (account.CanGenerateAccountSasUri)
                container.CreateIfNotExistsAsync().Wait();
            return container;
        }

        private static (string connectionString, string containerName) ParseConnectionString(string connectionString)
        {
            var start = connectionString.IndexOf(";Container=", StringComparison.OrdinalIgnoreCase);
            return start >= 0
                ? (connectionString.Substring(0, start), connectionString.Substring(start + ";Container=".Length))
                : (connectionString, "");
        }

        /// <summary>
        ///     Expects this configuration to represent a container in an Azure Blob Storage account,
        ///     and takes a blob name as parameter ; returns a new storage configuration that connects
        ///     to a single-blob event stream stored in that event.
        /// </summary>
        public StorageConfiguration MonoBlob(string blobName)
        {
            var client = GetBlobContainerClient();
            var driver = new MonoBlobStorageDriver(client.GetAppendBlobClient(blobName));
            return new StorageConfiguration(driver);
        }
    }
}
