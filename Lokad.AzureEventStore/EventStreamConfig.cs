using Azure.Storage.Blobs;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
namespace Lokad.AzureEventStore;

/// <summary>
/// Additional configuration settings.
/// </summary>
public class EventStreamConfig(string stateCache)
{
    /// <summary> Name of the blob/file which contains a JSON document with additional fields. </summary>
    private const string Config = "config";

    /// <summary>
    /// The location of the projection cache blob.
    /// </summary>
    public string StateCache { get; set; } = stateCache;

    /// <summary>
    /// From a storageConfiguration, create a blob or a file that contains additional fields. 
    /// </summary>
    public async Task CreateConfigBlob(StorageConfiguration storageConfiguration, CancellationToken cancellationToken = default)
    {
        if (storageConfiguration.IsAzureStorage)
        {
            var container = storageConfiguration.GetBlobContainerClient();
            var configBlobClient = container.GetBlobClient(Config);
            await configBlobClient.UploadAsync(BinaryData.FromString(JsonConvert.SerializeObject(this)), overwrite: true, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        else
        {
            await using var stream = File.Open(Path.Combine(storageConfiguration.FilePath, Config), FileMode.OpenOrCreate, FileAccess.ReadWrite);
            await using var writer = new JsonTextWriter(new StreamWriter(stream));
            JsonSerializer.Create().Serialize(writer, this);
        }
    }
    /// <summary>
    /// Check if a blob named "config" exists and download a json of additional settings.
    /// </summary>
    /// <returns> An EventStreamConfig with the additional settings or null if the file was not found. </returns>
    public static EventStreamConfig GetEventStreamConfigFromAzureBlob(BlobContainerClient container)
    {
        var configBlobClient = container.GetBlobClient(Config);
        if (!configBlobClient.Exists()) return null;
        var blobContent = configBlobClient.DownloadContent().Value.Content.ToString();
        return JsonConvert.DeserializeObject<EventStreamConfig>(blobContent);
    }
}