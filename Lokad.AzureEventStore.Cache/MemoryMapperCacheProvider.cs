using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Lokad.MemoryMapping;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace Lokad.AzureEventStore.Cache
{
    /// <summary>
    ///     Provides a blob container to cache a <see cref="IMemoryMappedFolder"/> entries.
    /// </summary>
    public class MemoryMapperCacheProvider
    {
        /// <summary>
        ///     Number of blobs uploaded or downloaded in parallel.
        /// </summary>
        private const int _threadCount = 7;

        /// <summary>
        ///     Blob container where are saved the memory-mapped entries.
        /// </summary>
        private readonly BlobContainerClient _container;

        /// <summary>
        ///     Timestamp of the files upload date.
        /// </summary>
        private const string _dateFormat = "yyyyMMddHHmmss";

        /// <summary>
        ///     Delay before uploading new entries.
        /// </summary>
        private readonly TimeSpan _uploadDelay;

        /// <summary>
        ///     Number of a state versions that can be kept in the blob container.
        /// </summary>
        private const int _copyLimit = 2;

        /// <summary>
        ///     Metadata key to flag if an upload is finished and can be downloaded.
        ///     It is placed on the first blob (ordered by name).
        /// </summary>
        private const string _uploadStatus = "available";

        public MemoryMapperCacheProvider(BlobContainerClient container, TimeSpan? uploadDelay = null)
        {
            _container = container;
            _uploadDelay = uploadDelay ?? TimeSpan.FromMinutes(120);
        }

        /// <summary> 
        ///     List all blobs for the specified state grouped by the creation timestamp.
        /// </summary>
        /// <remarks>
        ///     Blobs are following the name's format stateName/timestamp/fileName.
        /// </remarks>
        private async Task<IGrouping<string, BlobItem>[]> GetGroupedBlobs(string stateName, CancellationToken cancellationToken)
        {
            var result = new List<BlobItem>();

            var list = _container.GetBlobsAsync(traits: BlobTraits.Metadata,
               states: BlobStates.None,
               prefix: stateName,
               cancellationToken: cancellationToken)
               .AsPages(null);

            await foreach (var page in list)
            {
                foreach (var item in page.Values)
                {
                    if (item is not BlobItem blob) continue;
                    result.Add(blob);
                }
            }
            return result.GroupBy(b => b.Name.Split("/")[1])
                         .OrderByDescending(kvp => kvp.Key)
                         .ToArray();
        }

        /// <summary>
        ///     Load blobs from the container into memory-mapped entries.
        /// </summary>
        public async Task LoadAsync(string stateName, IMemoryMappedFolder folder, CancellationToken cancellationToken = default)
        {
            if (!await _container.ExistsAsync(cancellationToken).ConfigureAwait(false))
                throw new InvalidOperationException("Container does not exist, can not load files from it.");

            // Clean the folder's content.
            var entries = folder.EnumerateEntryNames();
            foreach (var entry in entries) 
            {
                folder.Delete(entry);
            }

            var groups = await GetGroupedBlobs(stateName, cancellationToken);

            if (groups == null || groups.Length == 0)
                return;

            var blobs = groups.FirstOrDefault(GroupIsComplete);
            if (blobs == null)
                return;

            var tasks = new Queue<Task>();
            foreach (var blob in blobs)
            {
                tasks.Enqueue(LoadBlobAsync(blob.Name, folder, cancellationToken));
                while (tasks.Count >= _threadCount)
                    await tasks.Dequeue();                            
            }

            while (tasks.Count > 0)
                await tasks.Dequeue();
        }

        /// <summary>
        ///     Checks if the first blob of the group ordered by name is flagged
        ///     as available in its metadata.
        /// </summary>
        private static bool GroupIsComplete(IEnumerable<BlobItem> blobs)
        {
            var blob = blobs.OrderBy(b => b.Name).First();

            return blob.Metadata.TryGetValue(_uploadStatus, out var available) 
                && bool.TryParse(available, out var status) && status;
        }

        /// <summary>
        ///     Downloads and uncompressed the blob into a memory-mapped entry.
        /// </summary>
        private async Task LoadBlobAsync(
            string blobName, 
            IMemoryMappedFolder folder,
            CancellationToken cancellationToken)
        {
            using (var blobStream = await _container.GetBlobClient(blobName).OpenReadAsync(cancellationToken: cancellationToken))
            {
                var lengthBytes = new byte[sizeof(long)];
                blobStream.Read(lengthBytes);

                // Blob's name is following the format stateName/timestamp/fileName
                var fileName = blobName.Split("/")[2];
                var entry = folder.CreateNew(fileName, MemoryMarshal.Cast<byte, long>(lengthBytes)[0]);
                using (var gs = new GZipStream(blobStream, CompressionMode.Decompress))
                {
                    ReadBytes(gs, entry);
                }
                entry.Dispose();
            }
        }

        private static void ReadBytes(GZipStream gs, IMemoryMappedEntry entry)
        {
            var target = entry.AsSpan;
            while (target.Length > 0)
            {
                var read = gs.Read(target);
                if (read == 0) throw new InvalidOperationException("No more byte available.");

                target = target[read..];
            }
        }

        /// <summary>
        ///     Deletes the old blobs.
        ///     Uploads one compressed blob for each memory-mapped entry.
        /// </summary>
        public async Task UploadToBlobsAsync(string stateName, IMemoryMappedFolder folder, CancellationToken cancellationToken = default)
        {
            var timestamp = DateTime.UtcNow;
            var groups = await GetGroupedBlobs(stateName, cancellationToken);
            var count = 0;
            var toDelete = new List<string>();
            foreach (var group in groups) 
            {
                if (DateTime.TryParseExact(group.Key, _dateFormat, null, DateTimeStyles.None, out var oldTimestamp)
                    && oldTimestamp >= timestamp.Subtract(_uploadDelay))
                    return;

                count++;
                if (count > _copyLimit)
                {
                    toDelete.AddRange(group.Select(b => b.Name));
                }
            }

            var files = folder.EnumerateEntryNames().OrderBy(f => f).ToArray();
            if (files.Length == 0) 
                return;

            var tasks = new Queue<Task>();

            var timedStateName = $"{stateName}/{timestamp.ToString(_dateFormat)}";
            foreach (var file in files)
            {
                tasks.Enqueue(UploadToBlobAsync(timedStateName, folder, file, cancellationToken));
                while (tasks.Count >= _threadCount)
                    await tasks.Dequeue();
            }

            while (tasks.Count > 0)
                await tasks.Dequeue();


            var blob = _container.GetBlobClient(timedStateName + "/" + files[0]);
            await blob.SetMetadataAsync(new Dictionary<string, string> { { _uploadStatus, "true" } });

            foreach (var delete in toDelete)
                await _container.GetBlobClient(delete).DeleteAsync();
        }

        private async Task UploadToBlobAsync(string stateName, IMemoryMappedFolder folder, string file, CancellationToken cancellationToken)
        {
            var blob = _container.GetBlobClient(stateName + "/" + file);
            using (var blobStream = await blob.OpenWriteAsync(true, cancellationToken: cancellationToken))
            {
                var mmf = folder.Open(file);
                var length = MemoryMarshal.Cast<long, byte>(new long[] { mmf.Length }).ToArray();
                blobStream.Write(length);

                using (var gs = new GZipStream(blobStream, CompressionLevel.Optimal))
                {
                    await gs.WriteAsync(mmf.AsMemory,cancellationToken);
                }
                mmf.Dispose();
            }                
        }
    }
}
