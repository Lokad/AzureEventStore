using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Azure.Storage;
using Azure.Storage.Blobs;
using Xunit;

namespace Lokad.AzureEventStore.Test.drivers
{
    public class azure_storage_driver : storage_driver
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

        [Fact(Skip = "Long")]
        public async Task compaction()
        {
            var sw = Stopwatch.StartNew();
            var bytes = new byte[8];
            var driver = (AzureStorageDriver)GetFreshStorageDriver();
            var p = await driver.GetPositionAsync();

            var seq = (int)await driver.GetLastKeyAsync();
            if (p == 0) seq = -1;

            for (var i = seq + 1; i < 2 * 50_000; ++i)
            {
                bytes[0] = (byte)(i >> 24);
                bytes[1] = (byte)(i >> 16);
                bytes[2] = (byte)(i >> 8);
                bytes[3] = (byte)i;

                while (true)
                {
                    var dwr = await driver.WriteAsync(p, new[] { new RawEvent((uint)i, bytes) });
                    if (!dwr.Success) continue;
                    p = dwr.NextPosition;
                    break;
                }

                if (i % 1000 == 0) Trace.WriteLine($"Emitted {i}");

                Assert.Null(driver.RunningCompaction);
            }

            Trace.WriteLine($"Written in {sw.Elapsed}");


            // Read entire stream, test equality, and measure duration
            sw.Restart();
            {
                var rp = 0L;
                var j = 0;
                while (rp < p)
                {
                    var drr = await driver.ReadAsync(rp, 4 * 1024 * 1024);
                    rp = drr.NextPosition;
                    foreach (var e in drr.Events)
                    {
                        bytes[0] = (byte)(j >> 24);
                        bytes[1] = (byte)(j >> 16);
                        bytes[2] = (byte)(j >> 8);
                        bytes[3] = (byte)j;

                        Assert.Equal((uint)j, e.Sequence);
                        Assert.Equal(bytes, e.Contents);

                        ++j;
                        if (j % 1000 == 0) Trace.WriteLine($"Read {j}");
                    }
                }

                Assert.Equal(j, 2 * 50_000);
            }

            Trace.WriteLine($"Read (non-compacted) in {sw.Elapsed}");

            // Append an event that starts the 3rd blob
            while (true)
            {
                bytes[3]++;
                var dwr = await driver.WriteAsync(p, new[] { new RawEvent((uint)2 * 50_000, bytes) });
                if (!dwr.Success) continue;
                p = dwr.NextPosition;
                break;
            }

            Assert.NotNull(driver.RunningCompaction);

            if (driver.RunningCompaction != null)
            {
                sw.Restart();
                await driver.RunningCompaction;
                Trace.WriteLine($"Compaction in {sw.Elapsed}");
            }

            // After the compaction, force a refresh and test:
            var wp = await driver.RefreshCache(CancellationToken.None);

            // Still the same write position
            Assert.Equal(p, wp);

            // The correct number of blobs (one compact, one being appended to)
            Assert.Equal(
                new[] { "events.00001.compact", "events.00002" },
                driver.Blobs.Select(b => b.Name));

            // Read all the stream again, measure duration
            {
                sw.Restart();
                var rp = 0L;
                var j = 0;
                while (rp < p)
                {
                    var drr = await driver.ReadAsync(rp, 4 * 1024 * 1024);
                    rp = drr.NextPosition;
                    foreach (var e in drr.Events)
                    {
                        bytes[0] = (byte)(j >> 24);
                        bytes[1] = (byte)(j >> 16);
                        bytes[2] = (byte)(j >> 8);
                        bytes[3] = (byte)j;

                        Assert.Equal((uint)j, e.Sequence);
                        Assert.Equal(bytes, e.Contents);

                        ++j;
                        if (j % 1000 == 0) Trace.WriteLine($"Read {j}");
                    }
                }

                Trace.WriteLine($"Read (compacted) in {sw.Elapsed}");

                Assert.Equal(j, 2 * 50_000 + 1);
            }
        }
    }
}
