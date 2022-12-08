using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Streams;
using Newtonsoft.Json.Linq;

namespace Lokad.AzureEventStore.Tool
{
    static partial class Program
    {
        /// <summary> Backs up a stream to another. </summary>
        private static void CmdBackup(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: backup <source> <dest> <max-seq>?");
                Console.WriteLine("Copy events from one stream to another.");
                return;
            }

            var srcname = args[0];
            var dstname = args[1];

            var maxseq = uint.MaxValue;
            if (args.Length >= 3)
                maxseq = uint.Parse(args[2]);

            BackupCopy(srcname, dstname, maxseq);
        }

        /// <summary> Copies from source to destination, returns last position in source. </summary>
        private static long BackupCopy(string srcname, string dstname, uint maxseq)
        { 
            var srcDriver = new StorageConfiguration(Parse(srcname)) {ReadOnly = true}.Connect();

            using (Release(srcDriver))
            {
                var dstDriver = new StorageConfiguration(Parse(dstname)).Connect();
                using (Release(dstDriver))
                {
                    var src = new EventStream<JObject>(srcDriver);
                    var dst = new MigrationStream<JObject>(dstDriver);

                    var sw = Stopwatch.StartNew();
                    var events = 0;
                    var initial = 0L;

                    Status("Connecting...");

                    Task.Run((Func<Task>) (async () =>
                    {
                        Status("Current source size: ...");
                        var maxPos = await srcDriver.GetPositionAsync();
                        Console.WriteLine("Current source size: {0:F2} MB", maxPos / (1024.0 * 1024.0));

                        Status("Current source seq: ...");
                        var maxSeq = await srcDriver.GetLastKeyAsync();
                        Console.WriteLine("Current source seq: {0}", maxSeq);

                        Status("Current destination seq: ...");
                        var bakSeq = await dstDriver.GetLastKeyAsync();
                        Console.WriteLine("Current destination seq: {0}", bakSeq);

                        if (bakSeq >= maxSeq)
                        {
                            Console.WriteLine("Destination already up-to-date.");
                            return;
                        }

                        var asAzure = ((ReadOnlyDriverWrapper) srcDriver).Wrapped as AzureStorageDriver;

                        if (asAzure != null)
                        {
                            for (var i = 0; i < asAzure.Blobs.Count; ++i)
                            {
                                Console.WriteLine("Blob {0}: {1:F2} MB from seq {2}",
                                    asAzure.Blobs[i].AppendBlob.Name,
                                    asAzure.Blobs[i].Bytes / (1024.0 * 1024.0),
                                    i < asAzure.FirstKey.Count ? asAzure.FirstKey[i] : maxSeq);
                            }
                        }

                        if (bakSeq > 0)
                        {
                            Status("Skipping to seq {0}...", bakSeq);
                            await src.DiscardUpTo(bakSeq + 1);
                            Console.WriteLine("Skipping to seq {0} done !", bakSeq);
                        }

                        initial = src.Position;

                        Func<bool> more;
                        do
                        {
                            var fetch = src.BackgroundFetchAsync();
                            var list = new List<KeyValuePair<uint, JObject>>();

                            JObject obj;
                            while ((obj = src.TryGetNext()) != null)
                                list.Add(new KeyValuePair<uint, JObject>(src.Sequence, obj));

                            events += list.Count;

                            while (list.Count > 0 && list[list.Count - 1].Key > maxseq)
                                list.RemoveAt(list.Count - 1);

                            await dst.WriteAsync(list);

                            Status("{0}/{1} ({2:F2}/{3:F2} MB)",
                                src.Sequence,
                                maxSeq,
                                src.Position / (1024.0 * 1024.0),
                                maxPos / (1024.0 * 1024.0));

                            more = await fetch;
                        } while (more());
                    })).Wait();

                    Console.WriteLine("{0} events ({1:F2} MB) in {2:F2}s.",
                        events,
                        (src.Position - initial) / (1024.0 * 1024.0),
                        sw.ElapsedMilliseconds / 1000.0);

                    return src.Position;
                }
            }
        }
    }
}