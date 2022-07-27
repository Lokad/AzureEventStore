using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ExampleProject.Events;
using Lokad.AzureEventStore;
using Lokad.AzureEventStore.Cache;

namespace ExampleProject
{
    public sealed class Program
    {
        public const string AccountName = "";
        public const string AccountKey = "";
        public const string LocalCache = @"C:\LokadData\example-cache";
        public const string ConnectionString = @"DefaultEndpointsProtocol=https;AccountName=" + AccountName + ";AccountKey=" + AccountKey + ";Container=example";

        static async Task Main(string[] args)
        {
            var config = new StorageConfiguration(ConnectionString);

            // The stream service owns a background process that we need
            // to be able to kill.
            var cts = new CancellationTokenSource();
            var svc = EventStreamService<IEvent, State>.StartNew(
                // This is where the events are stored
                storage: config,
                // These are the projections used to turn the event stream
                // into an up-to-date state (our example only has one projection)
                projections: new[] { new Projection() },
                // This is where we would write the projection snapshots, if
                // we had implemented them.
                projectionCache: new MappedCacheProvider(LocalCache),
                // This is used by the service to emit messages about what is happening
                log: new Log(),
                // This cancellation token stops the background process.
                cancel: cts.Token);

            svc.RefreshPeriod = 10 * 60;

            // Once the stream is fully loaded, save it to cache.
            _ = svc.Ready.ContinueWith(_ => svc.TrySaveAsync(cts.Token));

            while (true)
            {
                if (svc.IsReady)
                    Console.WriteLine("{0} words in index", svc.LocalState.Index.Count);

                var line = Console.ReadLine();
                if (line == null || line == "QUIT") break;

                if (!svc.IsReady)
                {
                    // The service starts catching up with the stream. This may
                    // take a short while if the stream becomes very long.
                    Console.WriteLine("Service not ready, please wait.");
                    continue;
                }

                if (line.StartsWith("file "))
                {
                    var path = line.Substring("file ".Length);
                    string text;

                    try
                    {
                        text = File.ReadAllText(path);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Error: {0}", e.Message);
                        continue;
                    }

                    if (text.Length > 65536) 
                        text = text.Substring(0, 65536);

                    var id =
                        await svc.TransactionAsync(transaction =>
                        {
                            var count = transaction.State.Documents.Count;
                            transaction.Add(new DocumentAdded(count, path, text));
                            return count;
                        }, default);

                    Console.WriteLine("Added as document {0}", id.More);

                    continue;
                }

                if (line.StartsWith("folder "))
                {
                    var dir = line.Substring("folder ".Length);

                    foreach (var path in Directory.GetFiles(dir))
                    {
                        var text = File.ReadAllText(path);

                        if (text.Length > 65536)
                            text = text.Substring(0, 65536);

                        var id =
                            await svc.TransactionAsync(transaction =>
                            {
                                var count = transaction.State.Documents.Count;
                                transaction.Add(new DocumentAdded(count, path, text));
                                return count;
                            }, default);

                        Console.WriteLine("Added document {0} = {1}", id.More, path);
                    }

                    continue;
                }

                if (line.StartsWith("find "))
                {
                    var word = line.Substring("find ".Length).ToLower();

                    var state = await svc.CurrentState(default);
                    if (!state.Index.TryGetValue(word, out var list))
                    {
                        Console.WriteLine("Word '{0}' appears in 0 documents", word);
                        continue;
                    }

                    var docs = state.DocumentLists[list];
                    Console.WriteLine("Word '{0}' appears in {1} documents", word, docs.Count);
                    foreach (var doc in docs)
                        Console.WriteLine("  {0}: {1}", doc, state.Documents[doc].Path);

                    continue;
                }
            }

            cts.Cancel();
        }

        /// <summary>
        ///     Implements <see cref="ILogAdapter"/> to display logging information to 
        ///     the console, for illustration purposes.
        /// </summary>
        private sealed class Log : ILogAdapter
        {
            public void Debug(string message) => Write(ConsoleColor.Gray, message);
            public void Info(string message) => Write(ConsoleColor.White, message);
            public void Warning(string message, Exception ex = null) => Write(ConsoleColor.Yellow, message, ex);
            public void Error(string message, Exception ex = null) => Write(ConsoleColor.Red, message, ex);

            private void Write(ConsoleColor color, string message, Exception exception = null)
            {
                var c = Console.ForegroundColor;
                Console.ForegroundColor = color;

                try
                {
                    Console.WriteLine("[ES] {0}", message);

                    if (exception != null)
                        Console.WriteLine("    {0}: {1}", exception.GetType(), exception.Message);
                }
                finally
                {
                    Console.ForegroundColor = c;
                }
            }
        }
    }
}
