using System;
using System.Threading;
using ExampleProject.Events;
using Lokad.AzureEventStore;

namespace ExampleProject
{
    public sealed class Program
    {
        public const string AccountName = "";
        public const string AccountKey = "";
        public const string ConnectionString = @"DefaultEndpointsProtocol=https;AccountName=" + AccountName + ";AccountKey=" + AccountKey + ";Container=exampleproject";

        static void Main(string[] args)
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
                projectionCache: null,
                // This is used by the service to emit messages about what is happening
                log: new Log(),
                // This cancellation token stops the background process.
                cancel: cts.Token);

            while (true)
            {
                var line = Console.ReadLine();
                if (line == null || line == "QUIT") break;

                if (!svc.IsReady)
                {
                    // The service starts catching up with the stream. This may
                    // take a short while if the stream becomes very long.
                    Console.WriteLine("Service not ready, please wait.");
                    continue;
                }

                var delete = line.StartsWith("--");
                if (delete) line = line.Substring(2);
                
                var words = line.Split(' ');
                foreach (var word in words)
                {
                    // Append either an update or a deletion event for each word in the
                    // provided sentence, then display the new count for that word.
                    var newCount = svc.AppendEventsAsync(s =>
                    {
                        // Look at the current count (and whether the word exists)
                        var exists = s.Bindings.TryGetValue(word, out int currentCount);
                        
                        // Deleting the entry: only generate a deletion event
                        // if there was an entry present.
                        if (delete)
                            return exists
                                ? svc.With(0, new ValueDeleted(word))
                                : svc.With(0);
                        
                        // Incrementing the entry
                        return svc.With(currentCount + 1, new ValueUpdated(word, currentCount + 1));

                    }, CancellationToken.None).Result.More;

                    Console.WriteLine("{0} -> {1}", word, newCount);
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
