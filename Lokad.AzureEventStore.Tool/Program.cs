using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Streams;
using Newtonsoft.Json.Linq;

namespace Lokad.AzureEventStore.Tool
{
    static partial class Program
    {
        static readonly IReadOnlyDictionary<string, Action<string[]>> Commands = new Dictionary<string, Action<string[]>>
        {
            { "use", CmdUse },
            { "alias", CmdAlias },
            { "backup", CmdBackup },
            { "append", CmdAppend },
            { "filter", CmdFilter },
            { "config", CmdConfig },
            { "fetch", CmdFetch },
            { "drop", CmdDrop },
            { "safe", CmdSafe },
            { "peek", CmdPeek }
        };

        static void Main()
        {
            // Increase the 255 maximum number of characters of the ReadLine method
            var bytes = new byte[2000];
            var inputStream = Console.OpenStandardInput(bytes.Length);
            Console.SetIn(new StreamReader(inputStream));

            Console.WriteLine("Azure Event Store - manipulation tool - v0.1");

            Console.ForegroundColor = ConsoleColor.White;
            Console.Write("> ");

            string line;
            while ((line = Console.ReadLine()) != null)
            {
                line = line.Trim();
                if (string.Equals(line, "quit", StringComparison.OrdinalIgnoreCase)) return;
                if (line == "")
                {
                    Console.Write("> ");
                    continue;
                }

                Console.ForegroundColor = ConsoleColor.Gray;

                var segs = line.Split(' ');
                Action<string[]> command;
                if (Commands.TryGetValue(segs[0], out command))
                {
                    var args = new List<string>();
                    for (var i = 1; i < segs.Length; ++i)
                    {
                        if (args.Count == 0 || !args[args.Count - 1].StartsWith("\"") ||
                            args[args.Count - 1].EndsWith("\""))
                            args.Add(segs[i]);
                        else
                            args[args.Count - 1] += " " + segs[i];
                    }

                    command(args.Select(a => a.StartsWith("\"") && a.EndsWith("\"")
                        ? a.Substring(1, a.Length - 2).Replace("\"\"", "\"")
                        : a).ToArray());
                }
                else
                {
                    Console.WriteLine("Unknown command `{0}`, available commands are:\n", segs[0]);
                    foreach (var k in Commands.Keys.Concat(new[] { "quit" }).OrderBy(s => s))
                        Console.WriteLine("  {0}", k);
                    Console.WriteLine("\nRun commands without parameters for help.");
                }

                Console.ForegroundColor = ConsoleColor.White;
                Console.Write("> ");
            }
        }

        private static void Status(string fmt, params object[] p)
        {
            Console.Write(fmt, p);
            Console.CursorLeft = 0;
        }

        private static string Parse(string value)
        {
            foreach (var k in ConfigurationManager.AppSettings.AllKeys)
            {
                if (value.Contains("$" + k))
                    value = value.Replace("$" + k, ConfigurationManager.AppSettings[k]);
            }

            return value;
        }

        private static void Show(EventData ed)
        {
            Console.WriteLine("Event #{0}", ed.Sequence);
            foreach (var k in ed.Event.Properties())
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.Write("  {0}: ", k.Name);
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine(k.Value.ToString());
            }
        }

        private static void Peek(IReadOnlyList<EventData> list)
        {
            if (list.Count == 0) return;
            Show(list[list.Count - 1]);
        }

        /// <summary> Displays the configuration values. </summary>
        private static void CmdConfig(string[] args)
        {
            Console.WriteLine("Available configuration keys: ");
            foreach (var key in ConfigurationManager.AppSettings.AllKeys)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.Write("  ${0} ", key);
                Console.ForegroundColor = ConsoleColor.Gray;
                Console.WriteLine(ConfigurationManager.AppSettings[key]);
            }
        }


        /// <summary> Drops events from <see cref="Fetched"/>. </summary>
        private static void CmdDrop(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: drop <name>");
                Console.WriteLine("Drops a fetched stream from memory.");
                return;
            }

            var name = args[0];
            IReadOnlyList<EventData> list;
            if (!Fetched.TryGetValue(name, out list))
            {
                Console.WriteLine("No fetched stream `{0}`.", name);
                return;
            }

            Console.WriteLine("Dropped `{0}` ({1} events).", name, list.Count);

            Fetched.Remove(name);
        }

        /// <summary> Fetches a stream's events and places it in <see cref="Fetched"/>. </summary>
        private static void CmdFetch(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage: fetch <name> <stream> <limit>?");
                Console.WriteLine("Downloads events from remote stream, stores in-memory.");
                Console.WriteLine("  limit: if provided, only fetch events after this seq (included).");
                return;
            }

            var name = args[0];
            if (Fetched.ContainsKey(name))
            {
                Console.WriteLine("Fetched stream `{0}` already exists.", name);
                Console.WriteLine("Use `drop {0}` to drop it.", name);
                return;
            }

            var limit = 0u;
            if (args.Length == 3)
            {
                if (!uint.TryParse(args[2], out limit))
                {
                    Console.WriteLine("Could not parse limit: {0}", args[2]);
                    return;
                }
            }

            var sw = Stopwatch.StartNew();

            var connection = new StorageConfiguration(Parse(args[1])) { ReadOnly = true };

            connection.Trace = true;

            var driver = connection.Connect();

            using (Release(driver))
            {
                var stream = new EventStream<JObject>(driver);
                var list = new List<EventData>();
                var start = 0L;

                Status("Connecting...");

                Task.Run((Func<Task>)(async () =>
                {
                    Status("Current size:");
                    var maxPos = await driver.GetPositionAsync();
                    Console.WriteLine("Current size: {0:F2} MB", maxPos / (1024.0 * 1024.0));

                    Status("Current seq:");
                    var maxSeq = await driver.GetLastKeyAsync();
                    Console.WriteLine("Current seq: {0}", maxSeq);

                    var asStatsDriver = driver as StatsDriverWrapper;
                    var asReadOnlyDriver = (asStatsDriver?.Inner ?? driver) as ReadOnlyDriverWrapper;
                    var asAzure = (asReadOnlyDriver?.Wrapped ?? asStatsDriver?.Inner ?? driver) as AzureStorageDriver;

                    if (asAzure != null)
                    {
                        for (var i = 0; i < asAzure.Blobs.Count; ++i)
                        {
                            Console.WriteLine("Blob {0}: {1:F2} MB from seq {2}",
                                asAzure.Blobs[i].Name,
                                asAzure.Blobs[i].Properties.ContentLength.Value / (1024.0 * 1024.0),
                                i < asAzure.FirstKey.Count ? asAzure.FirstKey[i] : maxSeq);
                        }
                    }

                    if (limit > 0)
                    {
                        Status($"Moving to seq {limit} ..");
                        await stream.DiscardUpTo(limit);
                        start = stream.Position;
                    }

                    Func<bool> more;
                    do
                    {
                        var fetch = stream.BackgroundFetchAsync();

                        JObject obj;
                        while ((obj = stream.TryGetNext()) != null)
                            list.Add(new EventData(stream.Sequence, obj));

                        Status("Fetching: {0}/{1} ({2:F2}/{3:F2} MB)",
                            stream.Sequence,
                            maxSeq,
                            (stream.Position - start) / (1024.0 * 1024.0),
                            (maxPos - start) / (1024.0 * 1024.0));

                        more = await fetch;
                    } while (more());

                })).Wait();

                Console.WriteLine("Fetched `{0}` ({1} events, {2:F2} MB) in {3:F2}s.",
                    name,
                    list.Count,
                    (stream.Position - start) / (1024.0 * 1024.0),
                    sw.ElapsedMilliseconds / 1000.0);

                Fetched.Add(name, list);

                Peek(list);

                if (Current == null) Current = list;
            }
        }

        private static void CmdAppend(string[] args)
        {
            if (args.Length != 1 && args.Length != 2)
            {
                Console.WriteLine("Usage: append <stream> <local>?");
                Console.WriteLine("Appends events to a stream, discarding sequence ids.");
                return;
            }

            // The stream from which to append
            IReadOnlyList<EventData> from;
            if (args.Length == 1)
            {
                if (Current == null)
                {
                    Console.WriteLine("No current stream available.");
                    return;
                }

                from = Current;
            }
            else
            {
                if (!Fetched.TryGetValue(args[1], out from))
                {
                    Console.WriteLine("Stream '{0}' does not exist.", args[1]);
                    return;
                }
            }

            // The stream to which to append
            var into = args[0];

            var dstDriver = new StorageConfiguration(Parse(into)).Connect();
            using (Release(dstDriver))
            {
                var dst = new MigrationStream<JObject>(dstDriver);

                var sw = Stopwatch.StartNew();
                var events = 0;

                Task.Run((Func<Task>)(async () =>
                {
                    Status("Current destination seq: ...");
                    var bakSeq = await dstDriver.GetLastKeyAsync();
                    Console.WriteLine("Current destination seq: {0}", bakSeq);

                    ++bakSeq;

                    while (events < @from.Count)
                    {
                        Status("Appending: {0}/{1}",
                            events,
                            @from.Count);

                        var list = new List<KeyValuePair<uint, JObject>>();
                        for (var i = 0; i < 1000 && events < @from.Count; ++i, ++events, ++bakSeq)
                            list.Add(new KeyValuePair<uint, JObject>(bakSeq, @from[events].Event));

                        events += list.Count;

                        await dst.WriteAsync(list);

                    }

                })).Wait();

                Console.WriteLine("Appended {0} events in {1:F2}s.",
                    events,
                    sw.ElapsedMilliseconds / 1000.0);
            }

        }

        /// <summary> Filters the current stream. </summary>
        private static void CmdFilter(string[] args)
        {
            var sw = Stopwatch.StartNew();

            if (args.Length == 0)
            {
                Console.WriteLine("Usage: filter <expression>");
                Console.WriteLine("Filters the current stream according to expressions. Examples:");
                Console.WriteLine("  filter Type == 'abc' ");
                Console.WriteLine("  filter Date < '2015-05-07' ");
                Console.WriteLine("  filter Error");
                return;
            }

            Func<uint, JObject, bool> condition;

            if (args.Length == 1)
            {
                var name = args[0];
                condition = (s, j) =>
                {
                    var p = j[name];
                    if (p == null) return false;
                    var jv = p as JValue;
                    if (jv == null) return true;
                    if (jv.Type == JTokenType.Boolean) return (bool)jv.Value;
                    if (jv.Type == JTokenType.String) return !string.IsNullOrEmpty((string)jv.Value);
                    if (jv.Type == JTokenType.Integer) return 0 != (int)jv.Value;
                    if (jv.Type == JTokenType.Float) return Math.Abs((double)jv.Value) > 0.001;
                    if (jv.Type == JTokenType.Null) return false;
                    return true;
                };
            }
            else if (args.Length == 3)
            {
                var data = Expression.Parameter(typeof(JObject));
                var seq = Expression.Parameter(typeof(uint));

                var left = ParseOperand(args[0], seq, data);
                var op = args[1];
                var right = ParseOperand(args[2], seq, data);
                var compare = Expression.Call(typeof(Program), nameof(CompareJValue), new Type[0], left, right);

                Expression result;

                switch (op)
                {
                    case "==":
                        result = Expression.Equal(compare, Expression.Constant(0));
                        break;
                    case "<":
                        result = Expression.LessThan(compare, Expression.Constant(0));
                        break;
                    case "<=":
                        result = Expression.LessThanOrEqual(compare, Expression.Constant(0));
                        break;
                    case ">":
                        result = Expression.GreaterThan(compare, Expression.Constant(0));
                        break;
                    case ">=":
                        result = Expression.GreaterThanOrEqual(compare, Expression.Constant(0));
                        break;
                    case "!=":
                        result = Expression.NotEqual(compare, Expression.Constant(0));
                        break;
                    default:
                        Console.WriteLine("Unknown comparison: {0}", op);
                        return;
                }

                var lambda = Expression.Lambda<Func<uint, JObject, bool>>(result, seq, data);
                condition = lambda.Compile();
            }
            else
            {
                Console.WriteLine("Could not parse expression.");
                return;
            }

            var list = Current.Where(s => condition(s.Sequence, s.Event)).ToArray();

            if (list.Length == 0)
            {
                Console.WriteLine("No events match the condition. Filter not applied.");
                return;
            }

            Console.WriteLine("Kept {0}/{1} events in {2:F2} s.",
                list.Length, Current.Count, sw.ElapsedMilliseconds / 1000.0);

            Current = list;
            Peek(Current);
        }

        /// <summary> Compares JValues. Called by computed expression. </summary>
        private static int CompareJValue(JValue a, JValue b)
        {
            if (a == null && b == null) return 0;
            if (a == null) return -1;
            if (b == null) return 1;

            if (a.Type == JTokenType.Boolean)
                if (b.Type == JTokenType.Boolean)
                    return Comparer<bool>.Default.Compare((bool)a.Value, (bool)b.Value);

            if (a.Type == JTokenType.String)
                if (b.Type == JTokenType.String)
                    return string.Compare((string)a.Value, (string)b.Value, StringComparison.Ordinal);

            if (a.Type == JTokenType.Float)
            {
                if (b.Type == JTokenType.Float)
                    return Comparer<float>.Default.Compare((float)a.Value, (float)b.Value);
                if (b.Type == JTokenType.Integer)
                    return Comparer<float>.Default.Compare((float)a.Value, (long)b.Value);
            }
            else if (a.Type == JTokenType.Integer)
            {
                if (b.Type == JTokenType.Float)
                    return Comparer<float>.Default.Compare((long)a.Value, (float)b.Value);
                if (b.Type == JTokenType.Integer)
                    return Comparer<long>.Default.Compare((long)a.Value, (long)b.Value);
            }

            return Comparer<JTokenType>.Default.Compare(a.Type, b.Type);
        }

        /// <summary>
        /// An operand, returns a JValue.
        /// </summary>
        private static Expression ParseOperand(string token, ParameterExpression seq, ParameterExpression obj)
        {
            if (token.Equals("seq", StringComparison.OrdinalIgnoreCase))
            {
                return Expression.New(
                    // ReSharper disable once AssignNullToNotNullAttribute
                    typeof(JValue).GetConstructor(new[] { typeof(long) }),
                    Expression.Convert(seq, typeof(long)));
            }

            if (token.StartsWith("'") && token.EndsWith("'"))
            {
                token = token.Substring(1, token.Length - 2).Replace("''", "'");
                return Expression.New(
                    // ReSharper disable once AssignNullToNotNullAttribute
                    typeof(JValue).GetConstructor(new[] { typeof(string) }),
                    Expression.Constant(token));
            }

            long asInt;
            if (long.TryParse(token, out asInt))
            {
                return Expression.New(
                    // ReSharper disable once AssignNullToNotNullAttribute
                    typeof(JValue).GetConstructor(new[] { typeof(long) }),
                    Expression.Constant(asInt));
            }

            double asNum;
            if (double.TryParse(token, out asNum))
            {
                return Expression.New(
                    // ReSharper disable once AssignNullToNotNullAttribute
                    typeof(JValue).GetConstructor(new[] { typeof(double) }),
                    Expression.Constant(asNum));
            }

            return Expression.Convert(
                Expression.Call(obj, "GetValue", new Type[0], Expression.Constant(token)),
                typeof(JValue));
        }

        /// <summary> Aliases the current stream under a certain name. </summary>
        private static void CmdAlias(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: alias <name>");
                Console.WriteLine("Aliases current stream under provided name.");
                return;
            }

            var name = args[0];
            if (Fetched.ContainsKey(name))
            {
                Console.WriteLine("Stream `{0}` already exists.", name);
                Console.WriteLine("Use `drop {0}` to drop it.", name);
                return;
            }

            if (Current == null)
            {
                Console.WriteLine("No current stream to alias.");
                return;
            }

            Console.WriteLine("Aliased `{0}` ({1} events).", name, Current.Count);
            Fetched.Add(name, Current);
        }

        /// <summary> Use the named fetched stream as the current one. </summary>
        private static void CmdUse(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: use <name>");
                Console.WriteLine("Uses named stream as current one.");
                return;
            }

            var name = args[0];
            IReadOnlyList<EventData> list;
            if (!Fetched.TryGetValue(name, out list))
            {
                Console.WriteLine("No fetched stream `{0}`.", name);
                return;
            }

            Console.WriteLine("Current is `{0}` ({1} events).", name, list.Count);

            Current = list;
            Peek(list);
        }

        /// <summary> Peek at the end of a stream. </summary>
        private static void CmdPeek(string[] args)
        {
            if (args.Length == 0)
                Console.WriteLine("Usage: peek <count> ; peek <name> <count>");

            var stream = Current;
            var count = 10;

            if (args.Length == 1)
                count = int.Parse(args[0]);

            if (args.Length == 2)
            {
                count = int.Parse(args[1]);
                if (!Fetched.TryGetValue(args[0], out stream))
                {
                    Console.WriteLine("No fetched stream `{0}`.", args[0]);
                    return;
                }
            }

            if (stream == null)
            {
                Console.WriteLine("No current stream.");
                return;
            }

            if (count < 1) count = 1;

            for (var i = 0; i < count && i < stream.Count; ++i)
            {
                Show(stream[stream.Count - i - 1]);
            }
        }

        /// <summary> The sequence number and JSON data for an event. </summary>
        public struct EventData
        {
            public readonly uint Sequence;
            public readonly JObject Event;

            public EventData(uint sequence, JObject e) : this()
            {
                Sequence = sequence;
                Event = e;
            }
        }

        #region State

        static readonly Dictionary<string, IReadOnlyList<EventData>> Fetched =
            new Dictionary<string, IReadOnlyList<EventData>>();

        static IReadOnlyList<EventData> Current { get; set; }

        #endregion

        #region Release

        /// <summary> Disposes a storage driver when no longer used. </summary>
        private static IDisposable Release(IStorageDriver sd)
        {
            while (true)
            {
                var release = sd as IDisposable;
                if (release != null) return release;

                var asWrap = sd as ReadOnlyDriverWrapper;
                if (asWrap == null) return new Disposable();

                sd = asWrap.Wrapped;
            }
        }

        private sealed class Disposable : IDisposable
        {
            public void Dispose() { }
        }

        #endregion
    }
}
