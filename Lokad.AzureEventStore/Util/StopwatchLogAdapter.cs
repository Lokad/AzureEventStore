using System;
using System.Diagnostics;

namespace Lokad.AzureEventStore.Util
{
    public static class LogAdapterExtension
    {
        private class TimestampLogAdapter : ILogAdapter
        {
            private readonly ILogAdapter _log;
            private readonly Stopwatch _sw;

            internal TimestampLogAdapter(ILogAdapter target)
            {
                _log = target ?? throw new ArgumentNullException(nameof(target));
                _sw = Stopwatch.StartNew();
            }

            private string Elapsed => _sw.Elapsed.ToString("mm':'ss'.'ff");

            public void Debug(string message)
            {
                _log.Debug(Elapsed + " " + message);
            }

            public void Info(string message)
            {
                _log.Info(Elapsed + " " + message);
            }

            public void Warning(string message, Exception ex = null)
            {
                _log.Warning(Elapsed + " " + message, ex);
            }

            public void Error(string message, Exception ex = null)
            {
                _log.Error(Elapsed + " " + message, ex);
            }
        }


        public static ILogAdapter Timestamped(this ILogAdapter log)
        {
            if (log == null)
                return null;

            return new TimestampLogAdapter(log);
        }
    }
}
