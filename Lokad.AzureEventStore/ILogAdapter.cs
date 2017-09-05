using System;

namespace Lokad.AzureEventStore
{
    /// <summary> To enable logging, implement this interface. </summary>
    public interface ILogAdapter
    {
        /// <summary> Logs a debug message. </summary>
        void Debug(string message);

        /// <summary> Logs an information message. </summary>
        void Info(string message);

        /// <summary> Logs a warning, possibly with an exception. </summary>
        void Warning(string message, Exception ex = null);

        /// <summary> Logs an error, possibly with an exception. </summary>
        void Error(string message, Exception ex = null);        
    }
}
