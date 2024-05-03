namespace Lokad.AzureEventStore.Wrapper
{
    /// <summary> The result of applying a transaction to the stream. </summary>
    /// <param name="Success"> Indicate if the event appending was successful or not. </param>
    /// <param name="Result"> Result of the event appending</param>
    public record TransactionResult(AppendResult Result, bool Success);

    /// <summary> The result of applying a transaction to the stream. </summary>
    /// <param name="Success"> Indicate if the event appending was successful or not. </param>
    /// <param name="Result"> Result of the event appending</param>
    public record TransactionResult<T>(AppendResult<T> Result, bool Success);
}