using System.Diagnostics;

namespace Lokad.AzureEventStore;

public static class Logging
{
    /// <summary> Activities related to event streams. Low granularity. </summary>
    public static readonly ActivitySource Stream =
        new("Lokad.AzureEventStore.Stream");

    /// <summary> Activities related to drivers. High granularity. </summary>
    public static readonly ActivitySource Drivers =
        new("Lokad.AzureEventStore.Drivers");

    public const string EventCount = "events.count";
    public const string EventSize = "events.size";

    public const string PositionWrite = "position.write";
    public const string PositionStream = "position.stream";
    public const string PositionRead = "position.read";

    public const string SeqProjection = "seq.projection";

    public const string UpkeepKind = "upkeep.kind";

    public const string Attempts = "attempts";
}
