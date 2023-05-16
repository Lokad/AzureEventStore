# Readiness of the event stream

This document clarifies as aspect of the CQRS+ES design when leveraging `Lokad.AzureEventStore` as it is typically done at Lokad.

The typical CQRS backend, as designed by Lokad, includes a `StateService` and a `StateServiceUnavailableException` (see annex).

There are two cases where `StateServiceUnavailableException` is thrown.

**At startup**, when reading from the event stream to produce the up-to-date state, there is a period when the state cannot be requested with `Stream.CurrentState`. This will immediately throw a `StreamNotReadyException`, which is then wrapped in a `StateServiceUnavailableException` with the message “StateService is not ready yet”. The only way to prevent this is to make the loading of the state faster (e.g. by using a cache), but it is not reasonable to expect the startup time to go below ~3 seconds. Anyway, this only applies when restarting the application.

**During normal execution**, it may happen that an attempt to refresh the state times out (because the communication with the Azure Blob takes longer then 5 seconds, when it should normally take ~15ms). This will also throw a `StateServiceUnavailableException`, but with the message `"StateService cannot be refreshed"`. When this happens, there is a usually communication issue with Azure Blob Storage, and 10 seconds will usually not solve the problem. We have observed delays up to 15 minutes to restore the Blob Storag downtime.

In both cases, the application will not be able to serve the page that was requested. As a rule of thumb, it's preferrable to keep exceptions in the logs, as it is preferrable to know that the problem happened in the first place. However, it is also possible to reduce the frequency of those occurences.

For the startup unavailability, await `Stream.Ready` before launching the web server. By doing so, the web server will not accept requests before it can handle them. This will result in the user receiving HTTP 504 errors from nginx, instead of HTTP 503 errors from the application, so we wouldn’t recommend it. If high availability is needed, then using a load balancer with a _status endpoint that tracks `Stream.IsReady` is better, so that the requests are forwarded to the application that can respond to them.

It is also possible add more resilience for unavailability during normal execution, for instance by using `.LocalState` instead of `.CurrentState` for non-authenticated users (the local state is not guaranteed to contain up-to-date values especially after a change was made, but it will never fail), or by having multiple servers and (instead of returning an HTTP 503 error page) to return a page with a bit of JavaScript to auto-retry the request and hope to hit the other server.

## Annex: C# fragment of CQRS

```csharp
using Lokad.AzureEventStore;
using Lokad.AzureEventStore.Projections;
using Foobar.Backend.Domain.Events;
using Foobar.Backend.Domain.Projections;
using Foobar.Backend.Domain.State;

namespace Foobar.Backend.Domain.Host;

public class StateServiceUnavailableException : Exception
{
    public StateServiceUnavailableException(Exception e) : base("StateService is not ready yet.", e)
    {
    }

    public StateServiceUnavailableException(string msg) : base(msg)
    {
    }
}

/// <summary> Provides read-write access to the shared application state. </summary>
public sealed class StateService
{
    public readonly EventStreamService<INewsEvent, NewsState> Stream;

    public static StateService StartNew(
        StorageConfiguration storage,
        IProjectionCacheProvider projectionCache,
        CancellationToken cancel)
    {
        return new StateService(storage, projectionCache, cancel);
    }

    private StateService(
        StorageConfiguration storage,
        IProjectionCacheProvider projectionCache,
        CancellationToken cancel)
    {
        var proj = new NewsStateProjection();

        Stream = EventStreamService<INewsEvent, NewsState>.StartNew(
            storage,
            new IProjection<INewsEvent>[] { proj },
            projectionCache,
            new LogAdapter(),
            cancel);

        Stream.Ready.ContinueWith(_ => Stream.TrySaveAsync(cancel));
    }

    /// <summary> Is the state ready ?  </summary>
    /// <remarks> Accessing the state before it's ready will throw exceptions. </remarks>
    public bool Ready => Stream.IsReady;

    /// <summary> The current state (same as from <see cref="Stream"/>) updated with the last changes. </summary>    
    public async Task<NewsState> Current(CancellationToken cancel)
    {
        using var cts = new CancellationTokenSource();

        try
        {
            var race = Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
            var state = Stream.CurrentState(cancel);

            await Task.WhenAny(race, state).ConfigureAwait(false);

            if (state.IsCompleted)
                return (await state);

            throw new StateServiceUnavailableException("StateService cannot be refreshed.");
        }
        catch (StreamNotReadyException e)
        {
            throw new StateServiceUnavailableException(e);
        }
        finally
        {
            cts.Cancel();
        }
    }

    /// <summary> The local current state, which may not have catched up at the call time. </summary>
    /// <remarks> To have an updated current state, call <see cref="Current"/>. </remarks>
    public FoobarState LocalCurrent => Stream.LocalState;
}
```
