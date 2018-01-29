using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;

namespace Lokad.AzureEventStore.Wrapper
{
    internal interface IInitFacade
    {
        Task<IReifiedProjection> Projection { get; }
        Task<uint> DiscardStreamUpTo(uint seq);
        void Reset();
    }
}
