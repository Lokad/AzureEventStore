using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Wrapper;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Lokad.AzureEventStore.Test.wrapper
{
    public class transaction
    {
        [DataContract]
        public class TstEvent
        {
            public TstEvent(uint value)
            {
                Value = value;
            }

            [DataMember]
            public uint Value { get; set; }
        }

        public record State(ImmutableArray<uint> Value);

        public class Projection : IProjection<TstEvent, State>
        {
            public string FullName => "Test-01";
            public Type State => typeof(State);
            public State Initial => new State(ImmutableArray<uint>.Empty);

            public State Apply(uint sequence, TstEvent e, State previous) =>
                new State(previous.Value.Add(e.Value));

            public Task<State> TryLoadAsync(Stream source, CancellationToken cancel) =>
                throw new NotSupportedException();

            public Task<bool> TrySaveAsync(Stream destination, State state, CancellationToken cancel) =>
                Task.FromResult(false);
        }

        private EventStreamWrapper<TstEvent, State> Init() =>
            new EventStreamWrapper<TstEvent, State>(
                new MemoryStorageDriver(),
                new[] { new Projection() }, null
            );

        [Fact]
        public async Task AppendNothing()
        {
            var ew = Init();
            var value = await ew.TransactionAsync(transaction => 10);

            Assert.Equal(10, value.More);
            Assert.Empty(ew.Current.Value);
        }

        [Fact]
        public async Task AppendOne()
        {
            var ew = Init();
            var value = await ew.TransactionAsync(transaction =>
            {
                transaction.Add(new TstEvent(15));
                return transaction.State.Value[0];
            });

            Assert.Equal(15u, value.More);
            Assert.Equal(new[] { 15u }, ew.Current.Value);
        }

        [Fact]
        public async Task Abort()
        {
            var ew = Init();
            var value = await ew.TransactionAsync(transaction =>
            {
                transaction.Add(new TstEvent(15));
                transaction.Abort();
                return 20;
            });

            Assert.Equal(20, value.More);
            Assert.Empty(ew.Current.Value);
        }
    }
}
