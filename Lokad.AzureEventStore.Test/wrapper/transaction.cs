using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Wrapper;
using System;
using System.Collections.Immutable;
using System.IO;
using System.Runtime.Serialization;
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
        public async Task OnCommit()
        {
            var abortInvoked = false;
            var commitAInvoked = false;
            var commitBInvoked = false;

            var ew = Init();
            var value = await ew.TransactionAsync(transaction =>
            {
                transaction.OnAbort += () => abortInvoked = true;
                transaction.OnCommit += e =>
                {
                    Assert.Equal(1, e.Count);
                    Assert.Equal(15u, e[0].Value);
                    commitAInvoked = true;
                };
                transaction.Add(new TstEvent(15));
                transaction.OnCommit += _ => commitBInvoked = true;
                return transaction.State.Value[0];
            });

            Assert.True(commitAInvoked);
            Assert.True(commitBInvoked);
            Assert.False(abortInvoked);
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

        [Fact]
        public async Task OnAbort()
        {
            var abortInvoked = false;
            var commitInvoked = false;

            var ew = Init();
            var value = await ew.TransactionAsync(transaction =>
            {
                transaction.Add(new TstEvent(15));
                transaction.OnAbort += () => abortInvoked = true;
                transaction.OnCommit += _ => commitInvoked = true;
                transaction.Abort();
                return 20;
            });

            Assert.True(abortInvoked);
            Assert.False(commitInvoked);
        }

        [Fact]
        public async Task OnAbortException()
        {
            var abortInvoked = false;
            var commitInvoked = false;

            var ew = Init();
            try
            {
                await ew.TransactionAsync(transaction =>
                {
                    transaction.Add(new TstEvent(15));
                    transaction.OnAbort += () => abortInvoked = true;
                    transaction.OnCommit += _ => commitInvoked = true;
                    throw new Exception("Bad");
                });

                Assert.True(false);
            }
            catch
            {
                Assert.True(abortInvoked);
                Assert.False(commitInvoked);
            }
        }
    }
}
