using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Wrapper;
using System;
using System.Collections.Immutable;
using System.Diagnostics;
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
            public TstEvent(int value)
            {
                Value = value;
            }

            [DataMember]
            public int Value { get; set; }
        }

        public record State(ImmutableArray<int> Value);

        public class Projection : IProjection<TstEvent, State>
        {
            public string FullName => "Test-01";
            public Type State => typeof(State);
            public State Initial(StateCreationContext stateCreationContext) => new State(ImmutableArray<int>.Empty);

            public State Apply(uint sequence, TstEvent e, State previous) =>
                e.Value < 0 
                    ? throw new ArgumentException("Expected positive") 
                    : new State(previous.Value.Add(e.Value));

            public Task<State> TryLoadAsync(Stream source, CancellationToken cancel) =>
                throw new NotSupportedException();

            public Task<bool> TrySaveAsync(Stream destination, State state, CancellationToken cancel) =>
                Task.FromResult(false);

            public Task<RestoredState<State>> TryRestoreAsync(StateCreationContext stateCreationContext, CancellationToken cancel = default)
            {
                return Task.FromResult<RestoredState<State>>(null);
            }

            public Task CommitAsync(State state, uint sequence, CancellationToken cancel = default)
            {
                return Task.CompletedTask;
            }

            public Task<State> UpkeepAsync(State state, CancellationToken cancel = default)
            {
                return Task.FromResult(state);
            }
        }

        private async Task<EventStreamWrapper<TstEvent, State>> Init()
        {
            var ew = new EventStreamWrapper<TstEvent, State>(
                new MemoryStorageDriver(),
                new[] { new Projection() }, 
                null,
                new StorageProvider(null));
            await ew.InitializeAsync();
            return ew;
        }
            

        [Fact]
        public async Task AppendNothing()
        {
            var ew = await Init();
            var value = await ew.TransactionAsync(transaction => 10);

            Assert.Equal(10, value.More);
            Assert.Empty(ew.Current.Value);
        }

        [Fact]
        public async Task AppendOne()
        {
            var ew = await Init();
            var value = await ew.TransactionAsync(transaction =>
            {
                transaction.Add(new TstEvent(15));
                return transaction.State.Value[0];
            });

            Assert.Equal(15, value.More);
            Assert.Equal(new[] { 15 }, ew.Current.Value);
        }

        [Fact]
        public async Task AppendTwo()
        {
            var ew = await Init();
            var value = await ew.TransactionAsync(transaction =>
            {
                Assert.Empty(transaction.State.Value);
                transaction.Add(new TstEvent(15));
                Assert.Single(transaction.State.Value);
                transaction.Add(new TstEvent(20));
                Assert.Equal(2, transaction.State.Value.Length);
            });

            Assert.Equal(2, value.Count);
            Assert.Equal(new[] { 15, 20 }, ew.Current.Value);
        }

        [Fact]
        public async Task AppendTwice()
        {
            var ew = await Init();
            await ew.TransactionAsync(transaction => 
                transaction.Add(new TstEvent(10)));

            var value = await ew.TransactionAsync(transaction =>
            {
                Assert.Single(transaction.State.Value);
                transaction.Add(new TstEvent(15));
                Assert.Equal(2, transaction.State.Value.Length);
                transaction.Add(new TstEvent(20));
                Assert.Equal(3, transaction.State.Value.Length);
            });

            Assert.Equal(2, value.Count);
            Assert.Equal(new[] { 10, 15, 20 }, ew.Current.Value);
        }

        [Fact]
        public async Task AppendThenThrow()
        {
            var ew = await Init();
            var value = await ew.TransactionAsync(transaction =>
                transaction.Add(new TstEvent(15)));

            try
            {
                await ew.TransactionAsync(transaction =>
                {
                    transaction.Add(new TstEvent(-1));
                    Assert.True(false);
                });
            }
            catch (ArgumentException e)
            {
                Assert.Equal("Expected positive", e.Message);
            }
        }

        [Fact]
        public async Task ThrowInterrupts()
        {
            var ew = await Init();

            try
            {
                await ew.TransactionAsync(transaction =>
                {
                    transaction.Add(new TstEvent(-1));
                    Assert.True(false);
                });
            }
            catch (ArgumentException e)
            {
                Assert.Equal("Expected positive", e.Message);
            }
        }

        [Fact]
        public async Task OnCommit()
        {
            var abortInvoked = false;
            var commitAInvoked = false;
            var commitBInvoked = false;

            var ew = await Init();
            var value = await ew.TransactionAsync(transaction =>
            {
                transaction.OnAbort += () => abortInvoked = true;
                transaction.OnCommit += e =>
                {
                    Assert.Equal(1, e.Count);
                    Assert.Equal(15, e[0].Value);
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
            var ew = await Init();
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

            var ew = await Init();
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

            var ew = await Init();
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
