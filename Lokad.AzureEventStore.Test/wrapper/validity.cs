using System;
using System.IO;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Wrapper;
using Xunit;
using Lokad.AzureEventStore.Cache;

namespace Lokad.AzureEventStore.Test.wrapper
{
    // Testing of EventStreamWrapper.CheckEvents
    public class events_validity
    {
        public sealed class VState
        {
            public VState(uint Sequence)
            {
                this.SeqState = Sequence;
            }
            public uint SeqState { get; private set; }
        }
        [DataContract]
        public sealed class VEvent
        {
            public VEvent(bool valid=true)
            {
                IsValid = valid;
            }
            [DataMember(Name = "TEvent", IsRequired = true, EmitDefaultValue = false)]
            public bool IsValid { get; private set; }
        }
        public class Projection : IProjection<VEvent, VState>
        {
            public Type State => typeof(VState);
            public VState Apply(uint _, VEvent e, VState s)
            {
                if (!e.IsValid)
                    throw new Exception("Validity test failed");
                return new VState(s.SeqState + 1);
            }
            public Task<VState> TryLoadAsync(Stream source, CancellationToken cancel)
            {
                // Load is not supported.
                return Task.FromResult(new VState(0));
            }

            public Task<bool> TrySaveAsync(Stream destination, VState state, CancellationToken cancel)
            {
                // Save is not supported.
                return Task.FromResult(true);
            }

            public VState Initial(StateCreationContext stateCreationContext) => new VState(0);

            public Task<RestoredState<VState>> TryRestoreAsync(StateCreationContext stateCreationContext, CancellationToken cancel = default)
            {
                return Task.FromResult<RestoredState<VState>>(null);
            }

            public Task CommitAsync(VState state, uint sequence, CancellationToken cancel = default)
            {
                return Task.CompletedTask;
            }

            public string FullName => "CheckValProj-01";
        }

        public sealed class TestLog : ILogAdapter
        {
            public void Debug(string message) => Write(ConsoleColor.Gray, message);
            public void Info(string message) => Write(ConsoleColor.White, message);
            public void Warning(string message, Exception ex = null) => Write(ConsoleColor.Yellow, message, ex);
            public void Error(string message, Exception ex = null) => Write(ConsoleColor.Red, message, ex);
            private void Write(ConsoleColor color, string message, Exception exception = null) { }
        }

        [Fact]
        public async Task valid_events_append()
        {
            var memory = new MemoryStorageDriver();
            var ew = new EventStreamWrapper<VEvent, VState>(
                memory,
                new[] { new Projection() }, 
                null, 
                new StorageProvider(null),
                new TestLog());
            await ew.InitializeAsync();
            await ew.AppendEventsAsync(new[] { 
                new VEvent(true), 
                new VEvent(true), 
                new VEvent(true),
                new VEvent(true),
                new VEvent(true),
            });
            Assert.Equal(5u, ew.Current.SeqState);            
        }

        [Fact]
        public async Task invalid_events_append()
        {
            var memory = new MemoryStorageDriver();
            var ew = new EventStreamWrapper<VEvent, VState>(
                memory,
                new[] { new Projection() }, 
                null, 
                new StorageProvider(null),
                new TestLog());
            await ew.InitializeAsync();
            try
            {
                await ew.AppendEventsAsync(new[] {
                    new VEvent(true),
                    new VEvent(true),
                    new VEvent(true),
                    new VEvent(false)
                });
            }
            catch (Exception ex)
            {
                // Checking if the state is untouched.
                Assert.Equal(0u, ew.Current.SeqState);
                // Checking the error type.
                Assert.Equal("An error occured while trying to apply a projection.", ex.Data["TryApplyMessage"].ToString()); 
                // If an exception was thrown, then it is ok and a corrupted event sequence does not pass.
                return;
            }
            throw new Exception("Expecting an exception in EventStreamWrapper.CheckEvents.");
        }

        [Fact]
        public async Task invalid_events_transaction()
        {
            var memory = new MemoryStorageDriver();
            var ew = new EventStreamWrapper<VEvent, VState>(
                memory,
                new[] { new Projection() }, 
                null, 
                new StorageProvider(null),
                new TestLog());
            await ew.InitializeAsync();
            try
            {
                await ew.TransactionAsync(transaction => 
                {
                    transaction.Add(new VEvent(true));
                    transaction.Add(new VEvent(true));
                    transaction.Add(new VEvent(true));
                    transaction.Add(new VEvent(false));
                });
            }
            catch (Exception ex)
            {
                // Checking if the state is untouched.
                Assert.Equal(0u, ew.Current.SeqState);
                // Checking the error type.
                Assert.Equal("Validity test failed", ex.Message);
                // If an exception was thrown, then it is ok and a corrupted event sequence does not pass.
                return;
            }
            throw new Exception("Expecting an exception in transaction.");
        }

        [Fact]
        public async Task valid_events_transaction()
        {
            var memory = new MemoryStorageDriver();
            var ew = new EventStreamWrapper<VEvent, VState>(
                memory,
                new[] { new Projection() }, 
                null, 
                new StorageProvider(null),
                new TestLog());
            await ew.InitializeAsync();
            await ew.TransactionAsync(transaction =>
            {
                transaction.Add(new VEvent(true));
                transaction.Add(new VEvent(true));
                transaction.Add(new VEvent(true));
                transaction.Add(new VEvent(true));
                transaction.Add(new VEvent(true));
            });
            Assert.Equal(5u, ew.Current.SeqState);
        }
    }
}
