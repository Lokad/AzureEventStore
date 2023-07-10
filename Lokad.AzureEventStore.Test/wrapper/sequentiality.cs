using System;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Streams;
using Lokad.AzureEventStore.Wrapper;
using Xunit;

namespace Lokad.AzureEventStore.Test.wrapper
{
    [DataContract]
    public class TstEvent
    {
        public TstEvent(uint seq)
        {
            Seq = seq;
        }

        [DataMember(Name  = "Account", IsRequired = true, EmitDefaultValue = true)]
        public uint Seq { get; set; }
    }

    public class CheckSequence
    {
        private CheckSequence()
        {
            this.LastEvt = 0;
            this.ExpectedNextEvt = 1;
        }

        private CheckSequence(uint evtSeqNum, uint expectedEvtNextSeq)
        {
            this.LastEvt = evtSeqNum;
            this.ExpectedNextEvt = expectedEvtNextSeq;
        }

        public CheckSequence WithNewEvent(TstEvent evt)
        {
            Assert.Equal(ExpectedNextEvt, evt.Seq);
            return new CheckSequence(evt.Seq, evt.Seq+1);
        }

        public void Serialize(Stream s)
        {
            using (var w = new BinaryWriter(s, encoding: Encoding.Default, leaveOpen:true))
            {
                w.Write(LastEvt);
                w.Write(ExpectedNextEvt);
            }
        }

        public static CheckSequence Deserialize(Stream s)
        {
            using (var r = new BinaryReader(s, encoding: Encoding.Default, leaveOpen: true))
            {
                uint lastEvt = r.ReadUInt32();
                uint expectedNext = r.ReadUInt32();
                return new CheckSequence(lastEvt, expectedNext);
            }
        }

        public class Projection : IProjection<TstEvent, CheckSequence>
        {
            public string FullName => "CheckSeqProj-01";
            public Type State => typeof(CheckSequence);
            public CheckSequence Initial(StateCreationContext stateCreationContext) => new CheckSequence();

            public CheckSequence Apply(uint sequence, TstEvent e, CheckSequence previous)
            {
                Assert.Equal(sequence, e.Seq);
                return previous.WithNewEvent(e);
            }

            public Task<CheckSequence> TryLoadAsync(Stream source, CancellationToken cancel)
            {
                return Task.FromResult(CheckSequence.Deserialize(source));
            }

            public Task<bool> TrySaveAsync(Stream destination, CheckSequence state, CancellationToken cancel)
            {
                state.Serialize(destination);
                return Task.FromResult(true);
            }

            public Task<RestoredState<CheckSequence>> TryRestoreAsync(StateCreationContext stateCreationContext, CancellationToken cancel = default)
            {
                return Task.FromResult<RestoredState<CheckSequence>>(null);
            }

            public Task CommitAsync(CheckSequence state, uint sequence, CancellationToken cancel = default)
            {
                return Task.CompletedTask;
            }
        }

        public uint LastEvt { get; }
        public uint ExpectedNextEvt { get; }
    }


    public class sequentiality
    {
        [Fact]
        public async Task without_restart()
        {
            var memory = new MemoryStorageDriver();
            var ew = new EventStreamWrapper<TstEvent, CheckSequence>(
                memory,
                new []{new CheckSequence.Projection()},  
                null,
                new StorageProvider(null));
            await ew.InitializeAsync();
            await ew.AppendEventsAsync(new[] { new TstEvent(1) });
            await ew.AppendEventsAsync(new[] { new TstEvent(2) });
            await ew.AppendEventsAsync(new[] { new TstEvent(3) });

            Assert.Equal(3u, ew.Current.LastEvt);

            // try to read: 
            var ew2 = new EventStreamWrapper<TstEvent, CheckSequence>(
                memory, new[] {new CheckSequence.Projection()}, null, new StorageProvider(null));
            
            await ew2.InitializeAsync();
            Assert.Equal(3u, ew2.Current.LastEvt);
        }
        
        [Fact]
        public async Task with_restart()
        {
            var memory = new MemoryStorageDriver();
            var cache = new Testing.InMemoryCache();
            var ew = new EventStreamWrapper<TstEvent, CheckSequence>(
                memory,
                new []{new CheckSequence.Projection()},
                (EventStream<TstEvent> _) => cache,
                new StorageProvider(null));
            await ew.InitializeAsync();
            await ew.AppendEventsAsync(new[] { new TstEvent(1) });
            await ew.AppendEventsAsync(new[] { new TstEvent(2) });
            await ew.AppendEventsAsync(new[] { new TstEvent(3) });
            await ew.TrySaveAsync();
            await ew.AppendEventsAsync(new[] { new TstEvent(4) });
            await ew.AppendEventsAsync(new[] { new TstEvent(5) });

            Assert.Equal(5u, ew.Current.LastEvt);

            // try to read: 
            var ew2 = new EventStreamWrapper<TstEvent, CheckSequence>(
                memory, new[] {new CheckSequence.Projection()},
                (EventStream<TstEvent> _) => cache, new StorageProvider(null));

            await ew2.InitializeAsync();
            Assert.Equal(5u, ew2.Current.LastEvt);
        }
    }
}