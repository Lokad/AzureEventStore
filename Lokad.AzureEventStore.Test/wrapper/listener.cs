using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Streams;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.wrapper
{
    [TestFixture]
    public sealed class listener
    {
        [DataContract]
        private class Ev
        {
            [DataMember]
            public int Value { get; set; }
        }

        private class Sta
        {
            public Sta(int sum) { Sum = sum; }
            public int Sum { get; }            
        }

        private class Project : IProjection<Ev, Sta>
        {
            public string FullName { get; } = "Project";
            public Type State { get; } = typeof(Sta);
            public Sta Initial { get; } = new Sta(0);

            public Sta Apply(uint sequence, Ev e, Sta previous) => new Sta(previous.Sum + e.Value);

            public Task<Sta> TryLoadAsync(Stream source, CancellationToken cancel) => 
                Task.FromResult(new Sta(new BinaryReader(source).ReadInt32()));

            public Task<bool> TrySaveAsync(Stream destination, Sta state, CancellationToken cancel)
            {
                using (var w = new BinaryWriter(destination, Encoding.UTF8, true)) 
                    w.Write(state.Sum);

                return Task.FromResult(true);
            }
        }

        private StorageConfiguration GetStream()
        {
            var s = Testing.InMemory;
            var es = new EventStream<Ev>(s);

            es.WriteAsync(new[]
            {
                new Ev {Value = 11},
                new Ev {Value = 25},
                new Ev {Value = 30}
            }, CancellationToken.None).Wait();

            return s;
        }

        [Test]
        public async Task all()
        {
            var evs = new List<int>();
            var seqs = new List<uint>();
            var ess = EventStreamService<Ev, Sta>.StartNew(
                GetStream(),
                new[] {new Project()},
                null,
                new[]
                {
                    Tuple.Create<EventStream<Ev>.Listener, uint>(
                        (e, s) =>
                        {
                            evs.Add(e.Value);
                            seqs.Add(s);
                        },
                        0u)
                },
                null,
                CancellationToken.None);

            await ess.Ready;

            CollectionAssert.AreEqual(new[] { 11, 25, 30 }, evs);
            CollectionAssert.AreEqual(new[] { 1u, 2u, 3u }, seqs);
            Assert.AreEqual(11 + 25 + 30, ess.LocalState.Sum);
        }

        [Test]
        public async Task skip_for_event()
        {
            var evs = new List<int>();
            var seqs = new List<uint>();
            var ess = EventStreamService<Ev, Sta>.StartNew(
                GetStream(),
                new[] { new Project() },
                null,
                new[]
                {
                    Tuple.Create<EventStream<Ev>.Listener, uint>(
                        (e, s) =>
                        {
                            evs.Add(e.Value);
                            seqs.Add(s);
                        },
                        2u)
                },
                null,
                CancellationToken.None);

            await ess.Ready;

            CollectionAssert.AreEqual(new[] { 25, 30 }, evs);
            CollectionAssert.AreEqual(new[] { 2u, 3u }, seqs);
            Assert.AreEqual(11 + 25 + 30, ess.LocalState.Sum);
        }


        [Test]
        public async Task skip_for_projection()
        {
            var stream = GetStream();
            var imm = new Testing.InMemoryCache();
            var ess1 = EventStreamService<Ev, Sta>.StartNew(
                stream,
                new[] { new Project() },
                imm,
                null,
                CancellationToken.None);

            await ess1.Ready;

            await ess1.TrySaveAsync();

            await ess1.AppendEventsAsync(new[] {new Ev {Value = 41}});

            var evs = new List<int>();
            var seqs = new List<uint>();
            var ess = EventStreamService<Ev, Sta>.StartNew(
                stream,
                new[] { new Project() },
                null,
                new[]
                {
                    Tuple.Create<EventStream<Ev>.Listener, uint>(
                        (e, s) =>
                        {
                            evs.Add(e.Value);
                            seqs.Add(s);
                        },
                        2u)
                },
                null,
                CancellationToken.None);

            await ess.Ready;

            CollectionAssert.AreEqual(new[] { 25, 30, 41}, evs);
            CollectionAssert.AreEqual(new[] { 2u, 3u, 4u }, seqs);
            Assert.AreEqual(11 + 25 + 30 + 41, ess.LocalState.Sum);
        }

    }
}
