using System;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Test.streams;
using Lokad.AzureEventStore.Wrapper;
using Moq;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.wrapper
{
    [TestFixture]
    public class initialization
    {
        private static Task<IReifiedProjection> ProjectionWithSeqnum(uint seqnum)
        {
            var mockedProjection =  new Mock<IReifiedProjection>();
            mockedProjection.SetupGet(proj => proj.Sequence).Returns(seqnum);
            return Task.FromResult(mockedProjection.Object);
        }

        internal class TestFacade : IInitFacade
        {
            private readonly Action _onReset;
            public int NbResets { get; private set; }

            public TestFacade(uint projectionSeq, uint latestEvent)
            {
                ProjectionSeqNum = projectionSeq;
                Projection = ProjectionWithSeqnum(projectionSeq);

                LatestEvent = latestEvent;
            }

            public uint ProjectionSeqNum { get; }
            public uint LatestEvent { get; }

            public Task<IReifiedProjection> Projection { get;  }

            public virtual Task<uint> DiscardStreamUpTo(uint seq)
                => Task.FromResult( event_stream_discard.ExpectedSequenceAfterDiscard(LatestEvent, seq));

            public void Reset()
            {
                NbResets++;
            }
        }

        #region test AdvanceStream
        [Test]
        public async Task advance_on_empty_stream()
        {
            var mock = new Mock<TestFacade>(MockBehavior.Default, 0u, 0u);
            var facade = mock.Object;
            await Initialization.Run(facade);

            mock.Verify( f => f.DiscardStreamUpTo(1));
        }

        [Test]
        public async Task advance_on_non_empty_stream_no_projection()
        {
            var mock = new Mock<TestFacade>(MockBehavior.Default, 0u, 10u);
            var facade = mock.Object;
            await Initialization.Run(facade);

            mock.Verify( f => f.DiscardStreamUpTo(1));
        }

        [Test]
        public async Task advance_on_regular_stream_projection()
        {
            var mock = new Mock<TestFacade>(MockBehavior.Default, 100u, 110u);
            var facade = mock.Object;
            await Initialization.Run(facade);

            mock.Verify( f => f.DiscardStreamUpTo(101));
        }
        #endregion

        #region test with no listeners
        [Test]
        public async Task empty_stream()
        {
            var facade = new TestFacade(0, 0);
            await Initialization.Run(facade);
            // no reset should be performed
            Assert.AreEqual(0, facade.NbResets);
        }

        [Test]
        public async Task regular_startup_1_event_ahead()
        {
            int nbReset = 0;
            var facade = new TestFacade( 100, 101);
            await Initialization.Run(facade);
            Assert.AreEqual(0, facade.NbResets);
        }

        [Test]
        public async Task regular_startup_2_events_ahead()
        {
            var facade = new TestFacade( 100, 102);
            await Initialization.Run(facade);
            Assert.AreEqual(0, facade.NbResets);
        }

        [Test]
        public async Task regular_startup_no_projection()
        {
            var facade = new TestFacade( 0, 10);
            await Initialization.Run(facade);
            Assert.AreEqual(0, facade.NbResets);
        }

        /// <summary>
        /// This tests implement the behaviour we want to change
        /// </summary>
        [Test]
        public async Task startup_with_no_newevents()
        {
            var facade = new TestFacade( 100, 100);
            await Initialization.Run(facade);
            Assert.AreEqual(0, facade.NbResets); 
        }

        /// <summary>
        ///  but on the other hand we still want the reset in this case:
        /// </summary>
        /// <returns></returns>
        [Test]
        public async Task startup_with_projection_ahead_of_events()
        {
            var facade = new TestFacade( 101, 100 );
            await Initialization.Run(facade);
            
            Assert.AreEqual(1, facade.NbResets); // we want to reset, really. that the projection
                                                 // is ahead of the event stream is a sure sign that
                                                 // one of them has been hacked
        }
        #endregion
    }
}
