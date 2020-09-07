using System;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Streams;
using Lokad.AzureEventStore.Test.streams;
using Lokad.AzureEventStore.Wrapper;
using Moq;
using Xunit;

namespace Lokad.AzureEventStore.Test.wrapper
{
    public class initialization
    {
        [InlineData(0, 0, 1, 0)] // empty_stream
        [InlineData(0, 10, 1, 0)] // non_empty_stream_no_projection
        [InlineData(100, 110, 101, 0)] // regular_startup_10_events_ahead
        [InlineData(98, 100, 99, 0)] // regular_startup_1_event_ahead
        [InlineData(99, 100, 100, 0)] // regular_startup_2_events_ahead
        [InlineData(100, 100, 101, 0)] // startup_with_no_new_event (We don't want this one to reset)
        [InlineData(101, 100, 102, 1)] // startup_with_projection_ahead_of_events
        [InlineData(102, 100, 103, 1)] // startup_with_projection_far_ahead_of_events
        [Theory]
        public async Task test(int projectionSeq, int streamSeq, int expectedRequestedDiscardSeq, int expectedResets)
        {
            if (streamSeq < 0 || projectionSeq < 0 || expectedRequestedDiscardSeq < 0 || expectedResets < 0)
                throw new Exception("positive int required");

            var projection = Mock.Of<IReifiedProjection>(proj => proj.Sequence == (uint)projectionSeq);
            var stream = new MockStream((uint)streamSeq);
            await EventStreamWrapper<object, object>.Catchup(projection, stream);

            Assert.Equal((uint)expectedRequestedDiscardSeq, stream.RequestedDiscardSeq);
            Assert.Equal(expectedResets, stream.NbResets);
        }

        private static Task<IReifiedProjection> ProjectionWithSeqnum(uint seqnum)
        {
            var mockedProjection =  new Mock<IReifiedProjection>();
            mockedProjection.SetupGet(proj => proj.Sequence).Returns(seqnum);
            return Task.FromResult(mockedProjection.Object);
        }

        private class MockStream : IEventStream
        {
            public readonly uint LatestEvent;
            public int NbResets { get; private set; }
            public uint? RequestedDiscardSeq { get; private set; }

            public MockStream(uint latestEvent)
            {
                LatestEvent = latestEvent;
            }

            public Task<uint> DiscardUpTo(uint seq, CancellationToken cancel = default(CancellationToken))
            {
                if( RequestedDiscardSeq.HasValue)
                    throw new Exception("Single use method only - this is a mock object");

                RequestedDiscardSeq = seq;
                Sequence = event_stream_discard.ExpectedSequenceAfterDiscard(LatestEvent, seq);
                return Task.FromResult(Sequence);
            }

            public uint Sequence { get; private set; }
            public void Reset()
            {
                NbResets++;
            }

            public Task<Func<bool>> BackgroundFetchAsync(CancellationToken cancel = default(CancellationToken))
            {
                throw new NotImplementedException(nameof(BackgroundFetchAsync) + " not mocked");
            }
        }
    }
}
