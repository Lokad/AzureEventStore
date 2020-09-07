using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Streams;
using Newtonsoft.Json;
using Xunit;

namespace Lokad.AzureEventStore.Test.streams
{
    [DataContract]
    public class LargeEvt
    {
        public static string BaseString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789,;:?./!%¨$-()+[]'^";
        private string _seq;

        [JsonConstructor]
        private LargeEvt(){}

        public LargeEvt(int seq, List<string> largeContent)
        {
            if (seq > 9999|| seq < 0)
            {
                throw new ArgumentException();
            }
            Seq = seq.ToString().PadLeft(4, '0');
            LargeContent = largeContent;
        }

        public int IntSeq => int.Parse(Seq);

        [DataMember]
        public string Seq { get; set; }

        [DataMember]
        public List<string> LargeContent { get; set; }
    }

    public class event_stream_discard
    {
        private IStorageDriver storeWithLargeEvents;

        // sequence number of first event that won't fit in the first 4Mb block
        private uint firstNotFitting;
        // sequence number of the last event written in the store
        private uint lastEvent;

        /// <summary>
        /// we check that the value of the <see cref="EventStream.Sequence"/> property abides
        /// this formula:
        /// </summary>
        internal static uint ExpectedSequenceAfterDiscard(uint lastEventInStream, uint requestedSeq)
        {
            if (lastEventInStream < requestedSeq)
            {
                return lastEventInStream;
            }
            else
            {
                return requestedSeq == 0 ? 0 : requestedSeq - 1;
            }
        }

        public event_stream_discard()
        {
            Console.WriteLine("> ctor");
            Task.Run(async () => await SetupImpl()).Wait();
            Console.WriteLine("< ctor");
        }

        private async Task SetupImpl()
        {
            Console.WriteLine("> setup");
            var baseStringArray = Enumerable.Repeat(LargeEvt.BaseString, 1000).ToList();
            var driver = new MemoryStorageDriver();
            var stream = new EventStream<LargeEvt>(driver);

            int seqNum = 0;
            long pos;
            while ((pos = driver.GetPosition()) <= 4 * 1024 * 1024)
            {
                seqNum++;
                var toWrite = new LargeEvt(seqNum, baseStringArray);
                await stream.WriteAsync(new[] { toWrite } );
            }

            firstNotFitting = checked((uint)seqNum);

            // add five additional events, just to be safe
            // and also to test DiscardUpTo(some events in the second 4Mb block)
            for (int i = 0; i < 5; ++i)
            {
                seqNum++;
                await stream.WriteAsync(new[] {new LargeEvt(seqNum, baseStringArray)});
            }

            lastEvent = checked((uint)seqNum);
            Console.WriteLine("< setup");
            storeWithLargeEvents = driver;
        }

        private async Task DiscardAndAssert(uint requestedSeq)
        {
            var stream = new EventStream<LargeEvt>(storeWithLargeEvents);
            var returnedSequence = await stream.DiscardUpTo(requestedSeq);
            var expectedSequence = ExpectedSequenceAfterDiscard(lastEvent, requestedSeq);

            Assert.Equal(expectedSequence, stream.Sequence);
            Assert.Equal(returnedSequence, stream.Sequence);

            if (requestedSeq == 0)
            {
                requestedSeq = 1;
            }


            var nextEvt = await stream.TryGetNextAsync();
            if (requestedSeq <= lastEvent)
            {
                Assert.NotNull(nextEvt);
                Assert.Equal(requestedSeq, (uint)nextEvt.IntSeq);
            }
            else
            {
                Assert.Null(nextEvt);
            }
        }

        private async Task FetchDiscardAndAssert(uint requestedSeq)
        {
            var stream = new EventStream<LargeEvt>(storeWithLargeEvents);
            await stream.FetchAsync();
            await DiscardAndAssert(requestedSeq);
        }

        #region DiscardUpTo only
        [Fact]
        public async Task discard_zero()
        {
            await DiscardAndAssert(0);
        }

        [Fact]
        public async Task discard_up_to_first() // that is to say, discard nothing (there is nothing before event 1)
            => await DiscardAndAssert(1);

        [Fact]
        public async Task discard_up_to_second() // that is, discard the first event only
            => await DiscardAndAssert(2);

        [Fact]
        public async Task discard_up_to_last_in_first_block()
            => await DiscardAndAssert(firstNotFitting-1);

        [Fact]
        public async Task discard_up_to_first_in_second_block()
            => await DiscardAndAssert(firstNotFitting);

        [Fact]
        public async Task discard_up_to_second_in_second_block()
            => await DiscardAndAssert(firstNotFitting+1);

        [Fact]
        public async Task discard_up_to_penultimate()
            => await DiscardAndAssert(lastEvent - 1);

        [Fact]
        public async Task discard_up_to_last()
            => await DiscardAndAssert(lastEvent);

        [Fact]
        public async Task discard_up_to_one_past_the_last()
            => await DiscardAndAssert(lastEvent + 1);

        [Fact]
        public async Task discard_up_to_two_past_the_last()
            => await DiscardAndAssert(lastEvent + 2);
        #endregion

        #region DiscardUpTo after Fetch
        [Fact]
        public async Task fetch_and_discard_zero()
            => await FetchDiscardAndAssert(0);

        [Fact]
        public async Task fetch_and_discard_up_to_first()
            => await FetchDiscardAndAssert(1);

        [Fact]
        public async Task fetch_and_discard_up_to_second()
            => await FetchDiscardAndAssert(2);

        [Fact]
        public async Task fetch_and_discard_up_to_last_of_first_block()
            => await FetchDiscardAndAssert(firstNotFitting-1);

        [Fact]
        public async Task fetch_and_discard_up_to_first_in_second_block()
            => await FetchDiscardAndAssert(firstNotFitting);

        [Fact]
        public async Task fetch_and_discard_up_to_second_in_second_block()
            => await FetchDiscardAndAssert(firstNotFitting+1);

        [Fact]
        public async Task fetch_and_discard_up_to_penultimate()
            => await FetchDiscardAndAssert(lastEvent - 1);

        [Fact]
        public async Task fetch_and_discard_up_to_last()
            => await FetchDiscardAndAssert(lastEvent);

        [Fact]
        public async Task fetch_and_discard_up_to_one_past_the_last()
            => await FetchDiscardAndAssert(lastEvent + 1);

        [Fact]
        public async Task fetch_and_discard_up_to_two_past_the_last()
            => await FetchDiscardAndAssert(lastEvent + 2);
        #endregion
    }
}
