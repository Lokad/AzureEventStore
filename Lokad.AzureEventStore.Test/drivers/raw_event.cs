using System;
using Lokad.AzureEventStore.Drivers;
using Xunit;

namespace Lokad.AzureEventStore.Test.drivers
{
    public sealed class raw_event
    {
        [Fact]
        public void creation()
        {
            var bytes = new byte[8*512];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte) (i * 37);

            var ev = new RawEvent(1337, bytes);

            Assert.Equal((uint) 1337, (uint) ev.Sequence);
            Assert.Equal(bytes, ev.Contents);
        }

        [Fact]
        public void not_multiple()
        {
            try
            {
                new RawEvent(1337, new byte[137]);
                Assert.True(false);
            }
            catch (ArgumentException e)
            {
                Assert.Equal("Content size 137 not a multiple of 8", e.Message);
            }
        }

        [Fact]
        public void too_large()
        {
            try
            {
                new RawEvent(1337, new byte[530000]);
                Assert.True(false);
            }
            catch (ArgumentException e)
            {
                Assert.Equal("Content size 530000 exceeds 512KB.", e.Message);
            }
        }
    }
}
