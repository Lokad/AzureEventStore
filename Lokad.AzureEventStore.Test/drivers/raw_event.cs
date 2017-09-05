using System;
using Lokad.AzureEventStore.Drivers;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.drivers
{
    [TestFixture]
    public sealed class raw_event
    {
        [Test]
        public void creation()
        {
            var bytes = new byte[8*512];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte) (i * 37);

            var ev = new RawEvent(1337, bytes);

            Assert.AreEqual((uint) 1337, (uint) ev.Sequence);
            CollectionAssert.AreEqual(bytes, ev.Contents);
        }

        [Test, ExpectedException(typeof (ArgumentException), ExpectedMessage = "Content size 137 not a multiple of 8")]
        public void not_multiple()
        {
            Assert.IsNotNull(new RawEvent(1337, new byte[137]));
        }

        [Test, ExpectedException(typeof(ArgumentException), ExpectedMessage = "Content size 530000 exceeds 512KB.")]
        public void too_large()
        {
            Assert.IsNotNull(new RawEvent(1337, new byte[530000]));
        }
    }
}
