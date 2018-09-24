using System;
using Lokad.AzureEventStore.Drivers;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.drivers
{
    [TestFixture]
    public sealed class azure_parse_nth
    {
        [Test]
        public void zero()
        {
            Assert.AreEqual(0, AzureHelpers.ParseNth("events.00000"));
        }

        [Test]
        public void compact_throws()
        {
            Assert.Throws<ArgumentException>(() => AzureHelpers.ParseNth("events.00000.compact"));
        }

        [Test]
        public void bad_throws()
        {
            Assert.Throws<ArgumentException>(() => AzureHelpers.ParseNth("events.00FF0"));
        }

        [Test]
        public void value()
        {
            Assert.AreEqual(28, AzureHelpers.ParseNth("events.00028"));
        }
    }
}
