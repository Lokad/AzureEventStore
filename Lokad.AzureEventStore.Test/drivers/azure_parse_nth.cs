using System;
using Lokad.AzureEventStore.Drivers;
using Xunit;

namespace Lokad.AzureEventStore.Test.drivers
{
    public sealed class azure_parse_nth
    {
        [Fact]
        public void zero()
        {
            Assert.Equal(0, AzureHelpers.ParseNth("events.00000"));
        }

        [Fact]
        public void compact_throws()
        {
            Assert.Throws<ArgumentException>(() => AzureHelpers.ParseNth("events.00000.compact"));
        }

        [Fact]
        public void bad_throws()
        {
            Assert.Throws<ArgumentException>(() => AzureHelpers.ParseNth("events.00FF0"));
        }

        [Fact]
        public void value()
        {
            Assert.Equal(28, AzureHelpers.ParseNth("events.00028"));
        }
    }
}
