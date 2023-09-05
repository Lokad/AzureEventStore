using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Streams;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Lokad.AzureEventStore.Test.streams
{
    public sealed class migration_stream
    {
        [Fact]
        public async Task nothing()
        {
            var driver = new MemoryStorageDriver();
            var stream = new MigrationStream<JObject>(driver);

            Assert.Equal(0U, await stream.LastWrittenAsync());
        }

        [Fact]
        public async Task already_filled()
        {
            var driver = new MemoryStorageDriver();
            driver.Write(0L, new[] {new RawEvent(42U, new byte[8])});

            var stream = new MigrationStream<JObject>(driver);

            Assert.Equal(42U, await stream.LastWrittenAsync());
        }

        [Fact]
        public async Task write()
        {
            var driver = new MemoryStorageDriver();
            var stream = new MigrationStream<JObject>(driver);

            await stream.WriteAsync(new[]
            {
                new KeyValuePair<uint, JObject>(11, new JObject {{"a", new JValue(10)}}),
                new KeyValuePair<uint, JObject>(42, new JObject {{"a", new JValue(20)}}),
            });

            Assert.Equal(42U, await stream.LastWrittenAsync());

            var written = driver.Read(0L, new byte[10240]);

            Assert.Equal(2, written.Events.Count);
            Assert.Equal((uint) 11, written.Events[0].Sequence);
            Assert.Equal("{\"a\":10}", Encoding.UTF8.GetString(written.Events[0].Contents.Span));
            Assert.Equal((uint) 42, written.Events[1].Sequence);
            Assert.Equal("{\"a\":20}", Encoding.UTF8.GetString(written.Events[1].Contents.Span));
        }
    }
}
