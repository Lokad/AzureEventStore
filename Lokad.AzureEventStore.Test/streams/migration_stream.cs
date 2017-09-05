using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Lokad.AzureEventStore.Streams;
using Newtonsoft.Json.Linq;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.streams
{
    [TestFixture]
    public sealed class migration_stream
    {
        [Test]
        public async Task nothing()
        {
            var driver = new MemoryStorageDriver();
            var stream = new MigrationStream<JObject>(driver);

            Assert.AreEqual((uint) 0U, (uint) await stream.LastWrittenAsync());
        }

        [Test]
        public async Task already_filled()
        {
            var driver = new MemoryStorageDriver();
            driver.Write(0L, new[] {new RawEvent(42U, new byte[8])});

            var stream = new MigrationStream<JObject>(driver);

            Assert.AreEqual((uint) 42U, (uint) await stream.LastWrittenAsync());
        }

        [Test]
        public async Task write()
        {
            var driver = new MemoryStorageDriver();
            var stream = new MigrationStream<JObject>(driver);

            await stream.WriteAsync(new[]
            {
                new KeyValuePair<uint, JObject>(11, new JObject {{"a", new JValue(10)}}),
                new KeyValuePair<uint, JObject>(42, new JObject {{"a", new JValue(20)}}),
            });

            Assert.AreEqual((uint) 42U, (uint) await stream.LastWrittenAsync());

            var written = driver.Read(0L, 10240);

            Assert.AreEqual((int) 2, (int) written.Events.Count);
            Assert.AreEqual((uint) 11, (uint) written.Events[0].Sequence);
            Assert.AreEqual("{\"a\":10}", Encoding.UTF8.GetString(written.Events[0].Contents));
            Assert.AreEqual((uint) 42, (uint) written.Events[1].Sequence);
            Assert.AreEqual("{\"a\":20}", Encoding.UTF8.GetString(written.Events[1].Contents));
        }
    }
}
