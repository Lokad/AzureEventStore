using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.drivers
{
    [TestFixture]
    internal abstract class storage_driver
    {
        [Test]
        public async Task initially_empty()
        {
            var lastKey = await _driver.GetLastKeyAsync();
            Assert.AreEqual((uint) 0U, (uint) lastKey);

            var lastPosition = await _driver.GetPositionAsync();
            Assert.AreEqual((long) 0L, (long) lastPosition);
        }

        [Test]
        public async Task no_read_on_empty()
        {
            var read = await _driver.ReadAsync(0, 1024);
            Assert.AreEqual((long) 0, (long) read.NextPosition);
            Assert.IsEmpty((IEnumerable) read.Events);
        }

        [Test]
        public async Task write_one_read_one()
        {
            var bytes = new byte[8*1024];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte) i;

            var write = await _driver.WriteAsync(0, new[] {new RawEvent(12, bytes)});
            Assert.IsTrue(write.Success);
            
            var nextPos = write.NextPosition;
            Assert.IsTrue(0 < nextPos);

            var read = await _driver.ReadAsync(0, 9*1024);
            
            Assert.AreEqual((long) nextPos, (long) read.NextPosition);
            Assert.AreEqual((int) 1, (int) read.Events.Count);

            CollectionAssert.AreEqual(bytes, read.Events[0].Contents);
            Assert.AreEqual((uint) 12, (uint) read.Events[0].Sequence);

            var oldPosition = read.NextPosition;
            read = await _driver.ReadAsync(oldPosition, 9*1024);
            Assert.AreEqual((long) oldPosition, (long) read.NextPosition);
            Assert.IsEmpty((IEnumerable) read.Events);
        }

        [Test]
        public async Task collide_writes()
        {
            var bytes = new byte[8 * 1024];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte)i;

            var write = await _driver.WriteAsync(0, new[] { new RawEvent(12, bytes) });
            Assert.IsTrue(write.Success);

            var nextPos = write.NextPosition;
            Assert.IsTrue(0 < nextPos);

            write = await _driver.WriteAsync(0, new[] { new RawEvent(12, bytes) });
            Assert.IsFalse(write.Success);            
            Assert.AreEqual((long) nextPos, (long) write.NextPosition);

            write = await _driver.WriteAsync(nextPos, new[] { new RawEvent(12, bytes) });
            Assert.IsTrue(write.Success);
            Assert.AreEqual((long) (nextPos * 2), (long) write.NextPosition);
        }

        [Test]
        public async Task two_writes()
        {
            var bytes = new byte[8 * 1024];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte)i;

            var write = await _driver.WriteAsync(0, new[] { new RawEvent(12, bytes) });
            var pos1 = write.NextPosition;

            write = await _driver.WriteAsync(pos1, new[] { new RawEvent(42, bytes) });
            var pos2 = write.NextPosition;

            var read = await _driver.ReadAsync(0, 9 * 1024);
            Assert.AreEqual((int) 1, (int) read.Events.Count);
            Assert.AreEqual((long) pos1, (long) read.NextPosition);
            Assert.AreEqual((uint) 12, (uint) read.Events[0].Sequence);

            read = await _driver.ReadAsync(pos1, 9 * 1024);
            Assert.AreEqual((int) 1, (int) read.Events.Count);
            Assert.AreEqual((long) pos2, (long) read.NextPosition);
            Assert.AreEqual((uint) 42, (uint) read.Events[0].Sequence);

            read = await _driver.ReadAsync(0, 1024 * 1024);
            Assert.AreEqual((int) 2, (int) read.Events.Count);
            Assert.AreEqual((long) pos2, (long) read.NextPosition);
            Assert.AreEqual((uint) 12, (uint) read.Events[0].Sequence);
            Assert.AreEqual((uint) 42, (uint) read.Events[1].Sequence);
        }

        [Test]
        public async Task seek()
        {
            var events = new List<long> {0L};

            Assert.AreEqual((long) 0L, (long) await _driver.SeekAsync(0U));

            for (var i = 0; i < 1024; ++i)
            {
                if (i % 16 == 0) Trace.WriteLine($"At {i}");
                var evs = new[] {new RawEvent((uint)i, new byte[i*8])};
                var result = await _driver.WriteAsync(events[i], evs);

                Assert.IsTrue(result.Success); // Single writer

                events.Add(result.NextPosition);

                Assert.IsTrue(events[i] >= await _driver.SeekAsync((uint)i));
            }
        }

        #region Polymorphism boilerplate
        
        protected abstract IStorageDriver GetFreshStorageDriver();

        protected virtual void DeleteStorageDriver()
        {
            (_driver as IDisposable)?.Dispose();
            _driver = null;
        }

        private IStorageDriver _driver;

        [SetUp]
        public void SetUp() { _driver = GetFreshStorageDriver(); }

        [TearDown]
        public void TearDown() { DeleteStorageDriver(); }

        #endregion
    }
}
