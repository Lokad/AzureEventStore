using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Drivers;
using Xunit;

namespace Lokad.AzureEventStore.Test.drivers
{
    public abstract class storage_driver : IDisposable
    {
        [Fact]
        public async Task initially_empty()
        {
            var lastKey = await _driver.GetLastKeyAsync();
            Assert.Equal(0U, lastKey);

            var lastPosition = await _driver.GetPositionAsync();
            Assert.Equal(0L, lastPosition);
        }

        [Fact]
        public async Task no_read_on_empty()
        {
            var read = await _driver.ReadAsync(0, 1024);
            Assert.Equal(0, read.NextPosition);
            Assert.Empty(read.Events);
        }

        [Fact]
        public async Task write_one_read_one()
        {
            var bytes = new byte[8*1024];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte) i;

            var write = await _driver.WriteAsync(0, new[] {new RawEvent(12, bytes)});
            Assert.True(write.Success);
            
            var nextPos = write.NextPosition;
            Assert.True(0 < nextPos);

            var read = await _driver.ReadAsync(0, 9*1024);
            
            Assert.Equal(nextPos, read.NextPosition);
            Assert.Equal(1, read.Events.Count);

            Assert.Equal(bytes, read.Events[0].Contents);
            Assert.Equal((uint) 12, read.Events[0].Sequence);

            var oldPosition = read.NextPosition;
            read = await _driver.ReadAsync(oldPosition, 9*1024);
            Assert.Equal(oldPosition, read.NextPosition);
            Assert.Empty(read.Events);
        }

        [Fact]
        public async Task collide_writes()
        {
            var bytes = new byte[8 * 1024];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte)i;

            var write = await _driver.WriteAsync(0, new[] { new RawEvent(12, bytes) });
            Assert.True(write.Success);

            var nextPos = write.NextPosition;
            Assert.True(0 < nextPos);

            write = await _driver.WriteAsync(0, new[] { new RawEvent(12, bytes) });
            Assert.False(write.Success);            
            Assert.Equal(nextPos, write.NextPosition);

            write = await _driver.WriteAsync(nextPos, new[] { new RawEvent(12, bytes) });
            Assert.True(write.Success);
            Assert.Equal(nextPos * 2, write.NextPosition);
        }

        [Fact]
        public async Task two_writes()
        {
            var bytes = new byte[8 * 1024];
            for (var i = 0; i < bytes.Length; ++i) bytes[i] = (byte)i;

            var write = await _driver.WriteAsync(0, new[] { new RawEvent(12, bytes) });
            var pos1 = write.NextPosition;

            write = await _driver.WriteAsync(pos1, new[] { new RawEvent(42, bytes) });
            var pos2 = write.NextPosition;

            var read = await _driver.ReadAsync(0, 9 * 1024);
            Assert.Equal(1, read.Events.Count);
            Assert.Equal(pos1, read.NextPosition);
            Assert.Equal((uint) 12, read.Events[0].Sequence);

            read = await _driver.ReadAsync(pos1, 9 * 1024);
            Assert.Equal(1, read.Events.Count);
            Assert.Equal(pos2, read.NextPosition);
            Assert.Equal((uint) 42, read.Events[0].Sequence);

            read = await _driver.ReadAsync(0, 1024 * 1024);
            Assert.Equal(2, read.Events.Count);
            Assert.Equal(pos2, read.NextPosition);
            Assert.Equal((uint) 12, read.Events[0].Sequence);
            Assert.Equal((uint) 42, read.Events[1].Sequence);
        }

        [Fact]
        public async Task seek()
        {
            var events = new List<long> {0L};

            Assert.Equal(0L, await _driver.SeekAsync(0U));

            for (var i = 0; i < 1024; ++i)
            {
                if (i % 16 == 0) Trace.WriteLine($"At {i}");
                var evs = new[] {new RawEvent((uint)i, new byte[i*8])};
                var result = await _driver.WriteAsync(events[i], evs);

                Assert.True(result.Success); // Single writer

                events.Add(result.NextPosition);

                Assert.True(events[i] >= await _driver.SeekAsync((uint)i));
            }
        }

        #region Polymorphism boilerplate
        
        internal abstract IStorageDriver GetFreshStorageDriver();

        protected virtual void DeleteStorageDriver()
        {
            (_driver as IDisposable)?.Dispose();
            _driver = null;
        }

        private IStorageDriver _driver;

        public storage_driver() { _driver = GetFreshStorageDriver(); }

        public void Dispose() { DeleteStorageDriver(); }

        #endregion
    }
}
