using System;
using System.IO;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using NUnit.Framework;

namespace Lokad.AzureEventStore.Test.projections
{
    [TestFixture]
    public sealed class file_projection_cache
    {
        [Test]
        public async Task load_empty()
        {
            var stream = await _file.OpenReadAsync("test");
            Assert.IsNull(stream);
        }

        [Test]
        public async Task save_load()
        {
            var bytes = new byte[10];
            using (var write = await _file.OpenWriteAsync("test"))
            {
                write.Write(bytes, 0, bytes.Length);        
            }

            using (var read = await _file.OpenReadAsync("test"))
            {
                var others = new byte[bytes.Length];
                var count = read.Read(others, 0, others.Length);

                Assert.AreEqual(bytes.Length, (int) count);
                CollectionAssert.AreEqual(bytes, others);
            }
        }

        [Test]
        public async Task save_load_wrong()
        {
            var bytes = new byte[10];
            using (var write = await _file.OpenWriteAsync("test"))
            {
                write.Write(bytes, 0, bytes.Length);
            }

            var stream = await _file.OpenReadAsync("wrong");
            Assert.IsNull(stream);
        }

        [Test]
        public async Task save_overwrite_load()
        {
            var bytes = new byte[10];
            using (var write = await _file.OpenWriteAsync("test"))
            {
                write.Write(bytes, 0, bytes.Length);
            }

            using (var write = await _file.OpenWriteAsync("test"))
            {
                write.Write(bytes, 0, bytes.Length / 2);
            }

            using (var read = await _file.OpenReadAsync("test"))
            {
                var others = new byte[bytes.Length];
                var count = read.Read(others, 0, others.Length);

                Assert.AreEqual(bytes.Length /  2, (int) count);
            }
        }

        [Test]
        public async Task write_write_lock()
        {
            using (await _file.OpenWriteAsync("test"))
            {
                var write2 = await _file.OpenWriteAsync("test");
                Assert.IsNull(write2);
            }
        }

        [Test]
        public async Task write_read_lock()
        {
            var bytes = new byte[10];
            using (var write = await _file.OpenWriteAsync("test"))
            {
                write.Write(bytes, 0, bytes.Length);
            }

            using (await _file.OpenWriteAsync("test"))
            {
                var read = await _file.OpenReadAsync("test");
                Assert.IsNull(read);
            }
        }
        #region Boilerplate 

        private IProjectionCacheProvider _file;
        private string _path;

        [SetUp]
        public void SetUp()
        {
            _path = @"C:\LokadData\AzureEventStore\FileStorageTests\" + Guid.NewGuid();
            _file = new FileProjectionCache(_path);
        }

        [TearDown]
        public void TearDown()
        {
            try
            {
                Directory.Delete(_path, true);
            }
            catch { }
        }

        #endregion
    }
}
