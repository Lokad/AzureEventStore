using System;
using System.IO;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Xunit;

namespace Lokad.AzureEventStore.Test.projections
{
    public sealed class file_projection_cache : IDisposable
    {
        [Fact]
        public async Task load_empty()
        {
            var stream = await _file.OpenReadAsync("test");
            Assert.Null(stream);
        }

        [Fact]
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

                Assert.Equal(bytes.Length, count);
                Assert.Equal(bytes, others);
            }
        }

        [Fact]
        public async Task save_load_wrong()
        {
            var bytes = new byte[10];
            using (var write = await _file.OpenWriteAsync("test"))
            {
                write.Write(bytes, 0, bytes.Length);
            }

            var stream = await _file.OpenReadAsync("wrong");
            Assert.Null(stream);
        }

        [Fact]
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

                Assert.Equal(bytes.Length /  2, count);
            }
        }

        [Fact]
        public async Task write_write_lock()
        {
            using (await _file.OpenWriteAsync("test"))
            {
                var write2 = await _file.OpenWriteAsync("test");
                Assert.Null(write2);
            }
        }

        [Fact]
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
                Assert.Null(read);
            }
        }
        #region Boilerplate 

        private IProjectionCacheProvider _file;
        private string _path;

        public file_projection_cache()
        {
            _path = @"C:\LokadData\AzureEventStore\FileStorageTests\" + Guid.NewGuid();
            _file = new FileProjectionCache(_path);
        }

        public void Dispose()
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
