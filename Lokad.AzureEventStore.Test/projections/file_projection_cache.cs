using System;
using System.IO;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Cache;
using Xunit;
using System.Collections.Generic;

namespace Lokad.AzureEventStore.Test.projections
{
    public sealed class file_projection_cache : IDisposable
    {
        private async Task<CacheCandidate> ExpectOne(Task<IEnumerable<Task<CacheCandidate>>> allT)
        {
            var all = await allT;
            Task<CacheCandidate> found = null;
            foreach (var ccT in all)
            {
                Assert.Null(found);
                found = ccT;
            }

            Assert.NotNull(found);
            return await found;
        }

        private async Task ExpectNone(Task<IEnumerable<Task<CacheCandidate>>> allT)
        {
            var all = await allT;
            Assert.Empty(all);
        }

        [Fact]
        public async Task load_empty()
        {
            Assert.Empty(await _file.OpenReadAsync("test"));
        }

        [Fact]
        public async Task save_load()
        {
            var bytes = new byte[10];
            await _file.TryWriteAsync("test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            using (var read = (await ExpectOne(_file.OpenReadAsync("test"))).Contents)
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
            await _file.TryWriteAsync("test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            await ExpectNone(_file.OpenReadAsync("wrong"));
        }

        [Fact]
        public async Task save_overwrite_load()
        {
            var bytes = new byte[10];
            await _file.TryWriteAsync("test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            await _file.TryWriteAsync("test", write =>
            {
                write.Write(bytes, 0, bytes.Length / 2);
                return Task.CompletedTask;
            });

            using (var read = (await ExpectOne(_file.OpenReadAsync("test"))).Contents)
            {
                var others = new byte[bytes.Length];
                var count = read.Read(others, 0, others.Length);

                Assert.Equal(bytes.Length /  2, count);
            }
        }

        [Fact]
        public async Task write_write_lock()
        {
            await _file.TryWriteAsync("test", _ =>
                _file.TryWriteAsync("test", _ =>
                {
                    // Should not be invoked.
                    Assert.True(false);
                    return Task.CompletedTask;
                }));
        }

        [Fact]
        public async Task write_read_lock()
        {
            var bytes = new byte[10];
            await _file.TryWriteAsync("test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            await _file.TryWriteAsync("test", _ =>
                ExpectNone(_file.OpenReadAsync("test")));
        }

        #region Boilerplate 

        private IProjectionCacheProvider _file;
        private string _path;

        public file_projection_cache()
        {
            _path = @"C:\LokadData\AzureEventStore\FileStorageTests\" + Guid.NewGuid();
            _file = new FileCacheProvider(_path);
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
