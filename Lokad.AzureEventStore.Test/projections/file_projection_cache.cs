using System;
using System.IO;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Lokad.AzureEventStore.Cache;
using Xunit;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace Lokad.AzureEventStore.Test.projections
{
    public sealed class file_projection_cache : IDisposable
    {
        private async Task<CacheCandidate> ExpectOne(Task<IAsyncEnumerable<CacheCandidate>> allT)
        {
            var all = await allT;
            CacheCandidate? found = null;
            await foreach (var ccT in all)
            {
                Assert.Null(found);
                found = ccT;
            }

            Assert.NotNull(found);
            return found.Value;
        }

        private async Task ExpectNone(Task<IAsyncEnumerable<CacheCandidate>> allT)
        {
            var all = await (await allT).ToArrayAsync();
            Assert.Empty(all);
        }

        [Fact]
        public async Task load_empty()
        {
            var candidates =
                await _file.OpenReadAsync(null, "test");
            Assert.Empty(await candidates.ToArrayAsync());
        }

        [Fact]
        public async Task save_load()
        {
            var bytes = new byte[10];
            await _file.TryWriteAsync(null, "test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            using (var read = (await ExpectOne(_file.OpenReadAsync(null, "test"))).Contents)
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
            await _file.TryWriteAsync(null, "test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            await ExpectNone(_file.OpenReadAsync(null, "wrong"));
        }

        [Fact]
        public async Task save_overwrite_load()
        {
            var bytes = new byte[10];
            await _file.TryWriteAsync(null, "test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            await _file.TryWriteAsync(null, "test", write =>
            {
                write.Write(bytes, 0, bytes.Length / 2);
                return Task.CompletedTask;
            });

            using (var read = (await ExpectOne(_file.OpenReadAsync(null, "test"))).Contents)
            {
                var others = new byte[bytes.Length];
                var count = read.Read(others, 0, others.Length);

                Assert.Equal(bytes.Length /  2, count);
            }
        }

        [Fact]
        public async Task write_write_lock()
        {
            await _file.TryWriteAsync(null, "test", _ =>
                _file.TryWriteAsync(null, "test", _ =>
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
            await _file.TryWriteAsync(null, "test", write =>
            {
                write.Write(bytes, 0, bytes.Length);
                return Task.CompletedTask;
            });

            await _file.TryWriteAsync(null, "test", _ =>
                ExpectNone(_file.OpenReadAsync(null, "test")));
        }

        #region Boilerplate 

        private IProjectionCacheProvider _file;
        private string _path;

        public file_projection_cache()
        {
            _path = Path.Combine(TestPaths.Root, "FileStorageTests", Guid.NewGuid().ToString());
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
