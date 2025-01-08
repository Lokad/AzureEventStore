using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Cache;
using Lokad.AzureEventStore.Projections;
using Moq;
using Xunit;

namespace Lokad.AzureEventStore.Test.projections
{
    public class reified_projection
    {
        internal virtual IReifiedProjection<int, string> Make(IProjection<int, string> projection,
            IProjectionCacheProvider cache = null,
            IProjectionFolderProvider folder = null)
        {
            return new ReifiedProjection<int, string>(projection, cache, folder);
        }

        #region Initialization 

        [Fact]
        public void initial()
        {
            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("initial");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof (string));

            var reified = Make(projection.Object);
            reified.CreateAsync();
            Assert.Equal("initial", reified.Current);
            Assert.Equal(0U, reified.Sequence);
        }

        [Fact]
        public void name_required()
        {
            try
            {
                var projection = new Mock<IProjection<int, string>>();
                projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("initial");
                projection.Setup(p => p.FullName).Returns((string)null);
                projection.Setup(p => p.State).Returns(typeof(string));

                Make(projection.Object);
                Assert.True(false);
            }
            catch (ArgumentException)
            {
            }
        }

        [Fact]
        public void name_constrained()
        {
            try
            {
                var projection = new Mock<IProjection<int, string>>();
                projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("initial");
                projection.Setup(p => p.FullName).Returns("2/3");
                projection.Setup(p => p.State).Returns(typeof(string));

                Make(projection.Object);
                Assert.True(false);
            }
            catch (ArgumentException)
            {

            }
        }
        
        [Fact]
        public async Task initial_required()
        {
            try
            {
                var projection = new Mock<IProjection<int, string>>();
                projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns((string)null);
                projection.Setup(p => p.FullName).Returns("test");
                projection.Setup(p => p.State).Returns(typeof(string));

                var reified = Make(projection.Object);
                await reified.CreateAsync();
                Assert.True(false);
            }
            catch (InvalidOperationException e)
            {
                Assert.Equal("Projection initial state must not be null", e.Message);
            }
        }

        #endregion

        #region Event application 

        [Fact]
        public void apply_event()
        {
            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Returns<uint, int, string>((seq, evt, state) => string.Format("{0}({1}:{2})", state, evt, seq));

            var reified = Make(projection.Object);
            reified.CreateAsync();
            
            Assert.Equal("I", reified.Current);
            Assert.Equal(0U, reified.Sequence);

            reified.Apply(1U, 13);

            Assert.Equal("I(13:1)", reified.Current);
            Assert.Equal(1U, reified.Sequence);
        }

        [Fact]
        public void apply_event_skip()
        {
            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Returns<uint, int, string>((seq, evt, state) => string.Format("{0}({1}:{2})", state, evt, seq));

            var reified = Make(projection.Object);
            reified.CreateAsync();
            reified.Apply(1U, 13);
            reified.Apply(4U, 42);

            Assert.Equal("I(13:1)(42:4)", reified.Current);
            Assert.Equal(4U, reified.Sequence);
        }

        [Fact]
        public virtual async Task apply_event_fails()
        {
            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Throws(new Exception("Boo."));

            var reified = Make(projection.Object);
            await reified.CreateAsync();
            try
            {
                reified.Apply(1U, 13);
                Assert.True(false);
            }
            catch (Exception e)
            {
                Assert.Equal("Boo.", e.Message);
            }

            Assert.Equal("I", reified.Current);
            Assert.Equal(1U, reified.Sequence);
        }

        [Fact]
        public async Task reapply_event()
        {
            try
            {
                var projection = new Mock<IProjection<int, string>>();
                projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
                projection.Setup(p => p.FullName).Returns("test");
                projection.Setup(p => p.State).Returns(typeof(string));
                projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                    .Returns<uint, int, string>((seq, evt, state) => string.Format("{0}({1}:{2})", state, evt, seq));

                var reified = Make(projection.Object);
                await reified.CreateAsync(CancellationToken.None);

                reified.Apply(1U, 13);
                reified.Apply(1U, 13);

                Assert.True(false);
            }
            catch (ArgumentException)
            {
            }
        }

        #endregion

        #region Loading 

        [Fact]
        public async Task load_state()
        {
            var cache = new Testing.InMemoryCache { { "test", new byte[]
            {
                0x02, 0x00, 0x00, 0x00, // Current position (beginning)
                0x30, 0x30, 0x30, 0x30, // Event data "0000"
                0x02, 0x00, 0x00, 0x00  // Current position (end)
            } } };

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TryLoadAsync(It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
                .Returns<Stream, CancellationToken>((s, c) =>
                {
                    var bytes = new byte[4];
                    Assert.Equal(0, s.Position);
                    s.Read(bytes, 0, 4);
                    return Task.FromResult(Encoding.UTF8.GetString(bytes)); 
                });

            var reified = Make(projection.Object, cache);

            await reified.CreateAsync(CancellationToken.None);

            Assert.Equal(2U, reified.Sequence);
            Assert.Equal("0000", reified.Current);
        }

        [Fact]
        public async Task load_state_from_projection_folder()
        {
            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.NeedsMemoryMappedFolder).Returns(true);
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TryRestoreAsync(It.IsAny<StateCreationContext>(), It.IsAny<CancellationToken>()))
                .Returns<StateCreationContext, CancellationToken>((s, c) =>
                {
                    var entry = s.MemoryMappedFolder.CreateNew("test", 100);
                    var bytes = new byte[]
                    {
                        0x02, 0x00, 0x00, 0x00, // Current position
                        0x30, 0x30, 0x30, 0x30, // Event data "0000"
                    };
                    bytes.CopyTo(entry.AsSpan);
                    var span = s.MemoryMappedFolder.Open("test").AsSpan;    
                    
                    return Task.FromResult(new RestoredState<string>(
                        MemoryMarshal.Cast<byte, uint>(span[..4])[0],
                        Encoding.UTF8.GetString(span.Slice(4, 4)))); 
                });

            var reified = Make(projection.Object, folder: new InMemoryFolderProvider());

            await reified.CreateAsync(CancellationToken.None);

            Assert.Equal(2U, reified.Sequence);
            Assert.Equal("0000", reified.Current);
        }

        [Fact]
        public async Task load_state_bad_terminator()
        {
            var cache = new Testing.InMemoryCache { { "test", new byte[]
            {
                0x02, 0x00, 0x00, 0x00, // Current position (beginning)
                0x30, 0x30, 0x30, 0x30, // Event data "0000"
                0x01, 0x00, 0x00, 0x00  // Current position (end)
            } } };

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            ReturnsExtensions.ReturnsAsync(projection.Setup(p => p.TryLoadAsync(It.IsAny<Stream>(), It.IsAny<CancellationToken>())), "bad");

            var reified = Make(projection.Object, cache);

            await reified.CreateAsync(CancellationToken.None);

            Assert.Equal(0U, reified.Sequence);
            Assert.Equal("I", reified.Current);
        }

        [Fact]
        public async Task load_state_truncated()
        {
            var cache = new Testing.InMemoryCache { { "test", new byte[]
            {
                0x02, 0x00, 0x00, 0x00, // Current position (beginning)
                0x30, 0x30, 0x30, 0x30, // Event data "0000"
                0x01, 0x00              // Current position (end)
            } } };

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            ReturnsExtensions.ReturnsAsync(projection.Setup(p => p.TryLoadAsync(It.IsAny<Stream>(), It.IsAny<CancellationToken>())), "bad");

            var reified = Make(projection.Object, cache);

            await reified.CreateAsync(CancellationToken.None);

            Assert.Equal(0U, reified.Sequence);
            Assert.Equal("I", reified.Current);
        }

        [Fact]
        public async Task load_state_bad_name()
        {
            var cache = new Testing.InMemoryCache
            {
                { "test", new byte[]
                {
                    0x02, 0x00, 0x00, 0x00, // Current position (beginning)
                    0x30, 0x30, 0x30, 0x30, // Event data "0000"
                    0x02, 0x00, 0x00, 0x00  // Current position (end)
                } },
                { "other", new byte[0] }
            };

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("other");
            projection.Setup(p => p.State).Returns(typeof(string));
            ReturnsExtensions.ReturnsAsync(projection.Setup(p => p.TryLoadAsync(It.IsAny<Stream>(), It.IsAny<CancellationToken>())), "bad");

            var reified = Make(projection.Object, cache);

            await reified.CreateAsync(CancellationToken.None);

            Assert.Equal(0U, reified.Sequence);
            Assert.Equal("I", reified.Current);
        }

        #endregion

        #region Saving

        [Fact]
        public async Task save()
        {
            var cache = new Testing.InMemoryCache();

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("0");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns<Stream, string, CancellationToken>((s, state, c) =>
                {
                    var bytes = Encoding.UTF8.GetBytes(state);
                    s.Write(bytes, 0, bytes.Length);
                    return Task.FromResult(true);
                });

            var reified = Make(projection.Object, cache);
            await reified.CreateAsync();
            await reified.TrySaveAsync(CancellationToken.None);

            Assert.Equal(new byte[]
            {
                0x00, 0x00, 0x00, 0x00, // Sequence number (first)
                0x30, // Contents "0"
                0x00, 0x00, 0x00, 0x00 // Sequence number (last)
            }, cache.Streams["test"].ToArray());
        }

        [Fact]
        public async Task save_failed()
        {
            var cache = new Testing.InMemoryCache();

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("0");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns<Stream, string, CancellationToken>((s, state, c) =>
                {
                    var bytes = Encoding.UTF8.GetBytes(state);
                    s.Write(bytes, 0, bytes.Length);
                    return Task.FromResult(false);
                });

            var reified = Make(projection.Object, cache);
            await reified.CreateAsync();
            await reified.TrySaveAsync(CancellationToken.None);

            Assert.False(cache.Streams.ContainsKey("test"));
        }

        [Fact]
        public async Task save_throws()
        {
            var cache = new Testing.InMemoryCache();

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("0");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new Exception("Projection.TrySaveAsync"));

            var reified = Make(projection.Object, cache);

            try
            {
                await reified.TrySaveAsync(CancellationToken.None);
                Assert.True(false);
            }
            catch (Exception e)
            {
                Assert.Equal("Projection.TrySaveAsync", e.Message);
            }

            Assert.Empty(cache.Streams);
        }

        [Fact]
        public async Task save_inconsistent()
        {
            var ms = new MemoryStream();

            var cache = new Testing.InMemoryCache();

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("0");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new Exception("Projection.TrySaveAsync"));

            var reified = Make(projection.Object, cache);

            reified.SetPossiblyInconsistent();

            await reified.TrySaveAsync(CancellationToken.None);

            Assert.False(cache.Streams.ContainsKey("test"));
        }

        [Fact]
        public async Task save_reset_inconsistent()
        {
            var cache = new Testing.InMemoryCache();

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("0");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Returns<Stream, string, CancellationToken>((s, state, c) =>
                {
                    var bytes = Encoding.UTF8.GetBytes(state);
                    s.Write(bytes, 0, bytes.Length);
                    return Task.FromResult(true);
                });

            var reified = Make(projection.Object, cache);
            reified.SetPossiblyInconsistent();
            reified.Reset();

            await reified.TrySaveAsync(CancellationToken.None);

            Assert.Equal(new byte[]
            {
                0x00, 0x00, 0x00, 0x00, // Sequence number (first)
                0x30, // Contents "0"
                0x00, 0x00, 0x00, 0x00 // Sequence number (last)
            }, cache.Streams["test"].ToArray());
        }

        [Fact]
        public virtual async Task save_auto_inconsistent()
        {
            var cache = new Testing.InMemoryCache();

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("0");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new Exception("Projection.TrySaveAsync"));

            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Throws(new Exception("Boo."));

            var reified = Make(projection.Object, cache);

            try { reified.Apply(1U, 13); /* Sets 'inconsistent' */ } catch {}

            await reified.TrySaveAsync(CancellationToken.None);

            Assert.Empty(cache.Streams);
        }

        #endregion
    }
}
