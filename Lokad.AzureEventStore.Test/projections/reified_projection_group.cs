using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Moq;
using Xunit;

namespace Lokad.AzureEventStore.Test.projections
{
    public sealed class Integer
    {
        public readonly int Value;
        public Integer(int i) { Value = i; }
    }

    public sealed class State
    {
        public readonly string S;
        public readonly Integer I;

        public State(string s, Integer i)
        {
            S = s;
            I = i;
            ++Creations;
        }

        public static int Creations;
    }

    public sealed class reified_projection_group : reified_projection
    {
        #region Parent class tests

        internal override IReifiedProjection<int, string> Make(IProjection<int, string> projection, IProjectionCacheProvider cache = null, IProjectionFolderProvider folder = null)
        {
            return new ReifiedProjectionGroup<int, string>(new[] {projection}, cache, folder);
        }

        #endregion

        #region Mocking boilerplate

        private Mock<IProjection<int, Integer>> MockInteger()
        {
            var mock = new Mock<IProjection<int, Integer>>();
            mock.Setup(p => p.FullName).Returns("integer");
            mock.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns(new Integer(0));
            mock.Setup(p => p.State).Returns(typeof (Integer));
            mock.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<Integer>()))
                .Returns<uint, int, Integer>((s, i, state) => new Integer(state.Value + i));

            return mock;
        }

        private Mock<IProjection<int, string>> MockString()
        {
            var mock = new Mock<IProjection<int, string>>();
            mock.Setup(p => p.FullName).Returns("string");
            mock.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            mock.Setup(p => p.State).Returns(typeof (string));
            mock.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Returns<uint, int, string>((seq, evt, state) => string.Format("{0}({1}:{2})", state, evt, seq));

            return mock;
        }

       
        #endregion

        [Fact]
        public async Task multi_initial()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });
            await reified.CreateAsync();

            Assert.Equal(0U, reified.Sequence);
            Assert.Equal(0, reified.Current.I.Value);
            Assert.Equal("I", reified.Current.S);
        }


        [Fact]
        public async Task multi_apply()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object 
            });
            await reified.CreateAsync();
            reified.Apply(1U, 10);

            Assert.Equal(1U, reified.Sequence);
            Assert.Equal(10, reified.Current.I.Value);
            Assert.Equal("I(10:1)", reified.Current.S);
        }

        [Fact]
        public async Task multi_apply_reset()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });
            await reified.CreateAsync();
            reified.Apply(1U, 10);

            Assert.NotNull(reified.Current); 

            reified.Reset();

            Assert.Equal(0U, reified.Sequence);
            Assert.Equal(0, reified.Current.I.Value);
            Assert.Equal("I", reified.Current.S);
        }


        [Fact]
        public async Task multi_apply_double()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });
            await reified.CreateAsync();
            try
            {
                reified.Apply(1U, 10);
                reified.Apply(1U, 10);
                Assert.True(false);
            }
            catch (ArgumentException)
            {
                ;
            }
        }

        [Fact]
        public async Task multi_apply_twice()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });
            await reified.CreateAsync();
            var oldcount = State.Creations;

            reified.Apply(1U, 10);
            reified.Apply(4U, 14);

            Assert.Equal(4U, reified.Sequence);
            Assert.Equal(oldcount, State.Creations);
            Assert.Equal(24, reified.Current.I.Value);
            Assert.Equal(oldcount+1, State.Creations);
            Assert.Equal("I(10:1)(14:4)", reified.Current.S);
            Assert.Equal(oldcount+1, State.Creations);
        }

        [Fact]
        public async Task multi_apply_separate()
        {
            var cache = new Testing.InMemoryCache { { "string", new byte[]
            {
                0x02, 0x00, 0x00, 0x00, // Current position (beginning)
                0x30, 0x30, 0x30, 0x30, // Event data "0000"
                0x02, 0x00, 0x00, 0x00  // Current position (end)
            } } };

            var str = MockString();

            str.Setup(p => p.TryLoadAsync(It.IsAny<Stream>(), It.IsAny<CancellationToken>()))
                .Returns<Stream, CancellationToken>((s, c) =>
                {
                    var bytes = new byte[4];
                    s.Read(bytes, 0, 4);
                    return Task.FromResult(Encoding.UTF8.GetString(bytes));
                });

            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                str.Object
            }, cache);

            await reified.CreateAsync(CancellationToken.None);

            reified.Apply(1U, 10);
            reified.Apply(4U, 14);

            Assert.Equal(4U, reified.Sequence);
            Assert.Equal(24, reified.Current.I.Value);
            Assert.Equal("0000(14:4)", reified.Current.S);
        }

        [Fact]
        public override async Task apply_event_fails()
        {
            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Throws(new Exception("Boo."));

            var reified = Make(projection.Object);
            await reified.CreateAsync();
            try { reified.Apply(1U, 13); } catch { }

            Assert.Equal("I", reified.Current);
            Assert.Equal(1U, reified.Sequence);
        }

        [Fact]
        public override async Task save_auto_inconsistent()
        {
            const string Name = "test";
            var cache = new Testing.InMemoryCache();

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial(It.IsAny<StateCreationContext>())).Returns("0");
            projection.Setup(p => p.FullName).Returns(Name);
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new Exception("Projection.TrySaveAsync"));

            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Throws(new Exception("Boo."));

            var reified = Make(projection.Object, cache);

            try { reified.Apply(1U, 13); } catch { } // Sets 'inconsistent'

            await reified.TrySaveAsync(CancellationToken.None);

            Assert.False(cache.Streams.ContainsKey(Name));
        }
    }
}
