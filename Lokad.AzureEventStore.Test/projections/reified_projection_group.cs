using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lokad.AzureEventStore.Projections;
using Moq;
using NUnit.Framework;

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

    [TestFixture]
    internal sealed class reified_projection_group : reified_projection
    {
        #region Parent class tests

        protected override IReifiedProjection<int, string> Make(IProjection<int, string> projection, IProjectionCacheProvider cache = null)
        {
            return new ReifiedProjectionGroup<int, string>(new[] {projection}, cache);
        }

        #endregion

        #region Mocking boilerplate

        private Mock<IProjection<int, Integer>> MockInteger()
        {
            var mock = new Mock<IProjection<int, Integer>>();
            mock.Setup(p => p.FullName).Returns("integer");
            mock.Setup(p => p.Initial).Returns(new Integer(0));
            mock.Setup(p => p.State).Returns(typeof (Integer));
            mock.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<Integer>()))
                .Returns<uint, int, Integer>((s, i, state) => new Integer(state.Value + i));

            return mock;
        }

        private Mock<IProjection<int, string>> MockString()
        {
            var mock = new Mock<IProjection<int, string>>();
            mock.Setup(p => p.FullName).Returns("string");
            mock.Setup(p => p.Initial).Returns("I");
            mock.Setup(p => p.State).Returns(typeof (string));
            mock.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Returns<uint, int, string>((seq, evt, state) => string.Format("{0}({1}:{2})", state, evt, seq));

            return mock;
        }

       
        #endregion

        [Test]
        public void multi_initial()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });

            Assert.AreEqual((uint) 0U, (uint) reified.Sequence);
            Assert.AreEqual((int) 0, (int) reified.Current.I.Value);
            Assert.AreEqual("I", reified.Current.S);
        }


        [Test]
        public void multi_apply()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });

            reified.Apply(1U, 10);

            Assert.AreEqual((uint) 1U, (uint) reified.Sequence);
            Assert.AreEqual((int) 10, (int) reified.Current.I.Value);
            Assert.AreEqual("I(10:1)", reified.Current.S);
        }

        [Test]
        public void multi_apply_reset()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });

            reified.Apply(1U, 10);

            Assert.IsNotNull(reified.Current); 

            reified.Reset();

            Assert.AreEqual((uint) 0U, (uint) reified.Sequence);
            Assert.AreEqual((int) 0, (int) reified.Current.I.Value);
            Assert.AreEqual("I", reified.Current.S);
        }


        [Test, ExpectedException(typeof(ArgumentException))]
        public void multi_apply_double()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });

            reified.Apply(1U, 10);
            reified.Apply(1U, 10);
        }

        [Test]
        public void multi_apply_twice()
        {
            var reified = new ReifiedProjectionGroup<int, State>(new IProjection<int>[]
            {
                MockInteger().Object,
                MockString().Object
            });

            var oldcount = State.Creations;

            reified.Apply(1U, 10);
            reified.Apply(4U, 14);

            Assert.AreEqual((uint) 4U, (uint) reified.Sequence);
            Assert.AreEqual(oldcount, State.Creations);
            Assert.AreEqual((int) 24, (int) reified.Current.I.Value);
            Assert.AreEqual(oldcount+1, State.Creations);
            Assert.AreEqual("I(10:1)(14:4)", reified.Current.S);
            Assert.AreEqual(oldcount+1, State.Creations);
        }

        [Test]
        public async Task multi_apply_separate()
        {
            var cache = new Mock<IProjectionCacheProvider>();
            ReturnsExtensions.ReturnsAsync(cache.Setup(c => c.OpenReadAsync("string")), new MemoryStream(new byte[]
                {
                    0x02, 0x00, 0x00, 0x00, // Current position (beginning)
                    0x30, 0x30, 0x30, 0x30, // Event data "0000"
                    0x02, 0x00, 0x00, 0x00  // Current position (end)
                }));

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
            }, cache.Object);

            await reified.TryLoadAsync(CancellationToken.None);

            reified.Apply(1U, 10);
            reified.Apply(4U, 14);

            Assert.AreEqual((uint) 4U, (uint) reified.Sequence);
            Assert.AreEqual((int) 24, (int) reified.Current.I.Value);
            Assert.AreEqual("0000(14:4)", reified.Current.S);
        }

        [Test]
        public override void apply_event_fails()
        {
            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial).Returns("I");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Throws(new Exception("Boo."));

            var reified = Make(projection.Object);

            Assert.Throws<AggregateException>(() => reified.Apply(1U, 13), "Boo.");

            Assert.AreEqual("I", reified.Current);
            Assert.AreEqual((uint) 1U, (uint) reified.Sequence);
        }

        [Test]
        public override async Task save_auto_inconsistent()
        {
            var ms = new MemoryStream();

            var cache = new Mock<IProjectionCacheProvider>();
            ReturnsExtensions.ReturnsAsync(cache.Setup(c => c.OpenWriteAsync("test")), ms);

            var projection = new Mock<IProjection<int, string>>();
            projection.Setup(p => p.Initial).Returns("0");
            projection.Setup(p => p.FullName).Returns("test");
            projection.Setup(p => p.State).Returns(typeof(string));
            projection.Setup(p => p.TrySaveAsync(It.IsAny<Stream>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .Throws(new Exception("Projection.TrySaveAsync"));

            projection.Setup(p => p.Apply(It.IsAny<uint>(), It.IsAny<int>(), It.IsAny<string>()))
                .Throws(new Exception("Boo."));

            var reified = Make(projection.Object, cache.Object);

            Assert.Throws<AggregateException>(() => reified.Apply(1U, 13), "Boo."); // Sets 'inconsistent'

            await reified.TrySaveAsync(CancellationToken.None);

            CollectionAssert.AreEqual(new byte[]
            {
                // Cut short
            }, ms.ToArray());
        }
    }
}
