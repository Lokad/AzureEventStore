using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Lokad.AzureEventStore.Streams
{
    /// <summary> A stream representing a small window in another stream. </summary>
    /// <remarks>
    ///     Reading from the bounded stream also reads from the underlying
    ///     stream, so it is a very bad idea to have multiple bounded streams
    ///     alive for the same stream.
    /// </remarks>
    public sealed class BoundedStream : Stream
    {
        /// <summary> The wrapped stream. </summary>
        public readonly Stream InnerStream;

        /// <summary> The offset of the start of the window. </summary>
        public readonly long Offset;

        public BoundedStream(Stream stream, long length)
        {
            InnerStream = stream;
            Offset = InnerStream.Position;
            Length = length;

            if (length < 0)
                throw new ArgumentException($"Negative length {length}", nameof(length));
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length { get; }

        public override long Position
        {
            get => InnerStream.Position - Offset;
            set => throw new NotSupportedException();
        }

        protected override void Dispose(bool disposing)
        {
            if (!disposing) return;
            InnerStream.Dispose();
        }

        public override void Flush() { }

        /// <summary>
        ///     Clip the count so that reading will not go beyond the end of the stream.
        /// </summary>
        private int Clip(int count)
        {
            var off = InnerStream.Position - Offset;
            return Length - off >= count ? count : (int)(Length - off);
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            InnerStream.Read(buffer, offset, Clip(count));

#if NET462
        public override IAsyncResult BeginRead(byte[] buffer, int offset, int count, AsyncCallback callback, object state) =>
            InnerStream.BeginRead(buffer, offset, Clip(count), callback, state);

        public override int EndRead(IAsyncResult asyncResult) =>
            InnerStream.EndRead(asyncResult);
#endif

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
            InnerStream.ReadAsync(buffer, offset, Clip(count), cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) =>
            (InnerStream.Position =
                origin == SeekOrigin.Begin ? offset + Offset :
                origin == SeekOrigin.End ? Offset + Length - offset :
                Offset + offset) - Offset;

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }
}
