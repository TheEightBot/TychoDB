using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace TychoDB;

public class ProgressStream : Stream
{
    private readonly Stream _innerStream;
    private readonly IProgress<double> _progress;
    private long _bytesRead = 0;

    public ProgressStream(Stream innerStream, IProgress<double> progress)
    {
        _innerStream = innerStream ?? throw new ArgumentNullException(nameof(innerStream));
        _progress = progress ?? throw new ArgumentNullException(nameof(progress));
    }

    public override bool CanRead => _innerStream.CanRead;

    public override bool CanSeek => _innerStream.CanSeek;

    public override bool CanWrite => _innerStream.CanWrite;

    public override long Length => _innerStream.Length;

    public override long Position
    {
        get => _innerStream.Position;
        set => _innerStream.Position = value;
    }

    public override void Flush() => _innerStream.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => _innerStream.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count)
    {
        int bytesRead = _innerStream.Read(buffer, offset, count);
        _bytesRead += bytesRead;
        _progress.Report(_bytesRead / (double)Length); // Report total bytes read
        return bytesRead;
    }

    public override long Seek(long offset, SeekOrigin origin) => _innerStream.Seek(offset, origin);

    public override void SetLength(long value) => _innerStream.SetLength(value);

    public override void Write(byte[] buffer, int offset, int count)
    {
        _innerStream.Write(buffer, offset, count);
        _bytesRead += count; // Assuming writing also tracks progress
        _progress.Report(_bytesRead / (double)Length);
    }

    // You might also override ReadAsync, WriteAsync for asynchronous operations
    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int bytesRead = await _innerStream.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        _bytesRead += bytesRead;
        _progress.Report(_bytesRead / (double)Length);
        return bytesRead;
    }
}
