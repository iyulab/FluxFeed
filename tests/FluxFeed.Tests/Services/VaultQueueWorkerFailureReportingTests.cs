using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Pins that a memorize which failed is reported to the queue as failed.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="IVaultPipeline.MemorizeAsync"/> does not throw on failure - it catches everything and
/// returns <c>MemorizeResult.Failed(...)</c>. The worker used to discard that return value and call
/// <c>CompleteAsync</c> unconditionally, so a document that failed to extract, chunk or embed was
/// recorded as <em>completed</em>: <c>completedCount</c> rose, <c>failedCount</c> did not, the
/// document was not indexed, and nothing in the queue said so.
/// </para>
/// <para>
/// The worker's own catch/retry block could not cover it either, since no exception ever reached it.
/// Only failures raised outside the pipeline (a missing file at entry resolution, for instance) were
/// ever visible - which is why a consumer could see failed jobs and still be missing documents that
/// the queue swore were done.
/// </para>
/// </remarks>
public sealed class VaultQueueWorkerFailureReportingTests
{
    private static (VaultQueueWorker Worker, IVaultQueueService Queue, IVaultPipeline Pipeline) Build(
        FileVaultOptions? options = null)
    {
        var queue = Substitute.For<IVaultQueueService>();
        var pipeline = Substitute.For<IVaultPipeline>();
        var storage = Substitute.For<IVaultStorageService>();
        storage.BasePath.Returns(Path.Combine(Path.GetTempPath(), "fluxfeed-tests", Guid.NewGuid().ToString("N")));

        var worker = new VaultQueueWorker(
            NullLogger.Instance,
            queue,
            VaultQueueWorker.ForSharedPipeline(pipeline),
            storage,
            options ?? new FileVaultOptions { EnableAutoRetry = false });

        return (worker, queue, pipeline);
    }

    private static VaultJob MemorizeJob() =>
        VaultJob.Create("/docs/report.pdf", "abc123", VaultJobType.Memorize);

    [Fact]
    public async Task FailedMemorize_IsReportedAsFailed_NotCompleted()
    {
        var (worker, queue, pipeline) = Build();
        pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Failed("Embedding provider rejected the batch", TimeSpan.FromSeconds(3)));
        var job = MemorizeJob();

        await worker.ProcessJobAsync(job, CancellationToken.None);

        await queue.DidNotReceive().CompleteAsync(job.Id, Arg.Any<CancellationToken>());
        await queue.Received(1).FailAsync(
            job.Id,
            Arg.Is<string>(m => m.Contains("Embedding provider rejected the batch", StringComparison.Ordinal)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SucceededMemorize_IsStillReportedAsCompleted()
    {
        var (worker, queue, pipeline) = Build();
        pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Succeeded(7, 4200, TimeSpan.FromSeconds(2)));
        var job = MemorizeJob();

        await worker.ProcessJobAsync(job, CancellationToken.None);

        await queue.Received(1).CompleteAsync(job.Id, Arg.Any<CancellationToken>());
        await queue.DidNotReceive().FailAsync(job.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task FailedRefresh_IsReportedAsFailed_NotCompleted()
    {
        var (worker, queue, pipeline) = Build();
        pipeline.RefreshAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Failed("no reader for .xyz", TimeSpan.Zero));
        var job = VaultJob.Create("/docs/report.xyz", "def456", VaultJobType.Refresh);

        await worker.ProcessJobAsync(job, CancellationToken.None);

        await queue.DidNotReceive().CompleteAsync(job.Id, Arg.Any<CancellationToken>());
        await queue.Received(1).FailAsync(job.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PermanentFailure_IsNotRetried_EvenWithAutoRetryOn()
    {
        // The incident behind this: a deterministic failure (no reader, missing file, corrupt
        // archive) fails identically on every attempt, so retrying it only multiplies the time the
        // queue head is blocked. One consumer measured ~26s per attempt, four attempts per job,
        // across 84 jobs - a 30-second problem turned into a 37-minute one.
        var (worker, queue, pipeline) = Build(new FileVaultOptions { EnableAutoRetry = true, RetryDelayMs = 0 });
        pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Failed(
                "no reader",
                TimeSpan.Zero,
                new IndexingFailure
                {
                    Stage = IndexingStage.Extraction,
                    ExceptionType = nameof(NotSupportedException)
                }));
        var job = MemorizeJob();

        await worker.ProcessJobAsync(job, CancellationToken.None);

        await queue.Received(1).FailAsync(job.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await queue.DidNotReceive().RetryAsync(job.Id, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task TransientFailure_IsStillRetried()
    {
        // The counterpart that must keep working: a provider timeout or a locked file is worth
        // another attempt, and narrowing retry must not quietly turn into never retrying.
        var (worker, queue, pipeline) = Build(new FileVaultOptions { EnableAutoRetry = true, RetryDelayMs = 0 });
        pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Failed(
                "timed out",
                TimeSpan.Zero,
                new IndexingFailure
                {
                    Stage = IndexingStage.Indexing,
                    ExceptionType = nameof(TimeoutException)
                }));
        var job = MemorizeJob();

        await worker.ProcessJobAsync(job, CancellationToken.None);

        await queue.Received(1).FailAsync(job.Id, Arg.Any<string>(), Arg.Any<CancellationToken>());
        await queue.Received(1).RetryAsync(job.Id, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task UnclassifiedFailure_IsRetried()
    {
        // Only what is confidently deterministic stops retrying. An unrecognised failure keeps
        // today's behaviour, so narrowing the retry cannot silently drop a recoverable job.
        var (worker, queue, pipeline) = Build(new FileVaultOptions { EnableAutoRetry = true, RetryDelayMs = 0 });
        pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Failed("something we have never seen", TimeSpan.Zero));
        var job = MemorizeJob();

        await worker.ProcessJobAsync(job, CancellationToken.None);

        await queue.Received(1).RetryAsync(job.Id, Arg.Any<CancellationToken>());
    }
}

/// <summary>
/// Pins which failures the library calls deterministic. This is the part that decides whether a
/// consumer's queue spends 26 seconds four times over on something that cannot succeed.
/// </summary>
public sealed class MemorizeFailureClassifierTests
{
    [Theory]
    [InlineData(nameof(FileNotFoundException))]
    [InlineData(nameof(DirectoryNotFoundException))]
    [InlineData(nameof(NotSupportedException))]
    [InlineData(nameof(InvalidDataException))]
    [InlineData(nameof(FormatException))]
    public void DeterministicFailures_ArePermanent(string exceptionType)
    {
        MemorizeFailureClassifier.Classify(exceptionType).Should().Be(MemorizeFailureKind.Permanent);
    }

    [Theory]
    [InlineData(nameof(HttpRequestException))]
    [InlineData(nameof(TimeoutException))]
    [InlineData(nameof(IOException))]
    public void RecoverableFailures_AreTransient(string exceptionType)
    {
        MemorizeFailureClassifier.Classify(exceptionType).Should().Be(MemorizeFailureKind.Transient);
    }

    [Fact]
    public void FileNotFound_IsNotSweptIntoTransientByItsIOExceptionBase()
    {
        // Matching is by exact type name precisely so the IO hierarchy cannot make a missing file
        // look retryable. Pinned because it is the one pair where inheritance and intent disagree.
        MemorizeFailureClassifier.Classify(nameof(FileNotFoundException))
            .Should().Be(MemorizeFailureKind.Permanent);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("SomeExceptionNobodyHasSeen")]
    public void UnrecognisedFailures_StayUnknown_SoTheyKeepBeingRetried(string? exceptionType)
    {
        // Being wrong about Permanent loses a recoverable job; being wrong about Unknown only costs
        // an attempt. The default leans to the cheaper mistake.
        MemorizeFailureClassifier.Classify(exceptionType).Should().Be(MemorizeFailureKind.Unknown);
    }
}
