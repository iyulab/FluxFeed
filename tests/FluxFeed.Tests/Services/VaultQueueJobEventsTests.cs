using System.Runtime.CompilerServices;
using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// <see cref="IVaultQueueService.JobStateChanged"/> reports every persisted job transition, and every queue event reaches its
/// handlers after the queue's lock is released, one handler at a time — a handler may call back into the queue, and one that
/// throws changes nothing about the transition.
/// </summary>
public sealed class VaultQueueJobEventsTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"VaultQueueJobEvents_{Guid.NewGuid():N}");

    public VaultQueueJobEventsTests() => Directory.CreateDirectory(_dir);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private VaultQueueService CreateQueue(int maxRetryCount = 3) => new(
        NullLogger<VaultQueueService>.Instance,
        MsOptions.Create(new FileVaultOptions { VaultBasePath = _dir, MaxRetryCount = maxRetryCount }));

    private static List<VaultJobStatus> Collect(IVaultQueueService queue)
    {
        var states = new List<VaultJobStatus>();
        var gate = new object();
        queue.JobStateChanged += (_, job) => { lock (gate) states.Add(job.Status); };
        return states;
    }

    [Fact]
    public async Task ASuccessfulJob_IsQueuedProcessingCompleted()
    {
        using var queue = CreateQueue();
        var states = Collect(queue);

        var job = await queue.EnqueueMemorizeAsync("hash-a", Path.Combine(_dir, "a.txt"), ct: Ct);
        (await queue.DequeueAsync(Ct))!.Id.Should().Be(job.Id);
        await queue.CompleteAsync(job.Id, Ct);

        states.Should().Equal(VaultJobStatus.Queued, VaultJobStatus.Processing, VaultJobStatus.Completed);
    }

    [Fact]
    public async Task AFailedJob_ReportsFailedWithItsError_AndARetryOrRequeueReportsQueued()
    {
        using var queue = CreateQueue();
        var jobs = new List<VaultJob>();
        queue.JobStateChanged += (_, j) => jobs.Add(j);

        var job = await queue.EnqueueMemorizeAsync("hash-b", Path.Combine(_dir, "b.txt"), ct: Ct);
        await queue.DequeueAsync(Ct);
        await queue.FailAsync(job.Id, "extractor crashed", Ct);
        (await queue.RetryAsync(job.Id, Ct)).Should().BeTrue();
        await queue.DequeueAsync(Ct);
        await queue.FailAsync(job.Id, "extractor crashed again", Ct);
        await queue.RequeueAsync(job.Id, Ct);

        jobs.Select(j => j.Status).Should().Equal(
            VaultJobStatus.Queued, VaultJobStatus.Processing, VaultJobStatus.Failed, VaultJobStatus.Queued,
            VaultJobStatus.Processing, VaultJobStatus.Failed, VaultJobStatus.Queued);
        jobs[2].ErrorMessage.Should().Be("extractor crashed");
    }

    [Fact]
    public async Task ACancelledJob_ReportsCancelled()
    {
        using var queue = CreateQueue();
        var states = Collect(queue);

        var job = await queue.EnqueueMemorizeAsync("hash-c", Path.Combine(_dir, "c.txt"), ct: Ct);
        (await queue.CancelAsync(job.Id, Ct)).Should().BeTrue();

        states.Should().Equal(VaultJobStatus.Queued, VaultJobStatus.Cancelled);
    }

    [Fact]
    public async Task AStuckJobPutBack_ReportsQueued()
    {
        using var queue = CreateQueue();
        var job = await queue.EnqueueMemorizeAsync("hash-d", Path.Combine(_dir, "d.txt"), ct: Ct);
        await queue.DequeueAsync(Ct);
        // The worker stopped mid-job: its failure report carries the already-cancelled stopping token, so the row stays
        // Processing while nothing runs it (VaultQueueCheckpointTests uses the same shape).
        using (var stopping = new CancellationTokenSource())
        {
            await stopping.CancelAsync();
            var report = () => queue.FailAsync(job.Id, "worker stopped", stopping.Token);
            await report.Should().ThrowAsync<OperationCanceledException>();
        }
        var jobs = new List<VaultJob>();
        queue.JobStateChanged += (_, j) => jobs.Add(j);

        (await queue.RecoverStuckJobsAsync(Ct)).Should().Be(1);

        jobs.Should().ContainSingle().Which.Should().Match<VaultJob>(j => j.Id == job.Id && j.Status == VaultJobStatus.Queued);
    }

    [Fact]
    public async Task AMergedEnqueue_WakesTheWorker_ButChangesNoState()
    {
        using var queue = CreateQueue();
        var enqueued = 0;
        queue.JobEnqueued += (_, _) => enqueued++;
        var states = Collect(queue);

        await queue.EnqueueMemorizeAsync("hash-e", Path.Combine(_dir, "e.txt"), ct: Ct);
        await queue.EnqueueMemorizeAsync("hash-e", Path.Combine(_dir, "e.txt"), ct: Ct);

        enqueued.Should().Be(2);
        states.Should().Equal(VaultJobStatus.Queued);
    }

    [Fact]
    public async Task ARequeue_WakesTheWorker()
    {
        using var queue = CreateQueue(maxRetryCount: 0);
        var job = await queue.EnqueueMemorizeAsync("hash-f", Path.Combine(_dir, "f.txt"), ct: Ct);
        await queue.DequeueAsync(Ct);
        await queue.FailAsync(job.Id, "boom", Ct);
        var woken = new List<Guid>();
        queue.JobEnqueued += (_, j) => woken.Add(j.Id);

        await queue.RequeueAsync(job.Id, Ct);

        woken.Should().Equal(job.Id);
    }

    /// <summary>
    /// A throwing JobCompleted subscriber used to make CompleteAsync throw, and the worker then recorded the job it had just
    /// completed as failed.
    /// </summary>
    [Fact]
    public async Task AThrowingHandler_LeavesTheJobCompleted_AndTheOtherHandlersRun()
    {
        using var queue = CreateQueue();
        queue.JobCompleted += (_, _) => throw new InvalidOperationException("handler bug");
        queue.JobStateChanged += (_, _) => throw new InvalidOperationException("handler bug");
        var completed = new List<Guid>();
        queue.JobCompleted += (_, j) => completed.Add(j.Id);

        var job = await queue.EnqueueMemorizeAsync("hash-g", Path.Combine(_dir, "g.txt"), ct: Ct);
        await queue.DequeueAsync(Ct);
        await queue.CompleteAsync(job.Id, Ct);

        completed.Should().Equal(job.Id);
        (await queue.GetJobAsync(job.Id, Ct))!.Status.Should().Be(VaultJobStatus.Completed);
    }

    [Fact]
    public async Task AHandlerMayCallBackIntoTheQueue()
    {
        using var queue = CreateQueue();
        var seen = new List<VaultJobStatus>();
        queue.JobStateChanged += (_, j) =>
        {
            // Under the queue's lock this would never return.
            var stored = queue.GetJobAsync(j.Id, CancellationToken.None).Wait(TimeSpan.FromSeconds(5));
            stored.Should().BeTrue("the lock is released before handlers run");
            seen.Add(j.Status);
        };

        var job = await queue.EnqueueMemorizeAsync("hash-h", Path.Combine(_dir, "h.txt"), ct: Ct);
        await queue.DequeueAsync(Ct);
        await queue.CompleteAsync(job.Id, Ct);

        seen.Should().Equal(VaultJobStatus.Queued, VaultJobStatus.Processing, VaultJobStatus.Completed);
    }

    /// <summary>
    /// A vault subscribes to the queue for upgrade completions; the queue outlives it, and the subscription used to keep
    /// every vault — one per scope, one per watched-file change — alive for the queue's lifetime.
    /// </summary>
    [Fact]
    public void AVaultThatIsDropped_IsNotKeptAliveByTheQueue()
    {
        using var queue = CreateQueue();

        var vault = MakeAndDropVault(queue);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        vault.IsAlive.Should().BeFalse();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private WeakReference MakeAndDropVault(IVaultQueueService queue)
    {
        var options = new FileVaultOptions { VaultBasePath = Path.Combine(_dir, ".vault"), EnableRealTimeWatch = false };
        var git = Substitute.For<IGitService>();
        var hasher = new ContentHasher();
        var storage = new VaultStorageService(NullLogger<VaultStorageService>.Instance, git, MsOptions.Create(options));
        var pipeline = new VaultPipeline(git, hasher, storage, NullLogger<VaultPipeline>.Instance,
            extractor: null, chunker: null, vectorStore: null, embeddingService: null);
        var vault = new VaultManager(hasher, git, pipeline, queue, Substitute.For<IFileWatcherService>(), storage,
            NullLogger<VaultManager>.Instance, MsOptions.Create(options));
        return new WeakReference(vault);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }
}
