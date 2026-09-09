using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// The two questions a consumer polls this queue to answer, and the two ways it could not answer them.
///
/// <para>
/// <b>"Is the worker alive?"</b> — <c>QueueStatistics.LastProcessedAt</c> was derived only from jobs whose
/// status is <c>Completed</c>, so a worker that was processing steadily but failing every job left it frozen.
/// A consumer polling it as a heartbeat read a healthy worker as stopped (reported after eight minutes of
/// exactly that, over nineteen failing jobs). The field is now <c>LastSucceededAt</c>, which is what it always
/// was, and <c>LastAttemptedAt</c> answers the liveness question beside it.
/// </para>
/// <para>
/// <b>"What is failing right now?"</b> — <c>GetJobsAsync</c> had a fixed oldest-first order and no offset, so
/// asking for fifty failures returned the fifty <i>oldest</i>, and paging meant loading a whole status into the
/// consumer and sorting it there.
/// </para>
/// </summary>
public class VaultQueueObservabilityTests : IDisposable
{
    private readonly string _testDir;
    private readonly FileVaultOptions _options;

    public VaultQueueObservabilityTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultObservabilityTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);
        _options = new FileVaultOptions { VaultBasePath = _testDir };
    }

    public void Dispose()
    {
        try
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; OS may still hold file handles briefly.
        }
    }

    private VaultQueueService CreateService() =>
        new(NullLogger<VaultQueueService>.Instance, MsOptions.Create(_options));

    // ---------------------------------------------------------------- #231 liveness

    [Fact]
    public async Task LastAttemptedAt_MovesForAFailingWorker_WhileLastSucceededAtStaysNull()
    {
        var queue = CreateService();
        await queue.EnqueueMemorizeAsync("hash-fail", Path.Combine(_testDir, "a.md"), ct: TestContext.Current.CancellationToken);

        var job = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        await queue.FailAsync(job!.Id, "extraction blew up", TestContext.Current.CancellationToken);

        var stats = await queue.GetStatisticsAsync(TestContext.Current.CancellationToken);

        stats.LastSucceededAt.Should().BeNull("nothing has succeeded — that is the honest answer");
        stats.LastAttemptedAt.Should().NotBeNull(
            "the worker picked up a job and finished with it; a consumer polling for liveness must see that, or " +
            "it reads a busy-but-failing worker as a stopped one");
        stats.FailedCount.Should().Be(1);
    }

    [Fact]
    public async Task LastAttemptedAt_MovesWhenAJobIsPickedUp_BeforeItReachesAnyTerminalState()
    {
        var queue = CreateService();
        await queue.EnqueueMemorizeAsync("hash-slow", Path.Combine(_testDir, "big.md"), ct: TestContext.Current.CancellationToken);

        var beforeDequeue = await queue.GetStatisticsAsync(TestContext.Current.CancellationToken);
        beforeDequeue.LastAttemptedAt.Should().BeNull("nothing has been attempted yet");

        await queue.DequeueAsync(TestContext.Current.CancellationToken);

        var whileProcessing = await queue.GetStatisticsAsync(TestContext.Current.CancellationToken);
        whileProcessing.LastAttemptedAt.Should().NotBeNull(
            "a worker inside a long job is alive; defining liveness from completions alone would call it stopped " +
            "for exactly as long as the job takes");
        whileProcessing.ProcessingCount.Should().Be(1);
    }

    [Fact]
    public async Task LastSucceededAt_TracksTheNewestCompletion()
    {
        var queue = CreateService();
        await queue.EnqueueMemorizeAsync("hash-ok", Path.Combine(_testDir, "a.md"), ct: TestContext.Current.CancellationToken);

        var job = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        await queue.CompleteAsync(job!.Id, TestContext.Current.CancellationToken);

        var stats = await queue.GetStatisticsAsync(TestContext.Current.CancellationToken);
        stats.LastSucceededAt.Should().NotBeNull();
        stats.LastAttemptedAt.Should().NotBeNull();
        stats.CompletedCount.Should().Be(1);
    }

    // ---------------------------------------------------------------- #232 listing

    [Fact]
    public async Task GetJobsAsync_NewestFirst_ReturnsTheMostRecentlyQueued()
    {
        var queue = CreateService();
        for (var i = 0; i < 5; i++)
        {
            await queue.EnqueueMemorizeAsync($"hash-{i}", Path.Combine(_testDir, $"{i}.md"), ct: TestContext.Current.CancellationToken);
            await Task.Delay(5, TestContext.Current.CancellationToken); // queued_at has to differ for an ordering assertion to mean anything
        }

        var newest = await queue.GetJobsAsync(VaultJobStatus.Queued, limit: 2, newestFirst: true, ct: TestContext.Current.CancellationToken);

        newest.Should().HaveCount(2);
        // Equal(params string[]) would read a "because" argument as a third expected element, so the
        // reason goes on a separate assertion rather than into this one.
        newest.Select(j => Path.GetFileName(j.FilePath)).Should().Equal(new[] { "4.md", "3.md" });
    }

    [Fact]
    public async Task GetJobsAsync_DefaultOrder_IsUnchanged()
    {
        var queue = CreateService();
        await queue.EnqueueMemorizeAsync("hash-0", Path.Combine(_testDir, "0.md"), ct: TestContext.Current.CancellationToken);
        await Task.Delay(5, TestContext.Current.CancellationToken);
        await queue.EnqueueMemorizeAsync("hash-1", Path.Combine(_testDir, "1.md"), ct: TestContext.Current.CancellationToken);

        var jobs = await queue.GetJobsAsync(VaultJobStatus.Queued, ct: TestContext.Current.CancellationToken);

        Path.GetFileName(jobs[0].FilePath).Should().Be("0.md",
            "the new parameters are opt-in; existing callers must see exactly the order they saw before");
    }

    [Fact]
    public async Task GetJobsAsync_Offset_PagesInSql()
    {
        var queue = CreateService();
        for (var i = 0; i < 5; i++)
        {
            await queue.EnqueueMemorizeAsync($"hash-{i}", Path.Combine(_testDir, $"{i}.md"), ct: TestContext.Current.CancellationToken);
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        var page2 = await queue.GetJobsAsync(VaultJobStatus.Queued, limit: 2, offset: 2, newestFirst: true, ct: TestContext.Current.CancellationToken);

        page2.Should().HaveCount(2);
        page2.Select(j => Path.GetFileName(j.FilePath)).Should().Equal(new[] { "2.md", "1.md" });
    }

    [Fact]
    public async Task GetJobsAsync_OffsetWithoutLimit_SkipsAndReturnsTheRest()
    {
        var queue = CreateService();
        for (var i = 0; i < 3; i++)
        {
            await queue.EnqueueMemorizeAsync($"hash-{i}", Path.Combine(_testDir, $"{i}.md"), ct: TestContext.Current.CancellationToken);
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        var rest = await queue.GetJobsAsync(VaultJobStatus.Queued, offset: 1, ct: TestContext.Current.CancellationToken);

        rest.Should().HaveCount(2, "SQLite needs a LIMIT to accept an OFFSET; an omitted limit must not silently " +
                                   "drop the offset or return nothing");
    }
}
