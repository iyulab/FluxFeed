using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// A git repository in FluxFeed lives per <b>entry</b> (<c>VaultEntry.VaultPath</c> = <c>&lt;EntryPath&gt;/vault</c>),
/// not per FileVault. Two jobs for two different files therefore commit into two different repositories and are
/// safe to run in parallel — that is the concurrency the worker exists to give. Two jobs for the <b>same</b> file
/// are the opposite: they write the same tree and race on the same <c>index.lock</c>.
///
/// <para>
/// Before this fix the queue offered no protection for that second case. <c>EnqueueJobAsync</c> inserted
/// unconditionally, and <c>DequeueAsync</c> selected purely by <c>status = Queued</c> ordered by priority — nothing
/// consulted what was already <c>Processing</c>. With <c>MaxConcurrentProcessing &gt; 1</c>, a Memorize and a
/// Refresh (or two Memorizes) for one file ran together and raced both the file writes and
/// <c>git add -A &amp;&amp; git commit</c>. Reported by a multi-tenant consumer that had to serialize at its own
/// layer to work around it.
/// </para>
/// </summary>
public class VaultQueueSameEntryConcurrencyTests : IDisposable
{
    private readonly string _testDir;
    private readonly FileVaultOptions _options;

    public VaultQueueSameEntryConcurrencyTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultSameEntryTests_" + Guid.NewGuid().ToString("N"));
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

    [Fact]
    public async Task DequeueAsync_DoesNotHandOutASecondJobForAnEntryAlreadyInFlight()
    {
        var queue = CreateService();
        const string hash = "hash-same-entry";
        var path = Path.Combine(_testDir, "notes.md");

        // Different job types, so this is not the coalescing case below — these are two genuinely
        // distinct pieces of work that nonetheless target one git repository.
        await queue.EnqueueMemorizeAsync(hash, path, ct: TestContext.Current.CancellationToken);
        await queue.EnqueueRefreshAsync(hash, path, ct: TestContext.Current.CancellationToken);

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        first.Should().NotBeNull("the first job for an idle entry is always available");

        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        second.Should().BeNull(
            "the entry's only git repository is already being written by the first job — handing out the second " +
            "is what races index.lock");

        // Once the entry is free the queue must hand the second job out, or the exclusion would be a leak.
        await queue.CompleteAsync(first!.Id, TestContext.Current.CancellationToken);
        var afterCompletion = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        afterCompletion.Should().NotBeNull("the entry is idle again");
        afterCompletion!.JobType.Should().Be(VaultJobType.Refresh);
    }

    [Fact]
    public async Task DequeueAsync_StillHandsOutJobsForOtherEntries_WhileOneEntryIsInFlight()
    {
        var queue = CreateService();
        await queue.EnqueueMemorizeAsync("hash-a", Path.Combine(_testDir, "a.md"), ct: TestContext.Current.CancellationToken);
        await queue.EnqueueMemorizeAsync("hash-b", Path.Combine(_testDir, "b.md"), ct: TestContext.Current.CancellationToken);

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        first.Should().NotBeNull();
        second.Should().NotBeNull(
            "different entries are different git repositories — serializing them would throw away the parallelism " +
            "MaxConcurrentProcessing exists to provide");
        second!.FilepathHash.Should().NotBe(first!.FilepathHash);
    }

    [Fact]
    public async Task EnqueueAsync_CoalescesASecondQueuedJobOfTheSameTypeForTheSameEntry()
    {
        var queue = CreateService();
        const string hash = "hash-coalesce";
        var path = Path.Combine(_testDir, "notes.md");

        var first = await queue.EnqueueMemorizeAsync(hash, path, ct: TestContext.Current.CancellationToken);
        var second = await queue.EnqueueMemorizeAsync(hash, path, ct: TestContext.Current.CancellationToken);

        second.Id.Should().Be(first.Id,
            "a second memorize for a file that is still waiting to be memorized is the same unit of work — the " +
            "caller waiting on it should wait on the job that already exists");

        var queued = await queue.GetJobsAsync(VaultJobStatus.Queued, ct: TestContext.Current.CancellationToken);
        queued.Should().HaveCount(1, "coalescing must not leave a duplicate row behind");
    }

    [Fact]
    public async Task EnqueueAsync_RaisesThePriorityOfTheJobItCoalescesInto()
    {
        var queue = CreateService();
        const string hash = "hash-priority";
        var path = Path.Combine(_testDir, "notes.md");

        await queue.EnqueueMemorizeAsync(hash, path, VaultJobPriority.Low, TestContext.Current.CancellationToken);
        var escalated = await queue.EnqueueMemorizeAsync(hash, path, VaultJobPriority.High, TestContext.Current.CancellationToken);

        escalated.Priority.Should().Be(VaultJobPriority.High,
            "coalescing must not silently discard an urgent request into a low-priority one already queued");

        var queued = await queue.GetJobsAsync(VaultJobStatus.Queued, ct: TestContext.Current.CancellationToken);
        queued.Should().ContainSingle().Which.Priority.Should().Be(VaultJobPriority.High);
    }

    [Fact]
    public async Task EnqueueAsync_DoesNotCoalesceIntoAJobThatIsAlreadyProcessing()
    {
        var queue = CreateService();
        const string hash = "hash-inflight";
        var path = Path.Combine(_testDir, "notes.md");

        await queue.EnqueueMemorizeAsync(hash, path, ct: TestContext.Current.CancellationToken);
        var inFlight = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        inFlight.Should().NotBeNull();

        var followUp = await queue.EnqueueMemorizeAsync(hash, path, ct: TestContext.Current.CancellationToken);

        followUp.Id.Should().NotBe(inFlight!.Id,
            "the in-flight job is already reading the file as it was; a request that arrives after it started is " +
            "asking about a later state and needs its own run");

        var queued = await queue.GetJobsAsync(VaultJobStatus.Queued, ct: TestContext.Current.CancellationToken);
        queued.Should().ContainSingle("the follow-up is waiting behind the in-flight job, not merged into it");
    }
}
