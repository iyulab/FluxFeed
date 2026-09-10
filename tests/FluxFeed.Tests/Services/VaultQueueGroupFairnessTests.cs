using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// One <c>IVaultQueueService</c> shared across tenants is the default registration, not an exotic
/// setup, and until now nothing kept one tenant's backlog from occupying the whole queue.
/// </summary>
/// <remarks>
/// <para>
/// Reported after an incident where one desk's 84 orphaned jobs held the queue head for 53 minutes
/// while another desk's two uploads waited. Clearing the orphans did not fix the shape: a single
/// legitimate long job from one desk still stopped every other desk, because <c>DequeueAsync</c>
/// ordered purely by priority and arrival and had no way to express "not more of that one".
/// </para>
/// <para>
/// The cap is deliberately <b>work-conserving</b> - it binds only while another group has work
/// waiting. A cap that always bound would buy fairness by idling workers, which is a price a
/// single-tenant deployment should never pay and which would make the feature not worth enabling.
/// </para>
/// </remarks>
public class VaultQueueGroupFairnessTests : IDisposable
{
    private readonly string _testDir;
    private readonly FileVaultOptions _options;

    public VaultQueueGroupFairnessTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultGroupFairnessTests_" + Guid.NewGuid().ToString("N"));
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
        GC.SuppressFinalize(this);
    }

    private VaultQueueService CreateService() =>
        new(NullLogger<VaultQueueService>.Instance, MsOptions.Create(_options));

    private Task EnqueueAsync(VaultQueueService queue, string hash, string group) =>
        queue.EnqueueMemorizeAsync(
            hash, Path.Combine(_testDir, hash + ".md"), groupKey: group,
            ct: TestContext.Current.CancellationToken);

    [Fact]
    public async Task ABacklogFromOneGroup_DoesNotStarveAnotherGroupThatArrivedLater()
    {
        // The reported shape, minimised: desk A queues a backlog, desk B queues one file afterwards.
        // Ordered purely by arrival, B waits behind all of A.
        _options.MaxInFlightPerGroup = 1;
        var queue = CreateService();
        for (var i = 0; i < 5; i++)
            await EnqueueAsync(queue, $"a{i}", "desk-a");
        await EnqueueAsync(queue, "b0", "desk-b");

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        first!.FilepathHash.Should().Be("a0", "arrival order still decides who goes first");
        second!.FilepathHash.Should().Be(
            "b0",
            "desk-a already holds its share, so the next slot goes to the desk that has none - " +
            "even though four of desk-a's jobs arrived earlier");
    }

    [Fact]
    public async Task AGroupAloneOnTheQueue_MayExceedItsShare()
    {
        // The work-conserving half. With nobody else waiting, holding desk-a to one job would idle
        // workers for no one's benefit.
        _options.MaxInFlightPerGroup = 1;
        var queue = CreateService();
        for (var i = 0; i < 3; i++)
            await EnqueueAsync(queue, $"a{i}", "desk-a");

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        first.Should().NotBeNull();
        second.Should().NotBeNull("no other group is waiting, so the cap must not idle a worker");
    }

    [Fact]
    public async Task TheCapStartsBindingAsSoonAsAnotherGroupQueues()
    {
        // Same run, two phases: alone it exceeds the share, then a competitor appears and it stops.
        _options.MaxInFlightPerGroup = 1;
        var queue = CreateService();
        await EnqueueAsync(queue, "a0", "desk-a");
        await EnqueueAsync(queue, "a1", "desk-a");

        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).Should().NotBeNull();
        (await queue.DequeueAsync(TestContext.Current.CancellationToken))
            .Should().NotBeNull("still alone");

        await EnqueueAsync(queue, "a2", "desk-a");
        await EnqueueAsync(queue, "b0", "desk-b");

        var next = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        next!.FilepathHash.Should().Be(
            "b0", "desk-a is over its share now that desk-b is waiting");
    }

    [Fact]
    public async Task UngroupedJobs_AreNeverCapped()
    {
        // Nothing changes for a consumer that passes no group key.
        _options.MaxInFlightPerGroup = 1;
        var queue = CreateService();
        await queue.EnqueueMemorizeAsync("u0", Path.Combine(_testDir, "u0.md"), ct: TestContext.Current.CancellationToken);
        await queue.EnqueueMemorizeAsync("u1", Path.Combine(_testDir, "u1.md"), ct: TestContext.Current.CancellationToken);
        await EnqueueAsync(queue, "b0", "desk-b");

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        first!.FilepathHash.Should().Be("u0");
        second!.FilepathHash.Should().Be("u1", "an ungrouped job has no share to exceed");
    }

    [Fact]
    public async Task CapOfZero_DisablesFairnessEntirely()
    {
        _options.MaxInFlightPerGroup = 0;
        var queue = CreateService();
        await EnqueueAsync(queue, "a0", "desk-a");
        await EnqueueAsync(queue, "a1", "desk-a");
        await EnqueueAsync(queue, "b0", "desk-b");

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        first!.FilepathHash.Should().Be("a0");
        second!.FilepathHash.Should().Be("a1", "with the cap off, arrival order is the only rule");
    }

    [Fact]
    public async Task ALargerCap_LetsAGroupHoldMoreSlotsBeforeYielding()
    {
        _options.MaxInFlightPerGroup = 2;
        var queue = CreateService();
        for (var i = 0; i < 4; i++)
            await EnqueueAsync(queue, $"a{i}", "desk-a");
        await EnqueueAsync(queue, "b0", "desk-b");

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var third = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        first!.FilepathHash.Should().Be("a0");
        second!.FilepathHash.Should().Be("a1", "two slots is desk-a's share");
        third!.FilepathHash.Should().Be("b0", "the third would be desk-a's third");
    }

    [Fact]
    public async Task FinishingAJob_ReturnsTheShareToTheGroup()
    {
        _options.MaxInFlightPerGroup = 1;
        var queue = CreateService();
        await EnqueueAsync(queue, "a0", "desk-a");
        await EnqueueAsync(queue, "a1", "desk-a");
        await EnqueueAsync(queue, "b0", "desk-b");

        var a = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        var b = await queue.DequeueAsync(TestContext.Current.CancellationToken);
        b!.FilepathHash.Should().Be("b0");

        await queue.CompleteAsync(a!.Id, TestContext.Current.CancellationToken);
        var next = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        next!.FilepathHash.Should().Be("a1", "desk-a is under its share again");
    }

    [Fact]
    public async Task PriorityStillDecidesAmongEligibleJobs()
    {
        // The cap decides who is eligible; priority still orders the eligible ones. A crawl enqueued
        // at Low must not push a user's upload out of the way inside the same group.
        _options.MaxInFlightPerGroup = 0;
        var queue = CreateService();
        await queue.EnqueueMemorizeAsync(
            "low", Path.Combine(_testDir, "low.md"), VaultJobPriority.Low,
            groupKey: "desk-a", ct: TestContext.Current.CancellationToken);
        await queue.EnqueueMemorizeAsync(
            "high", Path.Combine(_testDir, "high.md"), VaultJobPriority.High,
            groupKey: "desk-a", ct: TestContext.Current.CancellationToken);

        var first = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        first!.FilepathHash.Should().Be("high");
    }

    [Fact]
    public async Task SameEntryExclusion_StillApplies_InsideAGroup()
    {
        // The 0.22.0 guarantee must survive: one entry's git repository is still never written twice
        // at once, whatever the group cap allows.
        _options.MaxInFlightPerGroup = 4;
        var queue = CreateService();
        var path = Path.Combine(_testDir, "one.md");
        await queue.EnqueueMemorizeAsync("same", path, groupKey: "desk-a", ct: TestContext.Current.CancellationToken);
        await queue.EnqueueRefreshAsync("same", path, groupKey: "desk-a", ct: TestContext.Current.CancellationToken);

        (await queue.DequeueAsync(TestContext.Current.CancellationToken)).Should().NotBeNull();
        var second = await queue.DequeueAsync(TestContext.Current.CancellationToken);

        second.Should().BeNull("the entry is in flight; the group cap does not relax that");
    }
}
