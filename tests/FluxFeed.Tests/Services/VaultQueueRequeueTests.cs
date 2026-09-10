using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Exceptions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// The operator's rerun path.
/// </summary>
/// <remarks>
/// The defect these cover is that the only rerun available enforced the automatic retry budget, so
/// it refused every job an operator would actually press the button for and accepted the one class
/// of job where pressing it achieves nothing.
/// </remarks>
public sealed class VaultQueueRequeueTests : IDisposable
{
    private readonly string _testDir =
        Path.Combine(Path.GetTempPath(), "VaultRequeueTests_" + Guid.NewGuid().ToString("N"));

    public VaultQueueRequeueTests() => Directory.CreateDirectory(_testDir);

    private VaultQueueService CreateQueue() =>
        new(NullLogger<VaultQueueService>.Instance,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _testDir }));

    [Fact]
    public async Task Requeue_revives_a_job_that_has_spent_its_whole_retry_budget()
    {
        // The case the operator button exists for, and the one the budget-respecting path refused.
        using var queue = CreateQueue();
        var job = await queue.EnqueueMemorizeAsync("hash-1", "/vault/a.txt");

        await ExhaustBudgetAsync(queue, job.Id);

        (await queue.RetryAsync(job.Id)).Should().BeFalse(
            "the automatic path is budget-bound, and this is what the operator ran into");

        await queue.RequeueAsync(job.Id);

        var revived = await queue.GetJobAsync(job.Id);
        revived!.Status.Should().Be(VaultJobStatus.Queued);
        revived.RetryCount.Should().Be(0, "an explicit instruction clears the automatic budget");
        revived.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public async Task Requeue_refuses_a_failure_no_attempt_can_change()
    {
        // Before the classification was persisted this call SUCCEEDED: a permanent failure never
        // spends budget, so the job looked retryable and was re-queued, only to fail identically.
        // The operator saw a button that appeared to work.
        using var queue = CreateQueue();
        var job = await queue.EnqueueMemorizeAsync("hash-2", "/vault/locked.xlsx");
        await queue.DequeueAsync();
        await queue.FailAsync(job.Id, "document is encrypted", MemorizeFailureKind.Permanent);

        var refusal = await Assert.ThrowsAsync<VaultJobNotRetryableException>(
            () => queue.RequeueAsync(job.Id));

        refusal.Reason.Should().Be(VaultRetryRefusal.PermanentFailure);
        refusal.JobId.Should().Be(job.Id);
        refusal.Message.Should().Contain("encrypted", "the operator needs the reason, not just a refusal");

        var untouched = await queue.GetJobAsync(job.Id);
        untouched!.Status.Should().Be(VaultJobStatus.Failed, "a refused request changes nothing");
    }

    [Fact]
    public async Task Requeue_allows_a_failure_that_was_never_classified()
    {
        // A row written before the classification existed reads as null, and null is not evidence
        // of permanence. Upgrading must not start refusing what it used to allow.
        using var queue = CreateQueue();
        var job = await queue.EnqueueMemorizeAsync("hash-3", "/vault/b.txt");
        await queue.DequeueAsync();
        await queue.FailAsync(job.Id, "provider timed out");

        await queue.RequeueAsync(job.Id);

        (await queue.GetJobAsync(job.Id))!.Status.Should().Be(VaultJobStatus.Queued);
    }

    [Fact]
    public async Task Requeue_separates_a_missing_job_from_one_it_will_not_run()
    {
        // Both used to be the same false, and they call for different answers: refresh the list
        // versus explain why this one is stuck.
        using var queue = CreateQueue();

        await Assert.ThrowsAsync<VaultJobNotFoundException>(() => queue.RequeueAsync(Guid.NewGuid()));

        var queued = await queue.EnqueueMemorizeAsync("hash-4", "/vault/c.txt");
        var refusal = await Assert.ThrowsAsync<VaultJobNotRetryableException>(
            () => queue.RequeueAsync(queued.Id));
        refusal.Reason.Should().Be(VaultRetryRefusal.NotFailed);
    }

    [Fact]
    public async Task A_permanent_failure_leaves_its_retry_budget_intact()
    {
        // Pins the fact the refusal above depends on. If a permanent failure ever started spending
        // budget, PermanentFailure and an exhausted budget would become indistinguishable again.
        using var queue = CreateQueue();
        var job = await queue.EnqueueMemorizeAsync("hash-5", "/vault/locked2.xlsx");
        await queue.DequeueAsync();
        await queue.FailAsync(job.Id, "document is encrypted", MemorizeFailureKind.Permanent);

        var failed = await queue.GetJobAsync(job.Id);
        failed!.RetryCount.Should().Be(0);
        failed.FailureKind.Should().Be(MemorizeFailureKind.Permanent);
        failed.CanRetry.Should().BeTrue(
            "which is exactly why the budget-bound path accepted it and achieved nothing");
    }

    private static async Task ExhaustBudgetAsync(VaultQueueService queue, Guid jobId)
    {
        // Drive it the way the worker does, until the budget itself says no. Counting attempts here
        // would encode MaxRetries in the test and quietly stop exhausting anything if it changed.
        while (true)
        {
            await queue.DequeueAsync();
            await queue.FailAsync(jobId, "provider timed out", MemorizeFailureKind.Transient);
            if (!await queue.RetryAsync(jobId))
                return;
        }
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
        catch (IOException)
        {
            // SQLite may still hold the file on Windows; a temp directory left behind is not worth
            // failing a test over.
        }
    }
}
