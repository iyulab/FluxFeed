using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Stuck-job recovery must tell a job abandoned by a dead process from a job this process is still
/// running. Resetting a running job hands it out a second time: <see cref="VaultQueueService.DequeueAsync"/>
/// judges "in flight" by status, which the reset has just cleared, so two pipelines run the same entry
/// at once and race its files.
/// </summary>
public sealed class VaultQueueRecoveryHeldJobsTests : IDisposable
{
    private readonly string _testDir;
    private readonly FileVaultOptions _options;

    public VaultQueueRecoveryHeldJobsTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultRecoveryHeldTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);
        _options = new FileVaultOptions { VaultBasePath = _testDir };
    }

    public void Dispose()
    {
        try
        {
            SqliteConnection.ClearAllPools();
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; the OS may still hold file handles briefly.
        }
    }

    private VaultQueueService CreateService() =>
        new(NullLogger<VaultQueueService>.Instance, MsOptions.Create(_options));

    [Fact]
    public async Task Recovery_leaves_a_job_this_process_is_running_alone()
    {
        using var queue = CreateService();
        var job = await queue.EnqueueMemorizeAsync("hash-running", Path.Combine(_testDir, "running.txt"));
        (await queue.DequeueAsync()).Should().NotBeNull();

        var recovered = await queue.RecoverStuckJobsAsync();

        recovered.Should().Be(0, "the job is still being processed by this process");
        (await queue.GetJobAsync(job.Id))!.Status.Should().Be(VaultJobStatus.Processing);
        (await queue.DequeueAsync()).Should().BeNull("a running job must not be handed out a second time");
    }

    [Fact]
    public async Task Recovery_resets_a_processing_job_no_live_process_holds()
    {
        using var queue = CreateService();
        var job = await queue.EnqueueMemorizeAsync("hash-orphan", Path.Combine(_testDir, "orphan.txt"));
        MarkProcessingAsIfAPreviousProcessDequeuedIt(job.Id);

        var recovered = await queue.RecoverStuckJobsAsync();

        recovered.Should().Be(1);
        (await queue.GetJobAsync(job.Id))!.Status.Should().Be(VaultJobStatus.Queued);
    }

    [Fact]
    public async Task Recovery_resets_a_job_whose_outcome_report_failed()
    {
        // A worker stopped mid-job reports its failure with an already-cancelled token, so the report
        // throws and the row stays Processing. The job is no longer running; recovery must be able to
        // hand it out again instead of treating it as held until the process restarts.
        using var queue = CreateService();
        var job = await queue.EnqueueMemorizeAsync("hash-unreported", Path.Combine(_testDir, "unreported.txt"));
        (await queue.DequeueAsync()).Should().NotBeNull();

        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        var report = () => queue.FailAsync(job.Id, "stopped", cancelled.Token);
        await report.Should().ThrowAsync<OperationCanceledException>();

        var recovered = await queue.RecoverStuckJobsAsync();

        recovered.Should().Be(1);
        (await queue.GetJobAsync(job.Id))!.Status.Should().Be(VaultJobStatus.Queued);
    }

    [Fact]
    public async Task Recovery_from_a_second_queue_instance_on_the_same_database_leaves_the_running_job_alone()
    {
        // A tenant re-created by VaultFactory gets a new queue instance over the same queue.db while
        // the previous worker may still be finishing a job past its stop grace period.
        using var first = CreateService();
        var job = await first.EnqueueMemorizeAsync("hash-shared", Path.Combine(_testDir, "shared.txt"));
        (await first.DequeueAsync()).Should().NotBeNull();

        using var second = CreateService();
        var recovered = await second.RecoverStuckJobsAsync();

        recovered.Should().Be(0);
        (await second.GetJobAsync(job.Id))!.Status.Should().Be(VaultJobStatus.Processing);
    }

    [Fact]
    public async Task A_job_retried_after_its_report_is_held_again_when_dequeued()
    {
        using var queue = CreateService();
        var job = await queue.EnqueueMemorizeAsync("hash-retry", Path.Combine(_testDir, "retry.txt"));
        (await queue.DequeueAsync()).Should().NotBeNull();
        await queue.FailAsync(job.Id, "transient");
        (await queue.RetryAsync(job.Id)).Should().BeTrue();
        (await queue.DequeueAsync()).Should().NotBeNull();

        var recovered = await queue.RecoverStuckJobsAsync();

        recovered.Should().Be(0);
    }

    private void MarkProcessingAsIfAPreviousProcessDequeuedIt(Guid jobId)
    {
        using var connection = new SqliteConnection($"Data Source={Path.Combine(_testDir, "queue.db")}");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "UPDATE vault_jobs SET status = @processing, started_at = @started WHERE id = @id";
        cmd.Parameters.AddWithValue("@processing", (int)VaultJobStatus.Processing);
        cmd.Parameters.AddWithValue("@started", DateTimeOffset.UtcNow.ToString("O"));
        cmd.Parameters.AddWithValue("@id", jobId.ToString());
        cmd.ExecuteNonQuery().Should().Be(1);
    }
}
