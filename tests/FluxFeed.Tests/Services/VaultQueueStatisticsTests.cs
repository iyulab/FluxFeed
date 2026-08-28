using AwesomeAssertions;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Regression tests for <see cref="VaultQueueService.GetStatisticsAsync"/>: <c>LastProcessedAt</c>
/// and <c>AverageProcessingTimeMs</c> must be derived from the persisted <c>vault_jobs</c> rows, not
/// from process-lifetime-only fields. A queue instance that never itself observed a completion (a
/// fresh instance after a restart, or a second instance pointed at the same vault) must still report
/// the completions recorded by whichever instance actually ran them.
/// </summary>
public class VaultQueueStatisticsTests : IDisposable
{
    private readonly string _testDir;
    private readonly FileVaultOptions _options;

    public VaultQueueStatisticsTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultStatisticsTests_" + Guid.NewGuid().ToString("N"));
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
        catch { /* best-effort */ }
    }

    private VaultQueueService CreateService() =>
        new(NullLogger<VaultQueueService>.Instance, MsOptions.Create(_options));

    [Fact]
    public async Task GetStatisticsAsync_ReportsLastProcessedAtAndAverageTime_ForCompletedJobs()
    {
        using var queue = CreateService();
        var job = await queue.EnqueueMemorizeAsync("hash1", Path.Combine(_testDir, "a.txt"));
        await queue.DequeueAsync();
        await Task.Delay(20); // ensure a measurable, non-zero processing duration
        await queue.CompleteAsync(job.Id);

        var stats = await queue.GetStatisticsAsync();

        stats.CompletedCount.Should().Be(1);
        stats.LastProcessedAt.Should().NotBeNull();
        stats.AverageProcessingTimeMs.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task GetStatisticsAsync_SurvivesInstanceRestart()
    {
        // Simulates the reported defect: a completion recorded by one process instance (or before
        // a restart) must still be visible to statistics reads on a fresh instance backed by the
        // same vault directory — the DB row is the source of truth, not in-memory state.
        using (var writer = CreateService())
        {
            var enqueued = await writer.EnqueueMemorizeAsync("hash2", Path.Combine(_testDir, "b.txt"));
            await writer.DequeueAsync();
            await Task.Delay(20);
            await writer.CompleteAsync(enqueued.Id);
        }

        using var reader = CreateService();
        var stats = await reader.GetStatisticsAsync();

        stats.CompletedCount.Should().Be(1);
        stats.LastProcessedAt.Should().NotBeNull();
        stats.AverageProcessingTimeMs.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task GetStatisticsAsync_WithNoCompletedJobs_ReturnsZeroAndNull()
    {
        using var queue = CreateService();
        await queue.EnqueueMemorizeAsync("hash3", Path.Combine(_testDir, "c.txt"));

        var stats = await queue.GetStatisticsAsync();

        stats.CompletedCount.Should().Be(0);
        stats.LastProcessedAt.Should().BeNull();
        stats.AverageProcessingTimeMs.Should().Be(0);
    }

    [Fact]
    public async Task GetStatisticsAsync_LastProcessedAt_IsTheMostRecentCompletion()
    {
        using var queue = CreateService();
        var first = await queue.EnqueueMemorizeAsync("hash4", Path.Combine(_testDir, "d.txt"));
        await queue.DequeueAsync();
        await queue.CompleteAsync(first.Id);

        await Task.Delay(20);

        var second = await queue.EnqueueMemorizeAsync("hash5", Path.Combine(_testDir, "e.txt"));
        await queue.DequeueAsync();
        await queue.CompleteAsync(second.Id);

        var stats = await queue.GetStatisticsAsync();

        stats.CompletedCount.Should().Be(2);
        var secondJob = await queue.GetJobAsync(second.Id);
        stats.LastProcessedAt.Should().Be(secondJob!.CompletedAt);
    }
}
