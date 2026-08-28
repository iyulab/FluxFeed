using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// The queue is the path a consumer's memorize actually travels, so this is where an unreadable
/// record has to stay recoverable: rewriting the record is what repairs it, and failing the job
/// instead would leave the entry permanently stuck. A sweep over every record has the opposite
/// requirement — one damaged record must not stop the rest from being visited.
/// </summary>
public class VaultBackgroundServiceRecordFailureTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;

    public VaultBackgroundServiceRecordFailureTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultBgRecordTests_" + Guid.NewGuid().ToString("N"));
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_vaultDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task MemorizeJob_WhenTheRecordIsUnreadable_RebuildsItAndCompletesTheJob()
    {
        // Arrange
        var sourcePath = Path.Combine(_testDir, "report.md");
        File.WriteAllText(sourcePath, "content");
        var filepathHash = CorruptRecordFor(sourcePath);

        var pipeline = Substitute.For<IVaultPipeline>();
        using var queue = new OneJobQueueService(
            VaultJob.Create(sourcePath, filepathHash, VaultJobType.Memorize, VaultJobPriority.Normal));

        using var service = CreateService(queue, pipeline);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Act
        _ = service.StartAsync(cts.Token);
        var completed = await queue.JobSettled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await service.StopAsync(CancellationToken.None);

        // Assert
        completed.Should().BeTrue("an unreadable record is rebuilt by the very write this job performs");
        await pipeline.Received(1).MemorizeAsync(
            Arg.Is<VaultEntry>(e => e.SourcePath == Path.GetFullPath(sourcePath)),
            Arg.Any<MemorizeOptions>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RecoverPartialRemovals_WithAnUnreadableRecord_StillRecoversTheOthers()
    {
        // Arrange
        var stuckPath = Path.Combine(_testDir, "stuck.md");
        var stuck = VaultEntry.Create(stuckPath, _vaultDir);
        stuck.MarkRemovalPending();
        stuck.SaveMetadata();

        CorruptRecordFor(Path.Combine(_testDir, "damaged.md"));

        var pipeline = Substitute.For<IVaultPipeline>();
        using var queue = new OneJobQueueService(job: null);
        using var service = CreateService(queue, pipeline);

        // Act
        await service.RecoverPartialRemovalsAsync(CancellationToken.None);

        // Assert - the sweep must not stop at the damaged record.
        queue.RemoveEnqueuedFor.Should().ContainSingle().Which.Should().Be(stuck.FilepathHash);
    }

    private VaultBackgroundService CreateService(IVaultQueueService queue, IVaultPipeline pipeline)
    {
        var provider = Substitute.For<IServiceProvider>();
        provider.GetService(typeof(IVaultPipeline)).Returns(pipeline);

        var scope = Substitute.For<IServiceScope>();
        scope.ServiceProvider.Returns(provider);

        var scopeFactory = Substitute.For<IServiceScopeFactory>();
        scopeFactory.CreateScope().Returns(scope);

        var storage = Substitute.For<IVaultStorageService>();
        storage.BasePath.Returns(_vaultDir);

        var options = MsOptions.Create(new FileVaultOptions
        {
            VaultBasePath = _vaultDir,
            EnableBackgroundProcessing = true,
            EnableAutoRetry = false
        });

        return new VaultBackgroundService(
            NullLogger<VaultBackgroundService>.Instance, queue, scopeFactory, storage, options);
    }

    private string CorruptRecordFor(string filePath)
    {
        var entry = VaultEntry.Create(filePath, _vaultDir);
        entry.SaveMetadata();
        File.WriteAllText(entry.MetaPath, "{ this is not a record");
        return entry.FilepathHash;
    }

    /// <summary>
    /// Hands out a single job and then nothing, and records how that job settled.
    /// </summary>
    private sealed class OneJobQueueService : IVaultQueueService, IDisposable
    {
        private readonly VaultJob? _job;
        private VaultJob? _pending;

        public OneJobQueueService(VaultJob? job)
        {
            _job = job;
            _pending = job;
        }

        public bool IsPaused { get; set; }
        public event EventHandler<VaultJob>? JobEnqueued;
        public event EventHandler<VaultJob>? JobCompleted;

        public readonly TaskCompletionSource<bool> JobSettled =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public readonly List<string> RemoveEnqueuedFor = [];

        public Task<VaultJob?> DequeueAsync(CancellationToken ct = default)
        {
            var next = Interlocked.Exchange(ref _pending, null);
            return Task.FromResult(next);
        }

        public Task CompleteAsync(Guid jobId, CancellationToken ct = default)
        {
            JobCompleted?.Invoke(this, _job!);
            JobSettled.TrySetResult(true);
            return Task.CompletedTask;
        }

        public Task FailAsync(Guid jobId, string errorMessage, CancellationToken ct = default)
        {
            JobSettled.TrySetResult(false);
            return Task.CompletedTask;
        }

        public Task<VaultJob> EnqueueRemoveAsync(string h, string p, CancellationToken ct = default)
        {
            RemoveEnqueuedFor.Add(h);
            var created = VaultJob.Create(p, h, VaultJobType.Remove, VaultJobPriority.Normal);
            JobEnqueued?.Invoke(this, created);
            return Task.FromResult(created);
        }

        public Task<int> RecoverStuckJobsAsync(CancellationToken ct = default) => Task.FromResult(0);
        public Task UpdateCheckpointAsync(Guid jobId, int lastCompletedChunkIndex, CancellationToken ct = default) => Task.CompletedTask;

        // The remaining interface members are unused by VaultBackgroundService in these tests
        public Task<VaultJob> EnqueueMemorizeAsync(string h, string p, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<VaultJob> EnqueueMemorizeAsync(string h, string p, VaultJobPriority pr, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<VaultJob> EnqueueRefreshAsync(string h, string p, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<VaultJob> EnqueueRefreshAsync(string h, string p, VaultJobPriority pr, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<VaultJob> EnqueueRemoveAsync(string h, string p, VaultJobPriority pr, CancellationToken ct = default) => EnqueueRemoveAsync(h, p, ct);
        public Task<IReadOnlyList<VaultJob>> EnqueueBatchAsync(IEnumerable<(string, string)> files, VaultJobType jobType = VaultJobType.Memorize, VaultJobPriority priority = VaultJobPriority.Normal, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<bool> RetryAsync(Guid jobId, CancellationToken ct = default) => Task.FromResult(false);
        public Task<bool> CancelAsync(Guid jobId, CancellationToken ct = default) => Task.FromResult(false);
        public Task<VaultJob?> GetJobAsync(Guid jobId, CancellationToken ct = default) => Task.FromResult<VaultJob?>(null);
        public Task<VaultJob> WaitForJobAsync(Guid jobId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<VaultJob>> GetJobsAsync(VaultJobStatus? s = null, VaultJobType? t = null, int? limit = null, CancellationToken ct = default) => Task.FromResult<IReadOnlyList<VaultJob>>([]);
        public Task<QueueStatistics> GetStatisticsAsync(CancellationToken ct = default) => Task.FromResult(new QueueStatistics());
        public Task<int> ClearCompletedAsync(CancellationToken ct = default) => Task.FromResult(0);
        public Task<int> ClearFailedAsync(CancellationToken ct = default) => Task.FromResult(0);
        public Task ClearAllAsync(CancellationToken ct = default) => Task.CompletedTask;
        public void Pause() => IsPaused = true;
        public void ResumeProcessing() => IsPaused = false;

        public void Dispose() { }
    }
}
