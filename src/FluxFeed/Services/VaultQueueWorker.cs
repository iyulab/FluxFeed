using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Domain.Exceptions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace FluxFeed.Services;

/// <summary>
/// A leased <see cref="IVaultPipeline"/> for one job; disposing it releases whatever the lease
/// holds (a DI scope for the container vault, nothing for a tenant's shared pipeline).
/// </summary>
public interface IVaultPipelineLease : IDisposable, IAsyncDisposable
{
    /// <summary>The pipeline to process the job with.</summary>
    IVaultPipeline Pipeline { get; }
}

/// <summary>
/// Consumes one <see cref="IVaultQueueService"/>: dequeues memorize / refresh / remove jobs and runs
/// them through a pipeline with bounded concurrency, checkpointing and retry. It is the worker
/// behind <see cref="VaultBackgroundService"/> (container vault, hosted) and behind every tenant
/// vault created by <see cref="VaultFactory"/> (owned by its <see cref="VaultContext"/>) — one loop,
/// two hosts, so a queue never exists without a consumer.
/// </summary>
public sealed partial class VaultQueueWorker : IDisposable, IAsyncDisposable
{
    private readonly ILogger _logger;
    private readonly IVaultQueueService _queueService;
    private readonly Func<IVaultPipelineLease> _leasePipeline;
    private readonly IVaultStorageService _storage;
    private readonly FileVaultOptions _options;
    private readonly SemaphoreSlim _concurrencyLimiter;
    private readonly SemaphoreSlim _jobSignal = new(0, int.MaxValue);
    private CancellationTokenSource? _ownedStop;
    private Task? _ownedRun;
    private bool _disposed;

    /// <summary>
    /// Idle timeout for the job signal wait — acts as health-check / stuck-job recovery cadence.
    /// </summary>
    private static readonly TimeSpan JobSignalTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Polling interval used only when background processing is disabled or queue is paused.
    /// </summary>
    private const int PausedPollingMs = 10000;

    /// <summary>How long <see cref="StopAsync"/> waits for in-flight jobs before giving up.</summary>
    private static readonly TimeSpan StopGrace = TimeSpan.FromSeconds(30);

    public VaultQueueWorker(
        ILogger logger,
        IVaultQueueService queueService,
        Func<IVaultPipelineLease> leasePipeline,
        IVaultStorageService storage,
        FileVaultOptions options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _queueService = queueService ?? throw new ArgumentNullException(nameof(queueService));
        _leasePipeline = leasePipeline ?? throw new ArgumentNullException(nameof(leasePipeline));
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _concurrencyLimiter = new SemaphoreSlim(_options.MaxConcurrentProcessing);
        _queueService.JobEnqueued += OnJobEnqueued;
    }

    /// <summary>Leases the scoped <see cref="IVaultPipeline"/> from a new DI scope per job (container vault).</summary>
    public static Func<IVaultPipelineLease> ForScopedPipeline(IServiceScopeFactory scopeFactory)
    {
        ArgumentNullException.ThrowIfNull(scopeFactory);
        return () => new ScopedPipelineLease(scopeFactory.CreateScope());
    }

    /// <summary>Leases one shared <see cref="IVaultPipeline"/> instance for every job (tenant vault).</summary>
    public static Func<IVaultPipelineLease> ForSharedPipeline(IVaultPipeline pipeline)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        var lease = new SharedPipelineLease(pipeline);
        return () => lease;
    }

    /// <summary>Whether at least one job is being processed right now.</summary>
    public bool HasActiveJobs => _concurrencyLimiter.CurrentCount < _options.MaxConcurrentProcessing;

    private void OnJobEnqueued(object? sender, VaultJob job) => _jobSignal.Release();

    /// <summary>
    /// Starts the consume loop on the thread pool, owned by this worker until <see cref="StopAsync"/>
    /// or disposal. Used by hosts that are not <c>IHostedService</c>s (the tenant factory).
    /// </summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_ownedRun is not null)
            return;

        _ownedStop = new CancellationTokenSource();
        _ownedRun = Task.Run(() => RunAsync(_ownedStop.Token));
    }

    /// <summary>
    /// Runs the consume loop until <paramref name="stoppingToken"/> is cancelled. Registers this
    /// worker with the queue for the loop's lifetime, recovers stuck jobs and partial removals first.
    /// </summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        LogServiceStarting(_logger);

        // Announce ourselves as the queue's consumer for the lifetime of this loop so that
        // WaitForJobAsync can distinguish "worker still busy" from "no worker will ever run".
        using var workerLease = _queueService.RegisterWorker();

        try
        {
            // Recover any stuck jobs from previous run
            var recovered = await _queueService.RecoverStuckJobsAsync(stoppingToken);
            if (recovered > 0)
            {
                LogRecoveredStuckJobs(_logger, recovered);
            }

            // Recover entries in partial removal or deleted states
            await RecoverPartialRemovalsAsync(stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            LogServiceStopped(_logger);
            return;
        }

        LogServiceStarted(_logger);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!_options.EnableBackgroundProcessing || _queueService.IsPaused)
                {
                    await Task.Delay(PausedPollingMs, stoppingToken);
                    continue;
                }

                var job = await _queueService.DequeueAsync(stoppingToken);

                if (job == null)
                {
                    // Wait for a job signal or health-check timeout — no busy polling
                    await _jobSignal.WaitAsync(JobSignalTimeout, stoppingToken);
                    continue;
                }

                // Process job with concurrency limit
                await _concurrencyLimiter.WaitAsync(stoppingToken);

                _ = ProcessJobAsync(job, stoppingToken)
                    .ContinueWith(
                        _ =>
                        {
                            _concurrencyLimiter.Release();

                            // The entry this job held is free again, and DequeueAsync excludes entries with a
                            // job in flight -- so a sibling job for the same file may have been passed over
                            // while this one ran. Without this signal the loop would only notice on its next
                            // enqueue or after the 30s health-check timeout, which a caller awaiting
                            // WaitForJobAsync feels directly.
                            _jobSignal.Release();
                        },
                        TaskScheduler.Default);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogServiceLoopError(_logger, ex);
                await Task.Delay(1000, stoppingToken);
            }
        }

        LogServiceStopped(_logger);
    }

    /// <summary>
    /// Waits (bounded) for in-flight jobs to finish. A loop started with <see cref="Start"/> is
    /// stopped first.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        LogServiceStopping(_logger);

        if (_ownedStop is { } stop)
        {
            await stop.CancelAsync();
            if (_ownedRun is { } run)
            {
                try { await run.WaitAsync(cancellationToken); }
                catch (OperationCanceledException) { /* loop ended by cancellation */ }
            }
        }

        await WaitForActiveJobsAsync(cancellationToken);
    }

    /// <summary>Waits up to a grace period for in-flight jobs to finish.</summary>
    public async Task WaitForActiveJobsAsync(CancellationToken cancellationToken = default)
    {
        var waitStart = DateTime.UtcNow;
        while (HasActiveJobs)
        {
            if (DateTime.UtcNow - waitStart > StopGrace)
            {
                LogTimeoutWaiting(_logger);
                break;
            }

            await Task.Delay(500, cancellationToken);
        }
    }

    /// <summary>
    /// Runs one job to completion: resolve the entry, lease a pipeline, dispatch by job type, and
    /// report the outcome to the queue.
    /// </summary>
    /// <remarks>
    /// Internal rather than private so the outcome reporting can be pinned by tests without driving
    /// the consume loop's timing. This is the real method the loop calls, not a test-only path.
    /// </remarks>
    internal async Task ProcessJobAsync(VaultJob job, CancellationToken ct)
    {
        try
        {
            LogProcessingJob(_logger, job.JobType, job.Id, job.FilePath);

            // Load or create entry. A record that cannot be read is reported and then rebuilt:
            // this job is about to rewrite it anyway, so failing would leave the entry stuck.
            var entry = LoadForRewrite(job.FilepathHash)
                        ?? VaultEntry.Create(job.FilePath, _storage.BasePath);

            // Async disposal: a per-job DI scope may hold services that only implement IAsyncDisposable
            // (e.g. an LLM adapter owning a native model handle behind an optional enrichment port);
            // IServiceScope.Dispose() throws for those and the job would fail *after* doing its work.
            await using var lease = _leasePipeline();
            var pipeline = lease.Pipeline;

            var memorizeOptions = new MemorizeOptions
            {
                MaxChunkSize = _options.Chunking.MaxChunkSize,
                OverlapSize = _options.Chunking.OverlapSize,
                Strategy = _options.Chunking.Strategy,
                Language = _options.Chunking.Language,
                // Wire checkpoint hooks so the pipeline uses per-chunk processing for crash-resilient
                // resume. After each chunk is fully embedded+stored, persist progress so a host
                // restart can resume from chunk N+1 rather than restarting from 0.
                StartFromChunkIndex = job.LastCompletedChunkIndex,
                CheckpointCallback = async (chunkIndex, callbackCt) =>
                    await _queueService.UpdateCheckpointAsync(job.Id, chunkIndex, callbackCt),
            };

            // The pipeline reports failure by returning, not by throwing - it catches everything and
            // hands back MemorizeResult.Failed. Discarding that result marked a failed document as
            // completed: nothing was indexed, failedCount never moved, and the queue said it was done.
            MemorizeResult? result = null;

            switch (job.JobType)
            {
                case VaultJobType.Memorize:
                    result = await pipeline.MemorizeAsync(entry, memorizeOptions, ct);
                    break;

                case VaultJobType.Refresh:
                    result = await pipeline.RefreshAsync(entry, memorizeOptions, ct);
                    break;

                case VaultJobType.Remove:
                    await ProcessRemoveJobAsync(entry, pipeline, ct);
                    break;

                default:
                    throw new InvalidOperationException($"Unknown job type: {job.JobType}");
            }

            if (result is { Success: false })
            {
                await ReportFailureAsync(job, result.ErrorMessage ?? "Memorize failed", result.FailureKind, ct);
                return;
            }

            await _queueService.CompleteAsync(job.Id, ct);
            LogCompletedJob(_logger, job.JobType, job.Id);
        }
        catch (FileNotFoundException ex)
        {
            LogFileNotFoundForJob(_logger, job.Id, job.FilePath);
            await _queueService.FailAsync(job.Id, $"File not found: {ex.Message}", ct);
        }
        catch (Exception ex)
        {
            LogFailedJob(_logger, ex, job.JobType, job.Id, job.FilePath);
            await ReportFailureAsync(
                job, ex.Message, MemorizeFailureClassifier.Classify(ex.GetType().Name), ct);
        }
    }

    /// <summary>
    /// Records a failed job and retries it unless the failure is one that cannot succeed on a
    /// later attempt.
    /// </summary>
    /// <remarks>
    /// Retrying a deterministic failure - a missing file, an extension no reader handles, a corrupt
    /// archive - cannot change its outcome, and each attempt holds the queue head for as long as the
    /// first one did. One consumer measured ~26 seconds per attempt across four attempts per job,
    /// which turned a 30-second problem into a 37-minute one for every other tenant sharing the queue.
    /// How many attempts and how long to wait stay configurable; whether an attempt can possibly
    /// help does not, because that is a property of the failure rather than of the deployment.
    /// </remarks>
    private async Task ReportFailureAsync(
        VaultJob job, string errorMessage, MemorizeFailureKind kind, CancellationToken ct)
    {
        await _queueService.FailAsync(job.Id, errorMessage, ct);

        // FailAsync writes the row; this snapshot is detached from it. Without transitioning the
        // snapshot too, CanRetry (which requires Status == Failed) is false for every dequeued job -
        // TryStart left it Processing - so EnableAutoRetry, RetryDelayMs and MaxRetries had no
        // effect on this path at all.
        job.Fail(errorMessage);

        if (kind == MemorizeFailureKind.Permanent)
        {
            LogPermanentFailureNotRetried(_logger, job.JobType, job.Id, errorMessage);
            return;
        }

        if (_options.EnableAutoRetry && job.CanRetry)
        {
            await Task.Delay(_options.RetryDelayMs, ct);
            await _queueService.RetryAsync(job.Id, ct);
        }
    }

    /// <summary>
    /// Processes a remove job with phased execution for atomicity.
    /// Phase 1: Delete vectors from vector store
    /// Phase 2: Delete storage (entry directory)
    /// </summary>
    private async Task ProcessRemoveJobAsync(VaultEntry entry, IVaultPipeline pipeline, CancellationToken ct)
    {
        LogProcessingRemove(_logger, entry.SourcePath);

        // Check if we're recovering from a partial removal
        if (entry.SyncStatus == SyncStatus.RemovalPartial && entry.RemovalPhase == "Vector")
        {
            // Vector already deleted, skip to storage deletion
            LogRecoveringPartialRemoval(_logger, entry.SourcePath);
        }
        else
        {
            // Phase 1: Mark as removal pending and delete from vector store
            entry.MarkRemovalPending();
            entry.SaveMetadata();

            try
            {
                await pipeline.RemoveAsync(entry, ct);

                // Mark vector phase complete
                entry.MarkRemovalPartial("Vector");
                entry.SaveMetadata();
                LogVectorRemovalCompleted(_logger, entry.SourcePath);
            }
            catch (Exception ex)
            {
                LogVectorRemovalFailed(_logger, ex, entry.SourcePath);
                entry.MarkSyncError($"Vector removal failed: {ex.Message}");
                entry.SaveMetadata();
                throw;
            }
        }

        // Phase 2: Delete entry storage
        try
        {
            await _storage.DeleteEntryStorageAsync(entry, ct);
            LogStorageRemovalCompleted(_logger, entry.SourcePath);
            // Entry directory is now deleted, no need to save metadata
        }
        catch (Exception ex)
        {
            LogStorageRemovalFailed(_logger, ex, entry.SourcePath);
            // Entry is in RemovalPartial state with Vector phase complete
            // Next retry will skip vector deletion
            throw;
        }
    }

    /// <summary>
    /// Loads an entry for a job that is about to rewrite its record: an unreadable record is
    /// reported and then rebuilt rather than failing the job.
    /// </summary>
    private VaultEntry? LoadForRewrite(string filepathHash)
    {
        try
        {
            return VaultEntry.LoadByHash(filepathHash, _storage.BasePath);
        }
        catch (VaultRecordUnreadableException ex)
        {
            LogRebuildingUnreadableRecord(_logger, ex, ex.RecordPath);
            return null;
        }
    }

    /// <summary>
    /// Loads an entry during a sweep over every record: an unreadable one is reported and skipped
    /// so the remaining entries are still visited.
    /// </summary>
    private VaultEntry? LoadForRecovery(string filepathHash)
    {
        try
        {
            return VaultEntry.LoadByHash(filepathHash, _storage.BasePath);
        }
        catch (VaultRecordUnreadableException ex)
        {
            LogSkippingUnreadableRecord(_logger, ex, ex.RecordPath);
            return null;
        }
    }

    /// <summary>
    /// Recovers entries that are in partial removal state from previous runs.
    /// Should be called during startup after RecoverStuckJobsAsync.
    /// </summary>
    public async Task RecoverPartialRemovalsAsync(CancellationToken ct)
    {
        if (!Directory.Exists(_storage.BasePath))
            return;

        var recovered = 0;
        foreach (var dir in Directory.GetDirectories(_storage.BasePath))
        {
            ct.ThrowIfCancellationRequested();

            var dirName = Path.GetFileName(dir);
            // One unreadable record must not stop the remaining entries from being recovered, but
            // it is reported rather than skipped in silence — a record that cannot be read is also
            // one this pass can never visit again.
            var entry = LoadForRecovery(dirName);

            if (entry == null)
                continue;

            // Check for entries stuck in removal states
            if (entry.SyncStatus == SyncStatus.RemovalPending ||
                entry.SyncStatus == SyncStatus.RemovalPartial)
            {
                LogRecoveringPartialRemovalStartup(_logger, entry.SourcePath, entry.SyncStatus, entry.RemovalPhase ?? "none");

                await _queueService.EnqueueRemoveAsync(
                    entry.FilepathHash,
                    entry.SourcePath,
                    VaultJobPriority.High,
                    ct);

                recovered++;
            }
            // Also recover entries marked as SourceDeleted that weren't queued
            else if (entry.SyncStatus == SyncStatus.SourceDeleted)
            {
                LogRequeueingSourceDeleted(_logger, entry.SourcePath);

                await _queueService.EnqueueRemoveAsync(
                    entry.FilepathHash,
                    entry.SourcePath,
                    VaultJobPriority.Normal,
                    ct);

                recovered++;
            }
        }

        if (recovered > 0)
        {
            LogRecoveredRemovalEntries(_logger, recovered);
        }
    }

    /// <summary>Stops an owned loop (without waiting for it) and releases the worker's resources.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        _queueService.JobEnqueued -= OnJobEnqueued;
        _ownedStop?.Cancel();
        _ownedStop?.Dispose();
        _concurrencyLimiter.Dispose();
        _jobSignal.Dispose();
    }

    /// <summary>Stops an owned loop, waits for in-flight jobs, then releases the worker's resources.</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        if (_ownedStop is not null)
            await StopAsync().ConfigureAwait(false);

        Dispose();
    }

    private sealed class ScopedPipelineLease : IVaultPipelineLease
    {
        private readonly IServiceScope _scope;
        public ScopedPipelineLease(IServiceScope scope)
        {
            _scope = scope;
            Pipeline = scope.ServiceProvider.GetRequiredService<IVaultPipeline>();
        }
        public IVaultPipeline Pipeline { get; }
        public void Dispose() => _scope.Dispose();
        public ValueTask DisposeAsync()
        {
            if (_scope is IAsyncDisposable asyncScope)
                return asyncScope.DisposeAsync();
            _scope.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SharedPipelineLease(IVaultPipeline pipeline) : IVaultPipelineLease
    {
        public IVaultPipeline Pipeline { get; } = pipeline;
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Debug, Message = "Vault queue worker starting...")]
    private static partial void LogServiceStarting(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Recovered {Count} stuck jobs from previous run")]
    private static partial void LogRecoveredStuckJobs(ILogger logger, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Vault queue worker started")]
    private static partial void LogServiceStarted(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in vault queue worker loop")]
    private static partial void LogServiceLoopError(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "Vault queue worker stopped")]
    private static partial void LogServiceStopped(ILogger logger);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Processing {JobType} job {JobId}: {FilePath}")]
    private static partial void LogProcessingJob(ILogger logger, VaultJobType jobType, Guid jobId, string filePath);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Completed {JobType} job {JobId}")]
    private static partial void LogCompletedJob(ILogger logger, VaultJobType jobType, Guid jobId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "File not found for job {JobId}: {FilePath}")]
    private static partial void LogFileNotFoundForJob(ILogger logger, Guid jobId, string filePath);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed {JobType} job {JobId}: {FilePath}")]
    private static partial void LogFailedJob(ILogger logger, Exception exception, VaultJobType jobType, Guid jobId, string filePath);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{JobType} job {JobId} failed for a reason that cannot succeed on a retry; not retrying: {Reason}")]
    private static partial void LogPermanentFailureNotRetried(ILogger logger, VaultJobType jobType, Guid jobId, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "Processing remove job for {SourcePath}")]
    private static partial void LogProcessingRemove(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Recovering partial removal for {SourcePath}, skipping vector deletion")]
    private static partial void LogRecoveringPartialRemoval(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Vector removal completed for {SourcePath}")]
    private static partial void LogVectorRemovalCompleted(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Error, Message = "Vector removal failed for {SourcePath}")]
    private static partial void LogVectorRemovalFailed(ILogger logger, Exception exception, string sourcePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Storage removal completed for {SourcePath}")]
    private static partial void LogStorageRemovalCompleted(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Error, Message = "Storage removal failed for {SourcePath}")]
    private static partial void LogStorageRemovalFailed(ILogger logger, Exception exception, string sourcePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Recovering partial removal for {SourcePath} (status: {Status}, phase: {Phase})")]
    private static partial void LogRecoveringPartialRemovalStartup(ILogger logger, string sourcePath, SyncStatus status, string phase);

    [LoggerMessage(Level = LogLevel.Information, Message = "Re-queueing source-deleted entry for {SourcePath}")]
    private static partial void LogRequeueingSourceDeleted(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Recovered {Count} entries in removal/deleted states")]
    private static partial void LogRecoveredRemovalEntries(ILogger logger, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Vault queue worker stopping...")]
    private static partial void LogServiceStopping(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Vault record unreadable, rebuilding it from scratch: {Path}")]
    private static partial void LogRebuildingUnreadableRecord(ILogger logger, Exception exception, string path);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Vault record unreadable, skipping it during recovery: {Path}")]
    private static partial void LogSkippingUnreadableRecord(ILogger logger, Exception exception, string path);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Timeout waiting for active jobs to complete")]
    private static partial void LogTimeoutWaiting(ILogger logger);

    #endregion
}
