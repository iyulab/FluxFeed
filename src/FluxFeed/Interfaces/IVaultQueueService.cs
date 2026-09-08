using FluxFeed.Domain.Entities;

namespace FluxFeed.Interfaces;

/// <summary>
/// Service for managing the vault processing queue.
/// Jobs are persisted to SQLite for crash recovery.
/// </summary>
public interface IVaultQueueService
{
    /// <summary>
    /// Enqueues a memorize job.
    /// </summary>
    Task<VaultJob> EnqueueMemorizeAsync(
        string filepathHash,
        string filePath,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues a memorize job with priority.
    /// </summary>
    Task<VaultJob> EnqueueMemorizeAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues a refresh job.
    /// </summary>
    Task<VaultJob> EnqueueRefreshAsync(
        string filepathHash,
        string filePath,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues a refresh job with priority.
    /// </summary>
    Task<VaultJob> EnqueueRefreshAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues a remove job.
    /// </summary>
    Task<VaultJob> EnqueueRemoveAsync(
        string filepathHash,
        string filePath,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues a remove job with priority.
    /// </summary>
    Task<VaultJob> EnqueueRemoveAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues multiple jobs.
    /// </summary>
    Task<IReadOnlyList<VaultJob>> EnqueueBatchAsync(
        IEnumerable<(string FilepathHash, string FilePath)> files,
        VaultJobType jobType = VaultJobType.Memorize,
        VaultJobPriority priority = VaultJobPriority.Normal,
        CancellationToken ct = default);

    /// <summary>
    /// Dequeues the next job for processing.
    /// Returns null if queue is empty or paused.
    /// </summary>
    Task<VaultJob?> DequeueAsync(CancellationToken ct = default);

    /// <summary>
    /// Marks a job as completed.
    /// </summary>
    Task CompleteAsync(Guid jobId, CancellationToken ct = default);

    /// <summary>
    /// Marks a job as failed.
    /// </summary>
    Task FailAsync(Guid jobId, string errorMessage, CancellationToken ct = default);

    /// <summary>
    /// Retries a failed job.
    /// </summary>
    Task<bool> RetryAsync(Guid jobId, CancellationToken ct = default);

    /// <summary>
    /// Cancels a queued or processing job.
    /// </summary>
    Task<bool> CancelAsync(Guid jobId, CancellationToken ct = default);

    /// <summary>
    /// Gets a job by ID.
    /// </summary>
    Task<VaultJob?> GetJobAsync(Guid jobId, CancellationToken ct = default);

    /// <summary>
    /// Asynchronously waits for a job to reach a terminal state (Completed, Failed, or Cancelled)
    /// and returns the terminal job. This is the completion primitive consumers should use instead of
    /// polling <see cref="GetJobAsync"/> or vault-entry stage. The wait is signal-driven (no polling):
    /// it resolves the instant the queue transitions the job, and resolves immediately for a job that
    /// is already terminal at call time (race-free).
    /// </summary>
    /// <param name="jobId">The job to await.</param>
    /// <param name="ct">Cancellation token; cancelling abandons the wait (the job itself is unaffected).</param>
    /// <returns>The job in its terminal state.</returns>
    /// <exception cref="InvalidOperationException">
    /// No job exists with the given id, or no worker registered via <see cref="RegisterWorker"/>
    /// within <see cref="Options.FileVaultOptions.WorkerStartupTimeout"/> while the job is still
    /// pending. The latter means nothing can ever complete the job (the queue is consumed only by
    /// a running <c>VaultBackgroundService</c>), so the wait fails fast instead of hanging.
    /// </exception>
    Task<VaultJob> WaitForJobAsync(Guid jobId, CancellationToken ct = default);

    /// <summary>
    /// Registers the caller as an active consumer of this queue for the lifetime of the returned
    /// lease. <see cref="WaitForJobAsync"/> uses the lease count to detect a job that can never be
    /// processed because no worker is running (for example, the hosted service was registered but
    /// never started because the application has no Generic Host). Implementations that do not
    /// track workers may keep the default no-op lease.
    /// </summary>
    /// <returns>A lease; dispose it when the worker stops consuming the queue.</returns>
    IDisposable RegisterWorker() => NoOpWorkerLease.Instance;

    /// <summary>
    /// Default lease returned by <see cref="RegisterWorker"/> when the implementation does not
    /// track workers.
    /// </summary>
    private sealed class NoOpWorkerLease : IDisposable
    {
        public static readonly NoOpWorkerLease Instance = new();
        public void Dispose() { }
    }

    /// <summary>
    /// Gets jobs with optional filters.
    /// </summary>
    Task<IReadOnlyList<VaultJob>> GetJobsAsync(
        VaultJobStatus? statusFilter = null,
        VaultJobType? typeFilter = null,
        int? limit = null,
        CancellationToken ct = default);

    /// <summary>
    /// Gets the current queue statistics.
    /// </summary>
    Task<QueueStatistics> GetStatisticsAsync(CancellationToken ct = default);

    /// <summary>
    /// Recovers stuck jobs (Processing → Queued) after crash.
    /// Should be called on startup. Preserves last_completed_chunk_index so that
    /// the embedding pipeline can resume from the checkpoint instead of restarting from chunk 0.
    /// </summary>
    Task<int> RecoverStuckJobsAsync(CancellationToken ct = default);

    /// <summary>
    /// Persists the index of the last fully committed chunk for a job in Processing state.
    /// Called by the embedding pipeline after each chunk is successfully embedded AND stored.
    /// On host restart + RecoverStuckJobsAsync, the pipeline reads this checkpoint via
    /// VaultJob.LastCompletedChunkIndex and skips already-committed chunks.
    /// </summary>
    /// <param name="jobId">Job to update.</param>
    /// <param name="lastCompletedChunkIndex">0-based chunk index that was just successfully stored.</param>
    /// <param name="ct">Cancellation token.</param>
    Task UpdateCheckpointAsync(Guid jobId, int lastCompletedChunkIndex, CancellationToken ct = default);

    /// <summary>
    /// Clears completed and cancelled jobs.
    /// </summary>
    Task<int> ClearCompletedAsync(CancellationToken ct = default);

    /// <summary>
    /// Clears failed jobs.
    /// </summary>
    Task<int> ClearFailedAsync(CancellationToken ct = default);

    /// <summary>
    /// Clears all jobs (use with caution).
    /// </summary>
    Task ClearAllAsync(CancellationToken ct = default);

    /// <summary>
    /// Pauses queue processing.
    /// </summary>
    void Pause();

    /// <summary>
    /// Resumes queue processing.
    /// </summary>
    void ResumeProcessing();

    /// <summary>
    /// Whether the queue is paused.
    /// </summary>
    bool IsPaused { get; }

    /// <summary>
    /// Event raised when a job is enqueued.
    /// </summary>
    event EventHandler<VaultJob>? JobEnqueued;

    /// <summary>
    /// Event raised when a job is completed.
    /// </summary>
    event EventHandler<VaultJob>? JobCompleted;
}

/// <summary>
/// Queue statistics summary.
/// </summary>
public sealed class QueueStatistics
{
    public int QueuedCount { get; init; }
    public int ProcessingCount { get; init; }
    public int CompletedCount { get; init; }
    public int FailedCount { get; init; }
    public int CancelledCount { get; init; }
    public int TotalCount => QueuedCount + ProcessingCount + CompletedCount + FailedCount + CancelledCount;
    public bool IsPaused { get; init; }
    public DateTimeOffset? LastProcessedAt { get; init; }
    public double AverageProcessingTimeMs { get; init; }
}
