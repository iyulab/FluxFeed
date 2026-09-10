using FluxFeed.Domain.Entities;

namespace FluxFeed.Interfaces;

/// <summary>
/// Service for managing the vault processing queue.
/// Jobs are persisted to SQLite for crash recovery.
/// </summary>
public interface IVaultQueueService
{
    /// <summary>
    /// Enqueues a memorize job, or merges into one already queued for the same file and type —
    /// raising that job to this priority when this request is the more urgent one.
    /// </summary>
    /// <remarks>
    /// One method rather than a with-priority overload beside a without: the two differed only by an
    /// argument that has a sensible default, which left every caller and every test double picking one
    /// arbitrarily. See <see cref="DequeueAsync"/> for why same-entry work is never run in parallel.
    /// </remarks>
    /// <param name="groupKey">
    /// Optional fairness group, e.g. the owner this file belongs to. Jobs sharing a group are held to
    /// <c>FileVaultOptions.MaxInFlightPerGroup</c> concurrent jobs while another group has work
    /// waiting, so one owner's backlog cannot occupy the whole queue. Null (the default) means
    /// ungrouped and is never capped. The queue does not derive this from the path - what counts as
    /// an owner is the caller's concept.
    /// </param>
    Task<VaultJob> EnqueueMemorizeAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues a refresh job, or merges into one already queued for the same file and type —
    /// raising that job to this priority when this request is the more urgent one.
    /// </summary>
    /// <remarks>
    /// One method rather than a with-priority overload beside a without: the two differed only by an
    /// argument that has a sensible default, which left every caller and every test double picking one
    /// arbitrarily. See <see cref="DequeueAsync"/> for why same-entry work is never run in parallel.
    /// </remarks>
    /// <param name="groupKey">
    /// Optional fairness group, e.g. the owner this file belongs to. Jobs sharing a group are held to
    /// <c>FileVaultOptions.MaxInFlightPerGroup</c> concurrent jobs while another group has work
    /// waiting, so one owner's backlog cannot occupy the whole queue. Null (the default) means
    /// ungrouped and is never capped. The queue does not derive this from the path - what counts as
    /// an owner is the caller's concept.
    /// </param>
    Task<VaultJob> EnqueueRefreshAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues a remove job, or merges into one already queued for the same file and type —
    /// raising that job to this priority when this request is the more urgent one.
    /// </summary>
    /// <remarks>
    /// One method rather than a with-priority overload beside a without: the two differed only by an
    /// argument that has a sensible default, which left every caller and every test double picking one
    /// arbitrarily. See <see cref="DequeueAsync"/> for why same-entry work is never run in parallel.
    /// </remarks>
    /// <param name="groupKey">
    /// Optional fairness group, e.g. the owner this file belongs to. Jobs sharing a group are held to
    /// <c>FileVaultOptions.MaxInFlightPerGroup</c> concurrent jobs while another group has work
    /// waiting, so one owner's backlog cannot occupy the whole queue. Null (the default) means
    /// ungrouped and is never capped. The queue does not derive this from the path - what counts as
    /// an owner is the caller's concept.
    /// </param>
    Task<VaultJob> EnqueueRemoveAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default);

    /// <summary>
    /// Enqueues multiple jobs.
    /// </summary>
    Task<IReadOnlyList<VaultJob>> EnqueueBatchAsync(
        IEnumerable<(string FilepathHash, string FilePath)> files,
        VaultJobType jobType = VaultJobType.Memorize,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default);

    /// <summary>
    /// Dequeues the next job for processing.
    /// Returns null if the queue is empty or paused, and also when every queued job belongs to a file
    /// that already has a job in flight.
    /// </summary>
    /// <remarks>
    /// A vault's git repository is per <b>entry</b> (<c>VaultEntry.VaultPath</c> = <c>&lt;EntryPath&gt;/vault</c>),
    /// so jobs for different files write different repositories and run in parallel up to
    /// <c>MaxConcurrentProcessing</c>. Jobs for the <i>same</i> file do not: they would race one working
    /// tree and one <c>index.lock</c>. This method therefore skips any entry that already has a job in
    /// <see cref="VaultJobStatus.Processing"/>; the skipped job is handed out as soon as that one finishes.
    /// A consumer does not need its own per-file lock on top of this.
    /// </remarks>
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
    /// Marks a job failed and records how the failure was classified.
    /// </summary>
    /// <remarks>
    /// The classification is persisted because it is read later, by a different caller: a rerun
    /// request has to know whether another attempt can possibly differ, and by then the exception
    /// that would answer that is gone. Passing null records "not classified", which is not the same
    /// as classifying it as retryable.
    /// </remarks>
    Task FailAsync(
        Guid jobId, string errorMessage, MemorizeFailureKind? failureKind, CancellationToken ct = default);

    /// <summary>
    /// Puts a failed job back in the queue on explicit instruction, clearing the automatic retry
    /// budget.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the operator's path, and it is deliberately not <see cref="RetryAsync"/>. That method
    /// enforces the automatic retry budget, which is correct for the worker deciding whether to keep
    /// going unattended - and wrong for a person, because the jobs a person asks about are precisely
    /// the ones that have used the budget up. Gated on it, the request succeeded only when it was
    /// not needed.
    /// </para>
    /// <para>
    /// Throws rather than returning a status: <see cref="Domain.Exceptions.VaultJobNotFoundException"/>
    /// when no such job exists, and <see cref="Domain.Exceptions.VaultJobNotRetryableException"/>
    /// carrying a <see cref="Domain.Exceptions.VaultRetryRefusal"/> when the job exists but will not
    /// be run - it is not in a failed state, or it failed for a reason no attempt can change. A
    /// single bool could not tell those apart, and each calls for a different answer to the person
    /// who pressed the button.
    /// </para>
    /// </remarks>
    Task RequeueAsync(Guid jobId, CancellationToken ct = default);

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
    /// Gets jobs with optional filters, ordering and paging in SQL.
    /// </summary>
    /// <param name="offset">Rows to skip. Paging happens in the database, so a page costs a page.</param>
    /// <param name="newestFirst">
    /// Orders by <c>queued_at</c> descending — what an observability caller almost always wants ("the latest
    /// N failures"). Priority is deliberately not part of this order: it decides what runs next, not what is
    /// most recent, and letting it in is how a "latest 50" listing ends up returning something else.
    /// The default (<c>false</c>) is the queue's own order (priority, then oldest first), unchanged.
    /// </param>
    /// <remarks>
    /// <paramref name="offset"/> and <paramref name="newestFirst"/> were added in 0.22.0 <i>before</i>
    /// <paramref name="ct"/> rather than after, so that the two paging parameters sit next to the filter they
    /// page. A caller that passed the token positionally as the fourth argument gets a compile error, which is
    /// the point: the alternative — appending them after <c>ct</c> — would have kept such a call compiling
    /// while it silently meant something else.
    /// </remarks>
    Task<IReadOnlyList<VaultJob>> GetJobsAsync(
        VaultJobStatus? statusFilter = null,
        VaultJobType? typeFilter = null,
        int? limit = null,
        int? offset = null,
        bool newestFirst = false,
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

    /// <summary>
    /// When a job last finished <b>successfully</b>. Named for what it is: this was called
    /// <c>LastProcessedAt</c> until 0.22.0 while only ever reflecting completions, so a queue that was
    /// working steadily and failing every job left it frozen and read as stopped. Pair it with
    /// <see cref="LastAttemptedAt"/>: this one answers "is it getting anywhere".
    /// </summary>
    public DateTimeOffset? LastSucceededAt { get; init; }

    /// <summary>
    /// When the queue last did anything at all — the newest of any job's start or finish, whatever its
    /// status. This is the liveness signal: a fresh value beside <c>ProcessingCount = 0</c> means "between
    /// jobs", a stale one means the worker really has stopped.
    /// </summary>
    public DateTimeOffset? LastAttemptedAt { get; init; }

    public double AverageProcessingTimeMs { get; init; }
}
