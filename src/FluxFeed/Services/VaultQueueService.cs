using System.Collections.Concurrent;
using System.Data;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Exceptions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Globalization;

namespace FluxFeed.Services;

/// <summary>
/// SQLite-backed vault processing queue service.
/// Provides persistence and crash recovery for processing jobs.
/// </summary>
public sealed partial class VaultQueueService : IVaultQueueService, IDisposable
{
    private readonly ILogger<VaultQueueService> _logger;
    private readonly string _connectionString;
    private readonly SemaphoreSlim _dbLock = new(1, 1);
    // Bridges the queue's terminal transitions to WaitForJobAsync without polling. Each awaiting
    // caller registers a TCS keyed by jobId; Complete/Fail/Cancel resolve and remove it.
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource<VaultJob>> _waiters = new();

    // Worker presence. A pending job can only ever reach a terminal state through a registered
    // worker (VaultBackgroundService), so WaitForJobAsync consults this instead of waiting forever
    // when the hosted service was registered but never started (no Generic Host).
    private readonly TimeSpan _workerStartupTimeout;

    /// <summary>
    /// How many jobs of one group may be Processing at once while another group has work queued.
    /// Read once at construction: a queue's fairness policy is fixed for its lifetime, and re-reading
    /// per dequeue would make the policy change under jobs already picked under the old one.
    /// </summary>
    private readonly int _maxInFlightPerGroup;
    private readonly object _workerLock = new();
    private int _activeWorkers;
    private TaskCompletionSource _workerAvailable = NewWorkerSignal();

    private bool _isPaused;
    private bool _disposed;

    public bool IsPaused => _isPaused;

    /// <summary>
    /// Whether at least one worker currently holds a lease from <see cref="RegisterWorker"/>.
    /// </summary>
    public bool HasActiveWorker => Volatile.Read(ref _activeWorkers) > 0;

    public event EventHandler<VaultJob>? JobEnqueued;
    public event EventHandler<VaultJob>? JobCompleted;

    public VaultQueueService(
        ILogger<VaultQueueService> logger,
        IOptions<FileVaultOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var opts = options?.Value ?? new FileVaultOptions();
        _workerStartupTimeout = opts.WorkerStartupTimeout;
        _maxInFlightPerGroup = opts.MaxInFlightPerGroup;
        var basePath = opts.VaultBasePath ?? Path.Combine(Directory.GetCurrentDirectory(), opts.VaultDirectoryName);
        Directory.CreateDirectory(basePath);

        var dbPath = Path.Combine(basePath, "queue.db");
        _connectionString = $"Data Source={dbPath};Mode=ReadWriteCreate;Cache=Shared";

        InitializeDatabase();
    }

    private void InitializeDatabase()
    {
        using var connection = CreateConnection();
        connection.Open();

        // Enable WAL mode for better concurrency
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA journal_mode=WAL;";
            cmd.ExecuteNonQuery();
        }

        // Create jobs table
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IF NOT EXISTS vault_jobs (
                    id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    filepath_hash TEXT NOT NULL,
                    job_type INTEGER NOT NULL,
                    status INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    queued_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    error_message TEXT,
                    last_completed_chunk_index INTEGER NOT NULL DEFAULT -1,
                    group_key TEXT,
                    failure_kind INTEGER
                );

                CREATE INDEX IF NOT EXISTS idx_jobs_status ON vault_jobs(status);
                CREATE INDEX IF NOT EXISTS idx_jobs_priority ON vault_jobs(priority DESC, queued_at ASC);
                CREATE INDEX IF NOT EXISTS idx_jobs_filepath_hash ON vault_jobs(filepath_hash);
                """;
            cmd.ExecuteNonQuery();
        }

        // Migrations: CREATE TABLE IF NOT EXISTS above only adds new columns to fresh databases;
        // existing tables from older versions need ALTER TABLE. Each one is idempotent.
        foreach (var alter in new[]
        {
            "ALTER TABLE vault_jobs ADD COLUMN last_completed_chunk_index INTEGER NOT NULL DEFAULT -1",
            // Nullable with no default: rows written before group fairness existed are ungrouped, and
            // an ungrouped job is never capped, so upgrading changes no behaviour on its own.
            "ALTER TABLE vault_jobs ADD COLUMN group_key TEXT",
            // Nullable with no default on purpose: a row written before this existed has no recorded
            // classification, and "not recorded" must stay distinguishable from "classified as
            // retryable". The rerun path reads null as permission to try, so upgrading refuses
            // nothing it did not refuse before.
            "ALTER TABLE vault_jobs ADD COLUMN failure_kind INTEGER"
        })
        {
            using var migrateCmd = connection.CreateCommand();
            migrateCmd.CommandText = alter;
            try
            {
                migrateCmd.ExecuteNonQuery();
            }
            catch (SqliteException ex)
                when (ex.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
            {
                // Column already exists — expected on every run after the first migration.
            }
        }

        // After the migrations, never with the CREATE TABLE batch: on a database that already has the
        // table, CREATE TABLE IF NOT EXISTS is a no-op and an index over a newly added column would
        // reference a column that does not exist yet.
        using (var indexCmd = connection.CreateCommand())
        {
            indexCmd.CommandText =
                "CREATE INDEX IF NOT EXISTS idx_jobs_group_key ON vault_jobs(group_key, status)";
            indexCmd.ExecuteNonQuery();
        }

        LogDatabaseInitialized(_logger);
    }

    private SqliteConnection CreateConnection() => new(_connectionString);

    #region Enqueue Methods

    public Task<VaultJob> EnqueueMemorizeAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default)
    {
        return EnqueueJobAsync(filepathHash, filePath, VaultJobType.Memorize, priority, groupKey, ct);
    }

    public Task<VaultJob> EnqueueRefreshAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default)
    {
        return EnqueueJobAsync(filepathHash, filePath, VaultJobType.Refresh, priority, groupKey, ct);
    }

    public Task<VaultJob> EnqueueRemoveAsync(
        string filepathHash,
        string filePath,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default)
    {
        return EnqueueJobAsync(filepathHash, filePath, VaultJobType.Remove, priority, groupKey, ct);
    }

    private async Task<VaultJob> EnqueueJobAsync(
        string filepathHash,
        string filePath,
        VaultJobType jobType,
        VaultJobPriority priority,
        string? groupKey,
        CancellationToken ct)
    {
        var fullPath = Path.GetFullPath(filePath);
        var job = VaultJob.Create(fullPath, filepathHash, jobType, priority, groupKey: groupKey);

        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            // A request for work that is already waiting is the same unit of work: one Memorize of a file
            // still sitting in the queue supersedes nothing and adds nothing, but as a second row it becomes
            // a second writer of that entry's git repository. Merge into the waiting job instead, and let the
            // caller wait on that one. Only Queued jobs merge -- a job already Processing has read the file
            // as it was, so a request arriving after it started is asking about a later state (see
            // VaultQueueSameEntryConcurrencyTests).
            var existing = await FindCoalescibleJobAsync(connection, filepathHash, jobType, priority, ct);
            if (existing is not null)
            {
                LogCoalesced(_logger, jobType, filePath, existing.Id);

                // Still a wake signal: the worker may be parked on its 30s health-check wait, and this
                // caller is about to await the merged job.
                JobEnqueued?.Invoke(this, existing);
                return existing;
            }

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO vault_jobs (id, file_path, filepath_hash, job_type, status, priority, queued_at, retry_count, max_retries, group_key)
                VALUES (@id, @file_path, @filepath_hash, @job_type, @status, @priority, @queued_at, @retry_count, @max_retries, @group_key)
                """;

            cmd.Parameters.AddWithValue("@id", job.Id.ToString());
            cmd.Parameters.AddWithValue("@file_path", job.FilePath);
            cmd.Parameters.AddWithValue("@filepath_hash", job.FilepathHash);
            cmd.Parameters.AddWithValue("@job_type", (int)job.JobType);
            cmd.Parameters.AddWithValue("@status", (int)job.Status);
            cmd.Parameters.AddWithValue("@priority", (int)job.Priority);
            cmd.Parameters.AddWithValue("@group_key", (object?)job.GroupKey ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@queued_at", job.QueuedAt.ToString("O"));
            cmd.Parameters.AddWithValue("@retry_count", job.RetryCount);
            cmd.Parameters.AddWithValue("@max_retries", 3);

            await cmd.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            _dbLock.Release();
        }

        LogEnqueued(_logger, jobType, filePath);
        JobEnqueued?.Invoke(this, job);

        return job;
    }

    public async Task<IReadOnlyList<VaultJob>> EnqueueBatchAsync(
        IEnumerable<(string FilepathHash, string FilePath)> files,
        VaultJobType jobType = VaultJobType.Memorize,
        VaultJobPriority priority = VaultJobPriority.Normal,
        string? groupKey = null,
        CancellationToken ct = default)
    {
        var jobs = new List<VaultJob>();

        foreach (var (filepathHash, filePath) in files)
        {
            var job = await EnqueueJobAsync(filepathHash, filePath, jobType, priority, groupKey, ct);
            jobs.Add(job);
        }

        return jobs;
    }

    #endregion

    #region Dequeue & Status Updates

    public async Task<VaultJob?> DequeueAsync(CancellationToken ct = default)
    {
        if (_isPaused)
            return null;

        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            // Get highest priority queued job
            await using var selectCmd = connection.CreateCommand();
            selectCmd.CommandText = """
                SELECT id, file_path, filepath_hash, job_type, status, priority, queued_at,
                       started_at, completed_at, retry_count, max_retries, error_message,
                       last_completed_chunk_index, group_key, failure_kind
                FROM vault_jobs AS j
                WHERE j.status = @status
                  AND j.filepath_hash NOT IN (
                      SELECT filepath_hash FROM vault_jobs WHERE status = @processing
                  )
                  AND (
                      j.group_key IS NULL
                      OR @max_in_flight_per_group <= 0
                      OR (
                          SELECT COUNT(*) FROM vault_jobs p
                          WHERE p.status = @processing AND p.group_key = j.group_key
                      ) < @max_in_flight_per_group
                      OR NOT EXISTS (
                          SELECT 1 FROM vault_jobs o
                          WHERE o.status = @status
                            AND (o.group_key IS NULL OR o.group_key <> j.group_key)
                      )
                  )
                ORDER BY j.priority DESC, j.queued_at ASC
                LIMIT 1
                """;
            selectCmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Queued);
            // A git repository in FluxFeed is per entry (VaultEntry.VaultPath = <EntryPath>/vault), so two
            // jobs for two files are safe to run together and two jobs for ONE file are not: they race the
            // same working tree and the same index.lock. Excluding entries that already have a job in flight
            // is what makes MaxConcurrentProcessing > 1 safe without serializing unrelated files.
            selectCmd.Parameters.AddWithValue("@processing", (int)VaultJobStatus.Processing);
            // Group fairness generalises the exclusion above: same-entry is a per-group cap whose
            // group is the entry and whose cap is 1. The last clause makes this cap work-conserving -
            // a group alone on the queue may exceed its share, so fairness never idles a worker that
            // has nothing else to do. Without it, a single-tenant deployment would lose
            // MaxConcurrentProcessing - 1 slots for no one's benefit and nobody would enable it.
            selectCmd.Parameters.AddWithValue("@max_in_flight_per_group", _maxInFlightPerGroup);

            await using var reader = await selectCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;

            var job = ReadJob(reader);

            // Update to processing
            await using var updateCmd = connection.CreateCommand();
            updateCmd.CommandText = """
                UPDATE vault_jobs
                SET status = @status, started_at = @started_at
                WHERE id = @id
                """;
            updateCmd.Parameters.AddWithValue("@id", job.Id.ToString());
            updateCmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Processing);
            updateCmd.Parameters.AddWithValue("@started_at", DateTimeOffset.UtcNow.ToString("O"));

            await updateCmd.ExecuteNonQueryAsync(ct);

            job.TryStart();
            LogDequeued(_logger, job.Id, job.FilePath);

            return job;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task CompleteAsync(Guid jobId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            var completedAt = DateTimeOffset.UtcNow;

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                UPDATE vault_jobs
                SET status = @status, completed_at = @completed_at, error_message = NULL
                WHERE id = @id
                """;
            cmd.Parameters.AddWithValue("@id", jobId.ToString());
            cmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Completed);
            cmd.Parameters.AddWithValue("@completed_at", completedAt.ToString("O"));

            await cmd.ExecuteNonQueryAsync(ct);

            LogCompleted(_logger, jobId);

            var job = await GetJobInternalAsync(connection, jobId, ct);
            if (job != null)
            {
                JobCompleted?.Invoke(this, job);
                SignalWaiter(job);
            }
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public Task FailAsync(Guid jobId, string errorMessage, CancellationToken ct = default)
        => FailAsync(jobId, errorMessage, failureKind: null, ct);

    public async Task FailAsync(
        Guid jobId, string errorMessage, MemorizeFailureKind? failureKind, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            await using var cmd = connection.CreateCommand();
            // The classification is written here rather than only logged, because the question it
            // answers is asked later and by someone else: an operator asking for this job to be run
            // again needs to know whether another attempt can possibly differ. Reclassifying at that
            // point is impossible - the exception is long gone.
            cmd.CommandText = """
                UPDATE vault_jobs
                SET status = @status, completed_at = @completed_at, error_message = @error_message,
                    failure_kind = @failure_kind
                WHERE id = @id
                """;
            cmd.Parameters.AddWithValue("@id", jobId.ToString());
            cmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Failed);
            cmd.Parameters.AddWithValue("@completed_at", DateTimeOffset.UtcNow.ToString("O"));
            cmd.Parameters.AddWithValue("@error_message", errorMessage);
            cmd.Parameters.AddWithValue(
                "@failure_kind", failureKind is null ? DBNull.Value : (int)failureKind.Value);

            await cmd.ExecuteNonQueryAsync(ct);
            LogFailed(_logger, jobId, errorMessage);

            // Release any caller awaiting terminal state (Failed is terminal).
            var job = await GetJobInternalAsync(connection, jobId, ct);
            if (job != null)
                SignalWaiter(job);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<bool> RetryAsync(Guid jobId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            // Check if retry is allowed
            var job = await GetJobInternalAsync(connection, jobId, ct);
            if (job == null || !job.CanRetry)
                return false;

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                UPDATE vault_jobs
                SET status = @status, started_at = NULL, completed_at = NULL,
                    error_message = NULL, retry_count = retry_count + 1
                WHERE id = @id AND status = @failed_status AND retry_count < max_retries
                """;
            cmd.Parameters.AddWithValue("@id", jobId.ToString());
            cmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Queued);
            cmd.Parameters.AddWithValue("@failed_status", (int)VaultJobStatus.Failed);

            var rows = await cmd.ExecuteNonQueryAsync(ct);
            if (rows > 0)
            {
                LogRetrying(_logger, jobId, job.RetryCount + 1);
                return true;
            }

            return false;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task RequeueAsync(Guid jobId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            var job = await GetJobInternalAsync(connection, jobId, ct)
                ?? throw new VaultJobNotFoundException(jobId);

            if (job.Status != VaultJobStatus.Failed)
                throw new VaultJobNotRetryableException(jobId, VaultRetryRefusal.NotFailed);

            // A recorded Permanent is the only classification that refuses. Unknown and Transient
            // both allow the attempt, and so does a row written before the classification was
            // persisted - "not recorded" is not evidence of anything.
            if (job.FailureKind == MemorizeFailureKind.Permanent)
            {
                throw new VaultJobNotRetryableException(
                    jobId,
                    VaultRetryRefusal.PermanentFailure,
                    job.ErrorMessage is null ? null : $"Last failure: {job.ErrorMessage}");
            }

            await using var cmd = connection.CreateCommand();
            // No retry_count guard here, unlike RetryAsync. The budget exists to stop the worker
            // looping unattended; a person asking for this job has already made that decision, and
            // the jobs they ask about are by definition the ones that used it all up.
            cmd.CommandText = """
                UPDATE vault_jobs
                SET status = @status, started_at = NULL, completed_at = NULL,
                    error_message = NULL, retry_count = 0, failure_kind = NULL
                WHERE id = @id AND status = @failed_status
                """;
            cmd.Parameters.AddWithValue("@id", jobId.ToString());
            cmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Queued);
            cmd.Parameters.AddWithValue("@failed_status", (int)VaultJobStatus.Failed);

            var rows = await cmd.ExecuteNonQueryAsync(ct);
            if (rows == 0)
            {
                // Someone moved it between the read above and this write.
                throw new VaultJobNotRetryableException(jobId, VaultRetryRefusal.NotFailed);
            }

            LogRequeuedByRequest(_logger, jobId);
            job.TryRequeueByRequest();
            SignalWaiter(job);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<bool> CancelAsync(Guid jobId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                UPDATE vault_jobs
                SET status = @status, completed_at = @completed_at
                WHERE id = @id AND status IN (@queued, @processing)
                """;
            cmd.Parameters.AddWithValue("@id", jobId.ToString());
            cmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Cancelled);
            cmd.Parameters.AddWithValue("@completed_at", DateTimeOffset.UtcNow.ToString("O"));
            cmd.Parameters.AddWithValue("@queued", (int)VaultJobStatus.Queued);
            cmd.Parameters.AddWithValue("@processing", (int)VaultJobStatus.Processing);

            var rows = await cmd.ExecuteNonQueryAsync(ct);
            if (rows > 0)
            {
                LogCancelled(_logger, jobId);

                // Release any caller awaiting terminal state (Cancelled is terminal).
                var job = await GetJobInternalAsync(connection, jobId, ct);
                if (job != null)
                    SignalWaiter(job);

                return true;
            }

            return false;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    #endregion

    #region Query Methods

    public async Task<VaultJob?> GetJobAsync(Guid jobId, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);
            return await GetJobInternalAsync(connection, jobId, ct);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<VaultJob> WaitForJobAsync(Guid jobId, CancellationToken ct = default)
    {
        // Fast path: resolve immediately for a job that is already terminal (race-free for the
        // common "completed before the caller started waiting" case).
        var current = await GetJobAsync(jobId, ct).ConfigureAwait(false);
        if (current is null)
            throw new InvalidOperationException($"No vault job found with id {jobId}.");
        if (IsTerminal(current.Status))
            return current;

        // Register a waiter, then re-read to close the window between the fast-path read and
        // registration (a terminal transition could have fired in between).
        var tcs = _waiters.GetOrAdd(jobId,
            _ => new TaskCompletionSource<VaultJob>(TaskCreationOptions.RunContinuationsAsynchronously));

        var afterRegister = await GetJobAsync(jobId, ct).ConfigureAwait(false);
        if (afterRegister is not null && IsTerminal(afterRegister.Status))
        {
            _waiters.TryRemove(jobId, out _);
            return afterRegister;
        }

        try
        {
            if (!HasActiveWorker)
            {
                // Nothing can complete this job until a worker exists. Give a worker that is still
                // starting up (host boot, a sibling hosted service racing ours) a bounded grace,
                // then fail with a configuration diagnosis instead of waiting forever.
                var workerSignal = Volatile.Read(ref _workerAvailable).Task;
                var completed = await Task.WhenAny(
                        tcs.Task,
                        workerSignal,
                        Task.Delay(_workerStartupTimeout, ct))
                    .ConfigureAwait(false);

                if (!ReferenceEquals(completed, tcs.Task) && !ReferenceEquals(completed, workerSignal))
                {
                    ct.ThrowIfCancellationRequested();
                    _waiters.TryRemove(jobId, out _);
                    throw new InvalidOperationException(
                        $"Vault job {jobId} cannot complete: no worker is consuming this queue " +
                        $"(waited {_workerStartupTimeout.TotalSeconds:0.#}s for one to register). " +
                        "The default worker, VaultBackgroundService, is an IHostedService: it only runs inside a " +
                        "Generic Host (Host.CreateApplicationBuilder / WebApplication) and only consumes the " +
                        "container-registered queue. Either host FluxFeed in a Generic Host, start the " +
                        "IHostedService yourself, or set FileVaultOptions.EnableBackgroundProcessing = false so " +
                        "MemorizeAsync processes inline. Vaults created through IVaultFactory own a separate " +
                        "queue that currently has no worker.");
                }
            }

            return await tcs.Task.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Abandon the wait; the job is unaffected.
            _waiters.TryRemove(jobId, out _);
            throw;
        }
    }

    /// <inheritdoc />
    public IDisposable RegisterWorker()
    {
        lock (_workerLock)
        {
            _activeWorkers++;
            _workerAvailable.TrySetResult();
        }

        return new WorkerLease(this);
    }

    private void ReleaseWorker()
    {
        lock (_workerLock)
        {
            if (--_activeWorkers == 0)
            {
                // Arm a fresh signal so a later waiter blocks until the next worker registers.
                _workerAvailable = NewWorkerSignal();
            }
        }
    }

    private static TaskCompletionSource NewWorkerSignal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class WorkerLease : IDisposable
    {
        private VaultQueueService? _owner;

        public WorkerLease(VaultQueueService owner) => _owner = owner;

        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _owner, null);
            owner?.ReleaseWorker();
        }
    }

    private static bool IsTerminal(VaultJobStatus status) =>
        status is VaultJobStatus.Completed or VaultJobStatus.Failed or VaultJobStatus.Cancelled;

    /// <summary>
    /// Resolves any caller awaiting this job via <see cref="WaitForJobAsync"/>. Safe to call when
    /// there is no waiter.
    /// </summary>
    private void SignalWaiter(VaultJob job)
    {
        if (_waiters.TryRemove(job.Id, out var tcs))
            tcs.TrySetResult(job);
    }

    private static async Task<VaultJob?> GetJobInternalAsync(SqliteConnection connection, Guid jobId, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT id, file_path, filepath_hash, job_type, status, priority, queued_at,
                   started_at, completed_at, retry_count, max_retries, error_message,
                   last_completed_chunk_index, group_key, failure_kind
            FROM vault_jobs
            WHERE id = @id
            """;
        cmd.Parameters.AddWithValue("@id", jobId.ToString());

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
            return ReadJob(reader);

        return null;
    }

    public async Task<IReadOnlyList<VaultJob>> GetJobsAsync(
        VaultJobStatus? statusFilter = null,
        VaultJobType? typeFilter = null,
        int? limit = null,
        int? offset = null,
        bool newestFirst = false,
        CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            var sql = """
                SELECT id, file_path, filepath_hash, job_type, status, priority, queued_at,
                       started_at, completed_at, retry_count, max_retries, error_message,
                       last_completed_chunk_index, group_key, failure_kind
                FROM vault_jobs
                WHERE 1=1
                """;

            if (statusFilter.HasValue)
                sql += " AND status = @status";
            if (typeFilter.HasValue)
                sql += " AND job_type = @job_type";

            // Observation order and dequeue order are different questions. Priority decides what runs next;
            // it has no business deciding what "the latest fifty failures" means, so newestFirst sorts purely
            // by recency. The default keeps the historic order so existing callers see no shift.
            sql += newestFirst
                ? " ORDER BY queued_at DESC"
                : " ORDER BY priority DESC, queued_at ASC";

            // SQLite will not take an OFFSET without a LIMIT; -1 is its documented "no limit" sentinel, so an
            // offset alone means "skip these, return the rest" rather than silently returning nothing.
            if (limit.HasValue || offset.HasValue)
                sql += " LIMIT @limit OFFSET @offset";

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;

            if (statusFilter.HasValue)
                cmd.Parameters.AddWithValue("@status", (int)statusFilter.Value);
            if (typeFilter.HasValue)
                cmd.Parameters.AddWithValue("@job_type", (int)typeFilter.Value);
            if (limit.HasValue || offset.HasValue)
            {
                cmd.Parameters.AddWithValue("@limit", limit ?? -1);
                cmd.Parameters.AddWithValue("@offset", offset ?? 0);
            }

            var jobs = new List<VaultJob>();
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                jobs.Add(ReadJob(reader));
            }

            return jobs;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<QueueStatistics> GetStatisticsAsync(CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            var counts = new Dictionary<VaultJobStatus, int>();
            await using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = """
                    SELECT status, COUNT(*) as count
                    FROM vault_jobs
                    GROUP BY status
                    """;

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var status = (VaultJobStatus)reader.GetInt32(0);
                    var count = reader.GetInt32(1);
                    counts[status] = count;
                }
            }

            var (lastSucceededAt, avgTime) = await GetCompletionStatsAsync(connection, ct);

            return new QueueStatistics
            {
                QueuedCount = counts.GetValueOrDefault(VaultJobStatus.Queued),
                ProcessingCount = counts.GetValueOrDefault(VaultJobStatus.Processing),
                CompletedCount = counts.GetValueOrDefault(VaultJobStatus.Completed),
                FailedCount = counts.GetValueOrDefault(VaultJobStatus.Failed),
                CancelledCount = counts.GetValueOrDefault(VaultJobStatus.Cancelled),
                IsPaused = _isPaused,
                LastSucceededAt = lastSucceededAt,
                LastAttemptedAt = await GetLastAttemptAsync(connection, ct),
                AverageProcessingTimeMs = avgTime
            };
        }
        finally
        {
            _dbLock.Release();
        }
    }

    // LastSucceededAt/AverageProcessingTimeMs are derived from the persisted vault_jobs rows (not
    // an in-memory cache) so a statistics read reflects completions recorded by any process
    // instance, not only ones this instance itself observed via CompleteAsync.
    private static async Task<(DateTimeOffset? LastSucceededAt, double AverageProcessingTimeMs)> GetCompletionStatsAsync(
        SqliteConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT started_at, completed_at
            FROM vault_jobs
            WHERE status = @status AND started_at IS NOT NULL AND completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 100
            """;
        cmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Completed);

        DateTimeOffset? lastSucceededAt = null;
        var durationsMs = new List<double>();

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var startedAt = DateTimeOffset.Parse(reader.GetString(0), CultureInfo.InvariantCulture);
            var completedAt = DateTimeOffset.Parse(reader.GetString(1), CultureInfo.InvariantCulture);
            lastSucceededAt ??= completedAt;
            durationsMs.Add((completedAt - startedAt).TotalMilliseconds);
        }

        var avgTime = durationsMs.Count > 0 ? durationsMs.Average() : 0;
        return (lastSucceededAt, avgTime);
    }

    /// <summary>
    /// The newest of <c>started_at</c> and <c>completed_at</c> over every job, whatever its status — the
    /// queue's liveness signal.
    /// </summary>
    /// <remarks>
    /// Deliberately not "the newest terminal event": a worker that picked a long job up two minutes ago and is
    /// still inside it is alive, and a definition drawn from completions alone would report it as stopped for
    /// exactly as long as the job runs — the same false negative that made a failing-but-working queue look
    /// frozen before <c>LastSucceededAt</c> was named honestly.
    /// </remarks>
    private static async Task<DateTimeOffset?> GetLastAttemptAsync(SqliteConnection connection, CancellationToken ct)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = """
            SELECT MAX(MAX(COALESCE(started_at, ''), COALESCE(completed_at, '')))
            FROM vault_jobs
            WHERE started_at IS NOT NULL OR completed_at IS NOT NULL
            """;

        var value = await cmd.ExecuteScalarAsync(ct);
        if (value is not string text || text.Length == 0)
            return null;

        return DateTimeOffset.Parse(text, CultureInfo.InvariantCulture);
    }

    #endregion

    #region Recovery & Cleanup

    public async Task<int> RecoverStuckJobsAsync(CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                UPDATE vault_jobs
                SET status = @queued, started_at = NULL
                WHERE status = @processing
                """;
            cmd.Parameters.AddWithValue("@queued", (int)VaultJobStatus.Queued);
            cmd.Parameters.AddWithValue("@processing", (int)VaultJobStatus.Processing);

            var recovered = await cmd.ExecuteNonQueryAsync(ct);

            if (recovered > 0)
            {
                LogRecovered(_logger, recovered);
            }

            return recovered;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task UpdateCheckpointAsync(Guid jobId, int lastCompletedChunkIndex, CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                UPDATE vault_jobs
                SET last_completed_chunk_index = @checkpoint
                WHERE id = @id AND status = @processing
                """;
            cmd.Parameters.AddWithValue("@checkpoint", lastCompletedChunkIndex);
            cmd.Parameters.AddWithValue("@id", jobId.ToString());
            cmd.Parameters.AddWithValue("@processing", (int)VaultJobStatus.Processing);

            await cmd.ExecuteNonQueryAsync(ct);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<int> ClearCompletedAsync(CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                DELETE FROM vault_jobs
                WHERE status IN (@completed, @cancelled)
                """;
            cmd.Parameters.AddWithValue("@completed", (int)VaultJobStatus.Completed);
            cmd.Parameters.AddWithValue("@cancelled", (int)VaultJobStatus.Cancelled);

            var deleted = await cmd.ExecuteNonQueryAsync(ct);
            LogClearedCompleted(_logger, deleted);

            return deleted;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task<int> ClearFailedAsync(CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = """
                DELETE FROM vault_jobs
                WHERE status = @failed
                """;
            cmd.Parameters.AddWithValue("@failed", (int)VaultJobStatus.Failed);

            var deleted = await cmd.ExecuteNonQueryAsync(ct);
            LogClearedFailed(_logger, deleted);

            return deleted;
        }
        finally
        {
            _dbLock.Release();
        }
    }

    public async Task ClearAllAsync(CancellationToken ct = default)
    {
        await _dbLock.WaitAsync(ct);
        try
        {
            await using var connection = CreateConnection();
            await connection.OpenAsync(ct);

            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "DELETE FROM vault_jobs";
            await cmd.ExecuteNonQueryAsync(ct);

            LogClearedAll(_logger);
        }
        finally
        {
            _dbLock.Release();
        }
    }

    #endregion

    #region Pause/Resume

    public void Pause()
    {
        _isPaused = true;
        LogPaused(_logger);
    }

    public void ResumeProcessing()
    {
        _isPaused = false;
        LogResumed(_logger);
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Returns the Queued job this request should merge into, or null when it should be inserted as a new
    /// row. Raises the found job's priority first when the incoming request is more urgent, so coalescing
    /// never demotes an escalation into a low-priority job that happened to be queued first.
    /// </summary>
    private static async Task<VaultJob?> FindCoalescibleJobAsync(
        SqliteConnection connection,
        string filepathHash,
        VaultJobType jobType,
        VaultJobPriority priority,
        CancellationToken ct)
    {
        VaultJob existing;

        await using (var findCmd = connection.CreateCommand())
        {
            findCmd.CommandText = """
                SELECT id, file_path, filepath_hash, job_type, status, priority, queued_at,
                       started_at, completed_at, retry_count, max_retries, error_message,
                       last_completed_chunk_index, group_key, failure_kind
                FROM vault_jobs
                WHERE filepath_hash = @filepath_hash AND job_type = @job_type AND status = @status
                ORDER BY priority DESC, queued_at ASC
                LIMIT 1
                """;
            findCmd.Parameters.AddWithValue("@filepath_hash", filepathHash);
            findCmd.Parameters.AddWithValue("@job_type", (int)jobType);
            findCmd.Parameters.AddWithValue("@status", (int)VaultJobStatus.Queued);

            await using var reader = await findCmd.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
                return null;

            existing = ReadJob(reader);
        }

        if (priority <= existing.Priority)
            return existing;

        await using (var raiseCmd = connection.CreateCommand())
        {
            raiseCmd.CommandText = "UPDATE vault_jobs SET priority = @priority WHERE id = @id";
            raiseCmd.Parameters.AddWithValue("@priority", (int)priority);
            raiseCmd.Parameters.AddWithValue("@id", existing.Id.ToString());
            await raiseCmd.ExecuteNonQueryAsync(ct);
        }

        return VaultJob.Restore(
            id: existing.Id,
            filePath: existing.FilePath,
            filepathHash: existing.FilepathHash,
            jobType: existing.JobType,
            status: existing.Status,
            priority: priority,
            queuedAt: existing.QueuedAt,
            startedAt: existing.StartedAt,
            completedAt: existing.CompletedAt,
            retryCount: existing.RetryCount,
            maxRetries: existing.MaxRetries,
            errorMessage: existing.ErrorMessage,
            lastCompletedChunkIndex: existing.LastCompletedChunkIndex);
    }

    private static VaultJob ReadJob(SqliteDataReader reader)
    {
        return VaultJob.Restore(
            id: Guid.Parse(reader.GetString(0)),
            filePath: reader.GetString(1),
            filepathHash: reader.GetString(2),
            jobType: (VaultJobType)reader.GetInt32(3),
            status: (VaultJobStatus)reader.GetInt32(4),
            priority: (VaultJobPriority)reader.GetInt32(5),
            queuedAt: DateTimeOffset.Parse(reader.GetString(6), CultureInfo.InvariantCulture),
            startedAt: reader.IsDBNull(7) ? null : DateTimeOffset.Parse(reader.GetString(7), CultureInfo.InvariantCulture),
            completedAt: reader.IsDBNull(8) ? null : DateTimeOffset.Parse(reader.GetString(8), CultureInfo.InvariantCulture),
            retryCount: reader.GetInt32(9),
            maxRetries: reader.GetInt32(10),
            errorMessage: reader.IsDBNull(11) ? null : reader.GetString(11),
            lastCompletedChunkIndex: reader.IsDBNull(12) ? -1 : reader.GetInt32(12),
            // By name, not by position. Not every query here selects the same trailing columns, so
            // an ordinal that means group_key in one statement means something else in the next -
            // and a mis-read column is silent, which is the failure mode this whole item is about.
            groupKey: ReadOptionalString(reader, "group_key"),
            failureKind: ReadOptionalInt32(reader, "failure_kind") is int kind
                ? (MemorizeFailureKind)kind
                : null
        );
    }

    private static int IndexOfColumn(SqliteDataReader reader, string name)
    {
        for (var i = 0; i < reader.FieldCount; i++)
        {
            if (string.Equals(reader.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    private static string? ReadOptionalString(SqliteDataReader reader, string name)
    {
        var i = IndexOfColumn(reader, name);
        return i < 0 || reader.IsDBNull(i) ? null : reader.GetString(i);
    }

    private static int? ReadOptionalInt32(SqliteDataReader reader, string name)
    {
        var i = IndexOfColumn(reader, name);
        return i < 0 || reader.IsDBNull(i) ? null : reader.GetInt32(i);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        // Fault any outstanding waiters so callers don't hang past disposal.
        foreach (var jobId in _waiters.Keys)
        {
            if (_waiters.TryRemove(jobId, out var tcs))
                tcs.TrySetException(new ObjectDisposedException(nameof(VaultQueueService)));
        }

        _dbLock.Dispose();
        _disposed = true;
    }

    #endregion

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Debug, Message = "Vault queue database initialized")]
    private static partial void LogDatabaseInitialized(ILogger logger);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Enqueued {JobType} job for {FilePath}")]
    private static partial void LogEnqueued(ILogger logger, VaultJobType jobType, string filePath);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Coalesced {JobType} request for {FilePath} into queued job {JobId}")]
    private static partial void LogCoalesced(ILogger logger, VaultJobType jobType, string filePath, Guid jobId);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Dequeued job {JobId} for {FilePath}")]
    private static partial void LogDequeued(ILogger logger, Guid jobId, string filePath);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Completed job {JobId}")]
    private static partial void LogCompleted(ILogger logger, Guid jobId);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed job {JobId}: {Error}")]
    private static partial void LogFailed(ILogger logger, Guid jobId, string error);
    [LoggerMessage(Level = LogLevel.Information, Message = "Requeued job {JobId} on request; the automatic retry budget was cleared")]
    private static partial void LogRequeuedByRequest(ILogger logger, Guid jobId);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Retrying job {JobId}, attempt {RetryCount}")]
    private static partial void LogRetrying(ILogger logger, Guid jobId, int retryCount);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Cancelled job {JobId}")]
    private static partial void LogCancelled(ILogger logger, Guid jobId);
    [LoggerMessage(Level = LogLevel.Information, Message = "Recovered {Count} stuck processing jobs")]
    private static partial void LogRecovered(ILogger logger, int count);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Cleared {Count} completed/cancelled jobs")]
    private static partial void LogClearedCompleted(ILogger logger, int count);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Cleared {Count} failed jobs")]
    private static partial void LogClearedFailed(ILogger logger, int count);
    [LoggerMessage(Level = LogLevel.Information, Message = "Cleared all queue jobs")]
    private static partial void LogClearedAll(ILogger logger);
    [LoggerMessage(Level = LogLevel.Information, Message = "Queue processing paused")]
    private static partial void LogPaused(ILogger logger);
    [LoggerMessage(Level = LogLevel.Information, Message = "Queue processing resumed")]
    private static partial void LogResumed(ILogger logger);

    #endregion
}
