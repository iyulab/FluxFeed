using System.Collections.Concurrent;
using System.Diagnostics;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Domain.Exceptions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FluxFeed.Services;

/// <summary>
/// Main vault implementation providing file-based tracking with queue-based processing.
/// </summary>
public sealed partial class VaultManager : IVault
{
    private readonly IContentHasher _hasher;
    private readonly IGitService _git;
    private readonly IVaultPipeline _pipeline;
    private readonly IVaultQueueService _queue;
    private readonly IFileWatcherService _fileWatcher;
    private readonly IVaultStorageService _storage;
    private readonly PatternMatcher _patternMatcher;
    private readonly ILogger<VaultManager> _logger;
    private readonly FileVaultOptions _options;

    private readonly ConcurrentDictionary<Guid, WatchedFolder> _watchedFolders = new();
    private DateTimeOffset? _lastSyncTime;

    public string VaultBasePath { get; }

    public VaultManager(
        IContentHasher hasher,
        IGitService git,
        IVaultPipeline pipeline,
        IVaultQueueService queue,
        IFileWatcherService fileWatcher,
        IVaultStorageService storage,
        ILogger<VaultManager> logger,
        IOptions<FileVaultOptions> options)
    {
        _hasher = hasher ?? throw new ArgumentNullException(nameof(hasher));
        _git = git ?? throw new ArgumentNullException(nameof(git));
        _pipeline = pipeline ?? throw new ArgumentNullException(nameof(pipeline));
        _queue = queue ?? throw new ArgumentNullException(nameof(queue));
        _fileWatcher = fileWatcher ?? throw new ArgumentNullException(nameof(fileWatcher));
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _patternMatcher = new PatternMatcher();

        VaultBasePath = _options.VaultBasePath ?? _options.VaultDirectoryName;
    }

    #region Core Commands

    public async Task<VaultEntry> MemorizeAsync(string filePath, CancellationToken ct = default)
    {
        var fullPath = Path.GetFullPath(filePath);

        if (!File.Exists(fullPath))
            throw new FileNotFoundException("Source file not found", fullPath);

        // Get or create entry
        var entry = await GetOrCreateEntryAsync(fullPath, ct);

        if (_options.EnableBackgroundProcessing)
        {
            // Queue memorize job (full pipeline: extract → chunk → embed → commit)
            await _queue.EnqueueMemorizeAsync(entry.FilepathHash, fullPath, ct);
            LogQueuedMemorize(_logger, fullPath);
        }
        else
        {
            // Execute pipeline directly (synchronous for tests/CLI)
            var result = await _pipeline.MemorizeAsync(entry, ct: ct);
            if (!result.Success)
            {
                LogMemorizeFailed(_logger, entry.SourcePath, result.ErrorMessage ?? "Unknown error");
            }
        }

        return entry;
    }

    public async Task<VaultEntry> MemorizeAsync(string filePath, bool waitForCompletion, CancellationToken ct = default)
    {
        // Without terminal-await, behavior is identical to the single-arg overload (pure additive).
        if (!waitForCompletion)
            return await MemorizeAsync(filePath, ct);

        var fullPath = Path.GetFullPath(filePath);

        if (!File.Exists(fullPath))
            throw new FileNotFoundException("Source file not found", fullPath);

        var entry = await GetOrCreateEntryAsync(fullPath, ct);

        if (_options.EnableBackgroundProcessing)
        {
            // Enqueue, then await the queue's terminal transition (signal-driven, no polling).
            var job = await _queue.EnqueueMemorizeAsync(entry.FilepathHash, fullPath, ct);
            LogQueuedMemorize(_logger, fullPath);

            var terminal = await _queue.WaitForJobAsync(job.Id, ct);
            if (terminal.Status == VaultJobStatus.Failed)
                throw new InvalidOperationException(
                    $"Memorize job failed for {fullPath}: {terminal.ErrorMessage ?? "unknown error"}");
            if (terminal.Status == VaultJobStatus.Cancelled)
                throw new OperationCanceledException($"Memorize job was cancelled for {fullPath}.");
        }
        else
        {
            // Inline mode is already terminal; surface failures instead of returning a stale entry.
            var result = await _pipeline.MemorizeAsync(entry, ct: ct);
            if (!result.Success)
            {
                LogMemorizeFailed(_logger, entry.SourcePath, result.ErrorMessage ?? "Unknown error");
                throw new InvalidOperationException(
                    $"Memorize failed for {fullPath}: {result.ErrorMessage ?? "unknown error"}");
            }
        }

        // Re-read the entry so callers see its terminal (Memorized) stage, not the early enqueue stage.
        return await GetByHashAsync(entry.FilepathHash, ct) ?? entry;
    }

    public async Task<VaultEntry> RefreshAsync(string filePath, CancellationToken ct = default)
    {
        var fullPath = Path.GetFullPath(filePath);

        var entry = await GetAsync(fullPath, ct);
        if (entry == null)
            throw new InvalidOperationException($"No vault entry exists for: {fullPath}. Use MemorizeAsync first.");

        // Refresh re-indexes refined content without re-extracting, so refined.md is the real
        // precondition — the same one VaultPipeline.RefreshAsync enforces. Guarding on Stage instead
        // let entries through that the pipeline then rejected: Stage does not imply refined.md
        // exists (a memorize that found no content skips refine, and Error says nothing about which
        // artifacts survive).
        if (!entry.RefinedExists)
            throw new InvalidOperationException(
                $"No refined content found at {entry.RefinedMdPath}. Run memorize first. Current stage: {entry.Stage}");

        if (_options.EnableBackgroundProcessing)
        {
            // Queue refresh job (chunk → embed → commit, skip extraction)
            await _queue.EnqueueRefreshAsync(entry.FilepathHash, fullPath, ct);
            LogQueuedRefresh(_logger, fullPath);
        }
        else
        {
            // Execute pipeline directly (synchronous for tests/CLI)
            var result = await _pipeline.RefreshAsync(entry, ct: ct);
            if (!result.Success)
            {
                LogRefreshFailed(_logger, entry.SourcePath, result.ErrorMessage ?? "Unknown error");
            }
        }

        return entry;
    }

    public async Task<SyncResult> SyncAsync(CancellationToken ct = default)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var memorizeCount = 0;
        var refreshCount = 0;
        var removeCount = 0;
        var skippedCount = 0;
        var newFilesCount = 0;
        var changedFilesCount = 0;
        var orphansDetected = 0;
        var orphansQueued = 0;
        var errors = new List<SyncError>();
        // ScanFolderAsync now runs its own reverse pass per watched folder (below); track what it
        // already surfaced so the cross-location sweep after the loop does not re-detect and
        // re-queue the same orphan a second time.
        var orphansSeenInFolderScan = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        // Scan all watched folders
        var folders = _watchedFolders.Values.ToList();
        foreach (var folder in folders)
        {
            if (folder.Status != WatcherStatus.Active)
                continue;

            try
            {
                var scanResult = await ScanFolderAsync(folder.Path, ct);

                // Queue jobs based on detected changes
                foreach (var change in scanResult.DetectedChanges)
                {
                    try
                    {
                        switch (change.RecommendedAction)
                        {
                            case ChangeAction.Memorize:
                                if (change.EntryExists)
                                    changedFilesCount++;
                                else
                                    newFilesCount++;

                                await _queue.EnqueueMemorizeAsync(
                                    FilepathHasher.ComputeHash(change.FilePath),
                                    change.FilePath, ct);
                                memorizeCount++;
                                break;

                            case ChangeAction.Refresh:
                                changedFilesCount++;
                                await _queue.EnqueueRefreshAsync(
                                    FilepathHasher.ComputeHash(change.FilePath),
                                    change.FilePath, ct);
                                refreshCount++;
                                break;

                            case ChangeAction.Remove:
                                orphansDetected++;
                                orphansSeenInFolderScan.Add(change.FilePath);
                                if (_options.AutoCleanupOrphans)
                                {
                                    await _queue.EnqueueRemoveAsync(
                                        FilepathHasher.ComputeHash(change.FilePath),
                                        change.FilePath, ct);
                                    orphansQueued++;
                                    removeCount++;
                                }
                                break;

                            case ChangeAction.None:
                                skippedCount++;
                                break;
                        }
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        errors.Add(new SyncError
                        {
                            FilePath = change.FilePath,
                            ErrorMessage = ex.Message,
                            Exception = ex
                        });
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                LogFailedToScanFolder(_logger, ex, folder.Path);
                errors.Add(new SyncError
                {
                    FilePath = folder.Path,
                    ErrorMessage = $"Failed to scan folder: {ex.Message}",
                    Exception = ex
                });
            }
        }

        // Also check orphaned entries across all locations (covers entries outside any watched
        // folder, e.g. a folder that was unwatched after the entry was created). Entries already
        // surfaced by a folder's own reverse pass above are excluded to avoid double-queuing.
        var orphanedEntries = (await GetOrphanedEntriesAsync(ct))
            .Where(e => !orphansSeenInFolderScan.Contains(e.SourcePath))
            .ToList();
        foreach (var entry in orphanedEntries)
        {
            if (!_options.AutoCleanupOrphans) continue;

            try
            {
                await _queue.EnqueueRemoveAsync(entry.FilepathHash, entry.SourcePath, ct);
                removeCount++;
                orphansQueued++;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add(new SyncError
                {
                    FilePath = entry.SourcePath,
                    ErrorMessage = ex.Message,
                    Exception = ex
                });
            }
        }

        _lastSyncTime = DateTimeOffset.UtcNow;

        return new SyncResult
        {
            MemorizeQueuedCount = memorizeCount,
            RefreshQueuedCount = refreshCount,
            RemoveQueuedCount = removeCount,
            SkippedCount = skippedCount,
            ErrorCount = errors.Count,
            Errors = errors,
            FoldersScanned = folders.Count,
            NewFilesDiscovered = newFilesCount,
            ChangedFilesDetected = changedFilesCount,
            OrphansDetected = orphansDetected + orphanedEntries.Count,
            OrphansQueued = orphansQueued,
            StartedAt = startedAt,
            CompletedAt = DateTimeOffset.UtcNow
        };
    }

    public async Task<ChangeDetectionResult> DetectChangesAsync(string filePath, CancellationToken ct = default)
    {
        var fullPath = Path.GetFullPath(filePath);
        var sourceExists = File.Exists(fullPath);
        var filepathHash = FilepathHasher.ComputeHash(fullPath);

        // Try to load existing entry
        var entry = VaultEntry.LoadByHash(filepathHash, _storage.BasePath);
        var entryExists = entry != null;

        // Check source changes (content hash)
        var sourceChanged = false;
        if (entryExists && sourceExists && entry!.SourceContentHash != null)
        {
            var currentHash = await _hasher.ComputeHashAsync(fullPath, ct);
            sourceChanged = !entry.SourceContentHash.Equals(currentHash);
        }

        // Check vault changes (git status)
        var vaultChanged = false;
        var modifiedVaultFiles = new List<string>();
        // Any entry past Source may own tracked vault files — including a failed one, which is how
        // a modified vault directory can be observed on an Error entry. Ordered comparison against
        // Extracted used to answer this, but Error sorts above the progress stages, so it silently
        // read as "most advanced" rather than "progress unknown".
        if (entryExists && entry!.Stage != ProcessingStage.Source)
        {
            var gitStatus = await _git.StatusAsync(entry.VaultPath, ct);
            if (gitStatus.ModifiedFiles.Count > 0)
            {
                vaultChanged = true;
                modifiedVaultFiles.AddRange(gitStatus.ModifiedFiles);
            }
        }

        // Determine recommended action
        var action = DetermineAction(entryExists, sourceExists, sourceChanged, vaultChanged,
            canRefresh: entry?.RefinedExists ?? false);

        // Update entry's SyncStatus based on detection results
        if (entry != null)
        {
            if (!sourceExists)
            {
                entry.UpdateSyncStatus(SyncStatus.SourceDeleted);
            }
            else if (sourceChanged)
            {
                entry.UpdateSyncStatus(SyncStatus.SourceModified);
            }
            else if (vaultChanged)
            {
                entry.UpdateSyncStatus(SyncStatus.VaultModified);
            }
            else if (entry.SyncStatus != SyncStatus.RemovalPending &&
                     entry.SyncStatus != SyncStatus.RemovalPartial &&
                     entry.SyncStatus != SyncStatus.Error)
            {
                // Only set InSync if not in a removal or error state
                entry.UpdateSyncStatus(SyncStatus.InSync);
            }

            entry.SaveMetadata();
        }

        // Collect file metadata
        string fileName = Path.GetFileName(fullPath);
        string fileExtension = Path.GetExtension(fullPath);
        long? fileSize = null;
        DateTimeOffset? fileModifiedAt = null;

        if (sourceExists)
        {
            try
            {
                var fileInfo = new FileInfo(fullPath);
                fileSize = fileInfo.Length;
                fileModifiedAt = new DateTimeOffset(fileInfo.LastWriteTimeUtc, TimeSpan.Zero);
            }
            catch
            {
                // Ignore metadata collection errors
            }
        }

        return new ChangeDetectionResult
        {
            FilePath = fullPath,
            EntryExists = entryExists,
            SourceChanged = sourceChanged,
            VaultChanged = vaultChanged,
            SourceExists = sourceExists,
            RecommendedAction = action,
            ModifiedVaultFiles = modifiedVaultFiles,

            // File metadata
            FileName = fileName,
            FileExtension = fileExtension,
            FileSize = fileSize,
            FileModifiedAt = fileModifiedAt,

            // Vault status
            Stage = entry?.Stage,
            SyncStatus = entry?.SyncStatus,
            ChunkCount = entry?.ChunkCount,
            LastError = entry?.LastError,
            FirstError = entry?.FirstError
        };
    }

    /// <param name="canRefresh">
    /// Whether a refresh could actually succeed for this entry (refined content present). Recommending
    /// a refresh that the pipeline is guaranteed to reject produces work that can never complete, and
    /// each failed attempt overwrites the entry's error with the rejection instead of its real cause.
    /// </param>
    private static ChangeAction DetermineAction(
        bool entryExists, bool sourceExists, bool sourceChanged, bool vaultChanged, bool canRefresh)
    {
        if (!sourceExists)
            return entryExists ? ChangeAction.Remove : ChangeAction.None;

        if (!entryExists)
            return ChangeAction.Memorize;

        if (sourceChanged)
            return ChangeAction.Memorize;

        if (vaultChanged)
            // Memorize is what the rejection message itself prescribes ("Run memorize first"), and it
            // re-extracts, so an entry that failed or produced no content can recover on its own.
            return canRefresh ? ChangeAction.Refresh : ChangeAction.Memorize;

        return ChangeAction.None;
    }

    #endregion

    #region Entry Management

    public Task<VaultEntry?> GetAsync(string filePath, CancellationToken ct = default)
    {
        var fullPath = Path.GetFullPath(filePath);
        var hash = FilepathHasher.ComputeHash(fullPath);
        return GetByHashAsync(hash, ct);
    }

    public Task<VaultEntry?> GetByHashAsync(string filepathHash, CancellationToken ct = default)
    {
        var entry = VaultEntry.LoadByHash(filepathHash, _storage.BasePath);
        return Task.FromResult(entry);
    }

    public Task<IReadOnlyList<VaultEntry>> ListAsync(ProcessingStage? stageFilter = null, CancellationToken ct = default)
    {
        var entries = new List<VaultEntry>();

        if (!Directory.Exists(_storage.BasePath))
            return Task.FromResult<IReadOnlyList<VaultEntry>>(entries);

        foreach (var dir in Directory.GetDirectories(_storage.BasePath))
        {
            var metaPath = Path.Combine(dir, "meta.json");
            if (!File.Exists(metaPath))
                continue;

            try
            {
                var dirName = Path.GetFileName(dir);
                var entry = VaultEntry.LoadByHash(dirName, _storage.BasePath);
                if (entry != null && (stageFilter == null || entry.Stage == stageFilter))
                {
                    entries.Add(entry);
                }
            }
            catch (Exception ex)
            {
                LogFailedToLoadEntry(_logger, ex, dir);
            }
        }

        return Task.FromResult<IReadOnlyList<VaultEntry>>(entries);
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> ListUnreadableAsync(CancellationToken ct = default)
    {
        var unreadable = new List<string>();

        if (!Directory.Exists(_storage.BasePath))
            return Task.FromResult<IReadOnlyList<string>>(unreadable);

        foreach (var dir in Directory.GetDirectories(_storage.BasePath))
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                VaultEntry.LoadByHash(Path.GetFileName(dir), _storage.BasePath);
            }
            catch (VaultRecordUnreadableException)
            {
                unreadable.Add(dir);
            }
        }

        return Task.FromResult<IReadOnlyList<string>>(unreadable);
    }

    public async Task RemoveAsync(string filePath, CancellationToken ct = default)
    {
        var fullPath = Path.GetFullPath(filePath);
        var entry = await GetAsync(fullPath, ct);

        if (entry == null)
        {
            LogNoEntryFound(_logger, fullPath);
            return;
        }

        if (_options.EnableBackgroundProcessing)
        {
            // Mark entry as pending removal so list queries reflect the state immediately
            entry.MarkRemovalPending();
            entry.SaveMetadata();

            // Queue remove job
            await _queue.EnqueueRemoveAsync(entry.FilepathHash, fullPath, ct);
            LogQueuedRemove(_logger, fullPath);
        }
        else
        {
            // Execute pipeline removal directly (synchronous for tests/CLI)
            await _pipeline.RemoveAsync(entry, ct);
            await _storage.DeleteEntryStorageAsync(entry, ct);
        }
    }

    public async Task RemoveAsync(IEnumerable<string> filePaths, CancellationToken ct = default)
    {
        foreach (var filePath in filePaths)
        {
            ct.ThrowIfCancellationRequested();
            await RemoveAsync(filePath, ct);
        }
    }

    /// <inheritdoc />
    public Task<int> PurgeAsync(CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(_options.VaultId))
            throw new InvalidOperationException(
                "PurgeAsync requires a tenant-scoped vault (FileVaultOptions.VaultId must be set). " +
                "For a single non-tenant vault, remove entries individually or clear the store directly.");

        return _pipeline.PurgeVectorsAsync(_options.VaultId, ct);
    }

    private async Task<VaultEntry> GetOrCreateEntryAsync(string fullPath, CancellationToken ct)
    {
        var filepathHash = FilepathHasher.ComputeHash(fullPath);
        var existing = LoadForRewrite(filepathHash);

        if (existing != null)
        {
            // Update content hash if source changed
            var currentHash = await _hasher.ComputeHashAsync(fullPath, ct);
            if (existing.SourceContentHash == null || !existing.SourceContentHash.Equals(currentHash))
            {
                existing.UpdateSourceContentHash(currentHash);
                existing.SaveMetadata();
            }
            return existing;
        }

        // Create new entry
        var entry = VaultEntry.Create(fullPath, _storage.BasePath);

        // Compute content hash
        var hash = await _hasher.ComputeHashAsync(fullPath, ct);
        entry.UpdateSourceContentHash(hash);

        // Initialize storage (creates directories, .gitignore, git init)
        await _storage.InitializeEntryAsync(entry, ct);

        // Save metadata
        entry.SaveMetadata();

        LogCreatedEntry(_logger, fullPath, entry.EntryPath);
        return entry;
    }

    /// <summary>
    /// Loads an entry on behalf of a caller that is about to rewrite its record.
    /// </summary>
    /// <remarks>
    /// An unreadable record is reported and then treated as absent here. Rewriting is precisely
    /// what repairs it, so failing instead would leave the entry permanently stuck — but it must
    /// not be discarded quietly, because the rebuilt record starts from an empty history.
    /// </remarks>
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

    #endregion

    #region Status & History

    public async Task<VaultStatus> StatusAsync(CancellationToken ct = default)
    {
        var entries = await ListAsync(ct: ct);
        var changedEntries = new List<VaultEntry>();

        var sourceCount = 0;
        var extractedCount = 0;
        var refinedCount = 0;
        var memorizedCount = 0;
        var staleCount = 0;
        var errorStageCount = 0;
        var changedSourceCount = 0;
        var changedVaultCount = 0;
        var orphanedCount = 0;
        long totalStorageSize = 0;

        // SyncStatus counts
        var inSyncCount = 0;
        var sourceModifiedCount = 0;
        var vaultModifiedCount = 0;
        var sourceDeletedCount = 0;
        var removalPendingCount = 0;
        var removalPartialCount = 0;
        var errorCount = 0;

        foreach (var entry in entries)
        {
            switch (entry.Stage)
            {
                case ProcessingStage.Source: sourceCount++; break;
                case ProcessingStage.Extracted: extractedCount++; break;
                case ProcessingStage.Refined: refinedCount++; break;
                case ProcessingStage.Memorized: memorizedCount++; break;
                case ProcessingStage.Stale: staleCount++; break;
                case ProcessingStage.Error: errorStageCount++; break;
            }

            // Count SyncStatus
            switch (entry.SyncStatus)
            {
                case SyncStatus.InSync: inSyncCount++; break;
                case SyncStatus.SourceModified: sourceModifiedCount++; break;
                case SyncStatus.VaultModified: vaultModifiedCount++; break;
                case SyncStatus.SourceDeleted: sourceDeletedCount++; break;
                case SyncStatus.RemovalPending: removalPendingCount++; break;
                case SyncStatus.RemovalPartial: removalPartialCount++; break;
                case SyncStatus.Error: errorCount++; break;
            }

            // Check if source file exists (orphaned check)
            if (!File.Exists(entry.SourcePath))
            {
                orphanedCount++;
                continue;
            }

            // Detect changes. The listing above already skipped records it could not read, so this
            // only fires if one became unreadable mid-sweep — report it and keep the rest of the
            // status intact rather than failing the whole query over a single entry.
            ChangeDetectionResult? changes = null;
            try
            {
                changes = await DetectChangesAsync(entry.SourcePath, ct);
            }
            catch (VaultRecordUnreadableException ex)
            {
                LogFailedToDetectChanges(_logger, ex, entry.SourcePath);
            }

            if (changes?.SourceChanged == true)
            {
                changedSourceCount++;
                changedEntries.Add(entry);
            }
            else if (changes?.VaultChanged == true)
            {
                changedVaultCount++;
                changedEntries.Add(entry);
            }

            // Calculate storage size
            if (Directory.Exists(entry.EntryPath))
            {
                totalStorageSize += GetDirectorySize(entry.EntryPath);
            }
        }

        // Queue status
        var queueStatus = await GetQueueStatusAsync(ct);

        // Watcher status
        var folders = _watchedFolders.Values.ToList();

        return new VaultStatus
        {
            TotalEntries = entries.Count,
            SourceCount = sourceCount,
            ExtractedCount = extractedCount,
            RefinedCount = refinedCount,
            MemorizedCount = memorizedCount,
            StaleCount = staleCount,
            ErrorStageCount = errorStageCount,
            ChangedSourceCount = changedSourceCount,
            ChangedVaultCount = changedVaultCount,
            ChangedEntries = changedEntries,
            InSyncCount = inSyncCount,
            SourceModifiedCount = sourceModifiedCount,
            VaultModifiedCount = vaultModifiedCount,
            SourceDeletedCount = sourceDeletedCount,
            RemovalPendingCount = removalPendingCount,
            RemovalPartialCount = removalPartialCount,
            ErrorCount = errorCount,
            ActiveWatcherCount = folders.Count(f => f.Status == WatcherStatus.Active),
            PausedWatcherCount = folders.Count(f => f.Status == WatcherStatus.Paused),
            ErrorWatcherCount = folders.Count(f => f.Status == WatcherStatus.Error),
            QueuedCount = queueStatus.QueuedCount,
            ProcessingCount = queueStatus.ProcessingCount,
            FailedCount = queueStatus.FailedCount,
            OrphanedCount = orphanedCount,
            LastSyncTime = _lastSyncTime,
            TotalStorageSizeBytes = totalStorageSize
        };
    }

    public async Task<string> DiffAsync(string filePath, CancellationToken ct = default)
    {
        var entry = await GetAsync(filePath, ct);
        if (entry == null || !Directory.Exists(entry.VaultPath))
            return "";

        return await _git.DiffAsync(entry.VaultPath, null, ct);
    }

    public async Task<IReadOnlyList<GitCommit>> LogAsync(string filePath, int maxCount = 10, CancellationToken ct = default)
    {
        var entry = await GetAsync(filePath, ct);
        if (entry == null || !Directory.Exists(entry.VaultPath))
            return [];

        return await _git.LogAsync(entry.VaultPath, maxCount, ct);
    }

    private static long GetDirectorySize(string path)
    {
        var info = new DirectoryInfo(path);
        if (!info.Exists) return 0;

        return info.EnumerateFiles("*", SearchOption.AllDirectories)
            .Sum(f => f.Length);
    }

    #endregion

    #region Folder Watching

    public async Task<WatchedFolder> AddWatchedFolderAsync(
        string folderPath,
        string? name = null,
        bool isRecursive = true,
        bool autoMemorize = false,
        string[]? includePatterns = null,
        string[]? excludePatterns = null,
        CancellationToken ct = default)
    {
        var fullPath = Path.GetFullPath(folderPath);

        if (!Directory.Exists(fullPath))
            throw new DirectoryNotFoundException($"Folder not found: {fullPath}");

        // Check if already watching
        var existing = _watchedFolders.Values.FirstOrDefault(f =>
            f.Path.Equals(fullPath, StringComparison.OrdinalIgnoreCase));
        if (existing != null)
        {
            LogFolderAlreadyWatched(_logger, fullPath);
            return existing;
        }

        var folder = WatchedFolder.Create(
            fullPath,
            name ?? Path.GetFileName(fullPath),
            isRecursive,
            autoMemorize);

        folder.SetPatterns(
            includePatterns ?? _options.DefaultIncludePatterns.ToArray(),
            excludePatterns ?? _options.DefaultExcludePatterns.ToArray());

        _watchedFolders[folder.Id] = folder;

        // Start watching
        await _fileWatcher.StartWatchingAsync(folder, ct);

        LogAddedWatchedFolder(_logger, folder.Name, folder.Path);
        return folder;
    }

    public Task<WatchedFolder?> GetWatchedFolderAsync(Guid folderId, CancellationToken ct = default)
    {
        _watchedFolders.TryGetValue(folderId, out var folder);
        return Task.FromResult(folder);
    }

    public Task<IReadOnlyList<WatchedFolder>> GetAllWatchedFoldersAsync(CancellationToken ct = default)
    {
        return Task.FromResult<IReadOnlyList<WatchedFolder>>(_watchedFolders.Values.ToList());
    }

    public async Task RemoveWatchedFolderAsync(Guid folderId, bool removeTrackedFiles = false, CancellationToken ct = default)
    {
        if (!_watchedFolders.TryRemove(folderId, out var folder))
            return;

        await _fileWatcher.StopWatchingAsync(folderId, ct);

        if (removeTrackedFiles)
        {
            var entries = await ListAsync(ct: ct);
            foreach (var entry in entries.Where(e => e.SourcePath.StartsWith(folder.Path, StringComparison.OrdinalIgnoreCase)))
            {
                await RemoveAsync(entry.SourcePath, ct);
            }
        }

        LogRemovedWatchedFolder(_logger, folder.Name, folder.Path);
    }

    public async Task PauseWatchingAsync(Guid folderId, CancellationToken ct = default)
    {
        if (!_watchedFolders.TryGetValue(folderId, out var folder))
            throw new KeyNotFoundException($"Watched folder not found: {folderId}");

        folder.Pause();
        await _fileWatcher.StopWatchingAsync(folderId, ct);
        LogPausedWatching(_logger, folder.Name);
    }

    public async Task ResumeWatchingAsync(Guid folderId, CancellationToken ct = default)
    {
        if (!_watchedFolders.TryGetValue(folderId, out var folder))
            throw new KeyNotFoundException($"Watched folder not found: {folderId}");

        folder.Resume();
        await _fileWatcher.StartWatchingAsync(folder, ct);
        LogResumedWatching(_logger, folder.Name);
    }

    public async Task<ScanResult> ScanFolderAsync(string folderPath, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        var fullPath = Path.GetFullPath(folderPath);

        if (!Directory.Exists(fullPath))
            throw new DirectoryNotFoundException($"Folder not found: {fullPath}");

        // Get watch options for this folder
        var folder = _watchedFolders.Values.FirstOrDefault(f =>
            f.Path.Equals(fullPath, StringComparison.OrdinalIgnoreCase));

        IEnumerable<string> includePatterns = folder != null
            ? folder.IncludePatterns
            : _options.DefaultIncludePatterns;
        IEnumerable<string> excludePatterns = folder != null
            ? folder.ExcludePatterns
            : _options.DefaultExcludePatterns;
        var isRecursive = folder?.IsRecursive ?? true;

        var searchOption = isRecursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
        var detectedChanges = new List<ChangeDetectionResult>();
        var errors = new List<ScanError>();
        var existingCount = 0;
        var skippedCount = 0;
        var scannedCount = 0;
        var newCount = 0;
        var changedCount = 0;
        var orphanedCount = 0;

        var files = Directory.EnumerateFiles(fullPath, "*", searchOption);

        foreach (var file in files)
        {
            ct.ThrowIfCancellationRequested();
            scannedCount++;

            // Check patterns
            if (!PatternMatcher.ShouldInclude(file, includePatterns, excludePatterns))
            {
                skippedCount++;
                continue;
            }

            try
            {
                var change = await DetectChangesAsync(file, ct);
                detectedChanges.Add(change);

                switch (change.RecommendedAction)
                {
                    case ChangeAction.Memorize when !change.EntryExists:
                        newCount++;
                        break;
                    case ChangeAction.Memorize:
                    case ChangeAction.Refresh:
                        changedCount++;
                        existingCount++;
                        break;
                    case ChangeAction.Remove:
                        orphanedCount++;
                        break;
                    case ChangeAction.None:
                        existingCount++;
                        break;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add(new ScanError { FilePath = file, ErrorMessage = ex.Message });
                LogFailedToDetectChanges(_logger, ex, file);
            }
        }

        // Reverse pass: Directory.EnumerateFiles above can only ever yield files that still exist,
        // so a tracked entry whose source was deleted is never visited by the forward loop and
        // DetermineAction's Remove branch can never fire from it. Surface those entries here by
        // walking tracked entries instead of disk files, scoped to this folder.
        var trackedEntries = await ListAsync(ct: ct);
        foreach (var entry in trackedEntries)
        {
            ct.ThrowIfCancellationRequested();

            if (File.Exists(entry.SourcePath) || !IsWithinScannedFolder(entry.SourcePath, fullPath, isRecursive))
                continue;

            try
            {
                var change = await DetectChangesAsync(entry.SourcePath, ct);
                if (change.RecommendedAction == ChangeAction.Remove)
                {
                    detectedChanges.Add(change);
                    orphanedCount++;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                errors.Add(new ScanError { FilePath = entry.SourcePath, ErrorMessage = ex.Message });
                LogFailedToDetectChanges(_logger, ex, entry.SourcePath);
            }
        }

        sw.Stop();
        LogScannedFolder(_logger, fullPath, newCount, changedCount, existingCount, sw.ElapsedMilliseconds);

        return new ScanResult
        {
            ScannedCount = scannedCount,
            NewFilesCount = newCount,
            ExistingFilesCount = existingCount,
            ChangedFilesCount = changedCount,
            SkippedFilesCount = skippedCount,
            OrphanedFilesCount = orphanedCount,
            DetectedChanges = detectedChanges,
            ErrorCount = errors.Count,
            Errors = errors,
            Duration = sw.Elapsed
        };
    }

    /// <summary>
    /// Whether a (possibly now-deleted) source path falls within the folder being scanned.
    /// </summary>
    /// <remarks>
    /// Compares against the source's parent directory rather than doing a string prefix match on
    /// the full path, so a folder named "C:\data\A" does not spuriously match a sibling "C:\data\AB".
    /// </remarks>
    private static bool IsWithinScannedFolder(string sourcePath, string folderFullPath, bool recursive)
    {
        var sourceDirectory = Path.GetDirectoryName(sourcePath);
        if (string.IsNullOrEmpty(sourceDirectory))
            return false;

        if (!recursive)
            return string.Equals(sourceDirectory, folderFullPath, StringComparison.OrdinalIgnoreCase);

        var normalizedFolder = folderFullPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        return sourceDirectory.Equals(normalizedFolder, StringComparison.OrdinalIgnoreCase) ||
               sourceDirectory.StartsWith(normalizedFolder + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);
    }

    public async Task<ScanResult> ScanFolderAsync(Guid folderId, CancellationToken ct = default)
    {
        if (!_watchedFolders.TryGetValue(folderId, out var folder))
            throw new KeyNotFoundException($"Watched folder not found: {folderId}");

        return await ScanFolderAsync(folder.Path, ct);
    }

    #endregion

    #region Queue Management

    public Task PauseQueueAsync(CancellationToken ct = default)
    {
        _queue.Pause();
        LogPausedQueue(_logger);
        return Task.CompletedTask;
    }

    public Task ResumeQueueAsync(CancellationToken ct = default)
    {
        _queue.ResumeProcessing();
        LogResumedQueue(_logger);
        return Task.CompletedTask;
    }

    public async Task<QueueStatus> GetQueueStatusAsync(CancellationToken ct = default)
    {
        var stats = await _queue.GetStatisticsAsync(ct);
        return new QueueStatus
        {
            QueuedCount = stats.QueuedCount,
            ProcessingCount = stats.ProcessingCount,
            CompletedCount = stats.CompletedCount,
            FailedCount = stats.FailedCount,
            IsPaused = _queue.IsPaused
        };
    }

    #endregion

    #region Orphan Management

    public async Task<int> CleanupOrphanedEntriesAsync(CancellationToken ct = default)
    {
        var orphans = await GetOrphanedEntriesAsync(ct);
        var cleanedCount = 0;

        foreach (var entry in orphans)
        {
            try
            {
                // Queue removal job
                await _queue.EnqueueRemoveAsync(entry.FilepathHash, entry.SourcePath, ct);
                cleanedCount++;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                LogFailedToQueueCleanup(_logger, ex, entry.SourcePath);
            }
        }

        if (cleanedCount > 0)
        {
            LogQueuedCleanup(_logger, cleanedCount);
        }

        return cleanedCount;
    }

    public async Task<IReadOnlyList<VaultEntry>> GetOrphanedEntriesAsync(CancellationToken ct = default)
    {
        var entries = await ListAsync(ct: ct);
        return entries.Where(e => !File.Exists(e.SourcePath)).ToList();
    }

    #endregion

    #region Status-based Queries

    public async Task<IReadOnlyList<VaultEntry>> ListByStatusAsync(SyncStatus status, CancellationToken ct = default)
    {
        var entries = await ListAsync(ct: ct);
        return entries.Where(e => e.SyncStatus == status).ToList();
    }

    public async Task<IReadOnlyList<VaultEntry>> GetPendingRemovalsAsync(CancellationToken ct = default)
    {
        var entries = await ListAsync(ct: ct);
        return entries.Where(e =>
            e.SyncStatus == SyncStatus.SourceDeleted ||
            e.SyncStatus == SyncStatus.RemovalPending ||
            e.SyncStatus == SyncStatus.RemovalPartial).ToList();
    }

    public async Task<IReadOnlyList<VaultEntry>> GetErrorEntriesAsync(CancellationToken ct = default)
    {
        var entries = await ListAsync(ct: ct);
        return entries.Where(e =>
            e.SyncStatus == SyncStatus.Error ||
            e.Stage == ProcessingStage.Error).ToList();
    }

    public async Task<IReadOnlyList<VaultEntry>> GetEntriesNeedingSyncAsync(CancellationToken ct = default)
    {
        var entries = await ListAsync(ct: ct);
        return entries.Where(e =>
            e.SyncStatus == SyncStatus.SourceModified ||
            e.SyncStatus == SyncStatus.VaultModified).ToList();
    }

    #endregion

    #region Search

    public async Task<VaultSearchResult> SearchAsync(string query, VaultSearchOptions? options = null, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        options ??= VaultSearchOptions.All();

        try
        {
            // Get all entries to filter by path scope
            var allEntries = await ListAsync(ProcessingStage.Memorized, ct);
            var entriesDict = allEntries.ToDictionary(e => e.FilepathHash, e => e);

            // Filter entries by path scope
            IReadOnlyList<VaultEntry> targetEntries;
            var searchedPaths = new List<string>();

            if (options.PathScope.Count == 0)
            {
                // Search all
                targetEntries = allEntries;
                searchedPaths.Add("*");
            }
            else
            {
                var filteredEntries = new List<VaultEntry>();

                foreach (var scope in options.PathScope)
                {
                    var normalizedScope = Path.GetFullPath(scope.TrimEnd('/', '\\'));
                    searchedPaths.Add(normalizedScope);

                    // Check if scope is a directory or file
                    if (Directory.Exists(normalizedScope))
                    {
                        // Directory scope - match all files under this directory
                        var matchingEntries = allEntries.Where(e =>
                            e.SourcePath.StartsWith(normalizedScope + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase) ||
                            e.SourcePath.Equals(normalizedScope, StringComparison.OrdinalIgnoreCase));
                        filteredEntries.AddRange(matchingEntries);
                    }
                    else if (File.Exists(normalizedScope))
                    {
                        // File scope - match exact file
                        var matchingEntry = allEntries.FirstOrDefault(e =>
                            e.SourcePath.Equals(normalizedScope, StringComparison.OrdinalIgnoreCase));
                        if (matchingEntry != null)
                        {
                            filteredEntries.Add(matchingEntry);
                        }
                    }
                    else
                    {
                        // Path doesn't exist - try to match as prefix pattern
                        var matchingEntries = allEntries.Where(e =>
                            e.SourcePath.StartsWith(normalizedScope, StringComparison.OrdinalIgnoreCase));
                        filteredEntries.AddRange(matchingEntries);
                    }
                }

                targetEntries = filteredEntries.Distinct().ToList();
            }

            if (targetEntries.Count == 0)
            {
                sw.Stop();
                return new VaultSearchResult
                {
                    Query = query,
                    Items = [],
                    TotalCount = 0,
                    SearchedPaths = searchedPaths,
                    DocumentsSearched = 0,
                    Duration = sw.Elapsed,
                    RequestedStrategy = options.SearchStrategy,
                    ExecutedStrategy = options.SearchStrategy
                };
            }

            // Get document IDs to filter search
            var documentIds = targetEntries.Select(e => e.FilepathHash).ToList();

            // Execute pipeline search with the requested strategy
            var pipelineResponse = await _pipeline.SearchAsync(
                query, documentIds, options.TopK, options.MinScore, options.SearchStrategy, ct);

            // Map to VaultSearchResultItem
            var items = pipelineResponse.Results.Select(r =>
            {
                entriesDict.TryGetValue(r.DocumentId, out var entry);
                return new VaultSearchResultItem
                {
                    Entry = entry!,
                    SourcePath = entry?.SourcePath ?? r.DocumentId,
                    FileName = entry?.FileName ?? Path.GetFileName(r.DocumentId),
                    ChunkIndex = r.ChunkIndex,
                    Content = options.IncludeContent ? r.Content : null,
                    Score = r.Score,
                    Metadata = options.IncludeMetadata ? r.Metadata : null
                };
            }).ToList();

            sw.Stop();

            LogSearchCompleted(_logger, query, targetEntries.Count, items.Count, sw.ElapsedMilliseconds);

            return new VaultSearchResult
            {
                Query = query,
                Items = items,
                TotalCount = items.Count,
                SearchedPaths = searchedPaths,
                DocumentsSearched = targetEntries.Count,
                Duration = sw.Elapsed,
                RequestedStrategy = options.SearchStrategy,
                ExecutedStrategy = pipelineResponse.ExecutedStrategy
            };
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            sw.Stop();
            throw;
        }
        catch (Exception ex)
        {
            sw.Stop();
            LogSearchFailed(_logger, ex, query);
            return VaultSearchResult.Error(query, ex.Message);
        }
    }

    #endregion

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Information, Message = "Queued memorize job for {FilePath}")]
    private static partial void LogQueuedMemorize(ILogger logger, string filePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Queued refresh job for {FilePath}")]
    private static partial void LogQueuedRefresh(ILogger logger, string filePath);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to scan folder {Path}")]
    private static partial void LogFailedToScanFolder(ILogger logger, Exception exception, string path);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to load vault entry from {Path}")]
    private static partial void LogFailedToLoadEntry(ILogger logger, Exception exception, string path);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Vault record unreadable, rebuilding it from scratch: {Path}")]
    private static partial void LogRebuildingUnreadableRecord(ILogger logger, Exception exception, string path);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No entry found for {FilePath}")]
    private static partial void LogNoEntryFound(ILogger logger, string filePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Queued remove job for {FilePath}")]
    private static partial void LogQueuedRemove(ILogger logger, string filePath);

    [LoggerMessage(Level = LogLevel.Error, Message = "Memorize failed for {FilePath}: {Error}")]
    private static partial void LogMemorizeFailed(ILogger logger, string filePath, string error);

    [LoggerMessage(Level = LogLevel.Error, Message = "Refresh failed for {FilePath}: {Error}")]
    private static partial void LogRefreshFailed(ILogger logger, string filePath, string error);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created vault entry for {FilePath} -> {EntryPath}")]
    private static partial void LogCreatedEntry(ILogger logger, string filePath, string entryPath);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Folder already being watched: {Path}")]
    private static partial void LogFolderAlreadyWatched(ILogger logger, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Added watched folder: {Name} ({Path})")]
    private static partial void LogAddedWatchedFolder(ILogger logger, string name, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Removed watched folder: {Name} ({Path})")]
    private static partial void LogRemovedWatchedFolder(ILogger logger, string name, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Paused watching folder: {Name}")]
    private static partial void LogPausedWatching(ILogger logger, string name);

    [LoggerMessage(Level = LogLevel.Information, Message = "Resumed watching folder: {Name}")]
    private static partial void LogResumedWatching(ILogger logger, string name);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to detect changes for file: {Path}")]
    private static partial void LogFailedToDetectChanges(ILogger logger, Exception exception, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Scanned folder {Path}: {New} new, {Changed} changed, {Existing} existing in {Duration}ms")]
    private static partial void LogScannedFolder(ILogger logger, string path, int @new, int changed, int existing, long duration);

    [LoggerMessage(Level = LogLevel.Information, Message = "Paused queue processing")]
    private static partial void LogPausedQueue(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Resumed queue processing")]
    private static partial void LogResumedQueue(ILogger logger);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to queue cleanup for orphaned entry: {Path}")]
    private static partial void LogFailedToQueueCleanup(ILogger logger, Exception exception, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Queued cleanup for {Count} orphaned entries")]
    private static partial void LogQueuedCleanup(ILogger logger, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Search '{Query}' in {Count} documents returned {Results} results in {Duration}ms")]
    private static partial void LogSearchCompleted(ILogger logger, string query, int count, int results, long duration);

    [LoggerMessage(Level = LogLevel.Error, Message = "Search failed for query: {Query}")]
    private static partial void LogSearchFailed(ILogger logger, Exception exception, string query);

    #endregion
}
