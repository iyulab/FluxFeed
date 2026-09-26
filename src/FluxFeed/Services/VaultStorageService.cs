using System.Text.Json;
using FluxFeed.Domain;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FluxFeed.Services;

/// <summary>
/// File-based vault storage service implementing the new directory structure.
/// </summary>
public sealed partial class VaultStorageService : IVaultStorageService
{
    private readonly ILogger<VaultStorageService> _logger;
    private readonly IGitService _gitService;
    private readonly string _basePath;

    /// <summary>
    /// Serializes the manifest's read-modify-write per entry. Each write is already whole (AtomicFile), but
    /// two overlapping updates each read the manifest before the other wrote it, and the later write drops
    /// the earlier one's change — a description silently lost and paid for again. Striped rather than one
    /// lock per entry so the set stays bounded however many entries a vault holds; process-wide because a
    /// vault's storage service is not the only instance that may touch an entry.
    /// </summary>
    private static readonly SemaphoreSlim[] s_manifestLocks =
        Enumerable.Range(0, 64).Select(_ => new SemaphoreSlim(1, 1)).ToArray();

    private static SemaphoreSlim ManifestLockFor(VaultEntry entry) =>
        s_manifestLocks[(int)((uint)Path.GetFullPath(entry.ImagesManifestPath)
            .GetHashCode(StringComparison.OrdinalIgnoreCase) % (uint)s_manifestLocks.Length)];

    private static async Task<T> WithManifestLockAsync<T>(VaultEntry entry, Func<Task<T>> update, CancellationToken ct)
    {
        var gate = ManifestLockFor(entry);
        await gate.WaitAsync(ct);
        try
        {
            return await update();
        }
        finally
        {
            gate.Release();
        }
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public VaultStorageService(
        ILogger<VaultStorageService> logger,
        IGitService gitService,
        IOptions<FileVaultOptions> options)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _gitService = gitService ?? throw new ArgumentNullException(nameof(gitService));

        var opts = options?.Value ?? new FileVaultOptions();
        _basePath = opts.VaultBasePath ?? Path.Combine(Directory.GetCurrentDirectory(), opts.VaultDirectoryName);

        // Ensure base path exists
        Directory.CreateDirectory(_basePath);
    }

    public string BasePath => _basePath;

    public async Task InitializeEntryAsync(VaultEntry entry, CancellationToken ct = default)
    {
        // Create entry directory
        Directory.CreateDirectory(entry.EntryPath);

        // Create vault subdirectory
        Directory.CreateDirectory(entry.VaultPath);

        // Initialize git in vault/ subdirectory (non-fatal — vault works without git)
        try
        {
            await _gitService.InitAsync(entry.VaultPath, ct);
        }
        catch (Exception ex)
        {
            LogGitInitFailed(_logger, entry.EntryPath, ex.Message);
        }

        // Save entry metadata — must always execute to prevent zombie entries
        entry.SaveMetadata();

        LogInitializedEntry(_logger, entry.EntryPath);
    }

    public async Task StoreExtractedContentAsync(VaultEntry entry, string content, CancellationToken ct = default)
    {
        Directory.CreateDirectory(entry.EntryPath);
        await AtomicFile.WriteAllTextAsync(entry.ExtractedMdPath, content, entry.EntryPath, ct);
        LogStoredExtracted(_logger, entry.Id);
    }

    public async Task<string?> GetExtractedContentAsync(VaultEntry entry, CancellationToken ct = default)
    {
        if (!File.Exists(entry.ExtractedMdPath))
            return null;

        return await AtomicFile.ReadAllTextAsync(entry.ExtractedMdPath, ct);
    }

    public async Task StoreRefinedContentAsync(VaultEntry entry, string content, CancellationToken ct = default)
    {
        Directory.CreateDirectory(entry.VaultPath);
        await AtomicFile.WriteAllTextAsync(entry.RefinedMdPath, content, entry.EntryPath, ct);
        LogStoredRefined(_logger, entry.Id);
    }

    public async Task<string?> GetRefinedContentAsync(VaultEntry entry, CancellationToken ct = default)
    {
        if (!File.Exists(entry.RefinedMdPath))
            return null;

        return await AtomicFile.ReadAllTextAsync(entry.RefinedMdPath, ct);
    }

    public async Task StoreContentSpansAsync(VaultEntry entry, ContentSpanSet? spans, CancellationToken ct = default)
    {
        if (spans is not { Spans.Count: > 0 })
        {
            if (File.Exists(entry.ExtractedSpansPath))
                File.Delete(entry.ExtractedSpansPath);
            return;
        }

        Directory.CreateDirectory(entry.EntryPath);
        await AtomicFile.WriteAllTextAsync(entry.ExtractedSpansPath, JsonSerializer.Serialize(spans, JsonOptions), entry.EntryPath, ct);
    }

    public async Task<ContentSpanSet?> GetContentSpansAsync(VaultEntry entry, CancellationToken ct = default)
    {
        if (!File.Exists(entry.ExtractedSpansPath))
            return null;

        var json = await AtomicFile.ReadAllTextAsync(entry.ExtractedSpansPath, ct);
        return JsonSerializer.Deserialize<ContentSpanSet>(json, JsonOptions);
    }

    public Task StoreImagesAsync(VaultEntry entry, IEnumerable<ImageArtifact> images, CancellationToken ct = default) =>
        WithManifestLockAsync(entry, async () => { await StoreImagesCoreAsync(entry, images, ct); return true; }, ct);

    private async Task StoreImagesCoreAsync(VaultEntry entry, IEnumerable<ImageArtifact> images, CancellationToken ct)
    {
        Directory.CreateDirectory(entry.ImagesPath);

        // Descriptions are expensive to produce and are keyed to the image, not to the run that
        // extracted it. A re-memorize re-extracts the same images, so carry a previous description
        // forward when the image is byte-identical — otherwise re-extraction would silently discard
        // every description and make enrichment pay again on every memorize. The same applies to a
        // recorded enrichment failure: without carrying it forward too, every MemorizeAsync call
        // (which re-extracts) would silently reset the attempt count, and a permanently-failed image
        // would be offered to the enricher again on the very next memorize.
        var previous = (await ReadPreviousManifestForRewriteAsync(entry, ct) ?? [])
            .Where(item => !string.IsNullOrWhiteSpace(item.Description) || item.LastEnrichmentFailure != null)
            .ToDictionary(item => item.Id, StringComparer.Ordinal);

        var manifest = new List<ImageManifestEntry>();
        var index = 0;

        foreach (var image in images)
        {
            var extension = GetExtensionFromContentType(image.ContentType);
            // Use original image ID for consistent naming (fixes index mismatch bug)
            var fileName = $"{image.Id}{extension}";
            var filePath = Path.Combine(entry.ImagesPath, fileName);

            var carriedDescription = image.Description;
            EnrichmentFailure? carriedFailure = null;
            if (previous.TryGetValue(image.Id, out var prior) &&
                await IsSameStoredImageAsync(filePath, image.Data, ct))
            {
                if (carriedDescription == null)
                    carriedDescription = prior.Description;
                carriedFailure = prior.LastEnrichmentFailure;
            }

            await AtomicFile.WriteAllBytesAsync(filePath, image.Data, entry.EntryPath, ct);

            manifest.Add(new ImageManifestEntry
            {
                Id = image.Id,
                FileName = fileName,
                ContentType = image.ContentType,
                Description = carriedDescription,
                AltText = image.AltText,
                Width = image.Width,
                Height = image.Height,
                Size = image.Data.Length,
                LastEnrichmentFailure = carriedFailure
            });

            index++;
        }

        // Write manifest
        var manifestJson = JsonSerializer.Serialize(manifest, JsonOptions);
        await AtomicFile.WriteAllTextAsync(entry.ImagesManifestPath, manifestJson, entry.EntryPath, ct);

        LogStoredImages(_logger, index, entry.Id);
    }

    public async Task<IReadOnlyList<ImageArtifact>> GetImagesAsync(VaultEntry entry, CancellationToken ct = default)
    {
        if (!File.Exists(entry.ImagesManifestPath))
            return [];

        var json = await AtomicFile.ReadAllTextAsync(entry.ImagesManifestPath, ct);
        var manifest = JsonSerializer.Deserialize<List<ImageManifestEntry>>(json, JsonOptions);

        if (manifest == null)
            return [];

        var images = new List<ImageArtifact>();
        foreach (var item in manifest)
        {
            var imagePath = Path.Combine(entry.ImagesPath, item.FileName);
            if (File.Exists(imagePath))
            {
                var data = await AtomicFile.ReadAllBytesAsync(imagePath, ct);
                images.Add(new ImageArtifact
                {
                    Id = item.Id,
                    Data = data,
                    ContentType = item.ContentType,
                    Description = item.Description,
                    AltText = item.AltText,
                    Width = item.Width,
                    Height = item.Height
                });
            }
        }

        return images;
    }

    public async Task<IReadOnlyList<VaultImage>> GetImageManifestAsync(VaultEntry entry, CancellationToken ct = default)
    {
        var manifest = await ReadManifestAsync(entry, ct);
        if (manifest == null)
            return [];

        return manifest
            .Where(item => File.Exists(Path.Combine(entry.ImagesPath, item.FileName)))
            .Select(item => new VaultImage
            {
                Id = item.Id,
                FileName = item.FileName,
                FilePath = Path.Combine(entry.ImagesPath, item.FileName),
                ContentType = item.ContentType,
                Description = item.Description,
                AltText = item.AltText,
                LastEnrichmentFailure = item.LastEnrichmentFailure
            })
            .ToList();
    }

    public Task<bool> SetImageDescriptionAsync(VaultEntry entry, string imageId, string description, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(imageId);
        return WithManifestLockAsync(entry, () => SetImageDescriptionCoreAsync(entry, imageId, description, ct), ct);
    }

    private async Task<bool> SetImageDescriptionCoreAsync(VaultEntry entry, string imageId, string description, CancellationToken ct)
    {

        var manifest = await ReadManifestAsync(entry, ct);
        var target = manifest?.FirstOrDefault(item => item.Id == imageId);
        if (target == null)
            return false;

        target.Description = description;
        target.LastEnrichmentFailure = null;

        var json = JsonSerializer.Serialize(manifest, JsonOptions);
        await AtomicFile.WriteAllTextAsync(entry.ImagesManifestPath, json, entry.EntryPath, ct);

        LogStoredImageDescription(_logger, imageId, entry.Id);
        return true;
    }

    public Task<bool> SetImageEnrichmentFailureAsync(
        VaultEntry entry,
        string imageId,
        string reason,
        int attemptCount,
        bool isPermanent,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(imageId);
        return WithManifestLockAsync(
            entry, () => SetImageEnrichmentFailureCoreAsync(entry, imageId, reason, attemptCount, isPermanent, ct), ct);
    }

    private async Task<bool> SetImageEnrichmentFailureCoreAsync(
        VaultEntry entry,
        string imageId,
        string reason,
        int attemptCount,
        bool isPermanent,
        CancellationToken ct)
    {

        var manifest = await ReadManifestAsync(entry, ct);
        var target = manifest?.FirstOrDefault(item => item.Id == imageId);
        if (target == null)
            return false;

        target.LastEnrichmentFailure = new EnrichmentFailure
        {
            Reason = reason,
            AttemptCount = attemptCount,
            IsPermanent = isPermanent,
            LastAttemptAt = DateTimeOffset.UtcNow
        };

        var json = JsonSerializer.Serialize(manifest, JsonOptions);
        await AtomicFile.WriteAllTextAsync(entry.ImagesManifestPath, json, entry.EntryPath, ct);

        LogStoredImageEnrichmentFailure(_logger, imageId, entry.Id, attemptCount, isPermanent);
        return true;
    }

    /// <summary>
    /// True when the file already on disk is byte-identical to the freshly extracted image — the
    /// condition under which a description written for it is still about the same picture.
    /// </summary>
    private static async Task<bool> IsSameStoredImageAsync(string filePath, byte[] data, CancellationToken ct)
    {
        if (!File.Exists(filePath))
            return false;

        var existing = await AtomicFile.ReadAllBytesAsync(filePath, ct);
        return existing.AsSpan().SequenceEqual(data);
    }

    /// <summary>
    /// Reads the manifest a rewrite is about to replace. An unreadable one is reported and treated as absent
    /// rather than failing the rewrite: the previous manifest only supplies descriptions to carry forward,
    /// and failing here would make a damaged manifest block the one operation that replaces it — every later
    /// attempt on the entry failing the same way until someone deletes the file.
    /// </summary>
    private async Task<List<ImageManifestEntry>?> ReadPreviousManifestForRewriteAsync(VaultEntry entry, CancellationToken ct)
    {
        try
        {
            return await ReadManifestAsync(entry, ct);
        }
        catch (JsonException ex)
        {
            LogRebuildingUnreadableManifest(_logger, entry.Id, entry.ImagesManifestPath, ex.Message);
            return null;
        }
    }

    private async Task<List<ImageManifestEntry>?> ReadManifestAsync(VaultEntry entry, CancellationToken ct)
    {
        if (!File.Exists(entry.ImagesManifestPath))
            return null;

        var json = await AtomicFile.ReadAllTextAsync(entry.ImagesManifestPath, ct);
        return JsonSerializer.Deserialize<List<ImageManifestEntry>>(json, JsonOptions);
    }

    public async Task<VaultTextContent> GetAllVaultContentAsync(VaultEntry entry, CancellationToken ct = default)
    {
        string? refinedContent = null;
        string? appendText = null;
        string? qaContent = null;

        if (File.Exists(entry.RefinedMdPath))
            refinedContent = await AtomicFile.ReadAllTextAsync(entry.RefinedMdPath, ct);

        if (File.Exists(entry.AppendTextPath))
            appendText = await AtomicFile.ReadAllTextAsync(entry.AppendTextPath, ct);

        if (File.Exists(entry.QaPath))
            qaContent = await AtomicFile.ReadAllTextAsync(entry.QaPath, ct);

        return new VaultTextContent
        {
            RefinedContent = refinedContent,
            AppendText = appendText,
            QaContent = qaContent
        };
    }

    public async Task StoreAppendTextAsync(VaultEntry entry, string content, CancellationToken ct = default)
    {
        Directory.CreateDirectory(entry.VaultPath);
        await AtomicFile.WriteAllTextAsync(entry.AppendTextPath, content, entry.EntryPath, ct);
        LogStoredAppendText(_logger, entry.Id);
    }

    public async Task StoreQaContentAsync(VaultEntry entry, string content, CancellationToken ct = default)
    {
        Directory.CreateDirectory(entry.VaultPath);
        await AtomicFile.WriteAllTextAsync(entry.QaPath, content, entry.EntryPath, ct);
        LogStoredQa(_logger, entry.Id);
    }

    /// <summary>
    /// Max attempts for the entry-directory deletion. A concurrent ListAsync enumeration can hold
    /// a transient meta.json handle (now opened with FileShare.Delete), and other transient holders
    /// (antivirus, indexers, git) may briefly lock files. Readers opened with FileShare.Delete let
    /// the per-file DeleteFile succeed, but RemoveDirectory can still race a not-yet-closed handle,
    /// so we retry with a short backoff before surfacing the failure.
    /// </summary>
    private const int DeleteMaxAttempts = 5;
    private const int DeleteRetryDelayMs = 100;

    public async Task DeleteEntryStorageAsync(VaultEntry entry, CancellationToken ct = default)
    {
        if (!Directory.Exists(entry.EntryPath))
            return;

        // Delete .git directory first (may have read-only files)
        var gitDir = Path.Combine(entry.VaultPath, ".git");
        if (Directory.Exists(gitDir))
        {
            SetAttributesNormal(new DirectoryInfo(gitDir));
        }

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                Directory.Delete(entry.EntryPath, recursive: true);
                LogDeletedStorage(_logger, entry.Id);
                return;
            }
            catch (Exception ex) when ((ex is IOException or UnauthorizedAccessException) && attempt < DeleteMaxAttempts)
            {
                LogStorageDeleteRetry(_logger, entry.Id, attempt, ex.Message);
                await Task.Delay(DeleteRetryDelayMs, ct);
            }
        }
    }

    public Task<long> GetStorageSizeAsync(VaultEntry entry, CancellationToken ct = default)
    {
        if (!Directory.Exists(entry.EntryPath))
            return Task.FromResult(0L);

        var size = new DirectoryInfo(entry.EntryPath)
            .EnumerateFiles("*", SearchOption.AllDirectories)
            .Sum(f => f.Length);

        return Task.FromResult(size);
    }

    public bool EntryStorageExists(VaultEntry entry)
    {
        return Directory.Exists(entry.EntryPath);
    }

    public IEnumerable<string> ListEntryDirectories()
    {
        if (!Directory.Exists(_basePath))
            yield break;

        foreach (var dir in Directory.GetDirectories(_basePath))
        {
            var dirName = Path.GetFileName(dir);
            // Only return directories that look like filepath hashes (16 hex chars)
            if (FilepathHasher.IsValidHash(dirName))
            {
                yield return dir;
            }
        }
    }

    private static void SetAttributesNormal(DirectoryInfo dir)
    {
        foreach (var subDir in dir.GetDirectories())
        {
            SetAttributesNormal(subDir);
        }

        foreach (var file in dir.GetFiles())
        {
            file.Attributes = FileAttributes.Normal;
        }
    }

    private static string GetExtensionFromContentType(string contentType) => contentType switch
    {
        "image/png" => ".png",
        "image/jpeg" => ".jpg",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "image/bmp" => ".bmp",
        "image/svg+xml" => ".svg",
        _ => ".bin"
    };

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Debug, Message = "Initialized entry storage at {EntryPath}")]
    private static partial void LogInitializedEntry(ILogger logger, string entryPath);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored extracted content for entry {EntryId}")]
    private static partial void LogStoredExtracted(ILogger logger, Guid entryId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored refined content for entry {EntryId}")]
    private static partial void LogStoredRefined(ILogger logger, Guid entryId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored {Count} images for entry {EntryId}")]
    private static partial void LogStoredImages(ILogger logger, int count, Guid entryId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored description for image {ImageId} of entry {EntryId}")]
    private static partial void LogStoredImageDescription(ILogger logger, string imageId, Guid entryId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Recorded enrichment failure for image {ImageId} of entry {EntryId}: attempt {AttemptCount}, permanent={IsPermanent}")]
    private static partial void LogStoredImageEnrichmentFailure(ILogger logger, string imageId, Guid entryId, int attemptCount, bool isPermanent);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored append-text for entry {EntryId}")]
    private static partial void LogStoredAppendText(ILogger logger, Guid entryId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stored QA content for entry {EntryId}")]
    private static partial void LogStoredQa(ILogger logger, Guid entryId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Deleted storage for entry {EntryId}")]
    private static partial void LogDeletedStorage(ILogger logger, Guid entryId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Storage delete for entry {EntryId} hit a transient lock (attempt {Attempt}), retrying: {Error}")]
    private static partial void LogStorageDeleteRetry(ILogger logger, Guid entryId, int attempt, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Image manifest for entry {EntryId} at {ManifestPath} is unreadable and is being rebuilt; descriptions it held are not carried forward: {Error}")]
    private static partial void LogRebuildingUnreadableManifest(ILogger logger, Guid entryId, string manifestPath, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Git initialization failed for {EntryPath}: {Error}. Vault will operate without version tracking.")]
    private static partial void LogGitInitFailed(ILogger logger, string entryPath, string error);

    #endregion

    private sealed class ImageManifestEntry
    {
        public string Id { get; init; } = string.Empty;
        public string FileName { get; init; } = string.Empty;
        public string ContentType { get; init; } = string.Empty;
        // Settable: enrichment fills this in per image, long after the manifest was written.
        public string? Description { get; set; }
        public string? AltText { get; init; }
        public int Width { get; init; }
        public int Height { get; init; }
        public long Size { get; init; }
        // Settable: recorded per attempt, cleared once Description is finally set.
        public EnrichmentFailure? LastEnrichmentFailure { get; set; }
    }
}
