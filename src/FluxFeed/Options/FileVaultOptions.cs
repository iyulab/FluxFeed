namespace FluxFeed.Options;

/// <summary>
/// Configuration options for FileVault.
/// </summary>
public sealed class FileVaultOptions
{
    /// <summary>
    /// Configuration section name.
    /// </summary>
    public const string SectionName = "FileVault";

    /// <summary>
    /// Default vault directory name (hidden folder marker).
    /// </summary>
    public const string DefaultVaultDirectoryName = ".vault";

    /// <summary>
    /// Vault directory name (hidden folder marker).
    /// Defaults to ".vault".
    /// </summary>
    public string VaultDirectoryName { get; set; } = DefaultVaultDirectoryName;

    /// <summary>
    /// Base path for vault data.
    /// If null, uses VaultDirectoryName relative to source file's directory.
    /// </summary>
    public string? VaultBasePath { get; set; }

    /// <summary>
    /// Identifier of the tenant/vault this instance serves. Set by <c>IVaultFactory</c> to the
    /// tenant id when creating a tenant-scoped vault. When set, memorized chunks are tagged with a
    /// <c>vault_id</c> metadata field so a multi-tenant consumer can bulk-purge one vault's vectors
    /// from the shared vector store via a single filtered delete (see <c>IVault.PurgeAsync</c>).
    /// Null for a single, non-tenant-scoped vault.
    /// </summary>
    public string? VaultId { get; set; }

    /// <summary>
    /// Maximum file size in megabytes to process.
    /// Larger files will be skipped.
    /// </summary>
    public int MaxFileSizeMB { get; set; } = 100;

    /// <summary>
    /// Enable real-time file watching.
    /// </summary>
    public bool EnableRealTimeWatch { get; set; } = true;

    /// <summary>
    /// Debounce delay in milliseconds for file change events.
    /// Multiple rapid changes within this window are merged into one event.
    /// </summary>
    public int DebounceDelayMs { get; set; } = 500;

    /// <summary>
    /// Internal buffer size for FileSystemWatcher in bytes.
    /// Larger buffers reduce the chance of missing events but use more memory.
    /// </summary>
    public int WatcherBufferSize { get; set; } = 65536;

    /// <summary>
    /// Number of versions to retain for each file.
    /// </summary>
    public int VersionRetentionCount { get; set; } = 5;

    /// <summary>
    /// Automatically cleanup orphaned files during sync.
    /// </summary>
    public bool AutoCleanupOrphans { get; set; }

    /// <summary>
    /// Default glob patterns for files to include.
    /// </summary>
    /// <remarks>
    /// Effective on the <b>discovery</b> paths only: <c>ScanFolderAsync</c>,
    /// <c>SyncAsync</c>, and folder-watcher events (files outside the patterns are
    /// skipped before change detection/queuing). Explicit single-file commands
    /// (<c>MemorizeAsync</c>/<c>RefreshAsync</c>) intentionally bypass patterns —
    /// an explicit call is an explicit intent, and silently skipping it would hide
    /// the caller's error. Per-folder patterns given to <c>AddWatchedFolderAsync</c>
    /// take precedence over these defaults. An empty list means "include all".
    /// </remarks>
    public List<string> DefaultIncludePatterns { get; set; } =
    [
        "*.pdf", "*.docx", "*.doc", "*.xlsx", "*.xls", "*.pptx", "*.ppt",
        "*.txt", "*.md", "*.rtf", "*.html", "*.htm",
        "*.json", "*.xml", "*.yaml", "*.yml", "*.csv"
    ];

    /// <summary>
    /// Default glob patterns for files to exclude.
    /// </summary>
    /// <remarks>
    /// Same effective scope as <see cref="DefaultIncludePatterns"/> (discovery paths
    /// only); exclusion takes precedence over inclusion. Defaults cover common
    /// system/temp artifacts (<c>Thumbs.db</c>, Office lock files <c>~$*</c>, etc.).
    /// </remarks>
    public List<string> DefaultExcludePatterns { get; set; } =
    [
        "~$*", "*.tmp", "*.temp", "*.bak", "*.swp",
        ".*", "Thumbs.db", "desktop.ini", ".DS_Store"
    ];

    /// <summary>
    /// Additional text file extensions for fallback extraction.
    /// These extensions are added to the built-in list when FileFlux is not available.
    /// Use lowercase with leading dot (e.g., ".myext").
    /// </summary>
    /// <remarks>
    /// Built-in extensions include common text, data, source code, and config formats.
    /// Add custom extensions here for domain-specific text files.
    /// </remarks>
    public HashSet<string> AdditionalTextExtensions { get; set; } = [];

    /// <summary>
    /// Default chunking options.
    /// </summary>
    public ChunkingDefaults Chunking { get; set; } = new();

    /// <summary>
    /// Opt-in contextual enrichment of text chunks at ingestion (Anthropic-style "contextual retrieval": a short,
    /// LLM-written summary of where the chunk sits in its document is prepended before embedding and keyword
    /// indexing). Off by default; needs both <see cref="ContextualEnrichmentDefaults.Enabled"/> and a registered
    /// <c>FluxIndex.Core.Application.Interfaces.IContextualEnrichmentService</c> (for example the FluxImprover-backed
    /// one from <c>FluxIndex.Integrations.FluxImprover</c>) — registering the service alone does nothing, so a container
    /// that has an enrichment service for other reasons never pays an LLM call per chunk by accident.
    /// </summary>
    public ContextualEnrichmentDefaults ContextualEnrichment { get; set; } = new();

    /// <summary>
    /// Consecutive failures an <c>IVaultImageEnricher</c> may accumulate for one image before the
    /// pipeline stops offering that image to it. The failure count is persisted in the image
    /// manifest, so it survives a process restart — unlike an in-memory counter, an image that has
    /// already exhausted this many attempts is not retried again just because the process restarted.
    /// A failed image below the ceiling is still retried on the next memorize/refresh, as before.
    /// </summary>
    public int MaxImageEnrichmentAttempts { get; set; } = 3;

    /// <summary>
    /// Maximum file size in bytes.
    /// </summary>
    public long MaxFileSizeBytes => MaxFileSizeMB * 1024L * 1024L;

    // === Queue Processing Options ===

    /// <summary>
    /// Maximum concurrent file processing operations.
    /// </summary>
    public int MaxConcurrentProcessing { get; set; } = 4;

    /// <summary>
    /// Polling interval in milliseconds when queue is empty.
    /// </summary>
    public int QueuePollingIntervalMs { get; set; } = 1000;

    /// <summary>
    /// Enable automatic retry on failure.
    /// </summary>
    public bool EnableAutoRetry { get; set; } = true;

    /// <summary>
    /// Maximum retry attempts for failed items.
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// Delay in milliseconds before retrying a failed item.
    /// </summary>
    public int RetryDelayMs { get; set; } = 5000;

    /// <summary>
    /// Enable background queue processing.
    /// </summary>
    public bool EnableBackgroundProcessing { get; set; } = true;

    /// <summary>
    /// How long a caller that waits for a queued job (<c>MemorizeAsync(..., waitForCompletion: true)</c>)
    /// tolerates the absence of a queue worker before failing. The background worker is an
    /// <c>IHostedService</c> and only runs inside a Generic Host; without one a queued job can never
    /// complete, so the wait throws an <see cref="InvalidOperationException"/> that names the fix
    /// instead of hanging forever. Long enough to cover a host that is still starting its hosted
    /// services; raise it if your host starts many services before the vault worker.
    /// </summary>
    public TimeSpan WorkerStartupTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The git executable used for vault history (<c>git</c> on PATH by default). Set an explicit
    /// path when git is installed but not on the process PATH.
    /// </summary>
    public string GitExecutablePath { get; set; } = "git";

    /// <summary>
    /// Whether to keep working without git. Vault history (<c>DiffAsync</c>, <c>LogAsync</c>,
    /// <c>GetContentAtCommitAsync</c>) needs the git CLI; when it cannot be started the vault
    /// fails fast with an <see cref="InvalidOperationException"/> that names this option. Set to
    /// <c>true</c> to accept a history-less vault instead (a warning is logged once).
    /// </summary>
    public bool AllowMissingGit { get; set; }
}

/// <summary>
/// Default chunking configuration.
/// </summary>
public sealed class ChunkingDefaults
{
    /// <summary>
    /// Maximum chunk size in tokens.
    /// </summary>
    public int MaxChunkSize { get; set; } = 1024;

    /// <summary>
    /// Overlap size between chunks in tokens.
    /// </summary>
    public int OverlapSize { get; set; } = 128;

    /// <summary>
    /// Default chunking strategy.
    /// </summary>
    public string Strategy { get; set; } = "Intelligent";

    /// <summary>
    /// Default language for chunking (null = auto-detect).
    /// </summary>
    public string? Language { get; set; }

    /// <summary>
    /// Per-format strategy overrides keyed by file extension (e.g., ".pdf", ".md").
    /// When set, the specified strategy is used for files matching the extension,
    /// overriding the global Strategy setting.
    /// </summary>
    public Dictionary<string, string> FormatStrategies { get; set; } = [];
}

/// <summary>
/// Settings for the opt-in contextual enrichment step (see <see cref="FileVaultOptions.ContextualEnrichment"/>).
/// </summary>
public sealed class ContextualEnrichmentDefaults
{
    /// <summary>Whether text chunks are enriched at all. Default false — every enrichment costs one LLM call per chunk.</summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// When true (default), a failed enrichment call logs a warning and the document is indexed with its plain chunks,
    /// each tagged <c>enrichment=failed</c>. When false, the failure propagates and the memorize fails.
    /// </summary>
    public bool ContinueOnError { get; set; } = true;
}
