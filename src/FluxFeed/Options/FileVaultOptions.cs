using FileFlux.Core;

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
    /// Base path for vault data (entry directories and the queue database).
    /// If null, one vault is used at <c>&lt;working directory&gt;/</c><see cref="VaultDirectoryName"/>
    /// (<c>.vault</c> by default) — resolved against the process working directory, not next to each
    /// source file. Services and hosts should set this explicitly, since their working directory is
    /// often not where the data should live.
    /// </summary>
    /// <remarks>
    /// With <c>IVaultFactory</c>, each tenant's vault is <c>&lt;base&gt;/&lt;tenantId&gt;/</c><see cref="VaultDirectoryName"/>,
    /// where <c>&lt;base&gt;</c> is this value or the same working-directory default.
    /// </remarks>
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
    /// Maximum source file size in megabytes. A folder scan skips larger files (counted as skipped), and memorizing
    /// one fails permanently with the size in the message instead of extracting it. Zero or less means no limit.
    /// </summary>
    public int MaxFileSizeMB { get; set; } = 100;

    /// <summary>
    /// Watch added folders for changes as they happen. When false, a watched folder is still registered (scans and sync
    /// use it) but no file-system watcher runs for it, so changes are picked up only by the next scan. Default: true.
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
    /// Additional file extensions read as plain text, on top of the built-in document and code extensions. They are used
    /// when no <c>IExtractor</c> handles the file (the plain-text fallback) and in the set of indexable extensions.
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
    /// Whether an entry whose source is unchanged is re-extracted because the current extractor is newer than the one
    /// that made its extraction (<see cref="FluxFeed.Domain.Entities.VaultEntry.ExtractedBy"/>). (Default:
    /// <see cref="ReextractionPolicy.Never"/> — re-extraction re-runs OCR and image description.)
    /// </summary>
    /// <remarks>
    /// When set, a refresh (<c>RefreshAsync</c>, a queued refresh job) of an outdated entry re-extracts — as
    /// <c>MemorizeAsync</c> does — unless the entry's vault has uncommitted edits, and change detection
    /// (<c>DetectChangesAsync</c>, <c>SyncAsync</c>) recommends Memorize for it. Re-extraction rewrites
    /// <c>refined.md</c> as a source change would. Entries extracted before identities were recorded count as
    /// outdated. <c>VaultStatus.OutdatedExtractionCount</c> counts them under this policy. To re-extract one file
    /// regardless of policy, call <c>MemorizeAsync</c> for it.
    /// </remarks>
    public ReextractionPolicy Reextraction { get; set; } = ReextractionPolicy.Never;

    /// <summary>
    /// Consecutive failures an <c>IVaultImageEnricher</c> may accumulate for one image before the
    /// pipeline stops offering that image to it. The failure count is persisted in the image
    /// manifest, so it survives a process restart — unlike an in-memory counter, an image that has
    /// already exhausted this many attempts is not retried again just because the process restarted.
    /// A failed image below the ceiling is still retried on the next memorize/refresh, as before.
    /// </summary>
    public int MaxImageEnrichmentAttempts { get; set; } = 3;

    /// <summary>
    /// The image content types the registered <see cref="Interfaces.IVaultImageEnricher"/> can read (for a vision model,
    /// typically <c>image/png</c>, <c>image/jpeg</c>, <c>image/webp</c>). An image of any other type is not offered to it:
    /// it is recorded at once as a permanent failure with the reason <c>unsupported_content_type:&lt;type&gt;</c>, so it
    /// can be counted apart from enricher failures and no call is spent on it. Null (the default) offers every image.
    /// </summary>
    public IReadOnlyList<string>? ImageEnrichmentContentTypes { get; set; }

    /// <summary>
    /// How the extractor's LLM refinement runs, when an <c>ILlmRefiner</c> is registered (without one there is no LLM
    /// refinement and this is not read). Null uses FileFlux's defaults: one whole-document pass, after which the entry has no
    /// page spans. <c>Scope = LlmRefineScope.Pages</c> refines page by page, checks each output against its page and keeps
    /// the spans; the per-page outcomes arrive as extraction hints (<c>llm_refine_pages_*</c>).
    /// </summary>
    public LlmRefineOptions? LlmRefine { get; set; }

    /// <summary>
    /// Index a file on its native chunks first and leave the slow LLM stages — image descriptions, contextual enrichment
    /// — for a later <c>IVault.UpgradeAsync</c>. With it on, memorize and refresh make the entry searchable without
    /// calling the image enricher or generating contexts (a context already stored for an unchanged passage of an
    /// unchanged document is reused), and record what is left in <see cref="Domain.Entities.VaultEntry.PendingEnrichment"/>;
    /// <c>IVault.GetPendingEnrichmentAsync</c> lists those entries. When to upgrade is the host's call. Default false:
    /// every stage runs inline, as before.
    /// </summary>
    public bool DeferEnrichment { get; set; }

    /// <summary>
    /// <see cref="MaxFileSizeMB"/> in bytes.
    /// </summary>
    public long MaxFileSizeBytes => MaxFileSizeMB * 1024L * 1024L;

    // === Queue Processing Options ===

    /// <summary>
    /// Maximum concurrent file processing operations.
    /// </summary>
    public int MaxConcurrentProcessing { get; set; } = 4;

    /// <summary>
    /// Enable automatic retry on failure.
    /// </summary>
    public bool EnableAutoRetry { get; set; } = true;

    /// <summary>
    /// How many jobs sharing a <c>groupKey</c> may be processing at once. Default: 1.
    /// Zero or less disables the cap entirely.
    /// </summary>
    /// <remarks>
    /// <para>
    /// One queue shared across tenants is the default registration, and without a cap one owner's
    /// backlog occupies the whole worker pool: every other owner waits behind it in arrival order,
    /// however small their work is.
    /// </para>
    /// <para>
    /// The cap is <b>work-conserving</b> - it binds only while some other group has work queued. A
    /// group alone on the queue may exceed its share and use the full
    /// <see cref="MaxConcurrentProcessing"/>, so a single-tenant deployment pays nothing for having
    /// this on. That is also why the default is the strictest useful value rather than off.
    /// </para>
    /// <para>
    /// Jobs enqueued without a group key are never capped, so this setting does nothing until a
    /// caller starts passing one.
    /// </para>
    /// </remarks>
    public int MaxInFlightPerGroup { get; set; } = 1;

    /// <summary>
    /// Fairness group this vault's jobs belong to on a shared queue. Null (the default) means
    /// ungrouped, and an ungrouped job is never capped.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A group is a property of the vault, not of each call: one vault is one owner, so the value is
    /// set once here and every enqueue path inherits it. Requiring it per call would mean repeating
    /// the same value at every call site and would reintroduce the failure mode that made priority
    /// unreachable - one path quietly not passing it.
    /// </para>
    /// <para>
    /// Falls back to <see cref="VaultId"/> when unset, so a multi-tenant deployment built on
    /// <c>VaultFactory</c> - which stamps the tenant id there - gets a fair share of a shared queue
    /// without wiring anything. That is the vault's own identity rather than something inferred from
    /// a path: what counts as an owner stays the caller's concept, and setting this explicitly
    /// overrides the fallback (to group several vaults together, for instance).
    /// </para>
    /// <para>
    /// The queue-level <c>IVaultQueueService.Enqueue*</c> methods still take the group per call,
    /// because one queue serves many vaults.
    /// </para>
    /// </remarks>
    public string? QueueGroupKey { get; set; }

    /// <summary>
    /// The group key this vault's jobs are actually enqueued with: <see cref="QueueGroupKey"/> when
    /// set, otherwise <see cref="VaultId"/>.
    /// </summary>
    public string? EffectiveQueueGroupKey =>
        string.IsNullOrEmpty(QueueGroupKey) ? VaultId : QueueGroupKey;

    /// <summary>
    /// How many times a failed job is retried (with <see cref="EnableAutoRetry"/>) before it stays failed. Jobs take the
    /// value of the options the queue service is built with, when they are enqueued. Default: 3.
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
    /// A call that succeeds with a blank context is not a failure under either setting: that chunk is indexed as it was,
    /// tagged <c>enrichment=empty</c>, and a warning is logged.
    /// </summary>
    public bool ContinueOnError { get; set; } = true;
}
