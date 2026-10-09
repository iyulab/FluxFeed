using FluxIndex.Core.Application.Services;
using System.Collections.Concurrent;
using System.Diagnostics;
using FluxGuard.Remote.RAG;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxFeed.Adapters;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Domain.Exceptions;
using FluxFeed.Domain.ValueObjects;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace FluxFeed.Services;

/// <summary>
/// Pipeline service for processing vault entries.
/// Simplified flow: Source → Extracted → Memorized (chunks stored in DB only).
/// </summary>
public sealed partial class VaultPipeline : IVaultPipeline
{
    /// <summary>
    /// Document file extensions suitable for vector embedding (Memorize).
    /// These formats contain natural language text that benefits from semantic search.
    /// </summary>
    /// <remarks>
    /// For code files, use file-read instead of Memorize.
    /// Code requires AST-based chunking for effective RAG, which is not yet supported.
    /// </remarks>
    public static readonly HashSet<string> DocumentExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        // Text and documentation
        ".txt", ".md", ".markdown", ".rst", ".rtf", ".log",
        // Web content (natural language)
        ".html", ".htm",
        // Data formats (structured but searchable)
        ".json", ".xml", ".yaml", ".yml", ".csv", ".tsv",
        // Markup and templates
        ".tex", ".bib"
    };

    /// <summary>
    /// Source code and config file extensions that can be read but are NOT recommended for Memorize.
    /// These files should be accessed via file-read when needed, not vector embedding.
    /// </summary>
    /// <remarks>
    /// Reason: Standard text chunking breaks code semantics (functions split across chunks).
    /// Effective code RAG requires AST-based chunking (Tree-sitter) and code-specific embeddings.
    /// See: https://blog.lancedb.com/rag-codebase-1/
    /// </remarks>
    public static readonly HashSet<string> CodeExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        // Source code - C family
        ".cs", ".c", ".cpp", ".cc", ".cxx", ".h", ".hpp", ".hxx", ".m", ".mm",
        // Source code - JVM
        ".java", ".kt", ".kts", ".scala", ".groovy", ".gradle",
        // Source code - Web
        ".js", ".ts", ".jsx", ".tsx", ".mjs", ".cjs", ".vue", ".svelte",
        ".css", ".scss", ".sass", ".less",
        // Source code - Scripting
        ".py", ".pyw", ".rb", ".php", ".pl", ".pm", ".lua", ".r", ".jl",
        // Source code - Systems
        ".go", ".rs", ".swift", ".dart", ".zig", ".nim", ".v", ".odin",
        // Source code - Functional
        ".hs", ".fs", ".fsx", ".ml", ".mli", ".clj", ".cljs", ".ex", ".exs", ".erl", ".elm",
        // Shell and scripts
        ".sh", ".bash", ".zsh", ".fish", ".ps1", ".psm1", ".bat", ".cmd",
        // Build and config
        ".makefile", ".dockerfile", ".cmake", ".meson", ".ninja",
        ".toml", ".ini", ".cfg", ".conf",
        ".editorconfig", ".gitignore", ".gitattributes", ".dockerignore",
        // SQL and query
        ".sql", ".graphql", ".gql",
        // Schema and protocol
        ".proto", ".thrift", ".avsc", ".fbs",
        // Templates
        ".sty", ".cls", ".njk", ".ejs", ".hbs", ".mustache", ".liquid", ".pug", ".jade"
    };

    private readonly IGitService _git;
    private readonly IContentHasher _hasher;
    private readonly IVaultStorageService _storage;
    private readonly ILogger<VaultPipeline> _logger;
    private readonly FileVaultOptions _options;

    // Integration services (optional)
    private readonly IExtractor? _extractor;
    private readonly IChunker? _chunker;
    private readonly IVectorStore? _vectorStore;
    private readonly IEmbeddingService? _embeddingService;
    private readonly IHybridSearchService? _hybridSearch;

    // Which keyword index hybrid fuses with the vector leg, decided once from what is wired in. When a keyword
    // service is registered this pipeline writes it at ingestion, so hybrid fuses over that same index (through
    // _hybridSearch, which is the registered service or the stock one built over the same parts) — otherwise a
    // registered text analyzer and keyword fields would reach the keyword strategy only, while hybrid ran over a
    // second index with its own tokenizer (the store's native FTS).
    private readonly HybridKeywordLeg _hybridKeywordLeg;

    // Logs for the stock HybridSearchService built above when no hybrid service is registered.
    private readonly ILoggerFactory? _loggerFactory;

    // One log line per distinct leg per process: the pipeline is scoped, so a per-instance line would repeat on
    // every request.
    private static readonly ConcurrentDictionary<string, byte> LoggedHybridLegs = new(StringComparer.Ordinal);
    private readonly IGraphRAGService? _graphRAGService;
    private readonly IKeywordSearchService? _keywordSearchService;
    private readonly IVaultImageEnricher? _imageEnricher;

    // Opt-in RAG poisoning/indirect-injection guard (FluxGuard.Remote), applied at ingestion —
    // before a poisoned chunk is ever embedded and stored, not just at retrieval time. Null by
    // default — nothing changes for consumers who don't supply one.
    private readonly IRAGSecurityPipeline? _ragSecurityPipeline;
    private readonly IContextualEnrichmentService? _contextualEnrichment;

    /// <summary>
    /// Value of the <c>chunk_kind</c> metadata tag on a chunk that holds an image description
    /// rather than document text.
    /// </summary>
    public const string ImageDescriptionChunkKind = "image_description";

    /// <summary>
    /// <c>chunk_kind</c> of a chunk holding table rows. With it: <see cref="TableIdMetadataKey"/> (when the chunk's
    /// table could be tied to the entry's stored tables), <c>table_piece</c>/<c>table_pieces</c>,
    /// <c>table_row_start</c>/<c>table_row_end</c>, and from the stored table <c>table_columns</c>, <c>table_section</c>,
    /// <c>table_caption</c>, <c>table_page</c>.
    /// </summary>
    public const string TableChunkKind = "table";

    /// <summary>Metadata key holding the <see cref="TableArtifact.Id"/> of the table a chunk's rows belong to.</summary>
    public const string TableIdMetadataKey = "table_id";

    /// <summary>
    /// Whether a GraphRAG service is wired into this pipeline. When false, a memorize call with
    /// <see cref="MemorizeOptions.EnableGraphRAG"/> == true will throw.
    /// </summary>
    public bool SupportsGraphRAG => _graphRAGService != null;

    /// <summary>
    /// Whether a keyword search service is wired into this pipeline. When true, every chunk this
    /// pipeline writes to the vector store is also written to the keyword index, keeping the two
    /// backends in sync for hybrid retrieval.
    /// </summary>
    public bool SupportsKeywordIndex => _keywordSearchService != null;

    /// <inheritdoc />
    public HybridKeywordLeg HybridKeywordLeg => _hybridKeywordLeg;

    /// <summary>
    /// Whether contextual enrichment will run for text chunks: a <see cref="IContextualEnrichmentService"/> (FluxIndex.Core port) is wired in
    /// <em>and</em> <see cref="FileVaultOptions.ContextualEnrichment"/> is enabled. Either one alone is a no-op.
    /// </summary>
    public bool SupportsContextualEnrichment => _contextualEnrichment != null && _options.ContextualEnrichment.Enabled;

    public VaultPipeline(
        IGitService git,
        IContentHasher hasher,
        IVaultStorageService storage,
        ILogger<VaultPipeline> logger,
        IOptions<FileVaultOptions>? options = null,
        IExtractor? extractor = null,
        IChunker? chunker = null,
        IVectorStore? vectorStore = null,
        IEmbeddingService? embeddingService = null,
        IHybridSearchService? hybridSearch = null,
        IGraphRAGService? graphRAGService = null,
        IKeywordSearchService? keywordSearchService = null,
        IVaultImageEnricher? imageEnricher = null,
        IRAGSecurityPipeline? ragSecurityPipeline = null,
        IContextualEnrichmentService? contextualEnrichment = null,
        ILoggerFactory? loggerFactory = null)
    {
        _git = git ?? throw new ArgumentNullException(nameof(git));
        _hasher = hasher ?? throw new ArgumentNullException(nameof(hasher));
        _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options = options?.Value ?? new FileVaultOptions();
        _extractor = extractor;
        _chunker = chunker;
        _vectorStore = vectorStore;
        // A keyword-only placeholder (FluxIndex's NoEmbeddingService — what a FluxIndex context without an embedder
        // registers) is no embedding service: the vault behaves as it does with none, instead of failing here when the
        // placeholder refuses to name an identity.
        _embeddingService = NoEmbeddingService.IsKeywordOnly(embeddingService) ? null : embeddingService;
        // The store's physical layout may depend on the embedding identity; bind before the first
        // store access so consumers never have to call BindIdentity themselves.
        VectorStoreIdentityBinding.EnsureBound(_vectorStore, _embeddingService);
        _graphRAGService = graphRAGService;
        _keywordSearchService = keywordSearchService;
        _loggerFactory = loggerFactory;

        if (keywordSearchService != null && vectorStore != null && embeddingService != null)
        {
            _hybridSearch = hybridSearch ?? new HybridSearchService(
                vectorStore, keywordSearchService, embeddingService,
                (_loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<HybridSearchService>());
            _hybridKeywordLeg = HybridKeywordLeg.KeywordIndex;
        }
        else if (vectorStore is INativeHybridSearch)
        {
            // No keyword index of ours: the store's native hybrid fuses over the keyword rows it wrote itself,
            // while a separately registered hybrid service would search a keyword index nothing fills.
            _hybridSearch = hybridSearch;
            _hybridKeywordLeg = HybridKeywordLeg.Native;
        }
        else
        {
            _hybridSearch = hybridSearch;
            _hybridKeywordLeg = hybridSearch != null ? HybridKeywordLeg.KeywordIndex : HybridKeywordLeg.None;
        }

        var legDescription = _hybridKeywordLeg switch
        {
            HybridKeywordLeg.KeywordIndex => $"keyword index ({(keywordSearchService ?? (object?)_hybridSearch)!.GetType().Name})",
            HybridKeywordLeg.Native => $"native ({vectorStore!.GetType().Name})",
            _ => "none",
        };
        if (LoggedHybridLegs.TryAdd(legDescription, 0))
            LogHybridKeywordLeg(_logger, legDescription);
        _imageEnricher = imageEnricher;
        _ragSecurityPipeline = ragSecurityPipeline;
        _contextualEnrichment = contextualEnrichment;
    }

    public async Task<MemorizeResult> MemorizeAsync(VaultEntry entry, MemorizeOptions? options = null, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        options ??= VaultDefaults();

        try
        {
            LogStartingMemorize(_logger, entry.SourcePath);

            // FileVaultOptions.MaxFileSizeMB: a file over the limit is not processed. Thrown as InvalidDataException so
            // the result classifies as a permanent failure — a retry reads the same size.
            if (_options.MaxFileSizeMB > 0 && File.Exists(entry.SourcePath)
                && new FileInfo(entry.SourcePath).Length is var sourceBytes && sourceBytes > _options.MaxFileSizeBytes)
            {
                throw new InvalidDataException(
                    $"Source file is {sourceBytes / (1024d * 1024d):F1} MB, over FileVaultOptions.MaxFileSizeMB ({_options.MaxFileSizeMB} MB); it was not processed.");
            }

            // Step 1: Backup user content if preserving (for re-memorize scenarios)
            string? existingQaContent = null;
            string? existingAppendText = null;

            if (_storage.EntryStorageExists(entry))
            {
                var vaultContent = await _storage.GetAllVaultContentAsync(entry, ct);

                if (options.PreserveQaContent && !string.IsNullOrWhiteSpace(vaultContent.QaContent))
                {
                    existingQaContent = vaultContent.QaContent;
                    LogBackupQaContent(_logger, existingQaContent.Length);
                }

                if (options.PreserveAppendText && !string.IsNullOrWhiteSpace(vaultContent.AppendText))
                {
                    existingAppendText = vaultContent.AppendText;
                    LogBackupAppendText(_logger, existingAppendText.Length);
                }
            }

            // Step 2: Initialize entry storage if needed
            if (!_storage.EntryStorageExists(entry))
            {
                await _storage.InitializeEntryAsync(entry, ct);
            }

            // Step 3: Extract content from source file → extracted.md
            await ExtractAsync(entry, ct);

            // Step 3.5: Describe extracted images (no-op without a registered enricher). With DeferEnrichment the
            // enricher is not called here: the images wait for an upgrade, and the entry records that they do.
            var extractedContent = await _storage.GetExtractedContentAsync(entry, ct);
            var defer = _options.DeferEnrichment;
            var describedImages = defer
                ? (await _storage.GetImageManifestAsync(entry, ct)).Count(i => i.IsDescribed)
                : await EnrichImagesAsync(entry, extractedContent, ct);
            var imagesPending = defer && await HasPendingImagesAsync(entry, ct);
            var extractionPending = (entry.PendingEnrichment & ExtractionStages) != EnrichmentStages.None;

            // Step 3.6: Nothing indexable — no text and no described image. An image-only document
            // whose images were described is NOT empty: the descriptions are its content. Neither is one whose images
            // are waiting for a deferred description, or whose pages wait for a deferred read (a scan has no text until
            // it is read): it goes on to refine, so an upgrade has refined.md to work from.
            if (string.IsNullOrWhiteSpace(extractedContent) && describedImages == 0 && !imagesPending && !extractionPending)
            {
                LogNoContentToIndex(_logger, entry.SourcePath);

                // This path returns before ChunkAndIndexAsync, so it carries the same
                // replace-on-index obligation itself: the entry is about to report 0 chunks, and
                // rows left behind from an earlier non-empty memorize would keep answering
                // searches for content the document no longer has.
                await RemoveAsync(entry, ct);

                string? emptyCommitHash = null;
                if (!options.SkipCommit)
                {
                    emptyCommitHash = await _git.CommitAsync(entry.VaultPath, "memorize: empty content (0 chunks)", ct);
                }

                MarkMemorizedWithIdentity(entry, 0);
                entry.SetPendingEnrichment(EnrichmentStages.None);
                entry.MarkInSync();
                entry.SaveMetadata();

                sw.Stop();
                LogMemorizeCompleted(_logger, entry.SourcePath, 0, sw.Elapsed.TotalSeconds);
                return MemorizeResult.Succeeded(0, 0, sw.Elapsed, emptyCommitHash);
            }

            // Step 4: Refine content → vault/refined.md
            await RefineAsync(entry, ct);

            // Step 5: Restore preserved user content
            if (!string.IsNullOrWhiteSpace(existingQaContent))
            {
                await _storage.StoreQaContentAsync(entry, existingQaContent, ct);
                LogRestoredQaContent(_logger);
            }

            if (!string.IsNullOrWhiteSpace(existingAppendText))
            {
                await _storage.StoreAppendTextAsync(entry, existingAppendText, ct);
                LogRestoredAppendText(_logger);
            }

            // Step 6: Chunk and index (shared with RefreshAsync)
            var result = await ChunkAndIndexAsync(entry, options, defer ? ContextGeneration.ReuseOnly : ContextGeneration.Generate, ct);

            // Step 7: Git commit
            string? commitHash = null;
            if (!options.SkipCommit)
            {
                var message = options.CommitMessage ?? $"memorize: {result.ChunkCount} chunks indexed";
                commitHash = await _git.CommitAsync(entry.VaultPath, message, ct);
            }

            // Step 8: Update entry state
            MarkMemorizedWithIdentity(entry, result.ChunkCount);
            entry.SetPendingEnrichment(PendingStages(entry, imagesPending, result.ContextPending));
            entry.MarkInSync(); // Set sync status to InSync after successful memorize
            entry.SaveMetadata();

            sw.Stop();
            LogMemorizeCompleted(_logger, entry.SourcePath, result.ChunkCount, sw.Elapsed.TotalSeconds);

            return MemorizeResult.Succeeded(result.ChunkCount, result.ContentLength, sw.Elapsed, commitHash);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // A cancelled run is not a failed document: the entry keeps its state and the job is recovered.
            sw.Stop();
            LogMemorizeFailed(_logger, ex, entry.SourcePath);
            entry.MarkError(ex.Message);
            entry.SaveMetadata();
            return ex is IndexingFailedException indexingFailure
                ? MemorizeResult.Failed(indexingFailure.InnerException?.Message ?? ex.Message,
                                        sw.Elapsed, indexingFailure.Failure)
                : MemorizeResult.Failed(ex.Message, sw.Elapsed, ex.GetType().Name);
        }
    }

    /// <summary>
    /// FluxFeed's revision of what it derives from an extraction (the content spans file, the image manifest's
    /// fields). Raise it when that changes, so entries extracted before the change read as outdated
    /// (<see cref="ExtractionIdentity.PipelineRevision"/>). 1: spans + image page numbers (0.42.0). 2: the tables file
    /// (extracted.tables.json, 0.45.0).
    /// </summary>
    internal const int ExtractionPipelineRevision = 2;

    /// <inheritdoc/>
    public ExtractionIdentity? CurrentExtractionIdentity =>
        (_extractor is null ? FallbackExtractionIdentity : _extractor.Identity) is { } identity
            ? identity with { PipelineRevision = ExtractionPipelineRevision }
            : null;

    /// <summary>The identity of the built-in plain-text read used when no extractor is registered.</summary>
    private static readonly ExtractionIdentity FallbackExtractionIdentity =
        ExtractionIdentity.FromAssembly("FluxFeed.PlainText", typeof(VaultPipeline).Assembly);

    public async Task<MemorizeResult> RefreshAsync(VaultEntry entry, MemorizeOptions? options = null, CancellationToken ct = default)
    {
        // FileVaultOptions.Reextraction: an extraction an older extractor made is redone here — the refresh a
        // consumer already schedules is where a newer extractor's output reaches an unchanged source. Not when the
        // vault has uncommitted edits: re-extraction rewrites refined.md, and a refresh exists to index those edits.
        if (CurrentExtractionIdentity is { } current
            && current.IsOutdatedBy(entry.ExtractedBy, _options.Reextraction)
            && File.Exists(entry.SourcePath))
        {
            if ((await _git.StatusAsync(entry.VaultPath, ct)).ModifiedFiles.Count == 0)
            {
                LogReextractingOutdated(_logger, entry.SourcePath, entry.ExtractedBy?.ToString() ?? "unrecorded", current.ToString());
                return await MemorizeAsync(entry, options ?? VaultDefaults($"re-extract: {current}"), ct);
            }

            LogOutdatedKeptForVaultEdits(_logger, entry.SourcePath);
        }

        var sw = Stopwatch.StartNew();
        options ??= VaultDefaults();

        try
        {
            LogStartingRefresh(_logger, entry.SourcePath);

            // Verify that extracted content exists
            if (!entry.RefinedExists)
            {
                throw new InvalidOperationException($"No refined content found at {entry.RefinedMdPath}. Run memorize first.");
            }

            // Retry any image that is still without a description. Already-described images cost
            // nothing here — the enricher is not called for them. With DeferEnrichment that is left to an upgrade.
            var defer = _options.DeferEnrichment;
            if (!defer)
            {
                await EnrichImagesAsync(entry, await _storage.GetExtractedContentAsync(entry, ct), ct);
            }

            var imagesPending = defer && await HasPendingImagesAsync(entry, ct);

            // Chunk and index vault content
            var result = await ChunkAndIndexAsync(entry, options, defer ? ContextGeneration.ReuseOnly : ContextGeneration.Generate, ct);

            // Git commit
            string? commitHash = null;
            if (!options.SkipCommit)
            {
                var message = options.CommitMessage ?? $"refresh: {result.ChunkCount} chunks re-indexed";
                commitHash = await _git.CommitAsync(entry.VaultPath, message, ct);
            }

            // Update entry state
            MarkMemorizedWithIdentity(entry, result.ChunkCount);
            entry.SetPendingEnrichment(PendingStages(entry, imagesPending, result.ContextPending));
            entry.MarkInSync(); // Set sync status to InSync after successful refresh
            entry.SaveMetadata();

            sw.Stop();
            LogRefreshCompleted(_logger, entry.SourcePath, result.ChunkCount, sw.Elapsed.TotalSeconds);

            return MemorizeResult.Succeeded(result.ChunkCount, result.ContentLength, sw.Elapsed, commitHash);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // A cancelled run is not a failed document: the entry keeps its state and the job is recovered.
            sw.Stop();
            LogRefreshFailed(_logger, ex, entry.SourcePath);
            entry.MarkError(ex.Message);
            entry.SaveMetadata();
            return ex is IndexingFailedException indexingFailure
                ? MemorizeResult.Failed(indexingFailure.InnerException?.Message ?? ex.Message,
                                        sw.Elapsed, indexingFailure.Failure)
                : MemorizeResult.Failed(ex.Message, sw.Elapsed, ex.GetType().Name);
        }
    }

    /// <inheritdoc/>
    public async Task<UpgradeSummary> UpgradeAsync(VaultEntry entry, MemorizeOptions? options = null, CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        options ??= VaultDefaults();

        try
        {
            LogStartingUpgrade(_logger, entry.SourcePath, entry.PendingEnrichment.ToString());

            // An upgrade works from what memorize already produced; it never re-extracts.
            if (!entry.RefinedExists)
            {
                throw new InvalidOperationException($"No refined content found at {entry.RefinedMdPath}. Run memorize first.");
            }

            // Stages that belong to extraction (page reads, LLM refinement) need the document again: extract it with
            // them, then chunk and index as usual — only chunks whose text changed are re-embedded. Without the source
            // file they stay pending and the other stages still run.
            var reextracted = false;
            if ((entry.PendingEnrichment & ExtractionStages) != EnrichmentStages.None)
            {
                if (File.Exists(entry.SourcePath))
                {
                    await ExtractAsync(entry, deferExtractionStages: false, ct);
                    await RefineAsync(entry, ct);
                    reextracted = true;
                }
                else
                {
                    LogUpgradeSourceMissing(_logger, entry.SourcePath);
                }
            }

            await EnrichImagesAsync(entry, await _storage.GetExtractedContentAsync(entry, ct), ct);
            var result = await ChunkAndIndexAsync(entry, options, ContextGeneration.Generate, ct);

            string? commitHash = null;
            if (!options.SkipCommit)
            {
                var message = options.CommitMessage
                    ?? $"upgrade: {result.ChunkCount} chunks ({result.ReEmbedded} re-embedded, {result.Kept} unchanged{(reextracted ? ", re-extracted" : "")})";
                commitHash = await _git.CommitAsync(entry.VaultPath, message, ct);
            }

            MarkMemorizedWithIdentity(entry, result.ChunkCount);
            // Images whose description failed (not yet permanently) are still pending; contexts were generated.
            var stillPending = PendingStages(entry, await HasPendingImagesAsync(entry, ct), result.ContextPending);
            entry.SetPendingEnrichment(stillPending);
            entry.MarkInSync();
            entry.SaveMetadata();

            sw.Stop();
            LogUpgradeCompleted(_logger, entry.SourcePath, result.ChunkCount, result.ReEmbedded, result.Kept, sw.Elapsed.TotalSeconds);
            return new UpgradeSummary(result.ChunkCount, result.ReEmbedded, result.Kept, stillPending, commitHash);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
        {
            // Recorded on the entry, then thrown: the caller (or the queue worker, which classifies it) decides.
            LogUpgradeFailed(_logger, ex, entry.SourcePath);
            entry.MarkError(ex.Message);
            entry.SaveMetadata();
            throw;
        }
    }

    /// <summary>
    /// The options a call without its own uses: the vault's chunking settings (<see cref="FileVaultOptions.Chunking"/>), as
    /// the queue worker passes them. Before, a direct call (background processing off) chunked with the
    /// <see cref="MemorizeOptions"/> defaults instead, whatever the vault was configured with.
    /// </summary>
    private MemorizeOptions VaultDefaults(string? commitMessage = null) => new()
    {
        MaxChunkSize = _options.Chunking.MaxChunkSize,
        OverlapSize = _options.Chunking.OverlapSize,
        Strategy = _options.Chunking.Strategy,
        Language = _options.Chunking.Language,
        CommitMessage = commitMessage,
    };

    /// <summary>Whether the entry has images an upgrade would still offer to a registered image enricher.</summary>
    private async Task<bool> HasPendingImagesAsync(VaultEntry entry, CancellationToken ct) =>
        _imageEnricher != null
        && (await _storage.GetImageManifestAsync(entry, ct)).Any(i => !i.IsDescribed && !i.ReadAsPage && i.LastEnrichmentFailure?.IsPermanent != true);

    /// <summary>The stages that belong to extraction: set by <see cref="ExtractAsync(VaultEntry, bool, CancellationToken)"/>, kept until an extraction runs them.</summary>
    private const EnrichmentStages ExtractionStages = EnrichmentStages.PageReads | EnrichmentStages.LlmRefinement;

    /// <summary>
    /// What is still pending after a pass: the extraction stages the last extraction left (memorize, refresh and the chunk
    /// stages do not run them), plus images and contexts as this pass found them.
    /// </summary>
    private static EnrichmentStages PendingStages(VaultEntry entry, bool imagesPending, bool contextPending) =>
        (entry.PendingEnrichment & ExtractionStages)
        | (imagesPending ? EnrichmentStages.ImageDescriptions : EnrichmentStages.None)
        | (contextPending ? EnrichmentStages.ContextualEnrichment : EnrichmentStages.None);

    /// <summary>Whether contextual enrichment may call the model (<see cref="Generate"/>) or only reuse stored contexts.</summary>
    private enum ContextGeneration
    {
        Generate,
        ReuseOnly,
    }

    /// <summary>What one chunk-and-index pass did.</summary>
    private readonly record struct IndexOutcome(int ChunkCount, int ContentLength, bool ContextPending = false, int ReEmbedded = 0, int Kept = 0);

    public Task ExtractAsync(VaultEntry entry, CancellationToken ct = default) =>
        ExtractAsync(entry, _options.DeferEnrichment, ct);

    /// <summary>
    /// Extracts the source. With <paramref name="deferExtractionStages"/>, the stages that only enrich an extraction —
    /// page reads (<see cref="FileVaultOptions.PageReading"/>) and LLM refinement (<see cref="FileVaultOptions.LlmRefine"/>)
    /// — are not run, and the entry records them in <see cref="VaultEntry.PendingEnrichment"/> for an upgrade; without it,
    /// they run and are cleared from it.
    /// </summary>
    private async Task ExtractAsync(VaultEntry entry, bool deferExtractionStages, CancellationToken ct)
    {
        LogExtractingContent(_logger, entry.SourcePath);

        var deferred = EnrichmentStages.None;
        if (deferExtractionStages && _options.PageReading?.SelectPages is not null)
            deferred |= EnrichmentStages.PageReads;
        if (deferExtractionStages && _options.LlmRefine is not null)
            deferred |= EnrichmentStages.LlmRefinement;
        var settings = new ExtractionSettings
        {
            LlmRefine = _options.LlmRefine,
            PageReading = (deferred & EnrichmentStages.PageReads) != 0 ? null : _options.PageReading,
            SkipLlmRefine = (deferred & EnrichmentStages.LlmRefinement) != 0,
        };

        // Calculate source content hash
        var contentHash = await _hasher.ComputeHashAsync(entry.SourcePath, ct);

        // Extract content
        string extractedContent;
        IReadOnlyDictionary<string, string>? extractionHints = null;
        IReadOnlyList<string>? extractionWarnings = null;
        IReadOnlyList<ContentSpan>? extractionSpans = null;
        IReadOnlyList<TableArtifact>? extractionTables = null;

        if (_extractor != null)
        {
            var result = await _extractor.ExtractAsync(entry.SourcePath, settings, ct);
            extractedContent = result.Content;
            extractionHints = result.Hints;
            extractionWarnings = result.Warnings;
            extractionSpans = result.Spans;
            extractionTables = result.Tables;

            // Identity and alt text come from the extractor as-is. Stored even when there are none: the manifest
            // describes this extraction, and a re-extraction that finds no images must not keep the previous ones.
            if (result.Images?.Count > 0 || File.Exists(entry.ImagesManifestPath))
            {
                await _storage.StoreImagesAsync(entry, result.Images ?? [], ct);
            }
        }
        else
        {
            extractedContent = await ExtractFallbackAsync(entry.SourcePath, ct);
        }

        // Store raw extracted content (not git-tracked), and where its stretches came from in the source. Always
        // written, even as "none": a re-extraction must not leave the previous extraction's locations behind.
        await _storage.StoreExtractedContentAsync(entry, extractedContent, ct);
        await _storage.StoreContentSpansAsync(
            entry,
            extractionSpans is { Count: > 0 } ? ContentSpanSet.For(extractedContent, extractionSpans) : null,
            ct);
        await _storage.StoreTablesAsync(entry, extractionTables ?? [], ct);

        // Update entry to Extracted stage, carrying the extractor's structured diagnostics so a
        // legitimate 0-chunk outcome (scanned/blank document) is explainable downstream instead of
        // looking like a silent success.
        entry.MarkExtracted(contentHash, extractionHints, extractionWarnings, CurrentExtractionIdentity);
        entry.SetPendingEnrichment((entry.PendingEnrichment & ~ExtractionStages) | deferred);
        entry.SaveMetadata();

        LogExtracted(_logger, extractedContent.Length, entry.ExtractedMdPath);
    }

    public async Task RefineAsync(VaultEntry entry, CancellationToken ct = default)
    {
        LogRefiningContent(_logger, entry.SourcePath);

        // Distinguish "extract has not run" from "extracted, but the document carries no text".
        // The latter is legitimate for an image-only document: its content lives in the image
        // descriptions produced by EnrichImagesAsync, which are indexed as their own chunks rather
        // than injected into refined.md (see BuildImageChunks).
        if (!entry.ExtractedExists)
        {
            throw new InvalidOperationException($"No extracted content found at {entry.ExtractedMdPath}. Run extract first.");
        }

        var extractedContent = await _storage.GetExtractedContentAsync(entry, ct) ?? string.Empty;

        // For now, refined content is the same as extracted content.
        // In the future, this is where LLM refinement of the text happens.
        var refinedContent = extractedContent;

        // Store refined content (git-tracked)
        await _storage.StoreRefinedContentAsync(entry, refinedContent, ct);

        // Update entry to Refined stage
        entry.MarkRefined();
        entry.SaveMetadata();

        LogRefined(_logger, refinedContent.Length, entry.RefinedMdPath);
    }

    /// <summary>
    /// Offers every stored image that has no description yet to the registered
    /// <see cref="IVaultImageEnricher"/> and persists what comes back, one image at a time.
    /// <para>
    /// The pipeline owns the policy the consumer would otherwise have to rebuild: already-described
    /// images are never offered again (idempotent across memorize and refresh), a failing image
    /// neither aborts the run nor poisons its siblings, and images that failed stay pending so the
    /// next run retries exactly those. Without a registered enricher this is a no-op.
    /// </para>
    /// </summary>
    /// <returns>The number of images that carry a description after this call.</returns>
    private async Task<int> EnrichImagesAsync(VaultEntry entry, string? documentText, CancellationToken ct)
    {
        var images = await _storage.GetImageManifestAsync(entry, ct);
        if (images.Count == 0)
            return 0;

        var described = images.Count(i => i.IsDescribed);

        if (_imageEnricher == null)
            return described;

        // A permanently-failed image (see RecordEnrichmentFailureAsync) is excluded from retry: it
        // will never succeed, and re-offering it every scan wastes the enricher's backing resource
        // (e.g. a shared vision API) on a call already known to be pointless.
        var skippedPermanent = images.Count(i => !i.IsDescribed && i.LastEnrichmentFailure?.IsPermanent == true);
        // An image on a page read as a page is already in the text: describing it would repeat that page.
        var pending = images.Where(i => !i.IsDescribed && !i.ReadAsPage && i.LastEnrichmentFailure?.IsPermanent != true).ToList();
        if (skippedPermanent > 0)
            LogSkippingPermanentlyFailedImages(_logger, skippedPermanent, entry.SourcePath);
        if (pending.Count == 0)
            return described;

        LogEnrichingImages(_logger, pending.Count, entry.SourcePath);

        var failed = 0;
        foreach (var image in pending)
        {
            ct.ThrowIfCancellationRequested();

            if (!EnricherReads(image.ContentType))
            {
                // Known in advance to fail: say why, and do not spend an enricher call on it.
                await _storage.SetImageEnrichmentFailureAsync(
                    entry, image.Id, $"{UnsupportedContentTypeReason}{image.ContentType}",
                    (image.LastEnrichmentFailure?.AttemptCount ?? 0) + 1, isPermanent: true, ct);
                failed++;
                continue;
            }

            string? description;
            try
            {
                description = await _imageEnricher.DescribeAsync(new VaultImageDescriptionRequest
                {
                    Image = image,
                    SourcePath = entry.SourcePath,
                    DocumentText = string.IsNullOrWhiteSpace(documentText) ? null : documentText
                }, ct);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // One image failing must not cost the document its other descriptions, nor the
                // memorize itself. It simply stays pending for the next run, unless this was its
                // last permitted attempt.
                LogImageEnrichmentFailed(_logger, ex, image.Id, entry.SourcePath);
                await RecordEnrichmentFailureAsync(entry, image, ex.Message, ct);
                failed++;
                continue;
            }

            if (string.IsNullOrWhiteSpace(description))
            {
                await RecordEnrichmentFailureAsync(entry, image, "enricher returned no description", ct);
                failed++;
                continue;
            }

            await _storage.SetImageDescriptionAsync(entry, image.Id, description, ct);
            described++;
        }

        LogEnrichedImages(_logger, described, failed, entry.SourcePath);
        return described;
    }

    /// <summary>
    /// Prefix of the enrichment failure reason recorded for an image whose content type the enricher does not read
    /// (<see cref="FileVaultOptions.ImageEnrichmentContentTypes"/>).
    /// </summary>
    public const string UnsupportedContentTypeReason = "unsupported_content_type:";

    private bool EnricherReads(string contentType)
    {
        if (_options.ImageEnrichmentContentTypes is not { Count: > 0 } accepted)
            return true;

        var mediaType = contentType.Split(';', 2)[0].Trim();
        return accepted.Any(a => string.Equals(a.Trim(), mediaType, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Persists one more failed attempt for this image, deriving <c>IsPermanent</c> from
    /// <see cref="FileVaultOptions.MaxImageEnrichmentAttempts"/>. The pipeline is the policy owner
    /// (storage just persists whatever it is told), matching the class-level doc on
    /// <see cref="EnrichImagesAsync"/>.
    /// </summary>
    private async Task RecordEnrichmentFailureAsync(VaultEntry entry, VaultImage image, string reason, CancellationToken ct)
    {
        var attemptCount = (image.LastEnrichmentFailure?.AttemptCount ?? 0) + 1;
        var isPermanent = attemptCount >= _options.MaxImageEnrichmentAttempts;

        await _storage.SetImageEnrichmentFailureAsync(entry, image.Id, reason, attemptCount, isPermanent, ct);

        if (isPermanent)
            LogImageEnrichmentPermanentlyFailed(_logger, image.Id, entry.SourcePath, attemptCount);
    }

    /// <summary>
    /// Turns described images into indexable chunks carrying <c>image_id</c> / <c>image_file</c> /
    /// <c>chunk_kind</c> metadata, so a consumer can attach the source image to a citation by reading
    /// metadata — no marker is ever written into the body text and therefore none has to be stripped
    /// back out on the way to a user.
    /// </summary>
    /// <remarks>
    /// Descriptions go through the same chunker as the body. They used to bypass it and become one
    /// chunk each, which left <see cref="MemorizeOptions.MaxChunkSize"/> unenforced on the one input
    /// this pipeline generates rather than reads: a single oversized description made the whole
    /// document's embedding request exceed the model's context window, and because indexing replaces
    /// rows by deleting first, the document lost its index instead of merely failing to update.
    /// <para>
    /// Bounding the description at the point it is written is not something a consumer can do for us
    /// — <c>IVaultImageEnricher</c> is a public port, so any implementation may return a long
    /// description, and some legitimately do (detailed tables, diagrams).
    /// </para>
    /// </remarks>
    private async Task<IReadOnlyList<VaultChunk>> BuildImageChunksAsync(
        VaultEntry entry,
        MemorizeOptions options,
        CancellationToken ct)
    {
        var images = await _storage.GetImageManifestAsync(entry, ct);
        var chunks = new List<VaultChunk>();

        foreach (var image in images.Where(image => image.IsDescribed))
        {
            var description = image.Description!;

            // The per-format strategy override is deliberately not applied here: it is keyed on the
            // source document's extension, and a description is prose produced by the enricher, not
            // content in that document's format.
            IReadOnlyList<string> parts;
            if (_chunker != null)
            {
                var located = await _chunker.ChunkAsync(
                    description,
                    spans: null,
                    new ChunkingOptions
                    {
                        MaxChunkSize = options.MaxChunkSize,
                        OverlapSize = options.OverlapSize,
                        Strategy = options.Strategy,
                        Language = options.Language
                    },
                    ct);
                parts = located.Select(c => c.Text).ToList();
            }
            else
            {
                parts = ChunkFallback(description, options.MaxChunkSize);
            }

            // A chunker that returns nothing for text that is not blank would silently drop the
            // image, turning an image-only document into an empty one. Fall back rather than lose it.
            if (parts.Count == 0 && !string.IsNullOrWhiteSpace(description))
            {
                parts = ChunkFallback(description, options.MaxChunkSize);
            }

            for (var part = 0; part < parts.Count; part++)
            {
                var metadata = new Dictionary<string, object>
                {
                    ["chunk_kind"] = ImageDescriptionChunkKind,
                    ["image_id"] = image.Id,
                    ["image_file"] = image.FileName
                };

                // The same page keys a text chunk carries, so a citation or a page-scoped evaluation reads an image
                // the way it reads the text around it.
                if (image.PageNumber is { } page)
                {
                    metadata[PageNumberMetadataKey] = page;
                    metadata[StartPageMetadataKey] = page;
                    metadata[EndPageMetadataKey] = page;
                }

                // A picture shown on several pages (a slide reused across a deck): the page keys above name the first,
                // and this one lists every page, so a citation can attribute the description to each.
                if (image.PageNumbers.Count > 1)
                {
                    metadata[PageNumbersMetadataKey] = string.Join(',', image.PageNumbers.Select(p => p.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                }

                chunks.Add(new VaultChunk(ChunkIdentity.ForImage(entry.FilepathHash, image.Id, part), parts[part], metadata));
            }
        }

        return chunks;
    }

    /// <summary>
    /// Runs about-to-be-indexed chunks through the opt-in <see cref="IRAGSecurityPipeline"/>
    /// (indirect prompt injection / RAG poisoning detection) before they are embedded and stored —
    /// catching poisoned content at the door rather than relying solely on a retrieval-time check.
    /// A chunk the pipeline suggests blocking is dropped from the batch entirely; one it suggests
    /// sanitizing has its content replaced with the pipeline's sanitized version.
    /// </summary>
    /// <summary>Metadata key holding the first page (1-based) a text chunk covers — the key FluxIndex's own FileFlux integration uses.</summary>
    public const string PageNumberMetadataKey = "pageNumber";

    /// <summary>
    /// Metadata key on an image description chunk listing every page that shows the image, ascending and comma-separated
    /// (<c>"1,4"</c>) — present only when there is more than one. <see cref="PageNumberMetadataKey"/> holds the first.
    /// </summary>
    public const string PageNumbersMetadataKey = "pageNumbers";

    /// <summary>Metadata key holding the first page (1-based) a text chunk covers.</summary>
    public const string StartPageMetadataKey = "ff_start_page";

    /// <summary>Metadata key holding the last page (1-based) a text chunk covers.</summary>
    public const string EndPageMetadataKey = "ff_end_page";

    /// <summary>Metadata key holding where, in seconds from the start of a recording, a text chunk begins.</summary>
    public const string StartSecondsMetadataKey = "ff_start_seconds";

    /// <summary>Metadata key holding where, in seconds from the start of a recording, a text chunk ends.</summary>
    public const string EndSecondsMetadataKey = "ff_end_seconds";

    /// <summary>
    /// Projects a chunk's source location onto metadata a consumer can read from a search hit (a page, a time).
    /// </summary>
    internal static IReadOnlyDictionary<string, object>? LocationMetadata(ContentLocation? location)
    {
        if (location is null || location.IsEmpty)
            return null;

        var metadata = new Dictionary<string, object>();
        if (location.StartPage is { } startPage)
        {
            metadata[PageNumberMetadataKey] = startPage;
            metadata[StartPageMetadataKey] = startPage;
        }
        if (location.EndPage is { } endPage)
            metadata[EndPageMetadataKey] = endPage;
        if (location.StartTime is { } startTime)
            metadata[StartSecondsMetadataKey] = startTime.TotalSeconds;
        if (location.EndTime is { } endTime)
            metadata[EndSecondsMetadataKey] = endTime.TotalSeconds;
        return metadata;
    }

    /// <summary>
    /// A text chunk's metadata: its source location, and for a piece of a table the table keys
    /// (<see cref="TableChunkKind"/>).
    /// </summary>
    internal static IReadOnlyDictionary<string, object>? TextChunkMetadata(ContentChunk chunk, IReadOnlyList<TableArtifact>? tables)
    {
        var location = LocationMetadata(chunk.Location);
        if (chunk.Table is not { } piece)
            return location;

        var metadata = location is null ? new Dictionary<string, object>() : new Dictionary<string, object>(location);
        metadata["chunk_kind"] = TableChunkKind;
        metadata["table_piece"] = piece.Piece;
        metadata["table_pieces"] = piece.Pieces;
        metadata["table_row_start"] = piece.RowStart;
        metadata["table_row_end"] = piece.RowEnd;

        if (tables is not null && piece.TableIndex >= 0 && piece.TableIndex < tables.Count)
        {
            var table = tables[piece.TableIndex];
            metadata[TableIdMetadataKey] = table.Id;
            if (table.Columns.Any(c => !string.IsNullOrWhiteSpace(c)))
                metadata["table_columns"] = string.Join(" | ", table.Columns);
            if (table.Section is { } section)
                metadata["table_section"] = section;
            if (table.Caption is { } caption)
                metadata["table_caption"] = caption;
            if (table.PageNumber is { } page)
                metadata["table_page"] = page;
        }

        return metadata;
    }

    private async Task<IReadOnlyList<ContentSpan>?> GetTrustedSpansAsync(VaultEntry entry, string? refinedContent, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(refinedContent))
            return null;
        if (await _storage.GetContentSpansAsync(entry, ct) is not { } set)
            return null;
        if (set.ContentHash == ContentSpanSet.Hash(refinedContent))
            return set.Spans;

        LogContentSpansStale(_logger, entry.SourcePath);
        return null;
    }

    /// <summary>Metadata key holding the LLM-written context summary that was prepended to an enriched chunk.</summary>
    public const string ContextSummaryMetadataKey = "context_summary";

    /// <summary>
    /// Metadata key recording what happened to a chunk in the enrichment step: <c>contextual</c> (a context was
    /// prepended), <c>empty</c> (the port succeeded but returned no context, so the chunk is indexed as it was) or
    /// <c>failed</c> (the port threw and <see cref="ContextualEnrichmentDefaults.ContinueOnError"/> degraded it).
    /// </summary>
    public const string EnrichmentMetadataKey = "enrichment";

    /// <summary>
    /// Asks the FluxIndex.Core <see cref="IContextualEnrichmentService"/> port for one context per text chunk and
    /// prepends it (context + blank line + original) — the same text then goes to the vector store, the keyword index
    /// and the stored chunk, so retrieval and display agree. The original passage is still on disk in <c>refined.md</c>;
    /// the context alone is kept in metadata so a consumer can strip it. A blank context leaves the content untouched
    /// but tags the chunk <c>empty</c> and logs a warning — otherwise it would look exactly like enrichment being off.
    /// </summary>
    private async Task<(List<VaultChunk> Chunks, bool Pending)> ApplyContextualEnrichmentAsync(
        List<VaultChunk> chunks,
        string fullDocumentText,
        string sourcePath,
        IReadOnlyDictionary<string, DocumentChunk> stored,
        bool generate,
        CancellationToken ct)
    {
        if (chunks.Count == 0 || string.IsNullOrWhiteSpace(fullDocumentText))
        {
            return (chunks, false);
        }

        // A context written for this passage of this same document text is reused instead of asking the model again:
        // the chunk id names the passage, the recorded document hash names the text the context was written from.
        var documentHash = ContextDocumentHash(fullDocumentText);
        var result = new VaultChunk[chunks.Count];
        var toGenerate = new List<int>();
        for (var i = 0; i < chunks.Count; i++)
        {
            if (stored.TryGetValue(chunks[i].Id, out var row)
                && MetadataText(row.Metadata, EnrichmentMetadataKey) == "contextual"
                && MetadataText(row.Metadata, ContextDocumentHashMetadataKey) == documentHash
                && MetadataText(row.Metadata, ContextSummaryMetadataKey) is { Length: > 0 } storedContext)
            {
                result[i] = WithContext(chunks[i], storedContext, documentHash);
            }
            else
            {
                toGenerate.Add(i);
            }
        }

        if (toGenerate.Count < chunks.Count)
        {
            LogContextualEnrichmentReused(_logger, chunks.Count - toGenerate.Count, chunks.Count, sourcePath);
        }

        if (toGenerate.Count == 0)
        {
            return (result.ToList(), false);
        }

        if (!generate)
        {
            // Deferred: the rest stays as it is until an upgrade generates its contexts.
            foreach (var i in toGenerate)
            {
                result[i] = chunks[i];
            }

            return (result.ToList(), true);
        }

        IReadOnlyList<string> contexts;
        var stopwatch = Stopwatch.StartNew();
        try
        {
            contexts = await _contextualEnrichment!.GenerateContextBatchAsync(
                toGenerate.Select(i => chunks[i].Content).ToList(),
                fullDocumentText,
                ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException && _options.ContextualEnrichment.ContinueOnError)
        {
            LogContextualEnrichmentFailed(_logger, sourcePath, toGenerate.Count, ex);
            foreach (var i in toGenerate)
            {
                result[i] = chunks[i] with { Metadata = WithMetadata(chunks[i].Metadata, EnrichmentMetadataKey, "failed") };
            }

            return (result.ToList(), false);
        }

        if (contexts.Count != toGenerate.Count)
        {
            throw new InvalidOperationException(
                $"Contextual enrichment returned {contexts.Count} context(s) for {toGenerate.Count} chunk(s) of '{sourcePath}'; the port must return exactly one per chunk, in order.");
        }

        var enrichedCount = 0;
        var emptyCount = 0;
        for (var k = 0; k < toGenerate.Count; k++)
        {
            var i = toGenerate[k];
            var context = contexts[k]?.Trim();
            if (string.IsNullOrEmpty(context))
            {
                emptyCount++;
                result[i] = chunks[i] with { Metadata = WithMetadata(chunks[i].Metadata, EnrichmentMetadataKey, "empty") };
                continue;
            }

            enrichedCount++;
            result[i] = WithContext(chunks[i], context, documentHash);
        }

        if (emptyCount > 0)
        {
            LogContextualEnrichmentReturnedNoContext(_logger, emptyCount, toGenerate.Count, sourcePath);
        }

        LogContextualEnrichmentApplied(_logger, enrichedCount, toGenerate.Count, sourcePath, stopwatch.ElapsedMilliseconds);
        return (result.ToList(), false);
    }

    /// <summary>
    /// Metadata key holding a hash of the document text a chunk's context was written from — what lets a later pass reuse
    /// the context only while that text is unchanged.
    /// </summary>
    public const string ContextDocumentHashMetadataKey = "context_doc_hash";

    private static VaultChunk WithContext(VaultChunk chunk, string context, string documentHash)
    {
        var metadata = WithMetadata(chunk.Metadata, ContextSummaryMetadataKey, context);
        metadata = WithMetadata(metadata, EnrichmentMetadataKey, "contextual");
        metadata = WithMetadata(metadata, ContextDocumentHashMetadataKey, documentHash);
        return chunk with { Content = context + "\n\n" + chunk.Content, Metadata = metadata };
    }

    private static string ContextDocumentHash(string text) =>
        Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text)));

    private static readonly IReadOnlyDictionary<string, DocumentChunk> EmptyStoredChunks = new Dictionary<string, DocumentChunk>();

    /// <summary>
    /// The rows currently stored under <paramref name="ids"/>. Empty when the store cannot say (no vector store, or a store
    /// that does not implement the lookup) — then nothing is reused or skipped, which is today's behaviour.
    /// </summary>
    private async Task<IReadOnlyDictionary<string, DocumentChunk>> GetStoredChunksAsync(IReadOnlyList<string> ids, CancellationToken ct)
    {
        if (_vectorStore is null || ids.Count == 0)
        {
            return EmptyStoredChunks;
        }

        try
        {
            var rows = await _vectorStore.GetChunksByIdsAsync(ids, ct);
            var byId = new Dictionary<string, DocumentChunk>(StringComparer.Ordinal);
            foreach (var row in rows)
            {
                byId[row.Id] = row;
            }

            return byId;
        }
        catch (Exception ex) when (ex is NotSupportedException or NotImplementedException)
        {
            return EmptyStoredChunks;
        }
    }

    /// <summary>Counts of one index pass: rows embedded and written, rows left as they were.</summary>
    private sealed class IndexCounters
    {
        public int ReEmbedded;
        public int Kept;
    }

    /// <summary>
    /// Whether the stored row is exactly the row about to be written — same text, position and metadata — so writing it
    /// again (and embedding its text again) would change nothing. Metadata values are compared in their JSON form, because
    /// a store may hand them back as JSON; any difference, including one of representation, means «write it».
    /// </summary>
    private static bool IsUnchanged(DocumentChunk candidate, IReadOnlyDictionary<string, DocumentChunk> stored) =>
        stored.TryGetValue(candidate.Id, out var row)
        && string.Equals(row.Content, candidate.Content, StringComparison.Ordinal)
        && row.ChunkIndex == candidate.ChunkIndex
        && row.TotalChunks == candidate.TotalChunks
        && SameMetadata(row.Metadata, candidate.Metadata);

    private static bool SameMetadata(IReadOnlyDictionary<string, object>? a, IReadOnlyDictionary<string, object>? b)
    {
        var left = a ?? EmptyMetadata;
        var right = b ?? EmptyMetadata;
        if (left.Count != right.Count)
        {
            return false;
        }

        foreach (var (key, value) in left)
        {
            if (!right.TryGetValue(key, out var other) || JsonForm(value) != JsonForm(other))
            {
                return false;
            }
        }

        return true;
    }

    private static readonly IReadOnlyDictionary<string, object> EmptyMetadata = new Dictionary<string, object>();

    private static string JsonForm(object? value) => value switch
    {
        null => "null",
        System.Text.Json.JsonElement json => json.GetRawText(),
        _ => System.Text.Json.JsonSerializer.Serialize(value),
    };

    private static IReadOnlyDictionary<string, object> WithMetadata(IReadOnlyDictionary<string, object>? existing, string key, object value)
    {
        var copy = existing is null
            ? new Dictionary<string, object>(StringComparer.Ordinal)
            : new Dictionary<string, object>(existing, StringComparer.Ordinal);
        copy[key] = value;
        return copy;
    }

    private async Task<List<VaultChunk>> ApplyRagSecurityAsync(
        List<VaultChunk> chunks,
        string sourcePath,
        CancellationToken ct)
    {
        if (chunks.Count == 0)
        {
            return chunks;
        }

        var documents = chunks.Select((c, i) => new RAGDocument
        {
            Id = i.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Content = c.Content,
            Source = sourcePath
        }).ToList();

        var validations = await _ragSecurityPipeline!.ValidateDocumentsAsync(documents, ct);
        var validationById = validations.ToDictionary(v => v.Document.Id ?? string.Empty);

        var filtered = new List<VaultChunk>(chunks.Count);
        var blockedCount = 0;
        for (var i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i];
            if (!validationById.TryGetValue(i.ToString(System.Globalization.CultureInfo.InvariantCulture), out var validation))
            {
                filtered.Add(chunk);
                continue;
            }

            if (validation.SuggestedAction == RAGAction.Block)
            {
                blockedCount++;
                continue;
            }

            filtered.Add(validation.SuggestedAction == RAGAction.Sanitize && validation.SanitizedContent != null
                ? chunk with { Content = validation.SanitizedContent }
                : chunk);
        }

        if (blockedCount > 0)
        {
            LogRagSecurityBlockedChunks(_logger, blockedCount, sourcePath);
        }

        return filtered;
    }

    public async Task RemoveAsync(VaultEntry entry, CancellationToken ct = default)
    {
        if (_vectorStore == null && _keywordSearchService == null)
        {
            LogNoVectorStoreSkipRemoval(_logger);
            return;
        }

        // Delete by document ID (filepath hash) from every backend this entry was written to.
        var documentId = entry.FilepathHash;

        // The graph leg keys its rows by chunk id, and the vector store is where the document's chunk ids are read from —
        // so read them before the rows go.
        IReadOnlyList<string> graphChunkIds = _graphRAGService != null && _vectorStore != null
            ? await _vectorStore.GetChunkIdsByDocumentIdAsync(documentId, ct)
            : [];

        if (_vectorStore != null)
            await _vectorStore.DeleteByDocumentIdAsync(documentId, ct);

        if (_keywordSearchService != null)
            await _keywordSearchService.DeleteByDocumentIdAsync(documentId, ct);

        if (graphChunkIds.Count > 0)
            await _graphRAGService!.ForgetChunksAsync(graphChunkIds, GraphPartitionFor(null), ct);

        LogRemovedChunks(_logger, documentId);
    }

    /// <inheritdoc />
    public async Task<VaultMoveIndexResult> ReassignAsync(VaultEntry entry, string previousSourcePath, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(previousSourcePath);

        var previousPath = Path.GetFullPath(previousSourcePath);
        var oldDocumentId = FilepathHasher.ComputeHash(previousPath);
        var newDocumentId = entry.FilepathHash;
        if (string.Equals(oldDocumentId, newDocumentId, StringComparison.Ordinal))
            throw new ArgumentException(
                $"'{previousPath}' and '{entry.SourcePath}' name the same vault entry; there are no index rows to re-key.",
                nameof(previousSourcePath));

        if (_vectorStore == null && _keywordSearchService == null)
        {
            LogNoVectorStoreSkipRemoval(_logger);
            return new VaultMoveIndexResult(0, 0, null);
        }

        // Everything is read and checked before any leg is written.
        var chunkIdMap = await BuildChunkIdMapAsync(oldDocumentId, newDocumentId, entry.SourcePath, ct);
        if (chunkIdMap.Count == 0)
            return new VaultMoveIndexResult(0, 0, null);

        var updates = ProvenanceUpdates(newDocumentId, entry.SourcePath);
        var inverseMap = chunkIdMap.ToDictionary(kv => kv.Value, kv => kv.Key, StringComparer.Ordinal);
        var inverseUpdates = ProvenanceUpdates(oldDocumentId, previousPath);
        var partition = GraphPartitionFor(null);

        var vectorMoved = 0;
        var keywordMoved = 0;
        var vectorDone = false;
        var keywordDone = false;
        var graphStarted = false;
        GraphReassignResult? graph = null;
        try
        {
            if (_vectorStore != null)
            {
                vectorMoved = await _vectorStore.ReassignDocumentAsync(oldDocumentId, newDocumentId, chunkIdMap, updates, ct);
                vectorDone = true;
            }

            if (_keywordSearchService != null)
            {
                keywordMoved = await _keywordSearchService.ReassignDocumentAsync(oldDocumentId, newDocumentId, chunkIdMap, updates, ct);
                keywordDone = true;
            }

            if (_graphRAGService != null)
            {
                graphStarted = true;
                graph = await _graphRAGService.ReassignChunksAsync(chunkIdMap, oldDocumentId, newDocumentId, partition, ct);
            }
        }
        catch (Exception ex)
        {
            // Put the legs already moved back where they were, so the entry is findable under one id, not split across
            // two. Best-effort: a failure here is logged and must not replace the error that caused it.
            LogMoveRollingBack(_logger, entry.SourcePath, ex.GetType().Name);
            // The graph call is a series of upserts, so a partial run is undone by mapping back whatever it reached.
            if (graphStarted)
                await TryUndoAsync(() => _graphRAGService!.ReassignChunksAsync(inverseMap, newDocumentId, oldDocumentId, partition, CancellationToken.None), entry);
            if (keywordDone)
                await TryUndoAsync(() => _keywordSearchService!.ReassignDocumentAsync(newDocumentId, oldDocumentId, inverseMap, inverseUpdates, CancellationToken.None), entry);
            if (vectorDone)
                await TryUndoAsync(() => _vectorStore!.ReassignDocumentAsync(newDocumentId, oldDocumentId, inverseMap, inverseUpdates, CancellationToken.None), entry);
            throw;
        }

        LogReassigned(_logger, previousPath, entry.SourcePath, vectorMoved, keywordMoved);
        return new VaultMoveIndexResult(vectorMoved, keywordMoved, graph);
    }

    private async Task TryUndoAsync(Func<Task> undo, VaultEntry entry)
    {
        try
        {
            await undo();
        }
        catch (Exception ex)
        {
            LogMoveRollbackFailed(_logger, ex, entry.SourcePath);
        }
    }

    /// <summary>The provenance keys <see cref="ApplyChunkMetadata"/> writes, with the values a chunk at <paramref name="sourcePath"/> carries.</summary>
    private static Dictionary<string, object?> ProvenanceUpdates(string documentId, string sourcePath) => new(StringComparer.Ordinal)
    {
        ["document_id"] = documentId,
        ["source_path"] = sourcePath,
        ["filepath_hash"] = documentId,
        ["file_name"] = Path.GetFileName(sourcePath)
    };

    /// <summary>
    /// Maps every chunk id the entry holds under <paramref name="oldDocumentId"/> to the id a memorize at the new path
    /// would write (<see cref="ChunkIdentity"/>): the same passage and occurrence, or the same image and part, under
    /// <paramref name="newDocumentId"/>. Each derived old id is checked against the stored one, so the map is exact or
    /// the call throws.
    /// </summary>
    /// <remarks>
    /// The identity is the chunker's raw output, and the stored content can differ from it: contextual enrichment
    /// prepends a context, which is also kept in metadata and is stripped here; the RAG security pipeline can replace
    /// the text outright, which cannot be undone, so such a chunk fails the check. Occurrence ordinals follow the stored
    /// chunk order, the order the memorize assigned them in. The keyword index cannot return a chunk's content by id, so
    /// its rows are mapped through the vector leg; an id only the keyword leg holds fails the check.
    /// </remarks>
    private async Task<IReadOnlyDictionary<string, string>> BuildChunkIdMapAsync(
        string oldDocumentId,
        string newDocumentId,
        string sourcePathForMessages,
        CancellationToken ct)
    {
        var chunks = _vectorStore == null
            ? []
            : (await _vectorStore.GetByDocumentIdAsync(oldDocumentId, ct)).OrderBy(c => c.ChunkIndex).ToList();
        var keywordIds = _keywordSearchService == null
            ? []
            : await _keywordSearchService.GetChunkIdsByDocumentIdAsync(oldDocumentId, ct);

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        var unreproducible = new List<string>();
        var textOccurrences = new Dictionary<string, int>(StringComparer.Ordinal);
        var imageParts = new Dictionary<string, int>(StringComparer.Ordinal);

        foreach (var chunk in chunks)
        {
            if (string.Equals(MetadataText(chunk.Metadata, "chunk_kind"), ImageDescriptionChunkKind, StringComparison.Ordinal))
            {
                var imageId = MetadataText(chunk.Metadata, "image_id");
                if (string.IsNullOrWhiteSpace(imageId))
                {
                    unreproducible.Add(chunk.Id);
                    continue;
                }

                imageParts.TryGetValue(imageId, out var part);
                imageParts[imageId] = part + 1;
                if (!string.Equals(ChunkIdentity.ForImage(oldDocumentId, imageId, part), chunk.Id, StringComparison.Ordinal))
                {
                    unreproducible.Add(chunk.Id);
                    continue;
                }

                map[chunk.Id] = ChunkIdentity.ForImage(newDocumentId, imageId, part);
                continue;
            }

            var passage = RawPassage(chunk);
            textOccurrences.TryGetValue(passage, out var occurrence);
            textOccurrences[passage] = occurrence + 1;
            if (!string.Equals(ChunkIdentity.ForText(oldDocumentId, passage, occurrence), chunk.Id, StringComparison.Ordinal))
            {
                unreproducible.Add(chunk.Id);
                continue;
            }

            map[chunk.Id] = ChunkIdentity.ForText(newDocumentId, passage, occurrence);
        }

        var vectorIds = chunks.Select(c => c.Id).ToHashSet(StringComparer.Ordinal);
        var keywordOnly = keywordIds.Where(id => !vectorIds.Contains(id)).ToList();

        if (unreproducible.Count > 0 || keywordOnly.Count > 0)
        {
            var reasons = new List<string>();
            if (unreproducible.Count > 0)
                reasons.Add($"{unreproducible.Count} chunk(s) whose id does not follow from their content ({string.Join(", ", unreproducible.Take(5))}) - rewritten by the RAG security pipeline or indexed under older ids; remove and memorize the entry again");
            if (keywordOnly.Count > 0)
                reasons.Add($"{keywordOnly.Count} keyword-index row(s) with no vector row ({string.Join(", ", keywordOnly.Take(5))}); run RepairKeywordIndexAsync first");
            throw new InvalidOperationException(
                $"The index rows of '{sourcePathForMessages}' cannot be re-keyed: {string.Join("; ", reasons)}. Nothing was changed.");
        }

        return map;
    }

    /// <summary>The chunker output a stored text chunk was identified by: its content without the prepended enrichment context.</summary>
    private static string RawPassage(DocumentChunk chunk)
    {
        var content = chunk.Content ?? string.Empty;
        if (!string.Equals(MetadataText(chunk.Metadata, EnrichmentMetadataKey), "contextual", StringComparison.Ordinal))
            return content;

        var context = MetadataText(chunk.Metadata, ContextSummaryMetadataKey);
        var prefix = context + "\n\n";
        return !string.IsNullOrEmpty(context) && content.StartsWith(prefix, StringComparison.Ordinal)
            ? content[prefix.Length..]
            : content;
    }

    /// <summary>A metadata value as text, whether the store hands it back as a string or as JSON.</summary>
    private static string? MetadataText(IReadOnlyDictionary<string, object>? metadata, string key)
    {
        if (metadata == null || !metadata.TryGetValue(key, out var value) || value == null)
            return null;

        return value switch
        {
            string s => s,
            System.Text.Json.JsonElement { ValueKind: System.Text.Json.JsonValueKind.String } json => json.GetString(),
            _ => value.ToString()
        };
    }

    /// <summary>
    /// Bulk-deletes every vector tagged with the given <paramref name="vaultId"/> from the shared
    /// vector store in a single filtered delete. Returns the number of vectors removed (0 if no
    /// vector store is configured). This is the tenant/vault purge primitive used by
    /// <c>IVault.PurgeAsync</c> — it replaces a per-entry delete loop.
    /// </summary>
    /// <remarks>
    /// Both legs are purged. <see cref="IKeywordSearchService"/> gained a filter-scoped bulk delete
    /// taking the same filter vocabulary as the vector store, so one filter object now reaches a
    /// tenant's keyword rows too — until then this could only purge vectors and logged a warning
    /// rather than claim a purge it could not perform.
    /// </remarks>
    public async Task<int> PurgeVectorsAsync(string vaultId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(vaultId);

        if (_vectorStore == null)
        {
            LogNoVectorStoreSkipRemoval(_logger);
            return 0;
        }

        var filter = new Dictionary<string, object> { ["vault_id"] = vaultId };
        var deleted = await _vectorStore.DeleteByFilterAsync(filter, ct);

        // The keyword leg takes the same filter. It used to have no tag-scoped bulk delete at all, so
        // this method cleared the vectors and left the keyword rows behind - a purge that reported
        // success while the tenant's text stayed searchable. The chunks carry vault_id on both legs
        // (ApplyChunkMetadata runs before either write), so one filter reaches both.
        if (_keywordSearchService != null)
        {
            var keywordDeleted = await _keywordSearchService.DeleteByFilterAsync(filter, ct);
            LogPurgedKeywordIndex(_logger, keywordDeleted, vaultId);
        }

        LogRemovedChunks(_logger, $"vault_id={vaultId}");

        // Still the vector count rather than a sum: both legs hold the same chunks, so adding them
        // would report twice the number of chunks that existed.
        return deleted;
    }

    /// <summary>
    /// Applies the standard chunk provenance metadata (source path, filepath hash, file name) and,
    /// when the vault is tenant-scoped, the <c>vault_id</c> tag used for tenant-bulk purge.
    /// <paramref name="extraMetadata"/> carries per-chunk provenance that is not uniform across the
    /// document — currently the image tags on an image-description chunk.
    /// </summary>
    private void ApplyChunkMetadata(
        DocumentChunk chunk,
        VaultEntry entry,
        IReadOnlyDictionary<string, object>? extraMetadata = null)
    {
        chunk.Metadata ??= new Dictionary<string, object>();
        // Provenance only: scope filters resolve FilterKeys.DocumentId to chunk.DocumentId in every store.
        chunk.Metadata["document_id"] = chunk.DocumentId;
        chunk.Metadata["source_path"] = entry.SourcePath;
        chunk.Metadata["filepath_hash"] = entry.FilepathHash;
        chunk.Metadata["file_name"] = entry.FileName;
        if (!string.IsNullOrEmpty(_options.VaultId))
            chunk.Metadata["vault_id"] = _options.VaultId;

        if (extraMetadata == null)
            return;

        foreach (var (key, value) in extraMetadata)
            chunk.Metadata[key] = value;
    }

    /// <summary>
    /// Builds the filter that scopes a search to <paramref name="docIdSet"/>, matching ANY of its
    /// elements. The key is <see cref="FilterKeys.DocumentId"/>, which every store and keyword index
    /// resolves to the chunk's own document id — so a chunk is in scope whether or not its metadata
    /// carries a copy. Null when no scope was requested.
    /// </summary>
    private static Dictionary<string, object>? BuildDocScopeFilter(HashSet<string>? docIdSet)
        => docIdSet == null ? null : new Dictionary<string, object> { [FilterKeys.DocumentId] = docIdSet };

    public async Task<VaultPipelineSearchResponse> SearchAsync(
        string query,
        IEnumerable<string>? documentIds = null,
        int topK = 10,
        float minScore = 0.0f,
        VaultSearchStrategy strategy = VaultSearchStrategy.Vector,
        CancellationToken ct = default)
    {
        if (_vectorStore == null || _embeddingService == null)
        {
            LogNoVectorStoreCannotSearch(_logger);
            return new VaultPipelineSearchResponse([], VaultSearchStrategy.Vector);
        }

        var docIdSet = documentIds?.ToHashSet(StringComparer.OrdinalIgnoreCase);

        if (strategy == VaultSearchStrategy.Hybrid)
        {
            // The leg was decided at construction (see _hybridKeywordLeg). Native hybrid is used only when no
            // keyword index of ours is wired in; either way a document-id scope reaches both legs before fusion —
            // as a metadata filter on the native call, or as HybridSearchOptions.Filters — so a scoped request gets
            // the fused ranking of the in-scope chunks, not a fused ranking of everything filtered afterwards.
            if (_hybridKeywordLeg == HybridKeywordLeg.Native && _vectorStore is INativeHybridSearch nativeHybrid)
            {
                var nativeResults = await NativeHybridSearchAsync(nativeHybrid, query, docIdSet, topK, minScore, ct);
                LogSearchResults(_logger, query, nativeResults.Count);
                return new VaultPipelineSearchResponse(nativeResults, VaultSearchStrategy.Hybrid);
            }

            if (_hybridKeywordLeg == HybridKeywordLeg.KeywordIndex && _hybridSearch != null)
            {
                var hybridResults = await HybridSearchAsync(query, docIdSet, topK, minScore, ct);
                LogSearchResults(_logger, query, hybridResults.Count);
                return new VaultPipelineSearchResponse(hybridResults, VaultSearchStrategy.Hybrid);
            }

            // Neither a native-hybrid store nor a hybrid service — degrade to vector and report
            // it truthfully (no silent mismatch).
            LogHybridUnavailableFallback(_logger);
        }

        if (strategy == VaultSearchStrategy.Keyword)
        {
            if (_keywordSearchService != null)
            {
                var keywordResults = await KeywordSearchAsync(query, docIdSet, topK, minScore, ct);
                LogSearchResults(_logger, query, keywordResults.Count);
                return new VaultPipelineSearchResponse(keywordResults, VaultSearchStrategy.Keyword);
            }

            // No IKeywordSearchService registered — degrade to vector and report it truthfully.
            LogKeywordUnavailableFallback(_logger);
        }

        var vectorResults = await VectorSearchAsync(query, docIdSet, topK, minScore, ct);
        LogSearchResults(_logger, query, vectorResults.Count);
        return new VaultPipelineSearchResponse(vectorResults, VaultSearchStrategy.Vector);
    }

    private async Task<IReadOnlyList<PipelineSearchResult>> VectorSearchAsync(
        string query,
        HashSet<string>? docIdSet,
        int topK,
        float minScore,
        CancellationToken ct)
    {
        // Generate query embedding
        var queryEmbedding = await _embeddingService!.GenerateQueryEmbeddingAsync(query, ct);

        // Push the document-id scope into the query itself — without this,
        // a scoped search over a shared index returns whatever survives filtering the global top N,
        // so scoped chunks that lose the unscoped ranking race get zero results even when they match
        // the query perfectly well. The client-side Where() below stays as a correctness backstop for
        // any store that can't fully honor the filter, matching IVectorStore.SearchAsync's own
        // documented contract (recall degrades, never correctness).
        var searchResults = await _vectorStore!.SearchAsync(queryEmbedding, topK * 2, minScore, BuildDocScopeFilter(docIdSet), ct);

        IEnumerable<FluxIndex.Core.Domain.Entities.DocumentChunk> filtered = searchResults;
        if (docIdSet != null)
            filtered = searchResults.Where(r => docIdSet.Contains(r.DocumentId));

        return filtered
            .Take(topK)
            .Select(r => new PipelineSearchResult
            {
                DocumentId = r.DocumentId,
                ChunkId = r.Id.ToString(),
                ChunkIndex = r.ChunkIndex,
                Content = r.Content,
                Score = r.Score ?? 0f,
                Metadata = r.Metadata
            })
            .ToList();
    }

    private async Task<IReadOnlyList<PipelineSearchResult>> KeywordSearchAsync(
        string query,
        HashSet<string>? docIdSet,
        int topK,
        float minScore,
        CancellationToken ct)
    {
        var options = new KeywordSearchOptions
        {
            // Over-fetch so document-id filtering does not starve the result set.
            MaxResults = topK * 2,
            MinScore = minScore,
            MetadataFilter = BuildDocScopeFilter(docIdSet)
        };

        var keywordResults = await _keywordSearchService!.SearchAsync(query, options, ct);

        IEnumerable<KeywordSearchResult> filtered = keywordResults;
        if (docIdSet != null)
            filtered = keywordResults.Where(r => docIdSet.Contains(r.Chunk.DocumentId));

        return filtered
            .Take(topK)
            .Select(r => new PipelineSearchResult
            {
                DocumentId = r.Chunk.DocumentId,
                ChunkId = r.Chunk.Id,
                ChunkIndex = r.Chunk.ChunkIndex,
                Content = r.Chunk.Content,
                Score = (float)r.Score,
                Metadata = r.Chunk.Metadata
            })
            .ToList();
    }

    private async Task<IReadOnlyList<PipelineSearchResult>> HybridSearchAsync(
        string query,
        HashSet<string>? docIdSet,
        int topK,
        float minScore,
        CancellationToken ct)
    {
        var options = new FluxIndex.Core.Domain.Models.HybridSearchOptions
        {
            // Over-fetch so document-id filtering does not starve the result set. The service reads the
            // candidate count per leg, not from MaxResults, so each leg needs it too — left at their
            // default of 10, two legs fuse to at most 20 rows whatever topK asks for.
            MaxResults = topK * 2,
            VectorOptions = new FluxIndex.Core.Domain.Models.VectorSearchOptions
            {
                MaxResults = topK * 2,
                // MinScore is a similarity floor on the vector leg, applied before fusion — the same
                // meaning it has for the Vector strategy and for the native hybrid path. It is not a
                // fused-score floor: fused scores are rank-sized (about 0.01), so a similarity-sized
                // threshold there would drop every hit.
                MinScore = minScore
            },
            SparseOptions = new FluxIndex.Core.Domain.Models.SparseSearchOptions { MaxResults = topK * 2 }
        };
        if (BuildDocScopeFilter(docIdSet) is { } scopeFilter)
            options.Filters = scopeFilter;

        var hybridResults = await _hybridSearch!.SearchAsync(query, options, ct);
        return ProjectHybrid(hybridResults, docIdSet, topK);
    }

    private async Task<IReadOnlyList<PipelineSearchResult>> NativeHybridSearchAsync(
        INativeHybridSearch nativeHybrid,
        string query,
        HashSet<string>? docIdSet,
        int topK,
        float minScore,
        CancellationToken ct)
    {
        // Native hybrid fuses dense vectors with the store's own keyword index (e.g. chunk_fts);
        // it takes the embedding plus the raw text query. The document-id scope goes in as a
        // filter so both legs are scoped before fusion; the client-side projection below stays as
        // the same correctness backstop the vector path keeps. Over-fetch to survive it.
        var queryEmbedding = await _embeddingService!.GenerateQueryEmbeddingAsync(query, ct);
        var nativeResults = await nativeHybrid.HybridSearchAsync(
            queryEmbedding, query, topK * 2, minScore, vectorWeight: null, BuildDocScopeFilter(docIdSet), ct);
        return ProjectHybrid(nativeResults, docIdSet, topK);
    }

    private static List<PipelineSearchResult> ProjectHybrid(
        IEnumerable<FluxIndex.Core.Domain.Models.HybridSearchResult> results,
        HashSet<string>? docIdSet,
        int topK)
    {
        IEnumerable<FluxIndex.Core.Domain.Models.HybridSearchResult> filtered = results;
        if (docIdSet != null)
            filtered = results.Where(r => docIdSet.Contains(r.Chunk.DocumentId));

        return filtered
            .Take(topK)
            .Select(r => new PipelineSearchResult
            {
                DocumentId = r.Chunk.DocumentId,
                ChunkId = r.Chunk.Id,
                ChunkIndex = r.Chunk.ChunkIndex,
                Content = r.Chunk.Content,
                Score = (float)r.FusedScore,
                Metadata = r.Chunk.Metadata
            })
            .ToList();
    }

    /// <summary>
    /// Marks entry as memorized with identity if available, falling back to dimension-only.
    /// </summary>
    private void MarkMemorizedWithIdentity(VaultEntry entry, int chunkCount)
    {
        if (_embeddingService is not null)
        {
            entry.MarkMemorized(chunkCount, _embeddingService.GetIdentity());
        }
        else
        {
            entry.MarkMemorized(chunkCount);
        }
    }

    private async Task<IndexOutcome> ChunkAndIndexAsync(
        VaultEntry entry,
        MemorizeOptions options,
        ContextGeneration contextGeneration,
        CancellationToken ct)
    {
        // Indexing a document REPLACES the rows previously written for it, and the replacement is
        // a SWAP: the previous rows are identified now and deleted only once the new ones are
        // durably written. Both callers reach the backends through here, so this belongs here
        // rather than at either call site.
        //
        // Chunk ids are derived from the document and the passage (ChunkIdentity), so the two
        // generations are compared as ID SETS rather than replaced wholesale: a passage that did not
        // change is written again under its old id - an update in place, one row before and after -
        // and only previous minus attempted is superseded, only attempted minus previous is rolled
        // back. Rows written before this scheme carry store-minted ids, so the first memorize after
        // upgrading finds nothing in common and replaces everything, exactly as before. No migration.
        //
        // Why not delete first (as this did until 2026-08-06): the delete succeeded, then indexing
        // threw - an embedding failure on ONE chunk fails the whole batch - and the document was
        // left with no index at all. It had been searchable before the re-index and was not after,
        // with nothing in the result reporting a loss rather than a failure. Measured in operation:
        // a 279-document re-index, one failure, that document went from 8 chunks to 0.
        //
        // Why not simply move the delete after indexing: it must still happen, or re-memorizing an
        // already-indexed file appends a second copy of every chunk instead of replacing it, and
        // each subsequent memorize adds another - search then returns the same chunk N times and
        // spends the caller's result budget on repeats. Capturing the old ids up front and deleting
        // exactly those keeps that guarantee while making failure non-destructive.
        //
        // The trade-off this accepts: between the write and the delete, both generations are
        // present, so a concurrent search can see a document twice. That window is bounded by the
        // indexing step, and returning a document twice for a few seconds is recoverable in a way
        // that returning it zero times is not.
        //
        // A RESUMED run is deliberately excluded. StartFromChunkIndex means chunks 0..N are already
        // committed and this run writes only the tail, so the previous rows are not a superseded
        // generation - they are this generation's prefix. Deleting them (before OR after) discards
        // the very rows the resume is built on: the unconditional delete that used to sit here
        // wiped them, and IndexChunksResumableAsync then skipped rewriting them on the recorded
        // assumption that they existed, so every host restart mid-indexing silently truncated the
        // document to its tail.
        var isResumedRun = options.CheckpointCallback != null && options.StartFromChunkIndex >= 0;

        IReadOnlyList<string> previousChunkIds = [];
        if (!isResumedRun)
        {
            previousChunkIds = await GetIndexedChunkIdsAsync(entry, ct);
        }

        // Get all vault content (refined.md + append-text.md + qa.md)
        var vaultContent = await _storage.GetAllVaultContentAsync(entry, ct);
        var combinedContent = vaultContent.GetCombinedContent();

        // Described images are content in their own right — an image-only document has no text but
        // is not empty. They are appended after the text chunks so text chunk indices (and with
        // them the resumable checkpoint) stay stable.
        var imageChunks = await BuildImageChunksAsync(entry, options, ct);

        if (string.IsNullOrWhiteSpace(combinedContent) && imageChunks.Count == 0)
        {
            // Replacing content with nothing is still a replacement: a document whose content is
            // gone must lose its rows rather than keep serving text it no longer has. This is the
            // one path where the swap commits with no new generation to swap to.
            await DeleteChunksAsync(previousChunkIds, GraphPartitionFor(options.GraphRAGOptions), ct);
            LogNoContentToIndex(_logger, entry.SourcePath);
            return new IndexOutcome(0, 0);
        }

        // Where stretches of the refined text came from (pages, time ranges). The offsets index the text extraction
        // produced; refined.md can be edited by hand, so they are trusted only while it still hashes the same. Refined
        // content is the first part of the combined content, so the offsets hold there; appended notes and Q&A carry
        // no location.
        var spans = await GetTrustedSpansAsync(entry, vaultContent.RefinedContent, ct);

        // Chunk the content
        IReadOnlyList<ContentChunk> chunks = [];

        if (string.IsNullOrWhiteSpace(combinedContent))
        {
            // Image-only document: nothing to chunk, the descriptions carry the meaning.
        }
        else if (_chunker != null)
        {
            // Resolve per-format strategy override from FormatStrategies
            var effectiveStrategy = options.Strategy;
            if (_options.Chunking.FormatStrategies.Count > 0)
            {
                var ext = Path.GetExtension(entry.SourcePath)?.ToLowerInvariant();
                if (!string.IsNullOrEmpty(ext) &&
                    _options.Chunking.FormatStrategies.TryGetValue(ext, out var formatStrategy))
                {
                    effectiveStrategy = formatStrategy;
                }
            }

            var chunkingOptions = new ChunkingOptions
            {
                MaxChunkSize = options.MaxChunkSize,
                OverlapSize = options.OverlapSize,
                Strategy = effectiveStrategy,
                Language = options.Language
            };
            chunks = await _chunker.ChunkAsync(combinedContent, spans, chunkingOptions, ct);
        }
        else
        {
            chunks = ChunkFallback(combinedContent, options.MaxChunkSize).Select(t => new ContentChunk(t)).ToList();
        }

        // Identity is taken from the chunker's raw output, before enrichment prepends a context or
        // security sanitizes: those change the wording of a passage, not which passage it is.
        var textIds = ChunkIdentity.ForTexts(entry.FilepathHash, chunks.Select(c => c.Text).ToList());
        // A table piece is tied to the stored table at its position only when the chunked content holds exactly the
        // tables extraction stored (hand edits or tables in notes would shift the positions).
        var storedTables = chunks.Any(c => c.Table is not null) ? await _storage.GetTablesAsync(entry, ct) : [];
        var tablesInChunks = chunks.Where(c => c.Table is not null).Select(c => c.Table!.TableIndex).Distinct().Count();
        var tables = storedTables.Count > 0 && storedTables.Count == tablesInChunks ? storedTables : null;
        var textChunks = chunks.Select((c, i) => new VaultChunk(textIds[i], c.Text, TextChunkMetadata(c, tables))).ToList();

        // What this document's rows hold now, by id: a stored context can be reused and a row identical to its
        // replacement need not be embedded again. A resumed run writes only its tail and compares nothing.
        var stored = isResumedRun
            ? EmptyStoredChunks
            : await GetStoredChunksAsync(textChunks.Select(c => c.Id).Concat(imageChunks.Select(c => c.Id)).ToList(), ct);

        var contextPending = false;
        if (SupportsContextualEnrichment)
        {
            // Text chunks only — image-description chunks are already a description, not a passage of the document.
            (textChunks, contextPending) = await ApplyContextualEnrichmentAsync(
                textChunks, combinedContent, entry.SourcePath, stored, contextGeneration == ContextGeneration.Generate, ct);
        }

        var allChunks = textChunks
            .Concat(imageChunks)
            .ToList();

        if (_ragSecurityPipeline != null)
        {
            allChunks = await ApplyRagSecurityAsync(allChunks, entry.SourcePath, ct);
        }

        LogCreatedChunks(_logger, allChunks.Count, combinedContent.Length);
        if (imageChunks.Count > 0)
        {
            LogCreatedImageChunks(_logger, imageChunks.Count, entry.SourcePath);
        }

        // Index to vector store
        var counters = new IndexCounters();
        if (_vectorStore != null && _embeddingService != null)
        {
            IReadOnlyList<DocumentChunk> written;

            // Every id this run tries to write, recorded before the write is attempted. This is what
            // the rollback deletes: the run already knows what it produced, so undoing it must not
            // be a discovery problem (see TryRollbackAsync).
            var attemptedChunkIds = new List<string>();
            try
            {
                written = await IndexChunksAsync(entry, allChunks, options, attemptedChunkIds, stored, counters, ct);
            }
            catch (Exception ex)
            {
                // The scale of what failed is known HERE and nowhere else — by the time the
                // exception reaches MemorizeAsync, the chunk list is out of scope and all that
                // survives is a message. Attaching it now is what lets the caller answer
                // "how much was it trying to index?" without parsing prose.
                // Roll the half-written generation back so the previous one is the only one left.
                // Without this the failure would leave both generations present and search would
                // return a mix of old chunks and whichever new ones landed before the throw.
                // Only the ids this run ADDED are removed: an id both generations share was updated
                // in place with the same passage, and deleting it would punch a hole in the
                // previous generation.
                //
                // Rollback is best-effort by construction: if it fails there is nothing further to
                // try, and letting its exception escape would replace the real indexing failure
                // with a cleanup failure - the caller would be told the wrong thing went wrong.
                var orphaned = await TryRollbackAsync(
                    entry, attemptedChunkIds.Except(previousChunkIds, StringComparer.Ordinal).ToList());

                // The rollback just removed rows the resume checkpoint may claim as committed (the
                // per-chunk path advances it after every store). Rewind it to where this run started,
                // or the retry resumes past chunks that are no longer there and the document is
                // silently truncated - the failure the checkpoint exists to prevent.
                await TryRewindCheckpointAsync(entry, options);

                // A cancelled run is rolled back like a failed one, but it is reported as the cancellation it is:
                // wrapping it would turn a host shutdown into an indexing failure of this document.
                if (ex is OperationCanceledException && ct.IsCancellationRequested)
                {
                    throw;
                }

                // Carried on the exception rather than in a field: this method's caller is an async
                // frame above, and mutable state written here does not travel back up to it.
                // The detail rides with the failure it describes.
                throw new IndexingFailedException(
                    new IndexingFailure
                    {
                        Stage = IndexingStage.Indexing,
                        ChunkCount = allChunks.Count,
                        ContentLength = allChunks.Sum(c => c.Content?.Length ?? 0),
                        ExceptionType = ex.GetType().Name,
                        OrphanedChunkIds = orphaned
                    },
                    ex);
            }

            // Only now is the previous generation superseded - the part of it this run did not
            // write again.
            var supersededChunkIds = previousChunkIds.Except(attemptedChunkIds, StringComparer.Ordinal).ToList();
            await DeleteChunksAsync(supersededChunkIds, GraphPartitionFor(options.GraphRAGOptions), ct);

            if (written.Count == 0 && supersededChunkIds.Count > 0)
            {
                LogRemovedChunks(_logger, entry.FilepathHash);
            }
        }
        else
        {
            LogNoVectorStoreSkipIndexing(_logger);
        }

        return new IndexOutcome(allChunks.Count, combinedContent.Length, contextPending, counters.ReEmbedded, counters.Kept);
    }

    /// <summary>
    /// Ids of the rows currently indexed for this entry on every leg, captured before a re-index so
    /// that the previous generation can be dropped after the new one is durably written rather than
    /// before. Each leg answers for its own rows: the union is what the swap supersedes, and
    /// <see cref="DeleteChunksAsync"/> offers every id to both legs, where an id a leg never held is
    /// a no-op.
    /// </summary>
    /// <remarks>
    /// Until 0.27.0 this read the vector store alone, on the stated assumption that both legs key
    /// their rows by the same chunk id. Nothing enforced that: the SQLite stores minted their own
    /// ids before FluxIndex 0.36.2 while the keyword index kept the pipeline's, so no keyword row
    /// written before then has a vector-side twin, and deleting on the keyword leg with the vector
    /// store's ids removed nothing — every re-index left the previous keyword generation searchable
    /// beside the new one. Measured on a real vault: 167 keyword rows, 108 vector rows, 0 ids in
    /// common. Enumerating the keyword leg itself is what lets an ordinary re-index heal it.
    /// </remarks>
    private async Task<IReadOnlyList<string>> GetIndexedChunkIdsAsync(VaultEntry entry, CancellationToken ct)
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);

        if (_vectorStore != null)
        {
            // Ids only: this call resolves which rows to supersede and uses nothing else about them.
            // Fetching each chunk in full made the response grow with the document, which is how an
            // ordinary few-MB file could exceed a store's transport limit here (FluxIndex 0.35.0).
            ids.UnionWith(await _vectorStore.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, ct));
        }

        if (_keywordSearchService != null)
        {
            ids.UnionWith(await _keywordSearchService.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, ct));
        }

        return [.. ids];
    }

    /// <inheritdoc />
    public async Task<IndexRowCounts> GetIndexRowCountsAsync(IReadOnlyList<VaultEntry> entries, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(entries);

        if (_vectorStore == null && _keywordSearchService == null)
        {
            return new IndexRowCounts(null, null, 0);
        }

        var vectorRows = _vectorStore == null ? (int?)null : 0;
        var keywordRows = _keywordSearchService == null ? (int?)null : 0;
        var mismatched = 0;

        foreach (var entry in entries)
        {
            ct.ThrowIfCancellationRequested();

            IReadOnlyList<string>? vectorIds = null;
            if (_vectorStore != null)
            {
                vectorIds = await _vectorStore.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, ct);
                vectorRows += vectorIds.Count;
            }

            IReadOnlyList<string>? keywordIds = null;
            if (_keywordSearchService != null)
            {
                keywordIds = await _keywordSearchService.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, ct);
                keywordRows += keywordIds.Count;
            }

            if (vectorIds != null && keywordIds != null
                && !vectorIds.ToHashSet(StringComparer.Ordinal).SetEquals(keywordIds))
            {
                mismatched++;
            }
        }

        return new IndexRowCounts(vectorRows, keywordRows, mismatched);
    }

    /// <inheritdoc />
    public Task<KeywordIndexRepairResult> RepairKeywordIndexAsync(IReadOnlyList<VaultEntry> entries, CancellationToken ct = default)
        => RepairKeywordIndexAsync(entries, KeywordIndexRepairScope.Mismatched, ct);

    /// <inheritdoc />
    public async Task<KeywordIndexRepairResult> RepairKeywordIndexAsync(IReadOnlyList<VaultEntry> entries, KeywordIndexRepairScope scope, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(entries);

        if (_keywordSearchService == null)
        {
            throw new InvalidOperationException(
                "No keyword index is registered, so there is no keyword leg to repair. Register an IKeywordSearchService " +
                "before calling RepairKeywordIndexAsync, or check IVaultPipeline.SupportsKeywordIndex first.");
        }

        if (_vectorStore == null)
        {
            throw new InvalidOperationException(
                "The keyword index is rebuilt from the vector store, and no IVectorStore is registered.");
        }

        var repaired = 0;
        var written = 0;
        var removed = 0;
        var failures = new List<KeywordIndexRepairFailure>();

        foreach (var entry in entries)
        {
            ct.ThrowIfCancellationRequested();

            // One entry's failure does not stop the others: a rebuild of a large vault is long, and stopping
            // partway leaves the rest written the old way with no record of where it stopped. Each entry is
            // write-then-remove, so a failed one keeps its previous rows and can be retried alone.
            try
            {
                var outcome = await RepairKeywordLegAsync(entry, scope, ct);
                if (outcome is { } done)
                {
                    repaired++;
                    written += done.Written;
                    removed += done.Removed;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !ct.IsCancellationRequested)
            {
                failures.Add(new KeywordIndexRepairFailure(entry.SourcePath, entry.FilepathHash, ex));
                LogKeywordLegRepairFailed(_logger, ex, entry.FilepathHash);
            }
        }

        var result = new KeywordIndexRepairResult(entries.Count, repaired, written, removed);
        return failures.Count == 0 ? result : throw new KeywordIndexRepairException(result, failures);
    }

    /// <summary>Rewrites one entry's keyword leg; <c>null</c> when <paramref name="scope"/> leaves it alone.</summary>
    private async Task<(int Written, int Removed)?> RepairKeywordLegAsync(VaultEntry entry, KeywordIndexRepairScope scope, CancellationToken ct)
    {
        var vectorIds = await _vectorStore!.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, ct);
        var keywordIds = await _keywordSearchService!.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, ct);
        var vectorIdSet = vectorIds.ToHashSet(StringComparer.Ordinal);
        // Legs that agree are only "nothing to do" when the question is drift. After an analyzer or
        // field-set change every row is present under the right id and every one is written the old
        // way - the id comparison cannot see that, so All rewrites them regardless.
        if (scope == KeywordIndexRepairScope.Mismatched && vectorIdSet.SetEquals(keywordIds))
        {
            return null;
        }

        // Write before removing, as a re-index swap does: the entry keeps answering keyword searches
        // throughout, and a failure partway leaves the previous rows rather than none. The vector store
        // returns each chunk with its metadata, so the keyword fields the current configuration reads
        // (title, file name, ...) are rebuilt along with the body.
        var chunks = (await _vectorStore!.GetByDocumentIdAsync(entry.FilepathHash, ct)).ToList();
        if (chunks.Count > 0)
        {
            await _keywordSearchService!.IndexChunksAsync(chunks, ct);
        }

        var stale = keywordIds.Where(id => !vectorIdSet.Contains(id)).ToList();
        if (stale.Count > 0)
        {
            await _keywordSearchService!.DeleteChunksAsync(stale, ct);
        }

        LogRepairedKeywordLeg(_logger, entry.FilepathHash, chunks.Count, stale.Count);
        return (chunks.Count, stale.Count);
    }

    /// <summary>
    /// Deletes the given chunk ids from every backend they were written to. Used for both halves of
    /// the swap: dropping the superseded generation on success, and dropping the partial one on
    /// failure.
    /// </summary>
    /// <param name="chunkIds">The chunks to delete from every leg.</param>
    /// <param name="graphPartition">
    /// The GraphRAG partition the chunks were built into, so the graph leg forgets them too; null when no graph was built
    /// from them (a rolled-back partial generation — the graph is built only after the swap commits).
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    private async Task DeleteChunksAsync(IReadOnlyCollection<string> chunkIds, string? graphPartition, CancellationToken ct)
    {
        if (chunkIds.Count == 0)
        {
            return;
        }

        if (_vectorStore != null)
        {
            foreach (var chunkId in chunkIds)
                await _vectorStore.DeleteAsync(chunkId, ct);
        }

        // One call for the whole set: a relational keyword index rewrites every shared term row the
        // chunks hold, and deleting chunk by chunk rewrote those rows once per chunk.
        if (_keywordSearchService != null)
            await _keywordSearchService.DeleteChunksAsync(chunkIds, ct);

        // The graph leg answers for its own rows too: communities, entity-to-chunk links and relationships derived from
        // these chunks would otherwise outlive them (FluxIndex ForgetChunksAsync is a no-op without a graph store).
        if (graphPartition != null && _graphRAGService != null)
            await _graphRAGService.ForgetChunksAsync(chunkIds, graphPartition, ct);
    }

    /// <summary>The graph partition a memorize requesting <paramref name="requested"/> builds into (see <see cref="ResolveGraphRagOptions"/>).</summary>
    private string GraphPartitionFor(GraphRAGBuildOptions? requested) =>
        ResolveGraphRagOptions(requested)?.Partition ?? GraphPartition.Default;

    /// <summary>
    /// Drops whatever the failed run managed to write, leaving the previous generation as the only
    /// one indexed. Swallows its own failures on purpose — see the call site.
    /// </summary>
    /// <param name="attemptedChunkIds">
    /// Every id this run tried to write that the previous generation did not own, recorded before
    /// each write was attempted. This — not a re-read of the store — is what the rollback deletes.
    /// </param>
    /// <returns>
    /// The ids the rollback could not remove, so the caller can report that the store still holds
    /// them. Empty when the rollback completed.
    /// </returns>
    /// <remarks>
    /// <para>
    /// This used to discover what to undo by re-reading the document's ids and subtracting the
    /// previous generation. Two things were wrong with that, and they are why the parameter changed.
    /// </para>
    /// <para>
    /// It could not run at all when the read failed. The rollback exists precisely because something
    /// has already gone wrong, so it is the path most likely to meet a store that is contended or
    /// refusing calls — and a single failed lookup left BOTH generations in the store with nothing
    /// recording which points belonged to which. Reported from a deployment where a keyword-index
    /// deadlock started the rollback and the vector-store lookup then exceeded a transport limit.
    /// The run already knew every id it produced; asking the store to tell it back converted
    /// information it held into information it had to request.
    /// </para>
    /// <para>
    /// It was also wrong on a resumed run. There the previous generation is deliberately empty —
    /// chunks 0..N are this generation's committed prefix, not a superseded one — so "everything
    /// present, minus nothing" meant the rollback deleted the prefix the resume is built on, while
    /// the checkpoint still claimed those chunks were committed. The next resume then wrote only the
    /// tail: the same silent truncation the swap above was introduced to end, reached through the
    /// failure path instead. Deleting only what THIS run attempted cannot touch the prefix.
    /// </para>
    /// </remarks>
    /// <param name="entry">The entry whose partial generation is being rolled back.</param>
    private async Task<IReadOnlyList<string>> TryRollbackAsync(
        VaultEntry entry,
        IReadOnlyList<string> attemptedChunkIds)
    {
        if (attemptedChunkIds.Count == 0)
        {
            return [];
        }

        // Deleted one at a time, continuing past a failure: a single id the store refuses must not
        // orphan every id after it. What could not be removed is returned rather than logged only,
        // so the failure the caller reports can say the store is still holding rows.
        var orphaned = new List<string>();

        foreach (var chunkId in attemptedChunkIds)
        {
            try
            {
                // CancellationToken.None throughout: a cancelled indexing run is exactly when the
                // rollback must still happen, and a token that is already cancelled would abort it.
                await DeleteChunksAsync([chunkId], graphPartition: null, CancellationToken.None);
            }
            catch (Exception ex)
            {
                orphaned.Add(chunkId);
                LogRollbackFailed(_logger, entry.SourcePath, ex);
            }
        }

        return orphaned;
    }

    /// <summary>
    /// Puts the resume checkpoint back to the value this run started from, after a rollback removed
    /// the rows the run had checkpointed. Best-effort like the rollback itself: a failure here is
    /// logged rather than allowed to replace the indexing failure being reported.
    /// </summary>
    private async Task TryRewindCheckpointAsync(VaultEntry entry, MemorizeOptions options)
    {
        if (options.CheckpointCallback == null)
        {
            return;
        }

        try
        {
            // CancellationToken.None for the same reason as the rollback: a cancelled run is exactly
            // when the checkpoint must not be left pointing past rows that were just removed.
            await options.CheckpointCallback(options.StartFromChunkIndex, CancellationToken.None);
        }
        catch (Exception ex)
        {
            LogCheckpointRewindFailed(_logger, entry.SourcePath, options.StartFromChunkIndex, ex);
        }
    }

    private async Task<IReadOnlyList<DocumentChunk>> IndexChunksAsync(
        VaultEntry entry,
        IReadOnlyList<VaultChunk> chunks,
        MemorizeOptions options,
        ICollection<string> attemptedChunkIds,
        IReadOnlyDictionary<string, DocumentChunk> stored,
        IndexCounters counters,
        CancellationToken ct)
    {
        // A row identical to its replacement is left in place rather than embedded and written again. Not with GraphRAG:
        // the graph is rebuilt from the written chunks, and a kept row carries no embedding to hand it.
        var keepUnchanged = stored.Count > 0 && !(options.EnableGraphRAG ?? (_graphRAGService != null));
        var keep = keepUnchanged ? stored : EmptyStoredChunks;

        // Branch: when CheckpointCallback is set (job-queue path), use per-chunk processing
        // for crash-resilient resume. Otherwise, use the existing batch path (faster for the
        // common case of one-shot direct API calls).
        IReadOnlyList<DocumentChunk> indexedChunks;
        if (options.CheckpointCallback != null)
        {
            indexedChunks = await IndexChunksResumableAsync(entry, chunks, options.StartFromChunkIndex, options.CheckpointCallback, attemptedChunkIds, keep, counters, ct);
        }
        else
        {
            indexedChunks = await IndexChunksBatchAsync(entry, chunks, attemptedChunkIds, keep, counters, ct);
        }

        // Keyword indexing — the third search backend alongside vector and graph. Unconditional
        // (like vector storage) rather than option-gated like GraphRAG: it is a core leg of hybrid
        // retrieval, not an optional enrichment, so "a keyword service is registered" is itself the
        // signal to use it.
        await IndexKeywordsIfConfiguredAsync(entry, indexedChunks, ct);

        // GraphRAG indexing — keeps the FileVault memorize path at parity with the SDK direct-index
        // path (Indexer.IndexAsync), which builds the entity graph after vector-store ingestion.
        await BuildGraphRagIfEnabledAsync(entry, indexedChunks, options, ct);

        return indexedChunks;
    }

    /// <summary>
    /// Writes the just-indexed chunks to the keyword index when one is registered. Without this,
    /// an ingestion-only pipeline like this one never populates the keyword leg of hybrid search —
    /// only FluxIndex's own <c>Indexer</c> does, per <c>INativeHybridSearch</c>'s documented
    /// population-gap warning.
    /// </summary>
    private async Task IndexKeywordsIfConfiguredAsync(
        VaultEntry entry,
        IReadOnlyList<DocumentChunk> indexedChunks,
        CancellationToken ct)
    {
        if (_keywordSearchService == null || indexedChunks.Count == 0)
        {
            return;
        }

        await _keywordSearchService.IndexChunksAsync(indexedChunks, ct);
        LogIndexedKeywordChunks(_logger, indexedChunks.Count, entry.FilepathHash);
    }

    /// <summary>
    /// Builds the GraphRAG entity index for the just-indexed chunks when GraphRAG is enabled.
    /// Mirrors the enable semantics of <c>IndexingOptions.EnableGraphRAG</c> on the SDK path:
    /// null = auto when a service is registered, true = force (throw if absent), false = off.
    /// </summary>
    private async Task BuildGraphRagIfEnabledAsync(
        VaultEntry entry,
        IReadOnlyList<DocumentChunk> indexedChunks,
        MemorizeOptions options,
        CancellationToken ct)
    {
        var enableGraphRAG = options.EnableGraphRAG ?? (_graphRAGService != null);
        if (!enableGraphRAG)
        {
            return;
        }

        if (_graphRAGService == null)
        {
            throw new InvalidOperationException(
                "GraphRAG is enabled but IGraphRAGService is not registered. " +
                "Register it via AddFullGraphRAG(), or set MemorizeOptions.EnableGraphRAG = false.");
        }

        // Nothing newly indexed this run (e.g. resumable path resumed past the last chunk) — skip.
        if (indexedChunks.Count == 0)
        {
            return;
        }

        LogBuildingGraphRagIndex(_logger, entry.FilepathHash, indexedChunks.Count);
        await _graphRAGService.BuildIndexAsync(indexedChunks, ResolveGraphRagOptions(options.GraphRAGOptions), ct);
        LogGraphRagIndexBuilt(_logger, entry.FilepathHash);
    }

    /// <summary>
    /// The GraphRAG build options a memorize runs with: a tenant-scoped vault writes its graph into the partition named
    /// by its <see cref="FileVaultOptions.VaultId"/> — the same key its chunks carry as <c>vault_id</c> on the vector and
    /// keyword legs — so vaults sharing one graph store neither merge each other's entities nor see each other's
    /// communities. The caller's options object is copied, never changed. A vault without a <c>VaultId</c> passes the
    /// options through as given.
    /// </summary>
    internal GraphRAGBuildOptions? ResolveGraphRagOptions(GraphRAGBuildOptions? requested)
    {
        if (string.IsNullOrEmpty(_options.VaultId))
        {
            return requested;
        }

        var partition = _options.VaultId;
        if (requested is null)
        {
            return new GraphRAGBuildOptions { Partition = partition };
        }

        if (requested.Partition == partition)
        {
            return requested;
        }

        if (requested.Partition != GraphPartition.Default)
        {
            throw new ArgumentException(
                $"MemorizeOptions.GraphRAGOptions.Partition is '{requested.Partition}', but this vault writes its graph into the partition of its VaultId '{partition}'.",
                nameof(requested));
        }

        return requested.WithPartition(partition);
    }

    /// <summary>
    /// Builds the store-bound chunk for a <see cref="VaultChunk"/>, keyed under the id the pipeline
    /// derived for it rather than the fresh GUID <see cref="DocumentChunk.Create"/> mints. The store
    /// keeps that id (IVectorStore contract: the caller's id is the row key), which is what makes a
    /// re-memorize an update and lets the generation swap work on id sets.
    /// </summary>
    private static DocumentChunk CreateDocumentChunk(string documentId, VaultChunk source, int chunkIndex, int totalChunks)
    {
        var chunk = DocumentChunk.Create(
            documentId: documentId,
            content: source.Content,
            chunkIndex: chunkIndex,
            totalChunks: totalChunks);
        chunk.Id = source.Id;
        return chunk;
    }

    /// <summary>
    /// Batch indexing path: one embedding call + one store call for all chunks.
    /// Used by direct callers of MemorizeAsync (no checkpoint hooks).
    /// </summary>
    /// <returns>The embedded chunks that were stored, for downstream GraphRAG indexing.</returns>
    private async Task<IReadOnlyList<DocumentChunk>> IndexChunksBatchAsync(
        VaultEntry entry,
        IReadOnlyList<VaultChunk> chunks,
        ICollection<string> attemptedChunkIds,
        IReadOnlyDictionary<string, DocumentChunk> keep,
        IndexCounters counters,
        CancellationToken ct)
    {
        var documentId = entry.FilepathHash;

        // Create document chunks
        var documentChunks = new List<DocumentChunk>();
        for (var i = 0; i < chunks.Count; i++)
        {
            var chunk = CreateDocumentChunk(documentId, chunks[i], i, chunks.Count);
            ApplyChunkMetadata(chunk, entry, chunks[i].Metadata);
            documentChunks.Add(chunk);
        }

        // Only rows that differ from what is stored are embedded and written.
        var changed = documentChunks.Where(c => !IsUnchanged(c, keep)).ToList();
        counters.Kept += documentChunks.Count - changed.Count;
        counters.ReEmbedded += changed.Count;

        if (changed.Count > 0)
        {
            var embeddingList = (await _embeddingService!.GenerateEmbeddingsBatchAsync(changed.Select(c => c.Content).ToList(), ct)).ToList();
            if (embeddingList.Count != changed.Count)
            {
                throw new InvalidOperationException(
                    $"Embedding count mismatch: expected {changed.Count}, got {embeddingList.Count}");
            }

            for (var i = 0; i < changed.Count; i++)
            {
                changed[i].SetEmbedding(embeddingList[i]);
            }
        }

        // Recorded before the write, not after: deleting an id that never landed is a no-op, while
        // missing one that did leaves a row nobody will ever look for again. The asymmetry decides
        // the order.
        foreach (var chunk in documentChunks)
        {
            attemptedChunkIds.Add(chunk.Id);
        }

        // Store in vector store
        var storedCount = changed.Count == 0 ? 0 : (await _vectorStore!.StoreBatchAsync(changed, ct)).Count();
        LogIndexedChunks(_logger, storedCount, documentId);
        if (changed.Count < documentChunks.Count)
        {
            LogKeptUnchangedChunks(_logger, documentChunks.Count - changed.Count, documentChunks.Count, documentId);
        }

        return documentChunks;
    }

    /// <summary>
    /// Resumable indexing path: per-chunk embed + store + checkpoint.
    /// Used by VaultBackgroundService so that host restarts can recover stuck jobs
    /// from the last committed chunk instead of restarting from chunk 0.
    /// Trade-off: ~5-10% slower than batch for the common no-crash case, but recovery
    /// becomes O(remaining_chunks) instead of O(total_chunks).
    /// </summary>
    /// <returns>
    /// The chunks newly embedded and stored in this run, for downstream GraphRAG indexing.
    /// On a resumed run this excludes already-committed chunks (skipped), so GraphRAG sees only
    /// the remaining tail; a full re-index (RefreshAsync) rebuilds the complete graph.
    /// </returns>
    private async Task<IReadOnlyList<DocumentChunk>> IndexChunksResumableAsync(
        VaultEntry entry,
        IReadOnlyList<VaultChunk> chunks,
        int startFromChunk,
        Func<int, CancellationToken, Task> checkpointCallback,
        ICollection<string> attemptedChunkIds,
        IReadOnlyDictionary<string, DocumentChunk> keep,
        IndexCounters counters,
        CancellationToken ct)
    {
        var documentId = entry.FilepathHash;
        var processedChunks = new List<DocumentChunk>();
        var skippedCount = 0;

        for (int i = 0; i < chunks.Count; i++)
        {
            ct.ThrowIfCancellationRequested();

            if (i <= startFromChunk)
            {
                // Already committed in a previous run; the corresponding row exists in vector_chunks.
                skippedCount++;
                continue;
            }

            var chunk = CreateDocumentChunk(documentId, chunks[i], i, chunks.Count);
            ApplyChunkMetadata(chunk, entry, chunks[i].Metadata);

            if (IsUnchanged(chunk, keep))
            {
                // The stored row is this row: nothing to embed or write. It is still this generation's row.
                attemptedChunkIds.Add(chunk.Id);
                await checkpointCallback(i, ct);
                processedChunks.Add(chunk);
                counters.Kept++;
                continue;
            }

            // Embed single chunk
            chunk.SetEmbedding(await _embeddingService!.GenerateEmbeddingAsync(chunks[i].Content, ct));
            counters.ReEmbedded++;

            // Recorded before the write for the same reason as the batch path: a chunk that lands
            // and is not recorded is a row the rollback will never delete.
            attemptedChunkIds.Add(chunk.Id);

            // Store single chunk (1-element batch — uses the same transactional path).
            // On commit success, the chunk row is durably in vector_chunks before we update the checkpoint.
            await _vectorStore!.StoreBatchAsync([chunk], ct);

            // Persist checkpoint AFTER the store transaction commits.
            // If the host crashes between StoreBatchAsync returning and UpdateCheckpointAsync
            // succeeding, recovery sees stale checkpoint (chunk i in DB but checkpoint = i-1)
            // and will redo chunk i — at most 1 chunk wasted, acceptable.
            await checkpointCallback(i, ct);
            processedChunks.Add(chunk);
        }

        if (skippedCount > 0)
            LogIndexedChunksResumable(_logger, processedChunks.Count, skippedCount, documentId);
        else
            LogIndexedChunks(_logger, processedChunks.Count, documentId);

        return processedChunks;
    }

    private async Task<string> ExtractFallbackAsync(string sourcePath, CancellationToken ct)
    {
        var extension = Path.GetExtension(sourcePath).ToLowerInvariant();

        // Check if file is readable as text (documents + code + custom)
        if (IsReadableTextExtension(extension))
        {
            return await File.ReadAllTextAsync(sourcePath, ct);
        }

        return $"[Content extraction required for {extension} files. Install FileFlux for full support.]";
    }

    /// <summary>
    /// Determines if a file extension is a readable text file (documents + code).
    /// Use this for fallback extraction.
    /// </summary>
    /// <param name="extension">File extension with leading dot (e.g., ".cs")</param>
    /// <returns>True if the file can be read as text</returns>
    public bool IsReadableTextExtension(string extension)
    {
        if (string.IsNullOrEmpty(extension))
            return false;

        // Check documents and code extensions
        if (DocumentExtensions.Contains(extension) || CodeExtensions.Contains(extension))
            return true;

        // Check user-configured additional extensions
        if (_options.AdditionalTextExtensions.Count > 0 &&
            _options.AdditionalTextExtensions.Contains(extension))
            return true;

        return false;
    }

    /// <summary>
    /// Determines if a file extension is suitable for Memorize (vector embedding).
    /// Only document files are recommended; code files should use file-read instead.
    /// </summary>
    /// <param name="extension">File extension with leading dot (e.g., ".md")</param>
    /// <returns>True if the file is recommended for vector embedding</returns>
    public static bool IsDocumentExtension(string extension)
    {
        if (string.IsNullOrEmpty(extension))
            return false;

        return DocumentExtensions.Contains(extension);
    }

    /// <summary>
    /// Determines if a file extension is a code/config file (read-only, not for Memorize).
    /// </summary>
    /// <param name="extension">File extension with leading dot (e.g., ".cs")</param>
    /// <returns>True if the file is a code/config file</returns>
    public static bool IsCodeExtension(string extension)
    {
        if (string.IsNullOrEmpty(extension))
            return false;

        return CodeExtensions.Contains(extension);
    }

    /// <summary>
    /// Gets all readable text extensions (documents + code + additional).
    /// </summary>
    public IReadOnlySet<string> GetAllReadableExtensions()
    {
        var all = new HashSet<string>(DocumentExtensions, StringComparer.OrdinalIgnoreCase);
        foreach (var ext in CodeExtensions)
            all.Add(ext);
        foreach (var ext in _options.AdditionalTextExtensions)
            all.Add(ext);
        return all;
    }

    /// <summary>
    /// Splits on line boundaries when it can and inside a line when it must, so the bound holds for
    /// every input. Grouping by line alone left a single long line as one oversized chunk — text
    /// with no newlines (a generated image description, a one-paragraph document) passed through
    /// unbounded, which defeats the purpose of the limit at exactly the input most likely to exceed
    /// an embedding model's context window.
    /// </summary>
    private static List<string> ChunkFallback(string content, int maxChunkSize)
    {
        if (maxChunkSize <= 0)
        {
            return [content];
        }

        var chunks = new List<string>();
        var currentChunk = new List<string>();
        var currentLength = 0;

        void Flush()
        {
            if (currentChunk.Count == 0) return;
            chunks.Add(string.Join('\n', currentChunk));
            currentChunk.Clear();
            currentLength = 0;
        }

        foreach (var line in content.Split('\n'))
        {
            // A line that cannot fit in any chunk is emitted on its own, hard-split. Anything
            // buffered goes out first so ordering is preserved.
            if (line.Length > maxChunkSize)
            {
                Flush();
                for (var offset = 0; offset < line.Length; offset += maxChunkSize)
                {
                    chunks.Add(line.Substring(offset, Math.Min(maxChunkSize, line.Length - offset)));
                }
                continue;
            }

            if (currentLength + line.Length > maxChunkSize)
            {
                Flush();
            }

            currentChunk.Add(line);
            currentLength += line.Length + 1;
        }

        Flush();
        return chunks;
    }

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting memorize for {SourcePath}")]
    private static partial void LogStartingMemorize(ILogger logger, string sourcePath);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Backing up existing QA content ({Length} chars)")]
    private static partial void LogBackupQaContent(ILogger logger, int length);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Backing up existing append-text ({Length} chars)")]
    private static partial void LogBackupAppendText(ILogger logger, int length);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Restored QA content")]
    private static partial void LogRestoredQaContent(ILogger logger);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Restored append-text content")]
    private static partial void LogRestoredAppendText(ILogger logger);
    [LoggerMessage(Level = LogLevel.Information, Message = "Memorize completed for {SourcePath}: {ChunkCount} chunks in {Duration:F2}s")]
    private static partial void LogMemorizeCompleted(ILogger logger, string sourcePath, int chunkCount, double duration);
    [LoggerMessage(Level = LogLevel.Error, Message = "Memorize failed for {SourcePath}")]
    private static partial void LogMemorizeFailed(ILogger logger, Exception exception, string sourcePath);
    [LoggerMessage(Level = LogLevel.Warning, Message = "RAG security pipeline blocked {BlockedCount} chunk(s) from indexing for {SourcePath}")]
    private static partial void LogRagSecurityBlockedChunks(ILogger logger, int blockedCount, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Starting refresh for {SourcePath}")]
    private static partial void LogStartingRefresh(ILogger logger, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Refresh completed for {SourcePath}: {ChunkCount} chunks in {Duration:F2}s")]
    private static partial void LogRefreshCompleted(ILogger logger, string sourcePath, int chunkCount, double duration);
    [LoggerMessage(Level = LogLevel.Error, Message = "Refresh failed for {SourcePath}")]
    private static partial void LogRefreshFailed(ILogger logger, Exception exception, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Extracting content from {SourcePath}")]
    private static partial void LogExtractingContent(ILogger logger, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Extracted {Length} chars to {Path}")]
    private static partial void LogExtracted(ILogger logger, int length, string path);
    [LoggerMessage(Level = LogLevel.Information, Message = "Refining content for {SourcePath}")]
    private static partial void LogRefiningContent(ILogger logger, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Refined {Length} chars to {Path}")]
    private static partial void LogRefined(ILogger logger, int length, string path);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Refined content of {SourcePath} no longer matches the text its source locations were recorded for (refined.md was edited); its chunks are indexed without page or time locations until it is re-extracted")]
    private static partial void LogContentSpansStale(ILogger logger, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Re-extracting {SourcePath} on refresh: its extraction was made by {RecordedExtractor}, the current extractor is {CurrentExtractor}")]
    private static partial void LogReextractingOutdated(ILogger logger, string sourcePath, string recordedExtractor, string currentExtractor);
    [LoggerMessage(Level = LogLevel.Information, Message = "Extraction of {SourcePath} is outdated but its vault has uncommitted edits; refreshing without re-extracting")]
    private static partial void LogOutdatedKeptForVaultEdits(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No vector store configured, skipping removal")]
    private static partial void LogNoVectorStoreSkipRemoval(ILogger logger);
    [LoggerMessage(Level = LogLevel.Information, Message = "Removed chunks for document {DocumentId}")]
    private static partial void LogRemovedChunks(ILogger logger, string documentId);
    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to roll back the partial index for {SourcePath}; both the previous and the partial generation may be present")]
    private static partial void LogRollbackFailed(ILogger logger, string sourcePath, Exception exception);
    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to rewind the resume checkpoint for {SourcePath} to {StartFromChunkIndex} after a rollback; a retry may skip chunks the rollback removed")]
    private static partial void LogCheckpointRewindFailed(ILogger logger, string sourcePath, int startFromChunkIndex, Exception exception);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Vector store or embedding service not configured, cannot search")]
    private static partial void LogNoVectorStoreCannotSearch(ILogger logger);
    [LoggerMessage(Level = LogLevel.Information, Message = "Search for '{Query}' returned {Count} results")]
    private static partial void LogSearchResults(ILogger logger, string query, int count);
    [LoggerMessage(Level = LogLevel.Information, Message = "Hybrid search keyword leg: {Leg}")]
    private static partial void LogHybridKeywordLeg(ILogger logger, string leg);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Hybrid search requested but IHybridSearchService is not registered; executing vector search (reported as ExecutedStrategy=Vector)")]
    private static partial void LogHybridUnavailableFallback(ILogger logger);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Keyword search requested but IKeywordSearchService is not registered; executing vector search (reported as ExecutedStrategy=Vector)")]
    private static partial void LogKeywordUnavailableFallback(ILogger logger);
    [LoggerMessage(Level = LogLevel.Warning, Message = "No content to index for {SourcePath}")]
    private static partial void LogNoContentToIndex(ILogger logger, string sourcePath);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Created {ChunkCount} chunks from {ContentLength} chars")]
    private static partial void LogCreatedChunks(ILogger logger, int chunkCount, int contentLength);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Created {ChunkCount} image description chunks for {SourcePath}")]
    private static partial void LogCreatedImageChunks(ILogger logger, int chunkCount, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Describing {PendingCount} extracted images for {SourcePath}")]
    private static partial void LogEnrichingImages(ILogger logger, int pendingCount, string sourcePath);
    [LoggerMessage(Level = LogLevel.Information, Message = "Image descriptions for {SourcePath}: {DescribedCount} available, {FailedCount} still pending")]
    private static partial void LogEnrichedImages(ILogger logger, int describedCount, int failedCount, string sourcePath);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Enricher failed for image {ImageId} of {SourcePath}; it stays pending for the next run")]
    private static partial void LogImageEnrichmentFailed(ILogger logger, Exception exception, string imageId, string sourcePath);
    [LoggerMessage(Level = LogLevel.Warning, Message = "Image {ImageId} of {SourcePath} reached the enrichment attempt ceiling ({AttemptCount}); it will not be retried again")]
    private static partial void LogImageEnrichmentPermanentlyFailed(ILogger logger, string imageId, string sourcePath, int attemptCount);
    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipping {Count} permanently-failed images for {SourcePath}")]
    private static partial void LogSkippingPermanentlyFailedImages(ILogger logger, int count, string sourcePath);
    [LoggerMessage(Level = LogLevel.Warning, Message = "No vector store or embedding service configured, skipping indexing")]
    private static partial void LogNoVectorStoreSkipIndexing(ILogger logger);
    [LoggerMessage(Level = LogLevel.Information, Message = "Indexed {Count} chunks for {DocumentId}")]
    private static partial void LogIndexedChunks(ILogger logger, int count, string documentId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Indexed {Processed} chunks (resumed; skipped {Skipped} already-committed) for {DocumentId}")]
    private static partial void LogIndexedChunksResumable(ILogger logger, int processed, int skipped, string documentId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Indexed {Count} chunks to keyword index for {DocumentId}")]
    private static partial void LogIndexedKeywordChunks(ILogger logger, int count, string documentId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Purged {ChunkCount} keyword index chunks for vault_id {VaultId}")]
    private static partial void LogPurgedKeywordIndex(ILogger logger, int chunkCount, string vaultId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Building GraphRAG index for {DocumentId} from {ChunkCount} chunks")]
    private static partial void LogBuildingGraphRagIndex(ILogger logger, string documentId, int chunkCount);
    [LoggerMessage(Level = LogLevel.Information, Message = "GraphRAG index built for {DocumentId}")]
    private static partial void LogGraphRagIndexBuilt(ILogger logger, string documentId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Contextual enrichment applied to {EnrichedCount}/{ChunkCount} chunks for {SourcePath} in {ElapsedMs} ms")]
    private static partial void LogContextualEnrichmentApplied(ILogger logger, int enrichedCount, int chunkCount, string sourcePath, long elapsedMs);

    [LoggerMessage(Level = LogLevel.Information, Message = "Rebuilt keyword leg of {FilepathHash} from the vector leg: {Written} rows written, {Removed} stale rows removed")]
    private static partial void LogRepairedKeywordLeg(ILogger logger, string filepathHash, int written, int removed);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Keyword leg of {FilepathHash} was not rewritten; the repair continues with the other entries")]
    private static partial void LogKeywordLegRepairFailed(ILogger logger, Exception exception, string filepathHash);

    [LoggerMessage(Level = LogLevel.Information, Message = "Re-keyed index rows of {PreviousPath} to {SourcePath}: {VectorChunks} vector and {KeywordChunks} keyword chunks moved, none re-embedded")]
    private static partial void LogReassigned(ILogger logger, string previousPath, string sourcePath, int vectorChunks, int keywordChunks);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Re-keying the index rows of {SourcePath} failed ({ExceptionType}); moving the legs already re-keyed back")]
    private static partial void LogMoveRollingBack(ILogger logger, string sourcePath, string exceptionType);

    [LoggerMessage(Level = LogLevel.Error, Message = "Moving a re-keyed leg of {SourcePath} back failed; its rows may be split between the old and the new document id")]
    private static partial void LogMoveRollbackFailed(ILogger logger, Exception exception, string sourcePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Reused {ReusedCount}/{ChunkCount} stored contexts for {SourcePath} (passage and document text unchanged)")]
    private static partial void LogContextualEnrichmentReused(ILogger logger, int reusedCount, int chunkCount, string sourcePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Kept {KeptCount}/{ChunkCount} unchanged chunks of {DocumentId} without re-embedding")]
    private static partial void LogKeptUnchangedChunks(ILogger logger, int keptCount, int chunkCount, string documentId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting upgrade for {SourcePath} (pending: {Pending})")]
    private static partial void LogStartingUpgrade(ILogger logger, string sourcePath, string pending);

    [LoggerMessage(Level = LogLevel.Information, Message = "Upgrade completed for {SourcePath}: {ChunkCount} chunks, {ReEmbedded} re-embedded, {Kept} unchanged, in {Seconds:F1}s")]
    private static partial void LogUpgradeCompleted(ILogger logger, string sourcePath, int chunkCount, int reEmbedded, int kept, double seconds);

    [LoggerMessage(Level = LogLevel.Error, Message = "Upgrade failed for {SourcePath}")]
    private static partial void LogUpgradeFailed(ILogger logger, Exception exception, string sourcePath);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Upgrade of {SourcePath}: the source file is gone, so page reads and LLM refinement stay pending")]
    private static partial void LogUpgradeSourceMissing(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Contextual enrichment failed for {SourcePath}; indexing {ChunkCount} chunks without context")]
    private static partial void LogContextualEnrichmentFailed(ILogger logger, string sourcePath, int chunkCount, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Contextual enrichment returned no context for {EmptyCount}/{ChunkCount} chunks of {SourcePath}; those chunks are indexed without context and tagged enrichment=empty")]
    private static partial void LogContextualEnrichmentReturnedNoContext(ILogger logger, int emptyCount, int chunkCount, string sourcePath);

    #endregion
}

/// <summary>
/// A unit of content on its way to the index, with any provenance metadata that applies to this
/// chunk alone. Document text chunks carry none; an image-description chunk carries its image tags.
/// <see cref="Id"/> is the id it is indexed under — derived from what the chunk is
/// (<see cref="ChunkIdentity"/>) before enrichment or sanitizing transform its <see cref="Content"/>,
/// so the same passage keeps the same id from one memorize to the next.
/// </summary>
internal sealed record VaultChunk(string Id, string Content, IReadOnlyDictionary<string, object>? Metadata);

/// <summary>
/// Interface for content extraction (FileFlux integration).
/// </summary>
public interface IExtractor
{
    /// <summary>
    /// Extracts <paramref name="sourcePath"/>. <paramref name="settings"/> carries what the vault asks of the extraction
    /// (from <see cref="Options.FileVaultOptions"/>); null means the extractor's defaults. An extractor with no use for a
    /// setting ignores it.
    /// </summary>
    Task<ExtractionResult> ExtractAsync(string sourcePath, ExtractionSettings? settings = null, CancellationToken ct = default);

    /// <summary>
    /// Which extractor this is and at what version — recorded on every entry it extracts
    /// (<see cref="VaultEntry.ExtractedBy"/>), so a refresh can tell an extraction made by an older version apart
    /// (<see cref="Options.FileVaultOptions.Reextraction"/>). Null when the extractor cannot say; its entries are then
    /// never judged outdated.
    /// </summary>
    ExtractionIdentity? Identity { get; }
}

/// <summary>
/// What a vault asks of its extractor, from <see cref="Options.FileVaultOptions"/>.
/// </summary>
public sealed record ExtractionSettings
{
    /// <summary><see cref="Options.FileVaultOptions.LlmRefine"/>.</summary>
    public FileFlux.Core.LlmRefineOptions? LlmRefine { get; init; }

    /// <summary><see cref="Options.FileVaultOptions.PageReading"/>.</summary>
    public FileFlux.Core.PageReadingOptions? PageReading { get; init; }

    /// <summary>
    /// Run no LLM refinement at all, whatever <see cref="LlmRefine"/> says — the vault defers it
    /// (<see cref="Options.FileVaultOptions.DeferEnrichment"/>) to a later upgrade.
    /// </summary>
    public bool SkipLlmRefine { get; init; }
}

/// <summary>
/// Result of content extraction.
/// </summary>
public sealed class ExtractionResult
{
    public string Content { get; init; } = "";

    /// <summary>
    /// Images the extractor pulled out of the document, with their identity and any alt text the
    /// source format carried. The pipeline stores them in the vault and, when an
    /// <see cref="IVaultImageEnricher"/> is registered, offers them for description.
    /// </summary>
    public IReadOnlyList<ImageArtifact>? Images { get; init; }

    /// <summary>
    /// Structured diagnostics reported by the extractor, passed through opaquely
    /// (the vault does not interpret keys or values). For the FileFlux extractor these are
    /// <c>RawContent.Hints</c> with scalar values, e.g.
    /// <c>extraction_failure_reason = no_text_layer</c> for an image-only/scanned PDF.
    /// Null or empty when the extractor reported none.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Hints { get; init; }

    /// <summary>
    /// Human-readable extraction warnings reported by the extractor, passed through verbatim
    /// (e.g. "image-only/scanned document ... requires OCR"). Null or empty when none.
    /// </summary>
    public IReadOnlyList<string>? Warnings { get; init; }

    /// <summary>
    /// Where stretches of <see cref="Content"/> came from in the source — pages of a paginated document, time ranges of
    /// a recording. Offsets index <see cref="Content"/>. The pipeline stores them beside the extracted text and hands
    /// them to the <see cref="IChunker"/> at memorize, so each chunk can say which page or second it covers. Null when
    /// the extractor has no locations.
    /// </summary>
    public IReadOnlyList<ContentSpan>? Spans { get; init; }

    /// <summary>
    /// The document's tables as structured rows, in document order — the same tables <see cref="Content"/> carries
    /// as text. The pipeline stores them beside the extracted text (<see cref="IVault.GetTablesAsync"/>) and ties the
    /// chunks holding their rows to them (<c>table_id</c>). Null when the extractor found none.
    /// </summary>
    public IReadOnlyList<TableArtifact>? Tables { get; init; }
}

/// <summary>
/// Interface for content chunking (FileFlux integration).
/// </summary>
public interface IChunker
{
    /// <summary>
    /// Splits <paramref name="content"/> into chunks.
    /// </summary>
    /// <param name="content">The text to chunk.</param>
    /// <param name="spans">
    /// Where stretches of <paramref name="content"/> came from in the source (offsets index <paramref name="content"/>),
    /// or null when unknown. A chunker that honours them sets each chunk's <see cref="ContentChunk.Location"/>.
    /// </param>
    /// <param name="options">Chunking options.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<IReadOnlyList<ContentChunk>> ChunkAsync(
        string content, IReadOnlyList<ContentSpan>? spans, ChunkingOptions options, CancellationToken ct = default);
}

/// <summary>A chunk of text and, when the chunker knew it, where it came from in the source.</summary>
/// <param name="Text">The chunk text.</param>
public sealed record ContentChunk(string Text)
{
    /// <summary>The pages or time range the chunk covers; null when unknown.</summary>
    public ContentLocation? Location { get; init; }

    /// <summary>
    /// Where the chunk sits in a table, when it holds table rows: which table of the content (0-based), which piece
    /// of it, and which body rows. Null for a chunk of prose.
    /// </summary>
    public ContentTablePiece? Table { get; init; }
}

/// <summary>
/// A chunk's place in a table of the chunked content.
/// </summary>
/// <param name="TableIndex">0-based position of the table among the tables of the content.</param>
/// <param name="Piece">1-based piece of the table this chunk is.</param>
/// <param name="Pieces">How many pieces the table was split into.</param>
/// <param name="RowStart">First body row (0-based, header excluded) the chunk holds.</param>
/// <param name="RowEnd">Last body row (0-based, inclusive) the chunk holds.</param>
public sealed record ContentTablePiece(int TableIndex, int Piece, int Pieces, int RowStart, int RowEnd);

/// <summary>The pages or time range a chunk covers in its source.</summary>
public sealed record ContentLocation
{
    /// <summary>First page (1-based) the chunk covers.</summary>
    public int? StartPage { get; init; }

    /// <summary>Last page (1-based) the chunk covers.</summary>
    public int? EndPage { get; init; }

    /// <summary>Start of the stretch of a recording the chunk covers.</summary>
    public TimeSpan? StartTime { get; init; }

    /// <summary>End of the stretch of a recording the chunk covers.</summary>
    public TimeSpan? EndTime { get; init; }

    /// <summary>True when no field is set.</summary>
    public bool IsEmpty => StartPage is null && EndPage is null && StartTime is null && EndTime is null;
}

/// <summary>
/// Options for chunking.
/// </summary>
public sealed class ChunkingOptions
{
    public int MaxChunkSize { get; set; } = 1024;
    public int OverlapSize { get; set; } = 128;
    public string Strategy { get; set; } = "Auto";
    public string? Language { get; set; }
}
