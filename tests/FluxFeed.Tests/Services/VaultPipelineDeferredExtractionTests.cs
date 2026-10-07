using AwesomeAssertions;
using LlmRefineOptions = FileFlux.Core.LlmRefineOptions;
using LlmRefineScope = FileFlux.Core.LlmRefineScope;
using PageReadingOptions = FileFlux.Core.PageReadingOptions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Domain.ValueObjects;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// With <see cref="FileVaultOptions.DeferEnrichment"/>, page reads and LLM refinement — the stages that make extraction slow —
/// are left for an upgrade: memorize extracts natively and the entry is searchable at once, and
/// <see cref="VaultPipeline.UpgradeAsync"/> extracts again with them and re-indexes what changed. A scan, which has no text
/// until its pages are read, is kept for that upgrade instead of being indexed as empty.
/// </summary>
public sealed class VaultPipelineDeferredExtractionTests : IDisposable
{
    private const string NativeText = "Native text of the first page.";
    private const string ReadText = "Text a vision model read from the scanned second page.";

    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;
    private readonly IGitService _git;
    private readonly List<DocumentChunk> _chunks = [];
    private readonly IVectorStore _vectorStore;

    private static readonly PageReadingOptions PageReading = new() { SelectPages = q => !q.HasTextLayer };
    private static readonly LlmRefineOptions LlmRefine = new() { Scope = LlmRefineScope.Pages };

    public VaultPipelineDeferredExtractionTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"VaultDeferExtract_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_vaultDir);

        _git = Substitute.For<IGitService>();
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("commit");
        _git.StatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new GitStatus { ModifiedFiles = [] });

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var batch = ((IEnumerable<DocumentChunk>)call[0]).ToList();
                _chunks.RemoveAll(c => batch.Any(b => b.Id == c.Id));
                _chunks.AddRange(batch);
                return Task.FromResult<IEnumerable<string>>(batch.Select(c => c.Id.ToString()).ToList());
            });

        _storage = new VaultStorageService(NullLogger<VaultStorageService>.Instance, _git,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, recursive: true); } catch (IOException) { }
        }
    }

    [Fact]
    public async Task Memorize_with_deferral_extracts_without_page_reads_or_refinement_and_records_them()
    {
        var extractor = new RecordingExtractor(scanned: false);
        var entry = Entry();

        var result = await Pipeline(extractor, defer: true).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);

        result.Success.Should().BeTrue();
        extractor.Settings.Should().ContainSingle();
        extractor.Settings[0].PageReading.Should().BeNull();
        extractor.Settings[0].SkipLlmRefine.Should().BeTrue();
        entry.PendingEnrichment.Should().Be(EnrichmentStages.PageReads | EnrichmentStages.LlmRefinement);
        _chunks.Should().Contain(c => c.Content.Contains("Native text", StringComparison.Ordinal), "the entry is searchable before the upgrade");
    }

    [Fact]
    public async Task Without_deferral_extraction_runs_every_stage_and_nothing_is_pending()
    {
        var extractor = new RecordingExtractor(scanned: false);
        var entry = Entry();

        await Pipeline(extractor, defer: false).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);

        extractor.Settings.Should().ContainSingle();
        extractor.Settings[0].PageReading.Should().BeSameAs(PageReading);
        extractor.Settings[0].SkipLlmRefine.Should().BeFalse();
        entry.PendingEnrichment.Should().Be(EnrichmentStages.None);
    }

    [Fact]
    public async Task A_scan_is_kept_for_the_upgrade_which_reads_its_pages_and_indexes_them()
    {
        var extractor = new RecordingExtractor(scanned: true);
        var entry = Entry();
        var pipeline = Pipeline(extractor, defer: true);

        var memorized = await pipeline.MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        memorized.Success.Should().BeTrue();
        entry.PendingEnrichment.Should().HaveFlag(EnrichmentStages.PageReads, "a scan has no text until its pages are read");
        entry.RefinedExists.Should().BeTrue("the upgrade works from what memorize left");

        var summary = await pipeline.UpgradeAsync(entry, ct: TestContext.Current.CancellationToken);

        extractor.Settings.Should().HaveCount(2);
        extractor.Settings[1].PageReading.Should().BeSameAs(PageReading);
        extractor.Settings[1].SkipLlmRefine.Should().BeFalse();
        summary.StillPending.Should().Be(EnrichmentStages.None);
        entry.PendingEnrichment.Should().Be(EnrichmentStages.None);
        _chunks.Should().Contain(c => c.Content.Contains("vision model read", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_refresh_keeps_the_extraction_stages_pending()
    {
        var extractor = new RecordingExtractor(scanned: false);
        var entry = Entry();
        var pipeline = Pipeline(extractor, defer: true);
        await pipeline.MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);

        await pipeline.RefreshAsync(entry, ct: TestContext.Current.CancellationToken);

        extractor.Settings.Should().ContainSingle("a refresh re-chunks the stored extraction");
        entry.PendingEnrichment.Should().Be(EnrichmentStages.PageReads | EnrichmentStages.LlmRefinement);
    }

    [Fact]
    public async Task An_upgrade_without_the_source_file_leaves_the_extraction_stages_pending()
    {
        var extractor = new RecordingExtractor(scanned: false);
        var entry = Entry();
        var pipeline = Pipeline(extractor, defer: true);
        await pipeline.MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        File.Delete(entry.SourcePath);

        var summary = await pipeline.UpgradeAsync(entry, ct: TestContext.Current.CancellationToken);

        extractor.Settings.Should().ContainSingle();
        summary.StillPending.Should().Be(EnrichmentStages.PageReads | EnrichmentStages.LlmRefinement);
    }

    private VaultPipeline Pipeline(IExtractor extractor, bool defer) => new(
        _git,
        new ContentHasher(),
        _storage,
        NullLogger<VaultPipeline>.Instance,
        options: MsOptions.Create(new FileVaultOptions
        {
            VaultBasePath = _vaultDir, DeferEnrichment = defer, PageReading = PageReading, LlmRefine = LlmRefine,
        }),
        extractor: extractor,
        vectorStore: _vectorStore,
        embeddingService: Embedder());

    private VaultEntry Entry()
    {
        var path = Path.Combine(_testDir, "report.pdf");
        File.WriteAllText(path, "source bytes");
        return VaultEntry.Create(path, _vaultDir);
    }

    private static IEmbeddingService Embedder()
    {
        var embedder = Substitute.For<IEmbeddingService>();
        embedder.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new[] { 0.1f, 0.2f, 0.3f }));
        embedder.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(((IEnumerable<string>)call[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f })));
        embedder.GetIdentity().Returns(new EmbeddingIdentity { Provider = "Test", Model = "test-embed", Dimension = 3 });
        return embedder;
    }

    /// <summary>
    /// Records the settings of each call. Natively the second page has no text (a scan); with page reading it is read.
    /// </summary>
    private sealed class RecordingExtractor(bool scanned) : IExtractor
    {
        public List<ExtractionSettings> Settings { get; } = [];

        public ExtractionIdentity? Identity { get; } = new("FileFlux", "1.0.0");

        public Task<ExtractionResult> ExtractAsync(string sourcePath, ExtractionSettings? settings = null, CancellationToken ct = default)
        {
            Settings.Add(settings ?? new ExtractionSettings());
            var read = settings?.PageReading is not null;
            var content = scanned
                ? (read ? ReadText : string.Empty)
                : (read ? NativeText + "\n\n" + ReadText : NativeText);
            return Task.FromResult(new ExtractionResult { Content = content });
        }
    }
}
