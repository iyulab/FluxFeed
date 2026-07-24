using FluentAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Pins the extracted-image enrichment contract: the consumer supplies only a describer, and the
/// pipeline owns when it is called, what is retried, and how a description reaches the index —
/// as its own chunk carrying image provenance in metadata, never as a marker in body text.
/// </summary>
public class VaultPipelineImageEnrichmentTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;
    private readonly IGitService _git;
    private readonly CapturingVectorStore _capture = new();
    private readonly IVectorStore _vectorStore;

    public VaultPipelineImageEnrichmentTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"VaultImg_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_vaultDir);

        _git = Substitute.For<IGitService>();
        _git.InitAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("commit");

        _vectorStore = _capture.Build();

        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            _git,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, recursive: true); } catch (IOException) { }
        }
    }

    // === Fixtures ===

    private sealed class StubExtractor(ExtractionResult result) : IExtractor
    {
        public Task<ExtractionResult> ExtractAsync(string sourcePath, CancellationToken ct = default)
            => Task.FromResult(result);
    }

    /// <summary>Records every call so idempotence and retry can be asserted per image.</summary>
    private sealed class RecordingEnricher(Func<VaultImageDescriptionRequest, string?> describe) : IVaultImageEnricher
    {
        public List<string> Calls { get; } = [];
        public List<VaultImageDescriptionRequest> Requests { get; } = [];

        public Task<string?> DescribeAsync(VaultImageDescriptionRequest request, CancellationToken ct = default)
        {
            Calls.Add(request.Image.Id);
            Requests.Add(request);
            return Task.FromResult(describe(request));
        }
    }

    /// <summary>Captures everything the pipeline stores so chunk metadata can be asserted.</summary>
    private sealed class CapturingVectorStore
    {
        public List<DocumentChunk> Chunks { get; } = [];

        public IVectorStore Build()
        {
            var store = Substitute.For<IVectorStore>();

            store.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var batch = ((IEnumerable<DocumentChunk>)call[0]).ToList();
                    Chunks.AddRange(batch);
                    return Task.FromResult<IEnumerable<string>>(batch.Select(c => c.Id.ToString()).ToList());
                });

            store.DeleteByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    Chunks.RemoveAll(c => c.DocumentId == (string)call[0]);
                    return Task.FromResult(true);
                });

            return store;
        }
    }

    private static IEmbeddingService CreateEmbeddingService()
    {
        var embedder = Substitute.For<IEmbeddingService>();
        embedder.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new[] { 0.1f, 0.2f, 0.3f }));
        embedder.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(
                ((IEnumerable<string>)call[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f })));
        embedder.GetIdentity().Returns(new EmbeddingIdentity
        {
            Provider = "Test",
            Model = "test-embed",
            Dimension = 3
        });
        return embedder;
    }

    private VaultPipeline CreatePipeline(ExtractionResult extraction, IVaultImageEnricher? enricher) => new(
        _git,
        new ContentHasher(),
        _storage,
        NullLogger<VaultPipeline>.Instance,
        options: MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }),
        extractor: new StubExtractor(extraction),
        vectorStore: _vectorStore,
        embeddingService: CreateEmbeddingService(),
        imageEnricher: enricher);

    private VaultEntry CreateEntry(string name)
    {
        var path = Path.Combine(_testDir, name);
        File.WriteAllText(path, "source bytes");
        return VaultEntry.Create(path, _vaultDir);
    }

    private static ImageArtifact Image(string id, string? altText = null) => new()
    {
        Id = id,
        Data = [1, 2, 3, 4],
        ContentType = "image/png",
        AltText = altText
    };

    // === Tests ===

    [Fact]
    public async Task MemorizeAsync_ImageOnlyDocument_IndexesDescriptionsAsChunks()
    {
        // A scanned/figure-only document has no text layer — before enrichment it memorized as
        // 0 chunks. Its images ARE its content, so a described image must be indexed.
        var entry = CreateEntry("figures.pdf");
        var enricher = new RecordingEnricher(_ => "Monthly revenue trend chart, peaking in March.");
        var pipeline = CreatePipeline(
            new ExtractionResult { Content = string.Empty, Images = [Image("img_000")] },
            enricher);

        var result = await pipeline.MemorizeAsync(entry);

        result.Success.Should().BeTrue();
        result.ChunkCount.Should().Be(1);

        var chunk = _capture.Chunks.Should().ContainSingle().Subject;
        chunk.Content.Should().Be("Monthly revenue trend chart, peaking in March.");
        chunk.Metadata!["chunk_kind"].Should().Be(VaultPipeline.ImageDescriptionChunkKind);
        chunk.Metadata["image_id"].Should().Be("img_000");
        chunk.Metadata["image_file"].Should().Be("img_000.png");
        // Standard provenance still applies.
        chunk.Metadata["file_name"].Should().Be("figures.pdf");
    }

    [Fact]
    public async Task MemorizeAsync_TextDocumentWithImage_AppendsImageChunkWithoutTouchingBodyText()
    {
        // The provenance lives in metadata, so nothing has to be injected into — and later stripped
        // out of — the text a user actually reads.
        var entry = CreateEntry("report.docx");
        var pipeline = CreatePipeline(
            new ExtractionResult { Content = "Quarterly report body.", Images = [Image("img_000")] },
            new RecordingEnricher(_ => "Bar chart of quarterly sales."));

        var result = await pipeline.MemorizeAsync(entry);

        result.ChunkCount.Should().Be(2);
        _capture.Chunks[0].Content.Should().Be("Quarterly report body.");
        _capture.Chunks[0].Metadata.Should().NotContainKey("image_id");
        _capture.Chunks[1].Metadata!["image_id"].Should().Be("img_000");

        var refined = await File.ReadAllTextAsync(entry.RefinedMdPath);
        refined.Should().Be("Quarterly report body.");
        refined.Should().NotContain("img_000");
    }

    [Fact]
    public async Task MemorizeAsync_WithoutEnricher_BehavesExactlyAsBefore()
    {
        // Backward compatibility: an existing consumer that registers no enricher sees images
        // stored as always, no image chunks, and an image-only document still memorizes as empty.
        var entry = CreateEntry("report.docx");
        var pipeline = CreatePipeline(
            new ExtractionResult { Content = "Quarterly report body.", Images = [Image("img_000")] },
            enricher: null);

        var result = await pipeline.MemorizeAsync(entry);

        result.ChunkCount.Should().Be(1);
        _capture.Chunks.Should().ContainSingle();
        _capture.Chunks[0].Metadata.Should().NotContainKey("image_id");
        (await _storage.GetImageManifestAsync(entry)).Should().ContainSingle()
            .Which.IsDescribed.Should().BeFalse();
    }

    [Fact]
    public async Task MemorizeAsync_ImageOnlyDocumentWithoutEnricher_StillMemorizesAsEmpty()
    {
        var entry = CreateEntry("scan.pdf");
        var pipeline = CreatePipeline(
            new ExtractionResult { Content = string.Empty, Images = [Image("img_000")] },
            enricher: null);

        var result = await pipeline.MemorizeAsync(entry);

        result.Success.Should().BeTrue();
        result.ChunkCount.Should().Be(0);
        _capture.Chunks.Should().BeEmpty();
    }

    [Fact]
    public async Task MemorizeAsync_RunTwice_DoesNotDescribeTheSameImageAgain()
    {
        // Idempotence is the pipeline's job: descriptions are persisted per image, so a re-memorize
        // costs no enricher calls (and no vision-model spend).
        var entry = CreateEntry("figures.pdf");
        var enricher = new RecordingEnricher(_ => "A chart.");
        var extraction = new ExtractionResult { Content = "Body.", Images = [Image("img_000")] };

        await CreatePipeline(extraction, enricher).MemorizeAsync(entry);
        await CreatePipeline(extraction, enricher).MemorizeAsync(entry);

        enricher.Calls.Should().ContainSingle().Which.Should().Be("img_000");
    }

    [Fact]
    public async Task MemorizeAsync_OneImageFails_OthersSucceedAndOnlyTheFailedOneIsRetried()
    {
        // Partial failure must not cost the document its other descriptions, nor abort the memorize,
        // nor make the successful images pay for the retry.
        var entry = CreateEntry("figures.pdf");
        var extraction = new ExtractionResult
        {
            Content = "Body.",
            Images = [Image("img_000"), Image("img_001")]
        };

        var failFirst = new RecordingEnricher(request =>
            request.Image.Id == "img_000" ? throw new InvalidOperationException("vision model down") : "Second figure.");

        var first = await CreatePipeline(extraction, failFirst).MemorizeAsync(entry);

        first.Success.Should().BeTrue();
        first.ChunkCount.Should().Be(2);   // 1 text + 1 described image
        _capture.Chunks.Should().ContainSingle(c => c.Metadata!.ContainsKey("image_id"))
            .Which.Metadata!["image_id"].Should().Be("img_001");

        var succeedAll = new RecordingEnricher(_ => "First figure.");
        var second = await CreatePipeline(extraction, succeedAll).MemorizeAsync(entry);

        // Only the image that failed is offered again.
        succeedAll.Calls.Should().ContainSingle().Which.Should().Be("img_000");
        second.ChunkCount.Should().Be(3);  // 1 text + 2 described images
    }

    [Fact]
    public async Task MemorizeAsync_EnricherReturnsNull_ImageStaysPendingForTheNextRun()
    {
        var entry = CreateEntry("figures.pdf");
        var extraction = new ExtractionResult { Content = "Body.", Images = [Image("img_000")] };

        var declines = new RecordingEnricher(_ => null);
        var result = await CreatePipeline(extraction, declines).MemorizeAsync(entry);

        result.Success.Should().BeTrue();
        result.ChunkCount.Should().Be(1);  // text only
        (await _storage.GetImageManifestAsync(entry))[0].IsDescribed.Should().BeFalse();

        var succeeds = new RecordingEnricher(_ => "A chart.");
        await CreatePipeline(extraction, succeeds).MemorizeAsync(entry);
        succeeds.Calls.Should().ContainSingle();
    }

    [Fact]
    public async Task RefreshAsync_RetriesPendingImagesAndReindexesDescriptions()
    {
        // Refresh re-indexes edited vault content; a still-undescribed image gets another chance
        // there too, without re-describing the ones that already succeeded.
        var entry = CreateEntry("figures.pdf");
        var extraction = new ExtractionResult { Content = "Body.", Images = [Image("img_000")] };

        await CreatePipeline(extraction, new RecordingEnricher(_ => null)).MemorizeAsync(entry);

        var succeeds = new RecordingEnricher(_ => "A chart.");
        var refreshed = await CreatePipeline(extraction, succeeds).RefreshAsync(entry);

        succeeds.Calls.Should().ContainSingle();
        refreshed.ChunkCount.Should().Be(2);
        _capture.Chunks.Should().ContainSingle(c => c.Metadata!.ContainsKey("image_id"));
    }

    [Fact]
    public async Task DescribeAsync_ReceivesDocumentContextAndAltText()
    {
        var entry = CreateEntry("report.docx");
        var enricher = new RecordingEnricher(_ => "A chart.");
        await CreatePipeline(
            new ExtractionResult { Content = "Quarterly body.", Images = [Image("img_000", altText: "sales chart")] },
            enricher).MemorizeAsync(entry);

        var request = enricher.Requests.Should().ContainSingle().Subject;
        request.SourcePath.Should().EndWith("report.docx");
        request.DocumentText.Should().Be("Quarterly body.");
        request.Image.AltText.Should().Be("sales chart");
        File.Exists(request.Image.FilePath).Should().BeTrue();
    }

    [Fact]
    public async Task DescribeAsync_ScannedDocument_ReceivesNullDocumentText()
    {
        // Honest signal: there is no text layer to build context from — the image is the content.
        var entry = CreateEntry("scan.pdf");
        var enricher = new RecordingEnricher(_ => "A scanned form.");
        await CreatePipeline(
            new ExtractionResult { Content = string.Empty, Images = [Image("img_000")] },
            enricher).MemorizeAsync(entry);

        enricher.Requests.Should().ContainSingle().Which.DocumentText.Should().BeNull();
    }
}
