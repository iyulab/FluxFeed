using AwesomeAssertions;
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
/// Verifies that VaultPipeline writes to the keyword index alongside the vector store, closing
/// the population gap documented on <c>INativeHybridSearch</c>: ingestion-only pipelines never
/// call <c>IKeywordSearchService.IndexChunkAsync</c>, so the keyword leg of hybrid search stays
/// empty unless the pipeline itself drives it. Mirrors VaultPipelineGraphRagTests, which guards
/// the same shape of gap for the graph leg.
/// </summary>
public sealed class VaultPipelineKeywordIndexTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    public VaultPipelineKeywordIndexTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FileVaultKeywordIndex_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_testDir);
        Directory.CreateDirectory(_vaultDir);

        _git = Substitute.For<IGitService>();
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns("commit-hash");

        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            _git,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<string>>(
                ((IEnumerable<DocumentChunk>)ci[0]).Select(_ => Guid.NewGuid().ToString()).ToList()));

        _embeddingService = Substitute.For<IEmbeddingService>();
        _embeddingService.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<float[]>>(
                ((IEnumerable<string>)ci[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f }).ToList()));
        _embeddingService.GetIdentity()
            .Returns(new EmbeddingIdentity { Provider = "Test", Model = "test", Dimension = 3 });
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch { /* ignore cleanup errors */ }
    }

    private VaultPipeline CreatePipeline(IKeywordSearchService? keywordSearch) =>
        new(
            _git,
            new ContentHasher(),
            _storage,
            NullLogger<VaultPipeline>.Instance,
            options: null,
            extractor: null,
            chunker: null,
            vectorStore: _vectorStore,
            embeddingService: _embeddingService,
            hybridSearch: null,
            graphRAGService: null,
            keywordSearchService: keywordSearch);

    private static IKeywordSearchService CreateKeywordMock()
    {
        var keyword = Substitute.For<IKeywordSearchService>();
        keyword.IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        keyword.DeleteByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        return keyword;
    }

    private async Task<(MemorizeResult Result, VaultEntry Entry)> MemorizeFileAsync(
        VaultPipeline pipeline, MemorizeOptions options)
    {
        var docPath = Path.Combine(_testDir, "doc.txt");
        await File.WriteAllTextAsync(docPath, "Alice works at Acme Corp. Bob manages the project in Seoul.");
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);
        var result = await pipeline.MemorizeAsync(entry, options);
        return (result, entry);
    }

    [Fact]
    public async Task Memorize_WithKeywordSearchServiceRegistered_IndexesChunksToKeywordStore()
    {
        var keyword = CreateKeywordMock();
        var pipeline = CreatePipeline(keyword);

        var (result, _) = await MemorizeFileAsync(pipeline, new MemorizeOptions { MaxChunkSize = 200 });

        result.Success.Should().BeTrue();
        pipeline.SupportsKeywordIndex.Should().BeTrue();
        await keyword.Received(1).IndexChunksAsync(
            Arg.Is<IEnumerable<DocumentChunk>>(c => c.Any()),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Memorize_WithoutKeywordSearchService_CompletesWithoutError()
    {
        var pipeline = CreatePipeline(keywordSearch: null);

        var (result, _) = await MemorizeFileAsync(pipeline, new MemorizeOptions { MaxChunkSize = 200 });

        result.Success.Should().BeTrue();
        result.ChunkCount.Should().BeGreaterThan(0);
        pipeline.SupportsKeywordIndex.Should().BeFalse();
    }

    [Fact]
    public async Task Remove_WithKeywordSearchServiceRegistered_DeletesFromKeywordStore()
    {
        var keyword = CreateKeywordMock();
        var pipeline = CreatePipeline(keyword);
        var (_, entry) = await MemorizeFileAsync(pipeline, new MemorizeOptions { MaxChunkSize = 200 });

        // Indexing replaces the entry's rows, so memorize itself deletes once. Count from here so
        // this stays a claim about RemoveAsync rather than about how the arrange step indexes.
        keyword.ClearReceivedCalls();

        await pipeline.RemoveAsync(entry, TestContext.Current.CancellationToken);

        await keyword.Received(1).DeleteByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Remove_WithoutKeywordSearchService_StillRemovesFromVectorStore()
    {
        var pipeline = CreatePipeline(keywordSearch: null);
        var (_, entry) = await MemorizeFileAsync(pipeline, new MemorizeOptions { MaxChunkSize = 200 });

        // See above: memorize deletes once as part of replacing the entry's rows.
        _vectorStore.ClearReceivedCalls();

        await pipeline.RemoveAsync(entry, TestContext.Current.CancellationToken);

        await _vectorStore.Received(1).DeleteByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PurgeVectorsAsync_WithKeywordSearchServiceRegistered_DoesNotTouchKeywordIndex()
    {
        // IKeywordSearchService has no tag/filter-scoped bulk delete (only per-document, per-chunk,
        // or clear-everything), so a tenant purge cannot reach only that tenant's keyword-index
        // rows. This pins the current, honestly-disclosed gap: PurgeVectorsAsync purges the vector
        // store only. If this starts failing, either the interface gained a filtered delete (update
        // this pipeline to use it) or the gap regressed silently the other way.
        var keyword = CreateKeywordMock();
        var pipeline = CreatePipeline(keyword);

        await pipeline.PurgeVectorsAsync("tenant-x", TestContext.Current.CancellationToken);

        await keyword.DidNotReceiveWithAnyArgs().DeleteByDocumentIdAsync(default!, TestContext.Current.CancellationToken);
        await keyword.DidNotReceiveWithAnyArgs().DeleteChunkAsync(default!, TestContext.Current.CancellationToken);
        await keyword.DidNotReceiveWithAnyArgs().ClearIndexAsync(TestContext.Current.CancellationToken);
    }
}
