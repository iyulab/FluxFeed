using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxFeed.Domain.Enums;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// VaultFactory threads five optional shared services (IVectorStore, IEmbeddingService,
/// IHybridSearchService, IGraphRAGService, IKeywordSearchService) from DI into every tenant's
/// VaultPipeline via a hand-built constructor call rather than plain DI resolution - none of
/// that threading had direct test coverage before this suite. It exists because cycle-235 found
/// one of the five (IHybridSearchService) silently hardcoded to null despite the other four
/// already being wired, which a test at this level would have caught immediately.
/// </summary>
public sealed class VaultFactoryTests : IDisposable
{
    private const string TenantId = "tenant-a";

    private readonly string _basePath;
    private readonly IGitService _git;
    private readonly IFileWatcherService _fileWatcher;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    public VaultFactoryTests()
    {
        _basePath = Path.Combine(Path.GetTempPath(), $"FileVaultFactory_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_basePath);

        _git = Substitute.For<IGitService>();
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns("commit-hash");

        _fileWatcher = Substitute.For<IFileWatcherService>();
        _fileWatcher.GetAllWatchers().Returns([]);

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<string>>(
                ((IEnumerable<DocumentChunk>)ci[0]).Select(_ => Guid.NewGuid().ToString()).ToList()));

        _embeddingService = Substitute.For<IEmbeddingService>();
        _embeddingService.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<float[]>>(
                ((IEnumerable<string>)ci[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f }).ToList()));
        _embeddingService.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new[] { 0.1f, 0.2f, 0.3f });
        _embeddingService.GetIdentity()
            .Returns(new EmbeddingIdentity { Provider = "Test", Model = "test", Dimension = 3 });
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_basePath))
                Directory.Delete(_basePath, recursive: true);
        }
        catch { /* ignore cleanup errors */ }
    }

    private VaultFactory CreateFactory(
        IHybridSearchService? hybridSearch = null,
        IGraphRAGService? graphRAGService = null,
        IKeywordSearchService? keywordSearchService = null) =>
        new(
            Substitute.For<IServiceProvider>(),
            NullLoggerFactory.Instance,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _basePath, EnableBackgroundProcessing = false }),
            new ContentHasher(),
            _git,
            _fileWatcher,
            extractor: null,
            chunker: null,
            vectorStore: _vectorStore,
            embeddingService: _embeddingService,
            hybridSearch: hybridSearch,
            graphRAGService: graphRAGService,
            keywordSearchService: keywordSearchService);

    private async Task MemorizeThroughTenantAsync(IVaultFactory factory)
    {
        var vault = factory.GetOrCreate(TenantId);
        var context = factory.GetContext(TenantId)!;

        var docDir = Path.Combine(_basePath, "sources");
        Directory.CreateDirectory(docDir);
        var docPath = Path.Combine(docDir, "doc.txt");
        await File.WriteAllTextAsync(docPath, "Alice works at Acme Corp. Bob manages the project in Seoul.");

        await vault.MemorizeAsync(docPath);
        _ = context; // context kept for callers that need VaultBasePath/Pipeline
    }

    [Fact]
    public async Task GetOrCreate_ThreadsVectorStoreAndEmbeddingService_TenantChunksAreStored()
    {
        using var factory = CreateFactory();

        await MemorizeThroughTenantAsync(factory);

        await _vectorStore.Received(1).StoreBatchAsync(
            Arg.Is<IEnumerable<DocumentChunk>>(c => c.Any()),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public void GetOrCreate_ThreadsGraphRAGService_PipelineReportsSupport()
    {
        var graph = Substitute.For<IGraphRAGService>();
        using var factory = CreateFactory(graphRAGService: graph);

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsGraphRAG.Should().BeTrue();
    }

    [Fact]
    public void GetOrCreate_WithoutGraphRAGService_PipelineReportsNoSupport()
    {
        using var factory = CreateFactory();

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsGraphRAG.Should().BeFalse();
    }

    [Fact]
    public void GetOrCreate_ThreadsKeywordSearchService_PipelineReportsSupport()
    {
        var keyword = Substitute.For<IKeywordSearchService>();
        using var factory = CreateFactory(keywordSearchService: keyword);

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsKeywordIndex.Should().BeTrue();
    }

    [Fact]
    public void GetOrCreate_WithoutKeywordSearchService_PipelineReportsNoSupport()
    {
        using var factory = CreateFactory();

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsKeywordIndex.Should().BeFalse();
    }

    [Fact]
    public async Task GetOrCreate_ThreadsHybridSearchService_UsedForHybridSearchStrategy()
    {
        // Regression guard for cycle-235's finding: hybridSearch used to be hardcoded to null in
        // VaultFactory's constructor call, so a tenant-scoped vault could never reach this path no
        // matter what was registered in DI.
        var hybrid = Substitute.For<IHybridSearchService>();
        hybrid.SearchAsync(Arg.Any<string>(), Arg.Any<FluxIndex.Core.Domain.Models.HybridSearchOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<FluxIndex.Core.Domain.Models.HybridSearchResult>>([]));
        using var factory = CreateFactory(hybridSearch: hybrid);

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;
        await pipeline.SearchAsync("query", strategy: VaultSearchStrategy.Hybrid);

        await hybrid.Received(1).SearchAsync(
            "query", Arg.Any<FluxIndex.Core.Domain.Models.HybridSearchOptions>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetOrCreate_WithoutAnyOptionalSharedServices_StillCreatesUsableVault()
    {
        using var factory = new VaultFactory(
            Substitute.For<IServiceProvider>(),
            NullLoggerFactory.Instance,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _basePath }),
            new ContentHasher(),
            _git,
            _fileWatcher);

        var vault = factory.GetOrCreate(TenantId);
        var status = await vault.StatusAsync();

        status.Should().NotBeNull();
    }
}
