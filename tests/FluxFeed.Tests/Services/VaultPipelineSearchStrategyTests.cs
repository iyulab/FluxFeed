using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.Models;
using FluxFeed.Interfaces;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Strategy routing in <see cref="VaultPipeline.SearchAsync"/> (ISSUE-161). Verifies that a hybrid
/// request is routed to <see cref="IHybridSearchService"/> and a keyword request to
/// <see cref="IKeywordSearchService"/> when each is available, and that both otherwise degrade to
/// vector while reporting the executed strategy truthfully (no silent mismatch).
/// </summary>
public class VaultPipelineSearchStrategyTests
{
    private readonly IGitService _git = Substitute.For<IGitService>();
    private readonly IContentHasher _hasher = Substitute.For<IContentHasher>();
    private readonly IVaultStorageService _storage = Substitute.For<IVaultStorageService>();
    private readonly IVectorStore _vectorStore = Substitute.For<IVectorStore>();
    private readonly IEmbeddingService _embedding = Substitute.For<IEmbeddingService>();

    private VaultPipeline CreatePipeline(IHybridSearchService? hybrid, IKeywordSearchService? keyword = null)
    {
        _embedding.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new float[] { 0.1f, 0.2f, 0.3f, 0.4f });

        _vectorStore.SearchAsync(Arg.Any<float[]>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<Dictionary<string, object>?>(), Arg.Any<CancellationToken>())
            .Returns(new List<DocumentChunk>
            {
                new() { Id = "vec-chunk", DocumentId = "doc-1", Content = "vector hit", ChunkIndex = 0, Score = 0.8f }
            });

        return new VaultPipeline(
            _git, _hasher, _storage, NullLogger<VaultPipeline>.Instance,
            options: null, extractor: null, chunker: null,
            vectorStore: _vectorStore, embeddingService: _embedding, hybridSearch: hybrid,
            keywordSearchService: keyword);
    }

    [Fact]
    public async Task SearchAsync_VectorRequest_ExecutesVector()
    {
        var hybrid = Substitute.For<IHybridSearchService>();
        var pipeline = CreatePipeline(hybrid);

        var response = await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Vector);
        response.Results.Should().ContainSingle().Which.DocumentId.Should().Be("doc-1");
        await hybrid.DidNotReceive().SearchAsync(Arg.Any<string>(), Arg.Any<HybridSearchOptions?>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SearchAsync_HybridRequest_WithHybridService_ExecutesHybrid()
    {
        var hybrid = Substitute.For<IHybridSearchService>();
        hybrid.SearchAsync(Arg.Any<string>(), Arg.Any<HybridSearchOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new List<HybridSearchResult>
            {
                new() { Chunk = new DocumentChunk { Id = "hyb-chunk", DocumentId = "doc-2", Content = "fused hit", ChunkIndex = 1 }, FusedScore = 0.95 }
            });
        var pipeline = CreatePipeline(hybrid);

        var response = await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Hybrid);
        var item = response.Results.Should().ContainSingle().Subject;
        item.DocumentId.Should().Be("doc-2");
        item.Content.Should().Be("fused hit");
        item.Score.Should().BeApproximately(0.95f, 0.0001f);
        await _vectorStore.DidNotReceive().SearchAsync(Arg.Any<float[]>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<Dictionary<string, object>?>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SearchAsync_HybridRequest_WithoutHybridService_DegradesToVectorTruthfully()
    {
        var pipeline = CreatePipeline(hybrid: null);

        var response = await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        // No IHybridSearchService registered: executes vector and says so (this is the #2 fix).
        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Vector);
        response.Results.Should().ContainSingle().Which.DocumentId.Should().Be("doc-1");
    }

    [Fact]
    public async Task SearchAsync_HybridRequest_FiltersByDocumentIds()
    {
        var hybrid = Substitute.For<IHybridSearchService>();
        hybrid.SearchAsync(Arg.Any<string>(), Arg.Any<HybridSearchOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new List<HybridSearchResult>
            {
                new() { Chunk = new DocumentChunk { Id = "a", DocumentId = "keep", Content = "in scope", ChunkIndex = 0 }, FusedScore = 0.9 },
                new() { Chunk = new DocumentChunk { Id = "b", DocumentId = "drop", Content = "out of scope", ChunkIndex = 0 }, FusedScore = 0.8 }
            });
        var pipeline = CreatePipeline(hybrid);

        var response = await pipeline.SearchAsync("q", documentIds: new[] { "keep" }, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Hybrid);
        response.Results.Should().ContainSingle().Which.DocumentId.Should().Be("keep");
    }

    // ISSUE-161 (pure-keyword search): routed through IKeywordSearchService directly, never through
    // IHybridSearchService with VectorWeight=0 (that path still burns an embedding call + vector
    // search it then discards — the "degenerate weighted hybrid" this strategy value exists to avoid).

    [Fact]
    public async Task SearchAsync_KeywordRequest_WithKeywordService_ExecutesKeyword_NoEmbeddingCall()
    {
        var keyword = Substitute.For<IKeywordSearchService>();
        keyword.SearchAsync(Arg.Any<string>(), Arg.Any<KeywordSearchOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new List<KeywordSearchResult>
            {
                new() { Chunk = new DocumentChunk { Id = "kw-chunk", DocumentId = "doc-3", Content = "bm25 hit", ChunkIndex = 2 }, Score = 4.2 }
            });
        var pipeline = CreatePipeline(hybrid: null, keyword: keyword);

        var response = await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Keyword, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Keyword);
        var item = response.Results.Should().ContainSingle().Subject;
        item.DocumentId.Should().Be("doc-3");
        item.Content.Should().Be("bm25 hit");
        item.Score.Should().BeApproximately(4.2f, 0.0001f);
        // The whole point of a dedicated Keyword strategy: no embedding call, no vector search.
        await _embedding.DidNotReceive().GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await _vectorStore.DidNotReceive().SearchAsync(Arg.Any<float[]>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<Dictionary<string, object>?>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SearchAsync_KeywordRequest_WithoutKeywordService_DegradesToVectorTruthfully()
    {
        var pipeline = CreatePipeline(hybrid: null, keyword: null);

        var response = await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Keyword, ct: TestContext.Current.CancellationToken);

        // No IKeywordSearchService registered: executes vector and says so (mirrors the Hybrid fallback).
        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Vector);
        response.Results.Should().ContainSingle().Which.DocumentId.Should().Be("doc-1");
    }

    [Fact]
    public async Task SearchAsync_KeywordRequest_FiltersByDocumentIds()
    {
        var keyword = Substitute.For<IKeywordSearchService>();
        keyword.SearchAsync(Arg.Any<string>(), Arg.Any<KeywordSearchOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new List<KeywordSearchResult>
            {
                new() { Chunk = new DocumentChunk { Id = "a", DocumentId = "keep", Content = "in scope", ChunkIndex = 0 }, Score = 3.0 },
                new() { Chunk = new DocumentChunk { Id = "b", DocumentId = "drop", Content = "out of scope", ChunkIndex = 0 }, Score = 2.0 }
            });
        var pipeline = CreatePipeline(hybrid: null, keyword: keyword);

        var response = await pipeline.SearchAsync("q", documentIds: new[] { "keep" }, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Keyword, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Keyword);
        response.Results.Should().ContainSingle().Which.DocumentId.Should().Be("keep");
    }

    // ISSUE-161 follow-up (hybrid-route-bypasses-populated-fts): store-native hybrid preference.

    private VaultPipeline CreatePipelineWithStore(IVectorStore store, IHybridSearchService? hybrid)
    {
        _embedding.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new float[] { 0.1f, 0.2f, 0.3f, 0.4f });
        return new VaultPipeline(
            _git, _hasher, _storage, NullLogger<VaultPipeline>.Instance,
            options: null, extractor: null, chunker: null,
            vectorStore: store, embeddingService: _embedding, hybridSearch: hybrid);
    }

    [Fact]
    public async Task SearchAsync_HybridRequest_PrefersStoreNativeHybrid_OverHybridService()
    {
        // Store exposes native hybrid (vec + its own populated keyword index, e.g. chunk_fts).
        var nativeStore = Substitute.For<IVectorStore, INativeHybridSearch>();
        ((INativeHybridSearch)nativeStore).HybridSearchAsync(
                Arg.Any<float[]>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<float?>(), Arg.Any<CancellationToken>())
            .Returns(new List<HybridSearchResult>
            {
                new() { Chunk = new DocumentChunk { Id = "n", DocumentId = "doc-native", Content = "native fused", ChunkIndex = 0 }, FusedScore = 0.99 }
            });
        var hybridService = Substitute.For<IHybridSearchService>();
        var pipeline = CreatePipelineWithStore(nativeStore, hybridService);

        var response = await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Hybrid);
        response.Results.Should().ContainSingle().Which.DocumentId.Should().Be("doc-native");
        // The empty Core retriever path (IHybridSearchService) must be bypassed when native is available.
        await hybridService.DidNotReceive().SearchAsync(Arg.Any<string>(), Arg.Any<HybridSearchOptions?>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SearchAsync_HybridRequest_StoreNativeHybrid_NoServiceRegistered_UsesNative()
    {
        var nativeStore = Substitute.For<IVectorStore, INativeHybridSearch>();
        ((INativeHybridSearch)nativeStore).HybridSearchAsync(
                Arg.Any<float[]>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<float?>(), Arg.Any<CancellationToken>())
            .Returns(new List<HybridSearchResult>
            {
                new() { Chunk = new DocumentChunk { Id = "n2", DocumentId = "doc-native-2", Content = "fts hit", ChunkIndex = 0 }, FusedScore = 0.88 }
            });
        var pipeline = CreatePipelineWithStore(nativeStore, hybrid: null);

        var response = await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Hybrid);
        response.Results.Should().ContainSingle().Which.Content.Should().Be("fts hit");
    }

    // FluxFeed docket #172 (VectorSearchAsync/HybridSearchAsync/KeywordSearchAsync never pushed a
    // filter into the underlying store, so a document-id scope was only ever applied as a client-side
    // Where() over an unscoped candidate window — silent-empty in any vault where unrelated content
    // outnumbers the scoped folder's own competitive matches). These assert the filter argument
    // itself reaches the store/service, not just that the (still over-fetched, still small) window
    // happened to contain the right rows.

    [Fact]
    public async Task VectorSearchAsync_ScopedToDocumentIds_PushesFilterIntoVectorStoreSearch()
    {
        var pipeline = CreatePipeline(hybrid: null);

        await pipeline.SearchAsync("q", documentIds: new[] { "keep" }, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);

        await _vectorStore.Received().SearchAsync(
            Arg.Any<float[]>(),
            Arg.Any<int>(),
            Arg.Any<float>(),
            Arg.Is<Dictionary<string, object>?>(f =>
                f != null && ((HashSet<string>)f["document_id"]).Contains("keep")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task VectorSearchAsync_UnscopedRequest_PassesNullFilter()
    {
        var pipeline = CreatePipeline(hybrid: null);

        await pipeline.SearchAsync("q", documentIds: null, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);

        await _vectorStore.Received().SearchAsync(
            Arg.Any<float[]>(), Arg.Any<int>(), Arg.Any<float>(), null, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task KeywordSearchAsync_ScopedToDocumentIds_PushesMetadataFilter()
    {
        var keyword = Substitute.For<IKeywordSearchService>();
        keyword.SearchAsync(Arg.Any<string>(), Arg.Any<KeywordSearchOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new List<KeywordSearchResult>());
        var pipeline = CreatePipeline(hybrid: null, keyword: keyword);

        await pipeline.SearchAsync("q", documentIds: new[] { "keep" }, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Keyword, ct: TestContext.Current.CancellationToken);

        await keyword.Received().SearchAsync(
            "q",
            Arg.Is<KeywordSearchOptions>(o =>
                o.MetadataFilter != null && ((HashSet<string>)o.MetadataFilter["document_id"]).Contains("keep")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task HybridSearchAsync_ScopedToDocumentIds_PushesFilters()
    {
        var hybrid = Substitute.For<IHybridSearchService>();
        hybrid.SearchAsync(Arg.Any<string>(), Arg.Any<HybridSearchOptions?>(), Arg.Any<CancellationToken>())
            .Returns(new List<HybridSearchResult>());
        var pipeline = CreatePipeline(hybrid);

        await pipeline.SearchAsync("q", documentIds: new[] { "keep" }, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        await hybrid.Received().SearchAsync(
            "q",
            Arg.Is<HybridSearchOptions>(o =>
                ((HashSet<string>)o.Filters["document_id"]).Contains("keep")),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SearchAsync_HybridRequest_Scoped_NativeHybridOnly_DegradesToScopedVector_NotUnscopedNative()
    {
        // Native hybrid can't honor a document-id scope (no filters parameter on its surface yet —
        // see the FluxFeed docket #172 companion note on INativeHybridSearch). With no
        // IHybridSearchService registered either, the scoped request must fall through to the
        // (filterable) vector leg rather than serve an out-of-scope native-hybrid result.
        var nativeStore = Substitute.For<IVectorStore, INativeHybridSearch>();
        nativeStore.SearchAsync(Arg.Any<float[]>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<Dictionary<string, object>?>(), Arg.Any<CancellationToken>())
            .Returns(new List<DocumentChunk>
            {
                new() { Id = "v", DocumentId = "keep", Content = "vector-scoped hit", ChunkIndex = 0, Score = 0.5f }
            });
        var pipeline = CreatePipelineWithStore(nativeStore, hybrid: null);

        var response = await pipeline.SearchAsync("q", documentIds: new[] { "keep" }, topK: 5, minScore: 0f, strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        response.ExecutedStrategy.Should().Be(VaultSearchStrategy.Vector);
        response.Results.Should().ContainSingle().Which.Content.Should().Be("vector-scoped hit");
        await ((INativeHybridSearch)nativeStore).DidNotReceive().HybridSearchAsync(
            Arg.Any<float[]>(), Arg.Any<string>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<float?>(), Arg.Any<CancellationToken>());
    }
}
