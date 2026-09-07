using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Application.Services.Base;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Application.Utilities;
using FluxFeed.Interfaces;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Chunks written before the "document_id" metadata tag existed must stay searchable.
/// <para>
/// <see cref="VaultPipeline"/> pushes a <c>{"document_id": …}</c> filter into the vector store on
/// every search that names a document set — and the caller names one on *every* search, the whole
/// vault when no scope is given. Chunks stored before that tag was written carry no such key, so a
/// filter built unconditionally matches none of them: not a scoped-search bug but a whole-vault
/// outage, silent because nothing logs or throws.
/// </para>
/// <para>
/// These tests exercise the real filter semantics by deriving the store double from
/// <see cref="VectorStoreBase"/> rather than substituting <see cref="IVectorStore"/>. A substitute
/// returns whatever it was told to and never applies the filter it is handed, which is why a suite
/// asserting only "the filter argument reached the store" stayed green while every real vault
/// returned nothing.
/// </para>
/// </summary>
public class VaultPipelineLegacyChunkScopeTests
{
    private readonly IGitService _git = Substitute.For<IGitService>();
    private readonly IContentHasher _hasher = Substitute.For<IContentHasher>();
    private readonly IVaultStorageService _storage = Substitute.For<IVaultStorageService>();
    private readonly IEmbeddingService _embedding = Substitute.For<IEmbeddingService>();

    private static readonly float[] QueryVector = [1f, 0f, 0f, 0f];

    private VaultPipeline CreatePipeline(IVectorStore store)
    {
        _embedding.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(QueryVector);

        return new VaultPipeline(
            _git, _hasher, _storage, NullLogger<VaultPipeline>.Instance,
            options: null, extractor: null, chunker: null,
            vectorStore: store, embeddingService: _embedding);
    }

    /// <summary>
    /// Stores a chunk the way a pre-tag release did: through <see cref="IVectorStore.StoreAsync"/>
    /// with provenance metadata but no "document_id" key.
    /// </summary>
    private static async Task StoreLegacyChunkAsync(IVectorStore store, string documentId, string id, string content, CancellationToken ct)
    {
        await store.StoreAsync(new DocumentChunk
        {
            Id = id,
            DocumentId = documentId,
            ChunkIndex = 0,
            Content = content,
            Embedding = [1f, 0f, 0f, 0f],
            Metadata = new Dictionary<string, object>
            {
                ["source_path"] = $"C:/vault/{documentId}.md",
                ["file_name"] = $"{documentId}.md"
            }
        }, ct);
    }

    [Fact]
    public async Task SearchAsync_ScopedToLegacyChunks_StillReturnsThem()
    {
        var store = new RecordingVectorStore();
        await StoreLegacyChunkAsync(store, "doc-a", "chunk-a", "alpha content", TestContext.Current.CancellationToken);
        await StoreLegacyChunkAsync(store, "doc-b", "chunk-b", "beta content", TestContext.Current.CancellationToken);
        var pipeline = CreatePipeline(store);

        var response = await pipeline.SearchAsync(
            "alpha", documentIds: ["doc-a"], topK: 10, minScore: 0f,
            strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);

        response.Results.Should().ContainSingle()
            .Which.DocumentId.Should().Be("doc-a");
    }

    /// <summary>
    /// The widest form of the outage: the caller names the whole vault, so an unscoped search is
    /// filtered exactly like a scoped one and returns nothing on a pre-tag vault.
    /// </summary>
    [Fact]
    public async Task SearchAsync_WholeVaultScope_OverLegacyChunks_StillReturnsThem()
    {
        var store = new RecordingVectorStore();
        await StoreLegacyChunkAsync(store, "doc-a", "chunk-a", "alpha content", TestContext.Current.CancellationToken);
        await StoreLegacyChunkAsync(store, "doc-b", "chunk-b", "beta content", TestContext.Current.CancellationToken);
        var pipeline = CreatePipeline(store);

        var response = await pipeline.SearchAsync(
            "alpha", documentIds: ["doc-a", "doc-b"], topK: 10, minScore: 0f,
            strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);

        response.Results.Should().HaveCount(2);
    }

    /// <summary>
    /// The backfill must not cost a re-embed: it carries no embedding, so the store leaves the
    /// stored vector alone. Asserted two ways — the chunk handed to <c>UpdateAsync</c> has none,
    /// and the row is still findable by similarity afterwards (a nulled vector would drop it).
    /// </summary>
    [Fact]
    public async Task SearchAsync_BackfillingLegacyChunks_PreservesStoredEmbedding()
    {
        var store = new RecordingVectorStore();
        await StoreLegacyChunkAsync(store, "doc-a", "chunk-a", "alpha content", TestContext.Current.CancellationToken);
        var pipeline = CreatePipeline(store);

        await pipeline.SearchAsync(
            "alpha", documentIds: ["doc-a"], topK: 10, minScore: 0f,
            strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);

        store.UpdatedChunks.Should().NotBeEmpty("the legacy chunk needs its tag written");
        store.UpdatedChunks.Should().OnlyContain(c => c.Embedding == null,
            "a backfill that carries an embedding would rewrite the vector it should preserve");
        store.Vectors["chunk-a"].Should().NotBeNull();

        var second = await pipeline.SearchAsync(
            "alpha", documentIds: ["doc-a"], topK: 10, minScore: 0f,
            strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);
        second.Results.Should().ContainSingle();
    }

    /// <summary>
    /// A vault whose chunks already carry the tag must not be rewritten — the backfill is a
    /// migration, not a per-search write.
    /// </summary>
    [Fact]
    public async Task SearchAsync_TaggedChunks_AreNotRewritten()
    {
        var store = new RecordingVectorStore();
        await store.StoreAsync(new DocumentChunk
        {
            Id = "chunk-a",
            DocumentId = "doc-a",
            ChunkIndex = 0,
            Content = "alpha content",
            Embedding = [1f, 0f, 0f, 0f],
            Metadata = new Dictionary<string, object> { ["document_id"] = "doc-a" }
        }, TestContext.Current.CancellationToken);
        var pipeline = CreatePipeline(store);

        await pipeline.SearchAsync(
            "alpha", documentIds: ["doc-a"], topK: 10, minScore: 0f,
            strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);

        store.UpdatedChunks.Should().BeEmpty();
    }

    /// <summary>
    /// The scan is a one-time migration per document, not a per-search cost: a second search must
    /// not re-enumerate documents the first one already settled.
    /// </summary>
    [Fact]
    public async Task SearchAsync_RepeatedSearches_ScanEachDocumentOnce()
    {
        var store = new RecordingVectorStore();
        await StoreLegacyChunkAsync(store, "doc-a", "chunk-a", "alpha content", TestContext.Current.CancellationToken);
        var pipeline = CreatePipeline(store);

        for (var i = 0; i < 3; i++)
        {
            await pipeline.SearchAsync(
                "alpha", documentIds: ["doc-a"], topK: 10, minScore: 0f,
                strategy: VaultSearchStrategy.Vector, ct: TestContext.Current.CancellationToken);
        }

        store.GetByDocumentIdCalls.Should().Be(1);
    }

    /// <summary>
    /// In-memory <see cref="VectorStoreBase"/> so the filter, the metadata backstop and the
    /// "no embedding means leave the vector alone" update contract are the real ones. Cosine
    /// similarity over the stored vectors; no candidate trimming, so recall effects are the base
    /// class's to demonstrate, not this double's.
    /// </summary>
    private sealed class RecordingVectorStore : VectorStoreBase
    {
        private readonly Dictionary<string, DocumentChunk> _chunks = [];

        public Dictionary<string, float[]> Vectors { get; } = [];
        public List<DocumentChunk> UpdatedChunks { get; } = [];
        public int GetByDocumentIdCalls { get; private set; }

        protected override Task<string> StoreCoreAsync(DocumentChunk chunk, CancellationToken cancellationToken)
        {
            _chunks[chunk.Id] = chunk;
            if (chunk.Embedding != null)
                Vectors[chunk.Id] = chunk.Embedding;
            return Task.FromResult(chunk.Id);
        }

        protected override Task<DocumentChunk?> GetCoreAsync(string id, CancellationToken cancellationToken)
            => Task.FromResult(_chunks.TryGetValue(id, out var chunk) ? chunk : null);

        protected override Task<IEnumerable<VectorSearchResult>> SearchCoreAsync(
            float[] queryEmbedding, int topK, Dictionary<string, object>? filters, CancellationToken cancellationToken)
        {
            var results = _chunks.Values
                .Where(c => Vectors.ContainsKey(c.Id))
                .Select(c => new VectorSearchResult(Strip(c), Cosine(queryEmbedding, Vectors[c.Id])));
            return Task.FromResult<IEnumerable<VectorSearchResult>>(results.ToList());
        }

        protected override Task<bool> UpdateCoreAsync(DocumentChunk chunk, CancellationToken cancellationToken)
        {
            if (!_chunks.TryGetValue(chunk.Id, out var existing))
                return Task.FromResult(false);

            UpdatedChunks.Add(chunk);
            existing.Content = chunk.Content;
            existing.Metadata = chunk.Metadata ?? [];
            // Mirrors the storage contract: only a chunk that carries an embedding rewrites the
            // stored vector. A backfill carries none, so the vector survives untouched.
            if (chunk.Embedding != null)
                Vectors[chunk.Id] = chunk.Embedding;
            return Task.FromResult(true);
        }

        protected override Task<IEnumerable<DocumentChunk>> GetByDocumentIdCoreAsync(
            string documentId, CancellationToken cancellationToken)
        {
            GetByDocumentIdCalls++;
            // The stored embedding is deliberately not projected back, exactly as the shipped
            // stores do — which is what makes the backfill embedding-free by construction.
            IEnumerable<DocumentChunk> found = _chunks.Values
                .Where(c => c.DocumentId == documentId)
                .Select(Strip)
                .ToList();
            return Task.FromResult(found);
        }

        protected override Task<bool> DeleteCoreAsync(string id, CancellationToken cancellationToken)
        {
            Vectors.Remove(id);
            return Task.FromResult(_chunks.Remove(id));
        }

        protected override Task<bool> DeleteByDocumentIdCoreAsync(string documentId, CancellationToken cancellationToken)
        {
            var ids = _chunks.Values.Where(c => c.DocumentId == documentId).Select(c => c.Id).ToList();
            foreach (var id in ids)
            {
                _chunks.Remove(id);
                Vectors.Remove(id);
            }
            return Task.FromResult(ids.Count > 0);
        }

        protected override Task<int> CountCoreAsync(CancellationToken cancellationToken)
            => Task.FromResult(_chunks.Count);

        protected override Task ClearCoreAsync(CancellationToken cancellationToken)
        {
            _chunks.Clear();
            Vectors.Clear();
            return Task.CompletedTask;
        }

        private static DocumentChunk Strip(DocumentChunk chunk) => new()
        {
            Id = chunk.Id,
            DocumentId = chunk.DocumentId,
            ChunkIndex = chunk.ChunkIndex,
            Content = chunk.Content,
            TokenCount = chunk.TokenCount,
            Metadata = chunk.Metadata,
            Embedding = null
        };

        private static float Cosine(float[] a, float[] b)
        {
            double dot = 0, na = 0, nb = 0;
            for (var i = 0; i < Math.Min(a.Length, b.Length); i++)
            {
                dot += a[i] * b[i];
                na += a[i] * a[i];
                nb += b[i] * b[i];
            }
            return na == 0 || nb == 0 ? 0f : (float)(dot / (Math.Sqrt(na) * Math.Sqrt(nb)));
        }
    }
}
