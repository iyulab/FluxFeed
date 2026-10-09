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
/// Chunks written without a "document_id" metadata copy must stay searchable.
/// <para>
/// <see cref="VaultPipeline"/> pushes a <c>{"document_id": …}</c> filter into the vector store on
/// every search that names a document set — and the caller names one on *every* search, the whole
/// vault when no scope is given. Chunks stored before the pipeline wrote a metadata copy of their
/// document id carry no such key. The store resolves the key to the chunk's own document id, so
/// they match; were it read as a metadata key, the filter would match none of them — not a
/// scoped-search bug but a whole-vault outage, silent because nothing logs or throws.
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
        _embedding.GenerateQueryEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
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
    /// A search is read-only. The scope is resolved by the store from each chunk's own document id,
    /// so nothing has to be written onto old chunks first and no document has to be read one by one —
    /// an earlier release migrated a metadata copy of the id on every scoped search, per document.
    /// </summary>
    [Fact]
    public async Task SearchAsync_OverLegacyChunks_NeitherWritesNorReadsPerDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = new RecordingVectorStore();
        await StoreLegacyChunkAsync(store, "doc-a", "a1", "alpha", ct);
        await StoreLegacyChunkAsync(store, "doc-b", "b1", "alpha", ct);
        var pipeline = CreatePipeline(store);

        var response = await pipeline.SearchAsync(
            "alpha", documentIds: ["doc-a", "doc-b"], topK: 10, minScore: 0f,
            strategy: VaultSearchStrategy.Vector, ct: ct);

        response.Results.Should().HaveCount(2);
        store.UpdatedChunks.Should().BeEmpty("a search must not rewrite stored chunks");
        store.GetByDocumentIdCalls.Should().Be(0, "a search must not read the vault document by document");
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

        protected override Task<IEnumerable<ScoredChunk>> SearchCoreAsync(
            float[] queryEmbedding, int topK, Dictionary<string, object>? filters, CancellationToken cancellationToken)
        {
            var results = _chunks.Values
                .Where(c => Vectors.ContainsKey(c.Id))
                .Select(c => new ScoredChunk(Strip(c), Cosine(queryEmbedding, Vectors[c.Id])));
            return Task.FromResult<IEnumerable<ScoredChunk>>(results.ToList());
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
