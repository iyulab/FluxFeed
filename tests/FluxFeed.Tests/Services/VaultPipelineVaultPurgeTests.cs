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
/// Verifies the shared-store tenant purge primitive: memorized chunks carry a vault_id tag so a
/// multi-tenant consumer can bulk-purge one vault's vectors via a single filtered delete
/// (D-C stage i — replaces AIMS's forked per-entry delete loop).
/// </summary>
public sealed class VaultPipelineVaultPurgeTests : IDisposable
{
    private const string TenantId = "tenant-x";

    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    public VaultPipelineVaultPurgeTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FileVaultPurge_{Guid.NewGuid():N}");
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

    private VaultPipeline CreatePipeline(string? vaultId, IKeywordSearchService? keywordSearch = null) =>
        new(
            _git,
            new ContentHasher(),
            _storage,
            NullLogger<VaultPipeline>.Instance,
            options: MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir, VaultId = vaultId }),
            extractor: null,
            chunker: null,
            vectorStore: _vectorStore,
            embeddingService: _embeddingService,
            hybridSearch: null,
            graphRAGService: null,
            keywordSearchService: keywordSearch);

    [Fact]
    public async Task MemorizeAsync_TagsStoredChunksWithVaultId()
    {
        var pipeline = CreatePipeline(TenantId);

        var docPath = Path.Combine(_testDir, "doc.txt");
        await File.WriteAllTextAsync(docPath, "Alice works at Acme Corp. Bob manages the project in Seoul.", TestContext.Current.CancellationToken);
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, TestContext.Current.CancellationToken);

        var result = await pipeline.MemorizeAsync(entry, new MemorizeOptions { MaxChunkSize = 200 }, TestContext.Current.CancellationToken);

        result.Success.Should().BeTrue();
        await _vectorStore.Received().StoreBatchAsync(
            Arg.Is<IEnumerable<DocumentChunk>>(chunks =>
                chunks.All(c => c.Metadata != null
                    && c.Metadata.ContainsKey("vault_id")
                    && (string)c.Metadata["vault_id"] == TenantId)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PurgeVectorsAsync_DeletesByVaultIdFilter()
    {
        var pipeline = CreatePipeline(TenantId);

        await pipeline.PurgeVectorsAsync(TenantId, TestContext.Current.CancellationToken);

        await _vectorStore.Received(1).DeleteByFilterAsync(
            Arg.Is<Dictionary<string, object>>(f =>
                f.ContainsKey("vault_id") && (string)f["vault_id"] == TenantId),
            Arg.Any<CancellationToken>());
    }

    // === Both legs, or the purge is a lie ===
    //
    // The keyword index had no tag-scoped bulk delete, so this method cleared the vectors, logged a
    // warning, and returned success while the tenant's text stayed searchable. A purge that leaves
    // half the data behind is worse than one that fails: the caller believes the tenant is gone.

    [Fact]
    public async Task PurgeVectorsAsync_AlsoPurgesTheKeywordIndex()
    {
        var keywordSearch = Substitute.For<IKeywordSearchService>();
        keywordSearch.DeleteByFilterAsync(
                Arg.Any<IReadOnlyDictionary<string, object>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(3));

        var pipeline = CreatePipeline(TenantId, keywordSearch);

        await pipeline.PurgeVectorsAsync(TenantId, TestContext.Current.CancellationToken);

        await keywordSearch.Received(1).DeleteByFilterAsync(
            Arg.Is<IReadOnlyDictionary<string, object>>(f =>
                f.ContainsKey("vault_id") && (string)f["vault_id"] == TenantId),
            Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// Both legs hold the same chunks, so summing the two counts would report twice as many chunks
    /// as ever existed. The vector count is the chunk count.
    /// </summary>
    [Fact]
    public async Task PurgeVectorsAsync_ReportsTheChunkCountOnce_NotTheSumOfBothLegs()
    {
        _vectorStore.DeleteByFilterAsync(
                Arg.Any<Dictionary<string, object>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(3));

        var keywordSearch = Substitute.For<IKeywordSearchService>();
        keywordSearch.DeleteByFilterAsync(
                Arg.Any<IReadOnlyDictionary<string, object>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(3));

        var removed = await CreatePipeline(TenantId, keywordSearch).PurgeVectorsAsync(TenantId, TestContext.Current.CancellationToken);

        removed.Should().Be(3);
    }

    [Fact]
    public async Task PurgeVectorsAsync_WithoutKeywordService_StillPurgesVectors()
    {
        var pipeline = CreatePipeline(TenantId);

        await pipeline.PurgeVectorsAsync(TenantId, TestContext.Current.CancellationToken);

        await _vectorStore.Received(1).DeleteByFilterAsync(
            Arg.Any<Dictionary<string, object>>(), Arg.Any<CancellationToken>());
    }
}
