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
/// Indexing a document replaces the rows previously written for it. Memorizing an already-indexed
/// file used to append a second copy of every chunk rather than replace it, and each further
/// memorize added another — a search then returned the same chunk once per memorize and spent the
/// caller's result budget on repeats.
///
/// These tests count the rows the backends actually hold rather than the calls the pipeline makes.
/// Entry metadata is deliberately not the coordinate: <c>ChunkCount</c> is overwritten rather than
/// summed, so it reads correct while the row leak is in progress — an assertion on it passes in the
/// defective state.
/// </summary>
public sealed class VaultPipelineReindexReplacementTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    /// <summary>Rows the vector store holds, as the pipeline's stores and deletes leave them.</summary>
    private readonly List<DocumentChunk> _vectorRows = [];

    /// <summary>Rows the keyword index holds, tracked the same way.</summary>
    private readonly List<DocumentChunk> _keywordRows = [];

    public VaultPipelineReindexReplacementTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FluxFeedReindex_{Guid.NewGuid():N}");
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
            .Returns(ci =>
            {
                var stored = ((IEnumerable<DocumentChunk>)ci[0]).ToList();
                _vectorRows.AddRange(stored);
                return Task.FromResult<IEnumerable<string>>(stored.Select(_ => Guid.NewGuid().ToString()).ToList());
            });
        _vectorStore.DeleteByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                _vectorRows.RemoveAll(c => c.DocumentId == (string)ci[0]);
                return Task.FromResult(true);
            });
        // Reading back and deleting by chunk id are modelled too, so the double stands for a store
        // rather than for one particular way of clearing it. Wiring only DeleteByDocumentIdAsync
        // made these tests assert the MECHANISM the pipeline happened to use: the row counts below
        // are the guarantee, and they must hold however the replacement is performed.
        _vectorStore.GetByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<DocumentChunk>>(
                _vectorRows.Where(c => c.DocumentId == (string)ci[0]).ToList()));
        _vectorStore.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(_vectorRows.RemoveAll(c => c.Id == (string)ci[0]) > 0));

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

    private IKeywordSearchService CreateKeywordIndex()
    {
        var keyword = Substitute.For<IKeywordSearchService>();
        keyword.IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                _keywordRows.AddRange((IEnumerable<DocumentChunk>)ci[0]);
                return Task.CompletedTask;
            });
        keyword.DeleteByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                _keywordRows.RemoveAll(c => c.DocumentId == (string)ci[0]);
                return Task.CompletedTask;
            });
        keyword.DeleteChunkAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                _keywordRows.RemoveAll(c => c.Id == (string)ci[0]);
                return Task.CompletedTask;
            });
        return keyword;
    }

    private VaultPipeline CreatePipeline(IKeywordSearchService? keywordSearch = null) =>
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

    private async Task<VaultEntry> CreateEntryAsync(string fileName, string content)
    {
        var docPath = Path.Combine(_testDir, fileName);
        await File.WriteAllTextAsync(docPath, content);
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);
        return entry;
    }

    private static MemorizeOptions Options() => new() { MaxChunkSize = 200 };

    [Fact]
    public async Task Memorize_RepeatedOnUnchangedContent_LeavesOneRowPerChunk()
    {
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync(
            "contract.txt",
            "The client pays a retainer each month. Either party may terminate with notice.");

        var first = await pipeline.MemorizeAsync(entry, Options());
        first.Success.Should().BeTrue();
        var chunkCount = _vectorRows.Count;
        chunkCount.Should().BeGreaterThan(0);

        await pipeline.MemorizeAsync(entry, Options());
        await pipeline.MemorizeAsync(entry, Options());

        _vectorRows.Should().HaveCount(chunkCount, "a third memorize replaces the rows rather than adding a third copy");
        _vectorRows.Select(c => c.ChunkIndex).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Memorize_AfterSourceContentChanged_LeavesOneRowPerChunk()
    {
        // The watch-driven path: a memorized file is edited and re-memorized. It reaches the same
        // code as an unchanged re-memorize, so it leaked the same way.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("invoice.txt", "Consulting hours billed for the quarter.");

        await pipeline.MemorizeAsync(entry, Options());
        var afterFirst = _vectorRows.Count;

        await File.WriteAllTextAsync(
            entry.SourcePath,
            "Consulting hours billed for the quarter. A revision line changes the content hash.");
        await pipeline.MemorizeAsync(entry, Options());

        _vectorRows.Select(c => c.ChunkIndex).Should().OnlyHaveUniqueItems();
        _vectorRows.Should().HaveCountGreaterThanOrEqualTo(afterFirst);
        _vectorRows.Should().OnlyContain(c => c.Content.Contains("revision line", StringComparison.Ordinal)
                                              || c.Content.Contains("Consulting hours", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Memorize_WhenContentBecomesEmpty_DropsTheRowsItNoLongerHas()
    {
        // A document whose content is gone must stop answering searches. This path returns before
        // the shared indexing step, so it carries the replace obligation on its own.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("notes.txt", "Rooms were measured before the tenant moved in.");

        await pipeline.MemorizeAsync(entry, Options());
        _vectorRows.Should().NotBeEmpty();

        await File.WriteAllTextAsync(entry.SourcePath, string.Empty);
        var result = await pipeline.MemorizeAsync(entry, Options());

        result.Success.Should().BeTrue();
        result.ChunkCount.Should().Be(0);
        _vectorRows.Should().BeEmpty("the entry reports zero chunks, so no row may still answer for it");
    }

    [Fact]
    public async Task Memorize_RepeatedWithKeywordIndexRegistered_LeavesOneKeywordRowPerChunk()
    {
        var pipeline = CreatePipeline(CreateKeywordIndex());
        var entry = await CreateEntryAsync("policy.txt", "Requests are reviewed within five business days.");

        await pipeline.MemorizeAsync(entry, Options());
        var chunkCount = _keywordRows.Count;
        chunkCount.Should().BeGreaterThan(0);

        await pipeline.MemorizeAsync(entry, Options());

        _keywordRows.Should().HaveCount(chunkCount);
        _keywordRows.Select(c => c.ChunkIndex).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Refresh_AfterMemorize_StillLeavesOneRowPerChunk()
    {
        // Refresh already replaced its rows before the shared step owned the removal; it must keep
        // doing so now that the removal moved.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("handbook.txt", "Expenses are reimbursed at cost with receipts.");

        await pipeline.MemorizeAsync(entry, Options());
        var chunkCount = _vectorRows.Count;

        var refreshed = await pipeline.RefreshAsync(entry, Options());

        refreshed.Success.Should().BeTrue();
        _vectorRows.Should().HaveCount(chunkCount);
        _vectorRows.Select(c => c.ChunkIndex).Should().OnlyHaveUniqueItems();
    }
}
