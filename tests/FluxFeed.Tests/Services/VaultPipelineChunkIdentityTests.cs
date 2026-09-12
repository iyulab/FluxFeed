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
/// Chunk identity through the pipeline: a chunk's id is derived from the document and the passage
/// (<see cref="ChunkIdentity"/>), so a re-memorize writes the same ids and the generation swap only
/// touches what changed — superseded = previous ∖ this run, rollback = this run ∖ previous.
/// The store double keeps one row per id (re-storing an id replaces the row), which is the
/// IVectorStore contract every FluxIndex store honours from 0.36.2.
/// </summary>
public sealed class VaultPipelineChunkIdentityTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    private readonly List<DocumentChunk> _vectorRows = [];
    private readonly List<string> _deletedIds = [];

    /// <summary>1-based ordinal of the single-embedding call that throws; null = never.</summary>
    private int? _failOnSingleEmbeddingCall;
    private int _singleEmbeddingCalls;

    // One line per chunk: each line fits in a chunk on its own and no two lines fit together.
    private const int ChunkSize = 35;
    private const string LineOne = "Alpha section, line one.";
    private const string LineTwo = "Bravo section, line two.";
    private const string LineThree = "Charlie section, line three.";

    public VaultPipelineChunkIdentityTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FluxFeedChunkId_{Guid.NewGuid():N}");
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
                foreach (var chunk in stored)
                {
                    _vectorRows.RemoveAll(r => r.Id == chunk.Id);
                    _vectorRows.Add(chunk);
                }
                return Task.FromResult<IEnumerable<string>>(stored.Select(c => c.Id).ToList());
            });
        _vectorStore.GetByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<DocumentChunk>>(
                _vectorRows.Where(c => c.DocumentId == (string)ci[0]).ToList()));
        _vectorStore.GetChunkIdsByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IReadOnlyList<string>>(
                _vectorRows.Where(c => c.DocumentId == (string)ci[0]).Select(c => c.Id).ToList()));
        _vectorStore.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var id = (string)ci[0];
                _deletedIds.Add(id);
                return Task.FromResult(_vectorRows.RemoveAll(c => c.Id == id) > 0);
            });

        _embeddingService = Substitute.For<IEmbeddingService>();
        _embeddingService.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<float[]>>(
                ((IEnumerable<string>)ci[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f }).ToList()));
        _embeddingService.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _singleEmbeddingCalls++;
                return _singleEmbeddingCalls == _failOnSingleEmbeddingCall
                    ? Task.FromException<float[]>(new InvalidOperationException("embedding provider rejected a chunk"))
                    : Task.FromResult(new[] { 0.1f, 0.2f, 0.3f });
            });
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

    private VaultPipeline CreatePipeline() =>
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
            keywordSearchService: null);

    private async Task<VaultEntry> CreateEntryAsync(string fileName, params string[] lines)
    {
        var docPath = Path.Combine(_testDir, fileName);
        await File.WriteAllTextAsync(docPath, string.Join('\n', lines));
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);
        return entry;
    }

    private static MemorizeOptions BatchOptions() => new() { MaxChunkSize = ChunkSize };

    private static MemorizeOptions PerChunkOptions() => new()
    {
        MaxChunkSize = ChunkSize,
        StartFromChunkIndex = -1,
        CheckpointCallback = (_, _) => Task.CompletedTask
    };

    private List<string> RowIds() => _vectorRows.Select(c => c.Id).ToList();

    [Fact]
    public async Task Memorize_Twice_Unchanged_WritesTheSameIds_AndDeletesNothing()
    {
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("contract.txt", LineOne, LineTwo, LineThree);

        (await pipeline.MemorizeAsync(entry, BatchOptions(), TestContext.Current.CancellationToken)).Success.Should().BeTrue();
        var firstGeneration = RowIds();
        firstGeneration.Should().HaveCount(3, "one chunk per line is the precondition");

        (await pipeline.MemorizeAsync(entry, BatchOptions(), TestContext.Current.CancellationToken)).Success.Should().BeTrue();

        RowIds().Should().BeEquivalentTo(firstGeneration, "an unchanged document writes the same ids again");
        _deletedIds.Should().BeEmpty("nothing was superseded, so nothing is deleted");
        _vectorRows.Select(c => c.ChunkIndex).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task Memorize_AfterOnePassageChanged_ReplacesOnlyThatPassage()
    {
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("contract.txt", LineOne, LineTwo, LineThree);
        await pipeline.MemorizeAsync(entry, BatchOptions(), TestContext.Current.CancellationToken);
        var firstGeneration = RowIds();
        var lineTwoId = _vectorRows.Single(c => c.Content.Contains("Bravo", StringComparison.Ordinal)).Id;

        await File.WriteAllTextAsync(entry.SourcePath, string.Join('\n', LineOne, "Bravo section, rewritten.", LineThree),
            TestContext.Current.CancellationToken);
        await pipeline.MemorizeAsync(entry, BatchOptions(), TestContext.Current.CancellationToken);

        _deletedIds.Should().ContainSingle().Which.Should().Be(lineTwoId, "only the rewritten passage is superseded");
        var secondGeneration = RowIds();
        secondGeneration.Should().HaveCount(3);
        secondGeneration.Should().Contain(firstGeneration.Where(id => id != lineTwoId), "unchanged passages keep their ids");
        secondGeneration.Should().NotContain(lineTwoId);
        _vectorRows.Single(c => c.Content.Contains("rewritten", StringComparison.Ordinal)).Id
            .Should().Be(ChunkIdentity.ForText(entry.FilepathHash, "Bravo section, rewritten.", 0));
    }

    [Fact]
    public async Task Memorize_WithRepeatedPassages_GivesEachOccurrenceItsOwnRow()
    {
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("notice.txt", LineOne, LineTwo, LineOne);

        await pipeline.MemorizeAsync(entry, BatchOptions(), TestContext.Current.CancellationToken);
        await pipeline.MemorizeAsync(entry, BatchOptions(), TestContext.Current.CancellationToken);

        _vectorRows.Should().HaveCount(3, "a repeated passage is two chunks, not one shadowing the other");
        RowIds().Should().OnlyHaveUniqueItems();
        RowIds().Should().BeEquivalentTo(
        [
            ChunkIdentity.ForText(entry.FilepathHash, LineOne, 0),
            ChunkIdentity.ForText(entry.FilepathHash, LineTwo, 0),
            ChunkIdentity.ForText(entry.FilepathHash, LineOne, 1),
        ]);
    }

    [Fact]
    public async Task Reindex_ThatFailsMidway_RollsBackOnlyTheNewIds_AndLeavesThePreviousGenerationWhole()
    {
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("manual.txt", LineOne, LineTwo, LineThree);
        await pipeline.MemorizeAsync(entry, PerChunkOptions(), TestContext.Current.CancellationToken);
        var previousGeneration = RowIds();
        previousGeneration.Should().HaveCount(3);

        // Line two changes and a fourth line appears. The per-chunk path embeds in order: line one
        // (unchanged, an update in place), the new line two (a new id), then the third call throws.
        await File.WriteAllTextAsync(entry.SourcePath,
            string.Join('\n', LineOne, "Bravo section, rewritten.", LineThree, "Delta section, line four."),
            TestContext.Current.CancellationToken);
        _singleEmbeddingCalls = 0;
        _failOnSingleEmbeddingCall = 3;

        var result = await pipeline.MemorizeAsync(entry, PerChunkOptions(), TestContext.Current.CancellationToken);

        result.Success.Should().BeFalse("the embedding failure is a failure");
        var rewrittenId = ChunkIdentity.ForText(entry.FilepathHash, "Bravo section, rewritten.", 0);
        _deletedIds.Should().ContainSingle().Which.Should().Be(rewrittenId,
            "the rollback removes what this run added and nothing the previous generation owned");
        RowIds().Should().BeEquivalentTo(previousGeneration,
            "the previous generation is left whole: its rows were either untouched or updated in place");
    }

    [Fact]
    public async Task Reindex_ThatSucceedsAfterAFailure_LandsTheNewGenerationExactly()
    {
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("manual.txt", LineOne, LineTwo, LineThree);
        await pipeline.MemorizeAsync(entry, PerChunkOptions(), TestContext.Current.CancellationToken);

        await File.WriteAllTextAsync(entry.SourcePath,
            string.Join('\n', LineOne, "Bravo section, rewritten.", LineThree, "Delta section, line four."),
            TestContext.Current.CancellationToken);
        _singleEmbeddingCalls = 0;
        _failOnSingleEmbeddingCall = 3;
        await pipeline.MemorizeAsync(entry, PerChunkOptions(), TestContext.Current.CancellationToken);

        _failOnSingleEmbeddingCall = null;
        var retry = await pipeline.MemorizeAsync(entry, PerChunkOptions(), TestContext.Current.CancellationToken);

        retry.Success.Should().BeTrue();
        RowIds().Should().BeEquivalentTo(
            ChunkIdentity.ForTexts(entry.FilepathHash,
                [LineOne, "Bravo section, rewritten.", LineThree, "Delta section, line four."]),
            "after the retry the store holds exactly the new generation");
        _deletedIds.Should().Contain(ChunkIdentity.ForText(entry.FilepathHash, LineTwo, 0),
            "the old line two is superseded once the new generation is durably written");
    }
}
