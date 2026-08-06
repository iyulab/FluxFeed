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
/// Re-indexing REPLACES a document's rows, and the replacement is a swap: the previous rows survive
/// until the new ones are written.
///
/// <para>
/// Before this, the rows were deleted first. Indexing embeds every chunk in one call, so one bad
/// chunk fails the whole document — and the document was then left with no index at all, having
/// been searchable moments earlier. Measured in operation: a 279-document re-index, one failure,
/// that document went from 8 chunks to 0. Nothing in the result distinguished "failed" from
/// "failed and destroyed what was there".
/// </para>
///
/// <para>
/// The counterpart guarantee — that a repeated memorize does not accumulate copies — lives in
/// <see cref="VaultPipelineReindexReplacementTests"/>. Both must hold at once: that is the whole
/// difficulty, since the obvious fix for either one breaks the other.
/// </para>
/// </summary>
public sealed class VaultPipelineIndexSwapTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    private readonly List<DocumentChunk> _vectorRows = [];

    /// <summary>Set to make the batch embedding call throw, standing in for a provider failure.</summary>
    private bool _failEmbedding;

    public VaultPipelineIndexSwapTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FluxFeedSwap_{Guid.NewGuid():N}");
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
                return Task.FromResult<IEnumerable<string>>(stored.Select(c => c.Id).ToList());
            });
        _vectorStore.GetByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<DocumentChunk>>(
                _vectorRows.Where(c => c.DocumentId == (string)ci[0]).ToList()));
        _vectorStore.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult(_vectorRows.RemoveAll(c => c.Id == (string)ci[0]) > 0));
        _vectorStore.DeleteByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                _vectorRows.RemoveAll(c => c.DocumentId == (string)ci[0]);
                return Task.FromResult(true);
            });

        _embeddingService = Substitute.For<IEmbeddingService>();
        _embeddingService.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci => _failEmbedding
                ? Task.FromException<IEnumerable<float[]>>(new InvalidOperationException("embedding provider rejected a chunk"))
                : Task.FromResult<IEnumerable<float[]>>(
                    ((IEnumerable<string>)ci[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f }).ToList()));
        _embeddingService.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => _failEmbedding
                ? Task.FromException<float[]>(new InvalidOperationException("embedding provider rejected a chunk"))
                : Task.FromResult(new[] { 0.1f, 0.2f, 0.3f }));
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
    public async Task Reindex_WhenEmbeddingFails_LeavesThePreviousIndexIntact()
    {
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync(
            "lease.txt",
            "The tenant pays rent monthly. The landlord maintains the roof and the heating.");

        await pipeline.MemorizeAsync(entry, Options());
        var indexed = _vectorRows.Select(c => c.Id).ToList();
        indexed.Should().NotBeEmpty("the precondition of this test is a document that IS indexed");

        // Re-index the same document; the provider now rejects the batch.
        _failEmbedding = true;
        await File.WriteAllTextAsync(entry.SourcePath, "The tenant pays rent monthly. A clause was added.");
        var result = await pipeline.MemorizeAsync(entry, Options());

        result.Success.Should().BeFalse("the embedding failure must still be reported as a failure");

        _vectorRows.Select(c => c.Id).Should().BeEquivalentTo(
            indexed,
            "a failed re-index leaves the document exactly as searchable as it was before");
    }

    [Fact]
    public async Task Reindex_WhenEmbeddingFails_DoesNotLeaveHalfOfTheNewGenerationBehind()
    {
        // The rollback half of the swap. Without it a failure leaves both generations present and
        // search returns a mix of old chunks and whichever new ones landed before the throw.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("policy.txt", "Claims are settled within thirty days.");

        await pipeline.MemorizeAsync(entry, Options());
        var before = _vectorRows.Count;

        _failEmbedding = true;
        await File.WriteAllTextAsync(entry.SourcePath, "Claims are settled within sixty days instead.");
        await pipeline.MemorizeAsync(entry, Options());

        _vectorRows.Should().HaveCount(before, "no partial generation may survive alongside the previous one");
        _vectorRows.Should().OnlyContain(
            c => c.Content.Contains("thirty days", StringComparison.Ordinal),
            "the surviving rows are the previous generation, not a mixture");
    }

    [Fact]
    public async Task Reindex_ThatSucceedsAfterAFailure_ReplacesTheOldGeneration()
    {
        // Recovery: the failure preserved the old rows, so the retry has to supersede them rather
        // than add to them -- otherwise "keep the old index on failure" would trade a loss for a leak.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("permit.txt", "Work may proceed between eight and six.");

        await pipeline.MemorizeAsync(entry, Options());
        var firstGeneration = _vectorRows.Count;

        _failEmbedding = true;
        await File.WriteAllTextAsync(entry.SourcePath, "Work may proceed between nine and five.");
        await pipeline.MemorizeAsync(entry, Options());

        _failEmbedding = false;
        var retry = await pipeline.MemorizeAsync(entry, Options());

        retry.Success.Should().BeTrue();
        _vectorRows.Should().HaveCount(firstGeneration, "the retry replaces the old generation rather than adding to it");
        _vectorRows.Should().OnlyContain(c => c.Content.Contains("nine and five", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ResumedRun_KeepsTheChunksAnEarlierRunAlreadyCommitted()
    {
        // StartFromChunkIndex means chunks 0..N are already in the store and this run writes only
        // the tail -- IndexChunksResumableAsync skips them on exactly that assumption. The
        // unconditional delete that used to open this method invalidated the assumption in the same
        // call: every host restart mid-indexing silently truncated the document to its tail, with
        // no failure anywhere to notice.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync(
            "handbook.txt",
            string.Join(" ", Enumerable.Range(0, 40).Select(i => $"Section {i} describes a separate procedure in detail.")));

        var checkpoints = new List<int>();
        var full = new MemorizeOptions
        {
            MaxChunkSize = 200,
            CheckpointCallback = (i, _) => { checkpoints.Add(i); return Task.CompletedTask; }
        };

        await pipeline.MemorizeAsync(entry, full);
        var committed = _vectorRows.Count;
        committed.Should().BeGreaterThan(1, "the document must chunk into several pieces for a resume to be meaningful");

        // Resume as the job queue does after a restart: everything up to the last checkpoint is
        // already committed, so only the tail is rewritten.
        var lastCheckpoint = checkpoints[^1];
        var resumed = new MemorizeOptions
        {
            MaxChunkSize = 200,
            StartFromChunkIndex = lastCheckpoint,
            CheckpointCallback = (_, _) => Task.CompletedTask
        };

        var result = await pipeline.MemorizeAsync(entry, resumed);

        result.Success.Should().BeTrue();
        _vectorRows.Should().HaveCount(
            committed,
            "a resumed run adds the missing tail; it neither drops the committed prefix nor duplicates it");
        _vectorRows.Select(c => c.ChunkIndex).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public async Task FailedReindex_ReportsWhereItFailedAndHowMuchItWasHandling()
    {
        // Before this, the only account of a failure was ErrorMessage -- prose, which changes
        // between releases and which a caller had to parse to answer operational questions.
        // The fields say which stage threw, how many chunks were in flight, and what type the
        // underlying exception was; the message stays for a human to read.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync(
            "report.txt",
            "First paragraph of the report.\n\nSecond paragraph with more detail.");

        await pipeline.MemorizeAsync(entry, Options());
        var indexedChunks = _vectorRows.Count;

        _failEmbedding = true;
        await File.WriteAllTextAsync(entry.SourcePath, "First paragraph revised.\n\nSecond paragraph revised.");
        var result = await pipeline.MemorizeAsync(entry, Options());

        result.Success.Should().BeFalse();
        result.Failure.Should().NotBeNull("a failure with no structured detail forces prose parsing");
        result.Failure!.Stage.Should().Be(IndexingStage.Indexing);
        result.Failure.ChunkCount.Should().BeGreaterThan(0, "the batch that failed had chunks in it");
        result.Failure.ContentLength.Should().BeGreaterThan(0);
        result.Failure.ExceptionType.Should().Be(nameof(InvalidOperationException));

        // And the swap guarantee still holds alongside the reporting.
        _vectorRows.Should().HaveCount(indexedChunks);
    }

    [Fact]
    public async Task SucceedingMemorize_CarriesNoFailureDetail()
    {
        // The detail is consumed when a result is built, so a later success -- or a later failure
        // of a different kind -- cannot inherit the previous one's chunk count.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync("clean.txt", "Nothing goes wrong here.");

        _failEmbedding = true;
        await pipeline.MemorizeAsync(entry, Options());

        _failEmbedding = false;
        var second = await pipeline.MemorizeAsync(entry, Options());

        second.Success.Should().BeTrue();
        second.Failure.Should().BeNull();
    }
}
