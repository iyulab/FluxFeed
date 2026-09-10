using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// The rollback runs precisely when something has already gone wrong, so it must not depend on the
/// store answering one more question.
///
/// <para>
/// Reported from a deployment: a keyword-index deadlock started the rollback, the rollback's own
/// vector-store lookup then exceeded the client's transport limit, and the rollback aborted. Both
/// the previous generation and the partial one were left in the collection, with nothing recording
/// which points belonged to which. Reproduced on four documents in one upload batch.
/// </para>
///
/// <para>
/// The run already knew every id it wrote. These tests pin that it undoes its own work from what it
/// recorded, not from what it can read back — including the case the old shape got silently wrong,
/// where a resumed run's committed prefix was deleted along with the failed tail.
/// </para>
/// </summary>
public sealed class VaultPipelineRollbackResilienceTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    private readonly List<DocumentChunk> _vectorRows = [];

    /// <summary>Makes every id lookup throw, standing in for a store that is refusing calls.</summary>
    private bool _failIdLookup;

    /// <summary>Makes embedding throw, standing in for a provider failure mid-index.</summary>
    private bool _failEmbedding;

    /// <summary>Ids the store refuses to delete, standing in for a partially available store.</summary>
    private readonly HashSet<string> _undeletableIds = new(StringComparer.Ordinal);

    public VaultPipelineRollbackResilienceTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FluxFeedRollback_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_testDir);
        Directory.CreateDirectory(_vaultDir);

        _git = Substitute.For<IGitService>();
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns("commit-hash");

        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            _git,
            MsOptions.Create(new FluxFeed.Options.FileVaultOptions { VaultBasePath = _vaultDir }));

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var stored = ((IEnumerable<DocumentChunk>)ci[0]).ToList();
                _vectorRows.AddRange(stored);
                return Task.FromResult<IEnumerable<string>>(stored.Select(c => c.Id).ToList());
            });
        _vectorStore.GetByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => _failIdLookup
                ? Task.FromException<IEnumerable<DocumentChunk>>(LookupFailure())
                : Task.FromResult<IEnumerable<DocumentChunk>>(
                    _vectorRows.Where(c => c.DocumentId == (string)ci[0]).ToList()));
        _vectorStore.GetChunkIdsByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci => _failIdLookup
                ? Task.FromException<IReadOnlyList<string>>(LookupFailure())
                : Task.FromResult<IReadOnlyList<string>>(
                    _vectorRows.Where(c => c.DocumentId == (string)ci[0]).Select(c => c.Id).ToList()));
        _vectorStore.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var id = (string)ci[0];
                if (_undeletableIds.Contains(id))
                    return Task.FromException<bool>(new InvalidOperationException($"store refused to delete {id}"));
                return Task.FromResult(_vectorRows.RemoveAll(c => c.Id == id) > 0);
            });

        _embeddingService = Substitute.For<IEmbeddingService>();
        _embeddingService.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci => _failEmbedding
                ? Task.FromException<IEnumerable<float[]>>(new InvalidOperationException("embedding provider rejected a chunk"))
                : Task.FromResult<IEnumerable<float[]>>(
                    ((IEnumerable<string>)ci[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f }).ToList()));
        _embeddingService.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => _failEmbedding
                ? Task.FromException<float[]>(new InvalidOperationException("embedding provider rejected a chunk"))
                : Task.FromResult(new[] { 0.1f, 0.2f, 0.3f }));
        _embeddingService.GetIdentity()
            .Returns(new EmbeddingIdentity { Provider = "Test", Model = "test", Dimension = 3 });
    }

    /// <summary>The shape of the reported failure: the store refuses the read outright.</summary>
    private static Exception LookupFailure() =>
        new InvalidOperationException("Received message exceeds the maximum configured message size.");

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch { /* ignore cleanup errors */ }
    }

    private VaultPipeline CreatePipeline(IKeywordSearchService? keywordSearchService = null) =>
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
            keywordSearchService: keywordSearchService);

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
    public async Task Rollback_WhenTheStoreRefusesEveryLookup_StillRemovesThePartialGeneration()
    {
        // The reported case, in order: the re-index captures the previous generation, writes its own
        // chunks, the keyword leg then deadlocks - and by the time the rollback runs the store is
        // refusing reads. The old rollback began by reading, so it aborted and left both generations.
        // Deleting what this run recorded needs no read at all.
        var keywordSearch = Substitute.For<IKeywordSearchService>();
        keywordSearch.IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                // The store becomes unreachable at the moment the rollback is about to need it.
                _failIdLookup = true;
                return Task.FromException(new InvalidOperationException("deadlock detected"));
            });

        var pipeline = CreatePipeline(keywordSearch);
        var entry = await CreateEntryAsync("invoice.txt", "Payment is due on the first of the month.");

        // First index: the keyword leg is allowed to succeed so a previous generation exists.
        keywordSearch.IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        await pipeline.MemorizeAsync(entry, Options(), TestContext.Current.CancellationToken);

        var previousGeneration = _vectorRows.Select(c => c.Id).ToList();
        previousGeneration.Should().NotBeEmpty("the precondition is a document that IS indexed");

        // Re-index: writes land, then the keyword leg throws and the store stops answering reads.
        keywordSearch.IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                _failIdLookup = true;
                return Task.FromException(new InvalidOperationException("deadlock detected"));
            });
        await File.WriteAllTextAsync(
            entry.SourcePath, "Payment is due on the fifteenth instead.", TestContext.Current.CancellationToken);

        var result = await pipeline.MemorizeAsync(entry, Options(), TestContext.Current.CancellationToken);

        result.Success.Should().BeFalse("the underlying failure is still a failure");
        _vectorRows.Select(c => c.Id).Should().BeEquivalentTo(
            previousGeneration,
            "a rollback that cannot read the store must still undo exactly what this run wrote");
    }

    [Fact]
    public async Task Rollback_OnAResumedRun_DoesNotDeleteTheCommittedPrefix()
    {
        // The defect found while fixing the above. On a resumed run the previous generation is
        // deliberately empty - chunks 0..N are THIS generation's committed prefix, not a superseded
        // one - so a rollback computed as "everything present, minus nothing" deleted the prefix the
        // resume is built on, while the checkpoint still claimed those chunks were committed. The
        // next resume then wrote only the tail: the same silent truncation the swap exists to end.
        var pipeline = CreatePipeline();
        var entry = await CreateEntryAsync(
            "manual.txt",
            string.Join(" ", Enumerable.Range(0, 40).Select(i => $"Section {i} describes a procedure in detail.")));

        var committed = new List<int>();
        var firstRun = new MemorizeOptions
        {
            MaxChunkSize = 200,
            StartFromChunkIndex = -1,
            CheckpointCallback = (i, _) => { committed.Add(i); return Task.CompletedTask; }
        };

        await pipeline.MemorizeAsync(entry, firstRun, TestContext.Current.CancellationToken);
        committed.Should().HaveCountGreaterThan(2, "this document must chunk into several rows");

        var prefixIds = _vectorRows.Select(c => c.Id).ToList();

        // Resume from partway. The tail fails, so the run rolls back - and must leave the prefix,
        // which it did not write this time, exactly where it is.
        _failEmbedding = true;
        var resumedRun = new MemorizeOptions
        {
            MaxChunkSize = 200,
            StartFromChunkIndex = committed[^2],
            CheckpointCallback = (_, _) => Task.CompletedTask
        };

        var result = await pipeline.MemorizeAsync(entry, resumedRun, TestContext.Current.CancellationToken);

        result.Success.Should().BeFalse("the embedding failure is a failure");
        _vectorRows.Select(c => c.Id).Should().BeEquivalentTo(
            prefixIds,
            "the committed prefix is what the resume is built on - rolling it back truncates the document");
    }

    [Fact]
    public async Task Rollback_ThatCannotDeleteEverything_ReportsWhatTheStoreStillHolds()
    {
        // "Unrecoverable now" and "unrecoverable forever" differ by whether it was written down. A
        // rollback that cannot finish must name the rows it left, not merely log that it failed -
        // otherwise the mixed generation persists silently and surfaces later as duplicate hits.
        var keywordSearch = Substitute.For<IKeywordSearchService>();
        keywordSearch.IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromException(new InvalidOperationException("deadlock detected")));

        var pipeline = CreatePipeline(keywordSearch);

        var entry = await CreateEntryAsync("report.txt", "The quarter closed ahead of plan.");

        // Every row this run writes is one the store will then refuse to delete: the vector write
        // succeeds, the keyword leg deadlocks, and the rollback meets a store that is half available.
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var stored = ((IEnumerable<DocumentChunk>)ci[0]).ToList();
                _vectorRows.AddRange(stored);
                foreach (var c in stored)
                    _undeletableIds.Add(c.Id);
                return Task.FromResult<IEnumerable<string>>(stored.Select(c => c.Id).ToList());
            });

        var result = await pipeline.MemorizeAsync(entry, Options(), TestContext.Current.CancellationToken);

        result.Success.Should().BeFalse("the keyword-index failure is a failure");

        var written = _vectorRows.Select(c => c.Id).ToList();
        written.Should().NotBeEmpty("the vector write succeeded before the keyword leg threw");

        var failure = result.Failure;
        failure.Should().NotBeNull("the caller has to be able to see what the failure left behind");
        failure!.OrphanedChunkIds.Should().BeEquivalentTo(
            written,
            "these are the rows the rollback could not remove, and the caller needs them to clean up");
    }
}
