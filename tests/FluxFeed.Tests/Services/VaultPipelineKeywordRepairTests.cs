using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Exceptions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// <see cref="VaultPipeline.RepairKeywordIndexAsync(IReadOnlyList{VaultEntry}, KeywordIndexRepairScope, CancellationToken)"/>
/// over several entries when one of them fails: the others are still rewritten and the failure is named.
/// </summary>
public sealed class VaultPipelineKeywordRepairTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"FluxFeedKeywordRepair_{Guid.NewGuid():N}");
    private readonly IVectorStore _vectorStore = Substitute.For<IVectorStore>();
    private readonly IKeywordSearchService _keyword = Substitute.For<IKeywordSearchService>();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public VaultPipelineKeywordRepairTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { }
    }

    [Fact]
    public async Task All_WhenOneEntryFails_RewritesTheOthers_AndNamesTheFailedEntry()
    {
        var entries = new[] { Entry("a.md"), Entry("broken.md"), Entry("c.md") };
        foreach (var entry in entries)
            Stored(entry, chunkCount: 2);
        _keyword.IndexChunksAsync(Arg.Is<IEnumerable<DocumentChunk>>(c => c.Any(x => x.DocumentId == entries[1].FilepathHash)), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException("statement timeout"));

        var act = () => Pipeline().RepairKeywordIndexAsync(entries, KeywordIndexRepairScope.All, Ct);

        var thrown = (await act.Should().ThrowAsync<KeywordIndexRepairException>()).Which;
        thrown.Failures.Should().ContainSingle().Which.SourcePath.Should().Be(entries[1].SourcePath);
        thrown.Failures[0].Error.Should().BeOfType<TimeoutException>();
        thrown.InnerException.Should().BeSameAs(thrown.Failures[0].Error);
        thrown.Result.Should().Be(new KeywordIndexRepairResult(EntriesChecked: 3, EntriesRepaired: 2, KeywordRowsWritten: 4, KeywordRowsRemoved: 0));
        thrown.Message.Should().Contain("1 of 3").And.Contain("broken.md");

        // The entry after the failed one was still rewritten.
        await _keyword.Received(1).IndexChunksAsync(
            Arg.Is<IEnumerable<DocumentChunk>>(c => c.Any(x => x.DocumentId == entries[2].FilepathHash)), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task All_WhenNothingFails_ReturnsTheCounts()
    {
        var entries = new[] { Entry("a.md"), Entry("b.md") };
        foreach (var entry in entries)
            Stored(entry, chunkCount: 3);

        var result = await Pipeline().RepairKeywordIndexAsync(entries, KeywordIndexRepairScope.All, Ct);

        result.Should().Be(new KeywordIndexRepairResult(2, 2, 6, 0));
    }

    [Fact]
    public async Task Cancellation_StopsTheRepair_InsteadOfBeingCountedAsAFailure()
    {
        var entries = new[] { Entry("a.md"), Entry("b.md") };
        foreach (var entry in entries)
            Stored(entry, chunkCount: 1);
        using var cts = new CancellationTokenSource();
        _keyword.IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                cts.Cancel();
                return Task.FromException(new OperationCanceledException(cts.Token));
            });

        var act = () => Pipeline().RepairKeywordIndexAsync(entries, KeywordIndexRepairScope.All, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        await _keyword.Received(1).IndexChunksAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>());
    }

    private VaultEntry Entry(string name) => VaultEntry.Create(Path.Combine(_root, name), Path.Combine(_root, ".vault"));

    private void Stored(VaultEntry entry, int chunkCount)
    {
        var chunks = Enumerable.Range(0, chunkCount)
            .Select(i => new DocumentChunk { Id = $"{entry.FilepathHash}_{i}", DocumentId = entry.FilepathHash, Content = $"chunk {i}", ChunkIndex = i })
            .ToList();
        var ids = chunks.Select(c => c.Id).ToList();
        _vectorStore.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>()).Returns(ids);
        _keyword.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>()).Returns(ids);
        _vectorStore.GetByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>()).Returns(chunks);
    }

    private VaultPipeline Pipeline()
    {
        var git = Substitute.For<IGitService>();
        var options = MsOptions.Create(new FileVaultOptions { VaultBasePath = Path.Combine(_root, ".vault") });
        var storage = new VaultStorageService(NullLogger<VaultStorageService>.Instance, git, options);
        return new VaultPipeline(
            git,
            new ContentHasher(),
            storage,
            NullLogger<VaultPipeline>.Instance,
            options: options,
            vectorStore: _vectorStore,
            embeddingService: Substitute.For<IEmbeddingService>(),
            keywordSearchService: _keyword);
    }
}
