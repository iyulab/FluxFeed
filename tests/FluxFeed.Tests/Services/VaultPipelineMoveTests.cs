using AwesomeAssertions;
using FluxFeed.Domain.Entities;
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
/// <see cref="VaultPipeline.ReassignAsync"/> against doubles: which chunk-id map it derives from stored chunks (enriched
/// and image chunks included), what it hands each leg, and how it undoes a partial move.
/// </summary>
public sealed class VaultPipelineMoveTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"FluxFeedPipelineMove_{Guid.NewGuid():N}");
    private readonly string _oldPath;
    private readonly string _newPath;
    private readonly string _oldHash;
    private readonly string _newHash;
    private readonly IVectorStore _vectorStore = Substitute.For<IVectorStore>();
    private readonly IKeywordSearchService _keyword = Substitute.For<IKeywordSearchService>();
    private readonly IGraphRAGService _graph = Substitute.For<IGraphRAGService>();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public VaultPipelineMoveTests()
    {
        Directory.CreateDirectory(_root);
        _oldPath = Path.Combine(_root, "in", "notes.md");
        _newPath = Path.Combine(_root, "out", "renamed.md");
        _oldHash = FilepathHasher.ComputeHash(_oldPath);
        _newHash = FilepathHasher.ComputeHash(_newPath);
        _vectorStore.ReassignDocumentAsync(default!, default!, default!, default, default)
            .ReturnsForAnyArgs(ci => ((IReadOnlyDictionary<string, string>)ci[2]).Count);
        _keyword.ReassignDocumentAsync(default!, default!, default!, default, default)
            .ReturnsForAnyArgs(ci => ((IReadOnlyDictionary<string, string>)ci[2]).Count);
        _graph.ReassignChunksAsync(default!, default!, default!, default!, default)
            .ReturnsForAnyArgs(new GraphReassignResult { EntitiesUpdated = 1 });
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { }
    }

    private VaultPipeline Pipeline(bool keyword = true, bool graph = true)
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
            keywordSearchService: keyword ? _keyword : null,
            graphRAGService: graph ? _graph : null);
    }

    private VaultEntry MovedEntry()
    {
        var entry = VaultEntry.Create(_oldPath, Path.Combine(_root, ".vault"));
        entry.Relocate(_newPath);
        return entry;
    }

    private void Stored(params DocumentChunk[] chunks)
    {
        _vectorStore.GetByDocumentIdAsync(_oldHash, Arg.Any<CancellationToken>()).Returns(chunks);
        _keyword.GetChunkIdsByDocumentIdAsync(_oldHash, Arg.Any<CancellationToken>()).Returns(chunks.Select(c => c.Id).ToList());
    }

    private DocumentChunk Text(string content, int index, int occurrence = 0, Dictionary<string, object>? metadata = null, string? raw = null) => new()
    {
        Id = ChunkIdentity.ForText(_oldHash, raw ?? content, occurrence),
        DocumentId = _oldHash,
        ChunkIndex = index,
        Content = content,
        Metadata = metadata ?? new Dictionary<string, object>()
    };

    private DocumentChunk Image(string imageId, int part, int index) => new()
    {
        Id = ChunkIdentity.ForImage(_oldHash, imageId, part),
        DocumentId = _oldHash,
        ChunkIndex = index,
        Content = $"description {imageId} {part}",
        Metadata = new Dictionary<string, object> { ["chunk_kind"] = VaultPipeline.ImageDescriptionChunkKind, ["image_id"] = imageId }
    };

    private IReadOnlyDictionary<string, string> CapturedMap()
    {
        var call = _vectorStore.ReceivedCalls().Single(c => c.GetMethodInfo().Name == nameof(IVectorStore.ReassignDocumentAsync));
        return (IReadOnlyDictionary<string, string>)call.GetArguments()[2]!;
    }

    [Fact]
    public async Task Reassign_MapsEachChunkToTheIdAMemorizeAtTheNewPathWrites_IncludingRepeatsEnrichedAndImageChunks()
    {
        Stored(
            Text("repeated line", 0),
            Text("repeated line", 1, occurrence: 1),
            Text("Context of the passage.\n\nthe passage", 2,
                metadata: new() { [VaultPipeline.EnrichmentMetadataKey] = "contextual", [VaultPipeline.ContextSummaryMetadataKey] = "Context of the passage." },
                raw: "the passage"),
            Image("img-1", 0, 3),
            Image("img-1", 1, 4));

        var result = await Pipeline().ReassignAsync(MovedEntry(), _oldPath, Ct);

        var map = CapturedMap();
        map.Values.Should().BeEquivalentTo(
        [
            ChunkIdentity.ForText(_newHash, "repeated line", 0),
            ChunkIdentity.ForText(_newHash, "repeated line", 1),
            ChunkIdentity.ForText(_newHash, "the passage", 0),
            ChunkIdentity.ForImage(_newHash, "img-1", 0),
            ChunkIdentity.ForImage(_newHash, "img-1", 1),
        ]);
        result.VectorChunksMoved.Should().Be(5);
        result.KeywordChunksMoved.Should().Be(5);
        result.Graph!.EntitiesUpdated.Should().Be(1);
    }

    [Fact]
    public async Task Reassign_HandsEveryLegTheSameMap_AndTheNewProvenance()
    {
        Stored(Text("alpha", 0), Text("bravo", 1));

        await Pipeline().ReassignAsync(MovedEntry(), _oldPath, Ct);

        var map = CapturedMap();
        await _vectorStore.Received(1).ReassignDocumentAsync(
            _oldHash, _newHash, map,
            Arg.Is<IReadOnlyDictionary<string, object?>>(u =>
                (string)u["source_path"]! == _newPath && (string)u["file_name"]! == "renamed.md"
                && (string)u["filepath_hash"]! == _newHash && (string)u["document_id"]! == _newHash),
            Arg.Any<CancellationToken>());
        await _keyword.Received(1).ReassignDocumentAsync(_oldHash, _newHash, map, Arg.Any<IReadOnlyDictionary<string, object?>>(), Arg.Any<CancellationToken>());
        await _graph.Received(1).ReassignChunksAsync(map, _oldHash, _newHash, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Reassign_WithAChunkWhoseContentNoLongerYieldsItsId_Throws_BeforeAnyLegIsWritten()
    {
        // What the RAG security pipeline leaves behind: the stored text is the sanitized one, the id the original's.
        Stored(Text("alpha", 0), Text("[redacted]", 1, raw: "the original passage"));

        var act = () => Pipeline().ReassignAsync(MovedEntry(), _oldPath, Ct);

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*cannot be re-keyed*");
        await _vectorStore.DidNotReceiveWithAnyArgs().ReassignDocumentAsync(default!, default!, default!, default, default);
        await _keyword.DidNotReceiveWithAnyArgs().ReassignDocumentAsync(default!, default!, default!, default, default);
    }

    [Fact]
    public async Task Reassign_WithAKeywordRowTheVectorLegDoesNotHold_Throws_AndPointsAtTheRepair()
    {
        Stored(Text("alpha", 0));
        _keyword.GetChunkIdsByDocumentIdAsync(_oldHash, Arg.Any<CancellationToken>()).Returns(["keyword-only-row"]);

        var act = () => Pipeline().ReassignAsync(MovedEntry(), _oldPath, Ct);

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*RepairKeywordIndexAsync*");
        await _vectorStore.DidNotReceiveWithAnyArgs().ReassignDocumentAsync(default!, default!, default!, default, default);
    }

    [Fact]
    public async Task Reassign_WhenALaterLegFails_MovesTheEarlierLegsBack_AndRethrows()
    {
        Stored(Text("alpha", 0), Text("bravo", 1));
        _graph.ReassignChunksAsync(Arg.Any<IReadOnlyDictionary<string, string>>(), _oldHash, _newHash, Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new TimeoutException("graph store unavailable"));

        var act = () => Pipeline().ReassignAsync(MovedEntry(), _oldPath, Ct);

        await act.Should().ThrowAsync<TimeoutException>();
        var map = CapturedForwardMap();
        var inverse = map.ToDictionary(kv => kv.Value, kv => kv.Key);
        await _keyword.Received(1).ReassignDocumentAsync(
            _newHash, _oldHash, Arg.Is<IReadOnlyDictionary<string, string>>(m => m.OrderBy(kv => kv.Key).SequenceEqual(inverse.OrderBy(kv => kv.Key))),
            Arg.Is<IReadOnlyDictionary<string, object?>>(u => (string)u["source_path"]! == _oldPath), Arg.Any<CancellationToken>());
        await _vectorStore.Received(1).ReassignDocumentAsync(
            _newHash, _oldHash, Arg.Any<IReadOnlyDictionary<string, string>>(),
            Arg.Is<IReadOnlyDictionary<string, object?>>(u => (string)u["document_id"]! == _oldHash), Arg.Any<CancellationToken>());
        await _graph.Received(1).ReassignChunksAsync(Arg.Any<IReadOnlyDictionary<string, string>>(), _newHash, _oldHash, Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Reassign_WithoutAKeywordIndexOrGraph_MovesTheVectorLegAlone()
    {
        Stored(Text("alpha", 0));

        var result = await Pipeline(keyword: false, graph: false).ReassignAsync(MovedEntry(), _oldPath, Ct);

        result.Should().Be(new VaultMoveIndexResult(1, 0, null));
    }

    private IReadOnlyDictionary<string, string> CapturedForwardMap()
    {
        var call = _vectorStore.ReceivedCalls()
            .First(c => c.GetMethodInfo().Name == nameof(IVectorStore.ReassignDocumentAsync) && (string)c.GetArguments()[0]! == _oldHash);
        return (IReadOnlyDictionary<string, string>)call.GetArguments()[2]!;
    }
}
