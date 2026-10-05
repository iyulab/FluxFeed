using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Verifies that the FileVault memorize path wires GraphRAG with semantics equivalent to the
/// SDK direct-index path (Indexer.IndexAsync): null = auto-when-registered, true = force,
/// false = off.
/// </summary>
public sealed class VaultPipelineGraphRagTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    public VaultPipelineGraphRagTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FileVaultGraphRag_{Guid.NewGuid():N}");
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

    private VaultPipeline CreatePipeline(IGraphRAGService? graphRAG, string? vaultId = null) =>
        new(
            _git,
            new ContentHasher(),
            _storage,
            NullLogger<VaultPipeline>.Instance,
            options: vaultId is null ? null : MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir, VaultId = vaultId }),
            extractor: null,
            chunker: null,
            vectorStore: _vectorStore,
            embeddingService: _embeddingService,
            hybridSearch: null,
            graphRAGService: graphRAG);

    private static IGraphRAGService CreateGraphMock()
    {
        var graph = Substitute.For<IGraphRAGService>();
        graph.BuildIndexAsync(
                Arg.Any<IEnumerable<DocumentChunk>>(),
                Arg.Any<GraphRAGBuildOptions?>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new GraphRAGIndex()));
        return graph;
    }

    private async Task<MemorizeResult> MemorizeFileAsync(VaultPipeline pipeline, MemorizeOptions options)
    {
        var docPath = Path.Combine(_testDir, "doc.txt");
        await File.WriteAllTextAsync(docPath, "Alice works at Acme Corp. Bob manages the project in Seoul.");
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);
        return await pipeline.MemorizeAsync(entry, options);
    }

    // Removing a document removes it from the graph leg too: the pipeline reads the document's chunk ids before the vector
    // rows go and hands them to ForgetChunksAsync, in the partition the vault builds its graph into.
    [Fact]
    public async Task Remove_ForgetsTheDocumentsChunksInTheVaultsGraphPartition()
    {
        var graph = CreateGraphMock();
        var pipeline = CreatePipeline(graph, vaultId: "tenant-a");
        var entry = VaultEntry.Create(Path.Combine(_testDir, "doc.txt"), _vaultDir);
        _vectorStore.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>(["c1", "c2"]));

        await pipeline.RemoveAsync(entry);

        await _vectorStore.Received(1).DeleteByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>());
        await graph.Received(1).ForgetChunksAsync(
            Arg.Is<IEnumerable<string>>(ids => ids.SequenceEqual(new[] { "c1", "c2" })),
            "tenant-a",
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Remove_WithoutAGraphService_DoesNotReadChunkIds()
    {
        var pipeline = CreatePipeline(graphRAG: null);
        var entry = VaultEntry.Create(Path.Combine(_testDir, "doc.txt"), _vaultDir);

        await pipeline.RemoveAsync(entry);

        await _vectorStore.Received(1).DeleteByDocumentIdAsync(entry.FilepathHash, Arg.Any<CancellationToken>());
        await _vectorStore.DidNotReceive().GetChunkIdsByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Memorize_WithGraphRagServiceRegistered_AutoBuildsGraphIndex()
    {
        var graph = CreateGraphMock();
        var pipeline = CreatePipeline(graph);

        var result = await MemorizeFileAsync(pipeline, new MemorizeOptions { MaxChunkSize = 200 });

        result.Success.Should().BeTrue();
        pipeline.SupportsGraphRAG.Should().BeTrue();
        await graph.Received(1).BuildIndexAsync(
            Arg.Is<IEnumerable<DocumentChunk>>(c => c.Any()),
            Arg.Any<GraphRAGBuildOptions?>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Memorize_GraphRagExplicitlyDisabled_SkipsGraphBuildEvenWhenRegistered()
    {
        var graph = CreateGraphMock();
        var pipeline = CreatePipeline(graph);

        var result = await MemorizeFileAsync(
            pipeline,
            new MemorizeOptions { MaxChunkSize = 200, EnableGraphRAG = false });

        result.Success.Should().BeTrue();
        await graph.DidNotReceive().BuildIndexAsync(
            Arg.Any<IEnumerable<DocumentChunk>>(),
            Arg.Any<GraphRAGBuildOptions?>(),
            Arg.Any<CancellationToken>());
    }

    // Stopping the host mid-document is not a failed document: before, the catch-all marked the entry Error and returned
    // a failure the queue counted against the job; now the cancellation propagates and the entry keeps its stage.
    [Fact]
    public async Task Memorize_CallerCancels_PropagatesAndDoesNotMarkTheEntryFailed()
    {
        using var cts = new CancellationTokenSource();
        var graph = Substitute.For<IGraphRAGService>();
        graph.BuildIndexAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<GraphRAGBuildOptions?>(), Arg.Any<CancellationToken>())
            .Returns<Task<GraphRAGIndex>>(_ =>
            {
                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });
        var pipeline = CreatePipeline(graph);
        var docPath = Path.Combine(_testDir, "doc.txt");
        await File.WriteAllTextAsync(docPath, "Alice works at Acme Corp. Bob manages the project in Seoul.", TestContext.Current.CancellationToken);
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, TestContext.Current.CancellationToken);

        var act = () => pipeline.MemorizeAsync(entry, new MemorizeOptions { MaxChunkSize = 200 }, cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
        entry.Stage.Should().NotBe(ProcessingStage.Error);
    }

    [Fact]
    public async Task Memorize_ForceEnableGraphRagWithoutService_FailsWithClearError()
    {
        var pipeline = CreatePipeline(graphRAG: null);

        var result = await MemorizeFileAsync(
            pipeline,
            new MemorizeOptions { MaxChunkSize = 200, EnableGraphRAG = true });

        result.Success.Should().BeFalse();
        result.ErrorMessage.Should().Contain("IGraphRAGService is not registered");
    }

    [Fact]
    public async Task Memorize_WithoutGraphRagService_CompletesWithoutGraphBuild()
    {
        var pipeline = CreatePipeline(graphRAG: null);

        var result = await MemorizeFileAsync(pipeline, new MemorizeOptions { MaxChunkSize = 200 });

        result.Success.Should().BeTrue();
        result.ChunkCount.Should().BeGreaterThan(0);
        pipeline.SupportsGraphRAG.Should().BeFalse();
    }

    [Fact]
    public async Task Memorize_PassesGraphRagBuildOptionsThrough()
    {
        var graph = CreateGraphMock();
        var pipeline = CreatePipeline(graph);
        var buildOptions = new GraphRAGBuildOptions { MaxChunks = 7 };

        await MemorizeFileAsync(
            pipeline,
            new MemorizeOptions { MaxChunkSize = 200, EnableGraphRAG = true, GraphRAGOptions = buildOptions });

        await graph.Received(1).BuildIndexAsync(
            Arg.Any<IEnumerable<DocumentChunk>>(),
            buildOptions,
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Memorize_InATenantScopedVault_BuildsTheGraphInThePartitionOfItsVaultId()
    {
        var graph = CreateGraphMock();
        var pipeline = CreatePipeline(graph, vaultId: "tenant-7");

        await MemorizeFileAsync(pipeline, new MemorizeOptions { MaxChunkSize = 200, EnableGraphRAG = true });

        await graph.Received(1).BuildIndexAsync(
            Arg.Any<IEnumerable<DocumentChunk>>(),
            Arg.Is<GraphRAGBuildOptions?>(o => o != null && o.Partition == "tenant-7"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Memorize_InATenantScopedVault_KeepsTheCallersOptions_AndDoesNotChangeThem()
    {
        var graph = CreateGraphMock();
        var pipeline = CreatePipeline(graph, vaultId: "tenant-7");
        var buildOptions = new GraphRAGBuildOptions { MaxChunks = 7 };

        await MemorizeFileAsync(
            pipeline,
            new MemorizeOptions { MaxChunkSize = 200, EnableGraphRAG = true, GraphRAGOptions = buildOptions });

        await graph.Received(1).BuildIndexAsync(
            Arg.Any<IEnumerable<DocumentChunk>>(),
            Arg.Is<GraphRAGBuildOptions?>(o => o != null && o.Partition == "tenant-7" && o.MaxChunks == 7),
            Arg.Any<CancellationToken>());
        buildOptions.Partition.Should().Be(GraphPartition.Default);
    }

    [Fact]
    public async Task Memorize_InATenantScopedVault_RefusesOptionsNamingAnotherPartition()
    {
        var graph = CreateGraphMock();
        var pipeline = CreatePipeline(graph, vaultId: "tenant-7");

        var result = await MemorizeFileAsync(
            pipeline,
            new MemorizeOptions { MaxChunkSize = 200, EnableGraphRAG = true, GraphRAGOptions = new GraphRAGBuildOptions { Partition = "tenant-8" } });

        result.Success.Should().BeFalse();
        result.ErrorMessage.Should().Contain("tenant-8").And.Contain("tenant-7");
        await graph.DidNotReceive().BuildIndexAsync(
            Arg.Any<IEnumerable<DocumentChunk>>(),
            Arg.Any<GraphRAGBuildOptions?>(),
            Arg.Any<CancellationToken>());
    }
}
