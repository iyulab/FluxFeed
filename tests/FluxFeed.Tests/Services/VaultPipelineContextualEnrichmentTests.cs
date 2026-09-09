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
using NSubstitute.ExceptionExtensions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Opt-in contextual enrichment at the chunk stage (BD-20260908-05) through FluxIndex.Core's
/// <see cref="IContextualEnrichmentService"/> port. The port is a substitute — what is under test is the pipeline's
/// wiring: the double gate (service + option), the text that reaches the store and the embedder, the metadata, and
/// the degrade path. Real storage on a temp directory, mocked git/vector/embedding — the same harness shape as
/// <see cref="VaultPipelineRagSecurityTests"/>.
/// </summary>
public sealed class VaultPipelineContextualEnrichmentTests : IDisposable
{
    private const string Document = "The north region closed the quarter at 1.2 million. Returns stayed under two percent.";

    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly IGitService _git;
    private readonly VaultStorageService _storage;
    private readonly ContentHasher _hasher;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embedding;
    private readonly List<DocumentChunk> _stored = [];

    public VaultPipelineContextualEnrichmentTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FluxFeedEnrichment_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_testDir);
        Directory.CreateDirectory(_vaultDir);

        _git = Substitute.For<IGitService>();
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("abc123");

        _hasher = new ContentHasher();
        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            _git,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.GetByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new List<DocumentChunk>());
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                var batch = ((IEnumerable<DocumentChunk>)callInfo[0]).ToList();
                _stored.AddRange(batch);
                return Task.FromResult(batch.Select(c => c.Id));
            });

        _embedding = Substitute.For<IEmbeddingService>();
        _embedding.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult<IEnumerable<float[]>>(
                ((IEnumerable<string>)callInfo[0]).Select(_ => new float[] { 0.1f, 0.2f, 0.3f }).ToList()));
        _embedding.GetIdentity().Returns(new EmbeddingIdentity { Provider = "Test", Model = "test-model", Dimension = 3 });
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

    private VaultPipeline CreatePipeline(IContextualEnrichmentService? enrichment, bool enabled, bool continueOnError = true) => new(
        _git, _hasher, _storage, NullLogger<VaultPipeline>.Instance,
        options: MsOptions.Create(new FileVaultOptions
        {
            VaultBasePath = _vaultDir,
            ContextualEnrichment = new ContextualEnrichmentDefaults { Enabled = enabled, ContinueOnError = continueOnError }
        }),
        extractor: null, chunker: null,
        vectorStore: _vectorStore, embeddingService: _embedding,
        contextualEnrichment: enrichment);

    private static IContextualEnrichmentService PortReturning(Func<IReadOnlyList<string>, IReadOnlyList<string>> contextsFor)
    {
        var port = Substitute.For<IContextualEnrichmentService>();
        port.GenerateContextBatchAsync(Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => Task.FromResult(contextsFor((IReadOnlyList<string>)callInfo[0])));
        return port;
    }

    private static IContextualEnrichmentService PortSummarizing(string context)
        => PortReturning(chunks => chunks.Select(_ => context).ToList());

    private async Task<MemorizeResult> MemorizeAsync(VaultPipeline pipeline)
    {
        var path = Path.Combine(_testDir, $"doc_{Guid.NewGuid():N}.txt");
        await File.WriteAllTextAsync(path, Document, TestContext.Current.CancellationToken);
        var entry = VaultEntry.Create(path, _vaultDir);
        await _storage.InitializeEntryAsync(entry, TestContext.Current.CancellationToken);
        return await pipeline.MemorizeAsync(entry, new MemorizeOptions { MaxChunkSize = 0, SkipCommit = true }, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ServiceRegistered_ButOptionOff_IsNeverCalled()
    {
        var port = PortSummarizing("ignored");
        var pipeline = CreatePipeline(port, enabled: false);
        pipeline.SupportsContextualEnrichment.Should().BeFalse();

        var result = await MemorizeAsync(pipeline);

        result.Success.Should().BeTrue();
        await port.DidNotReceiveWithAnyArgs().GenerateContextBatchAsync(default!, default!, default);
        _stored.Should().OnlyContain(c => c.Content == Document && !c.Metadata.ContainsKey(VaultPipeline.EnrichmentMetadataKey));
    }

    [Fact]
    public async Task OptionOn_ButNoService_IsAPlainMemorize()
    {
        var pipeline = CreatePipeline(enrichment: null, enabled: true);

        pipeline.SupportsContextualEnrichment.Should().BeFalse();
        var result = await MemorizeAsync(pipeline);

        result.Success.Should().BeTrue();
        _stored.Should().OnlyContain(c => c.Content == Document);
    }

    [Fact]
    public async Task Enabled_PrependsTheContext_AndRecordsItInMetadata()
    {
        var port = PortSummarizing("Quarterly sales report, north region section.");
        var pipeline = CreatePipeline(port, enabled: true);
        pipeline.SupportsContextualEnrichment.Should().BeTrue();

        var result = await MemorizeAsync(pipeline);

        result.Success.Should().BeTrue();
        result.ChunkCount.Should().Be(1);
        var stored = _stored.Should().ContainSingle().Subject;
        stored.Content.Should().Be("Quarterly sales report, north region section.\n\n" + Document);
        stored.Metadata[VaultPipeline.ContextSummaryMetadataKey].Should().Be("Quarterly sales report, north region section.");
        stored.Metadata[VaultPipeline.EnrichmentMetadataKey].Should().Be("contextual");

        // The embedding saw the enriched text, not the plain passage.
        await _embedding.Received(1).GenerateEmbeddingsBatchAsync(
            Arg.Is<IEnumerable<string>>(texts => texts.All(t => t.StartsWith("Quarterly sales report"))),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Enabled_PassesTheChunkTexts_AndTheWholeDocument()
    {
        var port = PortSummarizing("ctx");
        var pipeline = CreatePipeline(port, enabled: true);

        await MemorizeAsync(pipeline);

        await port.Received(1).GenerateContextBatchAsync(
            Arg.Is<IReadOnlyList<string>>(chunks => chunks.Count == 1 && chunks[0] == Document),
            Arg.Is<string>(doc => doc.Contains(Document)),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task BlankContext_LeavesTheChunkUntouched()
    {
        var port = PortSummarizing("   ");
        var pipeline = CreatePipeline(port, enabled: true);

        var result = await MemorizeAsync(pipeline);

        result.Success.Should().BeTrue();
        var stored = _stored.Should().ContainSingle().Subject;
        stored.Content.Should().Be(Document);
        stored.Metadata.Should().NotContainKey(VaultPipeline.EnrichmentMetadataKey);
    }

    [Fact]
    public async Task PortThrows_WithContinueOnError_IndexesPlainChunksTaggedFailed()
    {
        var port = Substitute.For<IContextualEnrichmentService>();
        port.GenerateContextBatchAsync(Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("llm down"));
        var pipeline = CreatePipeline(port, enabled: true);

        var result = await MemorizeAsync(pipeline);

        result.Success.Should().BeTrue("ContinueOnError defaults to true — the document is still indexed");
        var stored = _stored.Should().ContainSingle().Subject;
        stored.Content.Should().Be(Document);
        stored.Metadata[VaultPipeline.EnrichmentMetadataKey].Should().Be("failed");
    }

    [Fact]
    public async Task PortThrows_WithoutContinueOnError_FailsTheMemorize()
    {
        var port = Substitute.For<IContextualEnrichmentService>();
        port.GenerateContextBatchAsync(Arg.Any<IReadOnlyList<string>>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("llm down"));
        var pipeline = CreatePipeline(port, enabled: true, continueOnError: false);

        var result = await MemorizeAsync(pipeline);

        result.Success.Should().BeFalse();
        _stored.Should().BeEmpty();
    }

    [Fact]
    public async Task PortReturnsWrongCount_IsAContractViolation_NotASilentMisalignment()
    {
        var port = PortReturning(_ => ["only one", "but two"]);
        var pipeline = CreatePipeline(port, enabled: true, continueOnError: false);

        var result = await MemorizeAsync(pipeline);

        result.Success.Should().BeFalse();
        result.ErrorMessage.Should().Contain("exactly one per chunk");
    }
}
