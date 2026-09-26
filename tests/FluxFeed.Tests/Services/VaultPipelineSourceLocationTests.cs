using AwesomeAssertions;

using IDocumentProcessorFactory = FileFlux.Core.IDocumentProcessorFactory;
using FluxIndex.Core.Application.Interfaces;
using IEmbeddingService = FluxIndex.Core.Application.Interfaces.IEmbeddingService;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxFeed.Adapters;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Where each chunk came from in its source (pages, time ranges) survives the staged pipeline: extraction reports spans,
/// the vault stores them beside the extracted text, and memorize hands them to the real FileFlux chunker, whose chunk
/// locations become search-hit metadata. Real storage and a real FileFlux chunker; the extractor and the vector store are
/// substitutes.
/// </summary>
public sealed class VaultPipelineSourceLocationTests : IDisposable
{
    // Each page is longer than one chunk's budget (MaxChunkSize counts tokens), so no chunk spans both pages.
    private static readonly string[] Pages =
    [
        string.Join(" ", Enumerable.Repeat("The north region closed the quarter at 1.2 million in revenue, ahead of the plan agreed in January.", 4)),
        string.Join(" ", Enumerable.Repeat("Returns stayed under two percent for the whole quarter, the lowest rate the company has recorded.", 4)),
    ];

    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embedding;
    private readonly ServiceProvider _fileFlux;
    private readonly List<DocumentChunk> _stored = [];

    public VaultPipelineSourceLocationTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"FluxFeedLocations_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_vaultDir);

        var git = Substitute.For<IGitService>();
        git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("abc123");
        _storage = new VaultStorageService(NullLogger<VaultStorageService>.Instance, git,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.GetByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new List<DocumentChunk>());
        _vectorStore.GetChunkIdsByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<string>>([]));
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

        var services = new ServiceCollection();
        FileFlux.ServiceCollectionExtensions.AddFileFlux(services);
        _fileFlux = services.BuildServiceProvider();
        _git = git;
    }

    private readonly IGitService _git;

    public void Dispose()
    {
        _fileFlux.Dispose();
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch { /* ignore cleanup errors */ }
    }

    private VaultPipeline CreatePipeline(IExtractor extractor, ILogger<VaultPipeline>? logger = null) => new(
        _git, new ContentHasher(), _storage, logger ?? NullLogger<VaultPipeline>.Instance,
        options: MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }),
        extractor: extractor,
        chunker: new FileFluxChunker(_fileFlux.GetRequiredService<IDocumentProcessorFactory>(), NullLogger<FileFluxChunker>.Instance),
        vectorStore: _vectorStore, embeddingService: _embedding);

    private static IExtractor ExtractorReturning(string content, IReadOnlyList<ContentSpan>? spans)
    {
        var extractor = Substitute.For<IExtractor>();
        extractor.ExtractAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ExtractionResult { Content = content, Spans = spans }));
        return extractor;
    }

    private static (string Text, List<ContentSpan> Spans) TwoPages()
    {
        var text = Pages[0] + "\n\n" + Pages[1];
        return (text, [new ContentSpan(0, Pages[0].Length) { Page = 1 }, new ContentSpan(Pages[0].Length + 2, text.Length) { Page = 2 }]);
    }

    private async Task<VaultEntry> NewEntryAsync()
    {
        var path = Path.Combine(_testDir, $"doc_{Guid.NewGuid():N}.pdf");
        await File.WriteAllTextAsync(path, "source bytes", TestContext.Current.CancellationToken);
        var entry = VaultEntry.Create(path, _vaultDir);
        await _storage.InitializeEntryAsync(entry, TestContext.Current.CancellationToken);
        return entry;
    }

    private static MemorizeOptions SmallChunks => new() { MaxChunkSize = 60, OverlapSize = 0, Strategy = "Paragraph", SkipCommit = true };

    [Fact]
    public async Task Memorize_ChunksCarryThePageTheyCameFrom()
    {
        var (text, spans) = TwoPages();
        var pipeline = CreatePipeline(ExtractorReturning(text, spans));
        var entry = await NewEntryAsync();

        var result = await pipeline.MemorizeAsync(entry, SmallChunks, TestContext.Current.CancellationToken);

        result.Success.Should().BeTrue();
        _stored.Should().NotBeEmpty();
        _stored.Should().OnlyContain(c => c.Metadata.ContainsKey(VaultPipeline.StartPageMetadataKey));
        _stored.Should().HaveCountGreaterThan(1);
        _stored.Where(c => !c.Content.Contains("north region", StringComparison.Ordinal))
            .Should().NotBeEmpty().And.OnlyContain(c => (int)c.Metadata[VaultPipeline.PageNumberMetadataKey] == 2 && (int)c.Metadata[VaultPipeline.EndPageMetadataKey] == 2);
        _stored.Where(c => !c.Content.Contains("Returns", StringComparison.Ordinal))
            .Should().NotBeEmpty().And.OnlyContain(c => (int)c.Metadata[VaultPipeline.StartPageMetadataKey] == 1 && (int)c.Metadata[VaultPipeline.EndPageMetadataKey] == 1);
    }

    [Fact]
    public async Task Memorize_TimedSpans_BecomeSeconds()
    {
        var (text, _) = TwoPages();
        var spans = new List<ContentSpan>
        {
            new(0, Pages[0].Length) { StartTime = TimeSpan.FromSeconds(0), EndTime = TimeSpan.FromSeconds(12.5) },
            new(Pages[0].Length + 2, text.Length) { StartTime = TimeSpan.FromSeconds(12.5), EndTime = TimeSpan.FromSeconds(30) },
        };
        var pipeline = CreatePipeline(ExtractorReturning(text, spans));

        await pipeline.MemorizeAsync(await NewEntryAsync(), SmallChunks, TestContext.Current.CancellationToken);

        var returns = _stored.Where(c => !c.Content.Contains("north region", StringComparison.Ordinal)).ToList();
        returns.Should().NotBeEmpty();
        returns.Should().OnlyContain(c => (double)c.Metadata[VaultPipeline.StartSecondsMetadataKey] == 12.5
            && (double)c.Metadata[VaultPipeline.EndSecondsMetadataKey] == 30.0
            && !c.Metadata.ContainsKey(VaultPipeline.StartPageMetadataKey));
    }

    // Positive control: without spans the same document yields chunks with no location.
    [Fact]
    public async Task Memorize_WithoutSpans_ChunksCarryNoLocation()
    {
        var (text, _) = TwoPages();
        var pipeline = CreatePipeline(ExtractorReturning(text, spans: null));
        var entry = await NewEntryAsync();

        await pipeline.MemorizeAsync(entry, SmallChunks, TestContext.Current.CancellationToken);

        _stored.Should().NotBeEmpty();
        _stored.Should().OnlyContain(c => !c.Metadata.ContainsKey(VaultPipeline.StartPageMetadataKey));
        File.Exists(entry.ExtractedSpansPath).Should().BeFalse();
    }

    // refined.md is git-tracked and may be edited by hand; the stored offsets then point at the wrong text, so they are
    // not used — the chunks index without a location and the pipeline says so.
    [Fact]
    public async Task Refresh_AfterRefinedWasEdited_DropsLocationsAndWarns()
    {
        var (text, spans) = TwoPages();
        var logger = Substitute.For<ILogger<VaultPipeline>>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        var pipeline = CreatePipeline(ExtractorReturning(text, spans), logger);
        var entry = await NewEntryAsync();
        await pipeline.MemorizeAsync(entry, SmallChunks, TestContext.Current.CancellationToken);

        await _storage.StoreRefinedContentAsync(entry, "An editor's note first.\n\n" + text, TestContext.Current.CancellationToken);
        _stored.Clear();
        await pipeline.RefreshAsync(entry, SmallChunks, TestContext.Current.CancellationToken);

        _stored.Should().NotBeEmpty();
        _stored.Should().OnlyContain(c => !c.Metadata.ContainsKey(VaultPipeline.StartPageMetadataKey));
        logger.ReceivedCalls().Should().Contain(call =>
            call.GetMethodInfo().Name == nameof(ILogger.Log)
            && (LogLevel)call.GetArguments()[0]! == LogLevel.Warning
            && call.GetArguments()[2]!.ToString()!.Contains("no longer matches", StringComparison.Ordinal));
    }
}
