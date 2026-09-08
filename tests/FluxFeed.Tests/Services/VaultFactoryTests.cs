using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
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
/// VaultFactory threads five optional shared services (IVectorStore, IEmbeddingService,
/// IHybridSearchService, IGraphRAGService, IKeywordSearchService) from DI into every tenant's
/// VaultPipeline via a hand-built constructor call rather than plain DI resolution - none of
/// that threading had direct test coverage before this suite. It exists because cycle-235 found
/// one of the five (IHybridSearchService) silently hardcoded to null despite the other four
/// already being wired, which a test at this level would have caught immediately.
/// </summary>
public sealed class VaultFactoryTests : IDisposable
{
    private const string TenantId = "tenant-a";

    private readonly string _basePath;
    private readonly IGitService _git;
    private readonly IFileWatcherService _fileWatcher;
    private readonly IVectorStore _vectorStore;
    private readonly IEmbeddingService _embeddingService;

    public VaultFactoryTests()
    {
        _basePath = Path.Combine(Path.GetTempPath(), $"FileVaultFactory_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_basePath);

        _git = Substitute.For<IGitService>();
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns("commit-hash");

        _fileWatcher = Substitute.For<IFileWatcherService>();
        _fileWatcher.GetAllWatchers().Returns([]);

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<string>>(
                ((IEnumerable<DocumentChunk>)ci[0]).Select(_ => Guid.NewGuid().ToString()).ToList()));

        _embeddingService = Substitute.For<IEmbeddingService>();
        _embeddingService.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<float[]>>(
                ((IEnumerable<string>)ci[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f }).ToList()));
        _embeddingService.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new[] { 0.1f, 0.2f, 0.3f });
        _embeddingService.GetIdentity()
            .Returns(new EmbeddingIdentity { Provider = "Test", Model = "test", Dimension = 3 });
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_basePath))
                Directory.Delete(_basePath, recursive: true);
        }
        catch { /* ignore cleanup errors */ }
    }

    private VaultFactory CreateFactory(
        IHybridSearchService? hybridSearch = null,
        IGraphRAGService? graphRAGService = null,
        IKeywordSearchService? keywordSearchService = null,
        bool backgroundProcessing = false) =>
        new(
            Substitute.For<IServiceProvider>(),
            NullLoggerFactory.Instance,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _basePath, EnableBackgroundProcessing = backgroundProcessing }),
            new ContentHasher(),
            _git,
            _fileWatcher,
            extractor: null,
            chunker: null,
            vectorStore: _vectorStore,
            embeddingService: _embeddingService,
            hybridSearch: hybridSearch,
            graphRAGService: graphRAGService,
            keywordSearchService: keywordSearchService);

    private async Task MemorizeThroughTenantAsync(IVaultFactory factory)
    {
        var vault = factory.GetOrCreate(TenantId);
        var context = factory.GetContext(TenantId)!;

        var docDir = Path.Combine(_basePath, "sources");
        Directory.CreateDirectory(docDir);
        var docPath = Path.Combine(docDir, "doc.txt");
        await File.WriteAllTextAsync(docPath, "Alice works at Acme Corp. Bob manages the project in Seoul.");

        await vault.MemorizeAsync(docPath);
        _ = context; // context kept for callers that need VaultBasePath/Pipeline
    }

    [Fact]
    public void GetOrCreate_CarriesEverySettableOptionToTheTenant()
    {
        // VaultFactory clones the default options per tenant with a hand-maintained copy list; three
        // options added in 0.20.0 were missing from it and tenants silently fell back to defaults. Give
        // every settable property a non-default value and require it to arrive on the tenant, so a new
        // option cannot be forgotten without failing here. VaultBasePath/VaultId are the factory's own.
        var source = new FileVaultOptions { VaultBasePath = _basePath, EnableBackgroundProcessing = false };
        var skip = new[] { nameof(FileVaultOptions.VaultBasePath), nameof(FileVaultOptions.VaultId), nameof(FileVaultOptions.EnableBackgroundProcessing) };
        var scalar = typeof(FileVaultOptions).GetProperties()
            .Where(p => p.CanWrite && !skip.Contains(p.Name) && p.PropertyType != typeof(ChunkingDefaults))
            .ToList();
        foreach (var p in scalar)
            p.SetValue(source, Distinct(p.PropertyType, p.GetValue(source), p.Name));
        var chunkingProps = typeof(ChunkingDefaults).GetProperties().Where(p => p.CanWrite).ToList();
        foreach (var p in chunkingProps)
            p.SetValue(source.Chunking, Distinct(p.PropertyType, p.GetValue(source.Chunking), p.Name));

        using var factory = new VaultFactory(
            Substitute.For<IServiceProvider>(), NullLoggerFactory.Instance, MsOptions.Create(source),
            new ContentHasher(), _git, _fileWatcher, vectorStore: _vectorStore, embeddingService: _embeddingService);
        factory.GetOrCreate(TenantId);
        var tenant = factory.GetContext(TenantId)!.Options;

        foreach (var p in scalar)
            p.GetValue(tenant).Should().BeEquivalentTo(p.GetValue(source), because: $"{p.Name} must survive CloneOptions");
        foreach (var p in chunkingProps)
            p.GetValue(tenant.Chunking).Should().BeEquivalentTo(p.GetValue(source.Chunking), because: $"Chunking.{p.Name} must survive CloneOptions");
    }

    private static object? Distinct(Type type, object? current, string name)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        if (type == typeof(bool)) return !(current is true);
        if (type == typeof(int)) return (current is int i ? i : 0) + 7;
        if (type == typeof(long)) return (current is long l ? l : 0) + 7;
        if (type == typeof(double)) return (current is double d ? d : 0) + 0.5;
        if (type == typeof(string)) return "distinct-" + name;
        if (type == typeof(TimeSpan)) return (current is TimeSpan t ? t : TimeSpan.Zero) + TimeSpan.FromSeconds(11);
        if (type.IsEnum)
        {
            var values = Enum.GetValues(type).Cast<object>().ToList();
            return values.First(v => !Equals(v, current));
        }
        if (type == typeof(List<string>) || type == typeof(IList<string>) || type == typeof(string[]))
            return type == typeof(string[]) ? new[] { "distinct-" + name } : new List<string> { "distinct-" + name };
        if (type == typeof(HashSet<string>)) return new HashSet<string>(StringComparer.OrdinalIgnoreCase) { ".distinct-" + name };
        if (type == typeof(Dictionary<string, string>)) return new Dictionary<string, string> { ["." + name] = "distinct" };
        throw new InvalidOperationException($"No distinct value generator for {name} ({type.Name}) - extend the test when FileVaultOptions grows a new kind of property");
    }

    [Fact]
    public async Task GetOrCreate_BackgroundProcessingOn_TenantQueueHasItsOwnWorker_MemorizeCompletes()
    {
        // Default options keep background processing on. The hosted VaultBackgroundService only
        // consumes the container queue, so a tenant queue needs the worker the factory now starts -
        // before this, the job sat Queued forever and waitForCompletion hung (or, since 0.20.0, threw).
        await using var factory = CreateFactory(backgroundProcessing: true);
        var vault = factory.GetOrCreate(TenantId);
        var context = factory.GetContext(TenantId)!;
        context.Worker.Should().NotBeNull();

        var docDir = Path.Combine(_basePath, "sources");
        Directory.CreateDirectory(docDir);
        var docPath = Path.Combine(docDir, "background.txt");
        await File.WriteAllTextAsync(docPath, "Tenant queues are consumed by a worker the factory owns.", TestContext.Current.CancellationToken);

        var entry = await vault.MemorizeAsync(docPath, waitForCompletion: true, TestContext.Current.CancellationToken);

        entry.Stage.Should().Be(ProcessingStage.Memorized);
        await _vectorStore.Received(1).StoreBatchAsync(Arg.Is<IEnumerable<DocumentChunk>>(c => c.Any()), Arg.Any<CancellationToken>());

        await factory.DisposeAsync(TenantId);
        factory.GetContext(TenantId).Should().BeNull();
    }

    [Fact]
    public async Task GetOrCreate_ThreadsVectorStoreAndEmbeddingService_TenantChunksAreStored()
    {
        using var factory = CreateFactory();

        await MemorizeThroughTenantAsync(factory);

        await _vectorStore.Received(1).StoreBatchAsync(
            Arg.Is<IEnumerable<DocumentChunk>>(c => c.Any()),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public void GetOrCreate_ThreadsGraphRAGService_PipelineReportsSupport()
    {
        var graph = Substitute.For<IGraphRAGService>();
        using var factory = CreateFactory(graphRAGService: graph);

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsGraphRAG.Should().BeTrue();
    }

    [Fact]
    public void GetOrCreate_WithoutGraphRAGService_PipelineReportsNoSupport()
    {
        using var factory = CreateFactory();

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsGraphRAG.Should().BeFalse();
    }

    [Fact]
    public void GetOrCreate_ThreadsKeywordSearchService_PipelineReportsSupport()
    {
        var keyword = Substitute.For<IKeywordSearchService>();
        using var factory = CreateFactory(keywordSearchService: keyword);

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsKeywordIndex.Should().BeTrue();
    }

    [Fact]
    public void GetOrCreate_WithoutKeywordSearchService_PipelineReportsNoSupport()
    {
        using var factory = CreateFactory();

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;

        pipeline.SupportsKeywordIndex.Should().BeFalse();
    }

    [Fact]
    public async Task GetOrCreate_ThreadsHybridSearchService_UsedForHybridSearchStrategy()
    {
        // Regression guard for cycle-235's finding: hybridSearch used to be hardcoded to null in
        // VaultFactory's constructor call, so a tenant-scoped vault could never reach this path no
        // matter what was registered in DI.
        var hybrid = Substitute.For<IHybridSearchService>();
        hybrid.SearchAsync(Arg.Any<string>(), Arg.Any<FluxIndex.Core.Domain.Models.HybridSearchOptions>(),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<FluxIndex.Core.Domain.Models.HybridSearchResult>>([]));
        using var factory = CreateFactory(hybridSearch: hybrid);

        factory.GetOrCreate(TenantId);
        var pipeline = (VaultPipeline)factory.GetContext(TenantId)!.Pipeline;
        await pipeline.SearchAsync("query", strategy: VaultSearchStrategy.Hybrid, ct: TestContext.Current.CancellationToken);

        await hybrid.Received(1).SearchAsync(
            "query", Arg.Any<FluxIndex.Core.Domain.Models.HybridSearchOptions>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetOrCreate_WithoutAnyOptionalSharedServices_StillCreatesUsableVault()
    {
        using var factory = new VaultFactory(
            Substitute.For<IServiceProvider>(),
            NullLoggerFactory.Instance,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _basePath }),
            new ContentHasher(),
            _git,
            _fileWatcher);

        var vault = factory.GetOrCreate(TenantId);
        var status = await vault.StatusAsync(TestContext.Current.CancellationToken);

        status.Should().NotBeNull();
    }
}
