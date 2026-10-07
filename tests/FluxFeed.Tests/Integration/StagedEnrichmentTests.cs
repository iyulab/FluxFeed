using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// Staged indexing on the README's real default stack (sqlite-vec): a re-index embeds only the chunks whose text
/// changed, a stored context is reused for an unchanged passage of an unchanged document, and with
/// <see cref="FileVaultOptions.DeferEnrichment"/> a file is searchable before its contexts exist and upgraded later.
/// </summary>
public sealed class StagedEnrichmentTests : IDisposable
{
    private const string Paragraphs =
        "# Field manual\n\n" +
        "The generator must be refuelled every six hours during continuous operation; log each refuel in the shift book.\n\n" +
        "Coolant pressure is read from the gauge on the north panel; a reading below two bar means the pump has lost prime.\n\n" +
        "Radio checks are performed on the hour with the base station; a missed check is escalated after fifteen minutes.\n\n" +
        "Night shifts hand over at the marmalade board, where open work orders are pinned until they are closed.\n";

    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-staged-" + Guid.NewGuid().ToString("N"));
    private readonly string _file;
    private readonly FailableEmbeddingService _embedder = new();
    private readonly CountingContextService _contexts = new();

    public StagedEnrichmentTests()
    {
        Directory.CreateDirectory(_root);
        _file = Path.Combine(_root, "field-manual.md");
        File.WriteAllText(_file, Paragraphs);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task ARefreshOfUnchangedContent_EmbedsNothingAgain()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var entry = await vault.MemorizeAsync(_file, Ct);
        entry.ChunkCount.Should().BeGreaterThanOrEqualTo(3);
        var embedded = _embedder.EmbeddedTexts;

        await vault.RefreshAsync(_file, Ct);

        _embedder.EmbeddedTexts.Should().Be(embedded, "every stored row is identical to its replacement");
        (await vault.SearchAsync("coolant pressure gauge", ct: Ct)).Items.Should().NotBeEmpty();
    }

    [Fact]
    public async Task OneChangedParagraph_EmbedsOnlyItsChunk()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await vault.MemorizeAsync(_file, Ct);
        var embedded = _embedder.EmbeddedTexts;

        await File.WriteAllTextAsync(_file, Paragraphs.Replace("marmalade board", "turquoise ledger", StringComparison.Ordinal), Ct);
        await vault.MemorizeAsync(_file, Ct);

        _embedder.EmbeddedTexts.Should().Be(embedded + 1);
        (await vault.SearchAsync("turquoise ledger work orders", ct: Ct)).Items.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Deferred_IsSearchableWithoutContexts_ThenTheUpgradeAddsThem_AndARefreshReusesThem()
    {
        await using var provider = BuildStack(o => o.DeferEnrichment = true, withContexts: true);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();

        var entry = await vault.MemorizeAsync(_file, Ct);

        _contexts.Chunks.Should().Be(0, "memorize defers the model calls");
        entry.PendingEnrichment.Should().Be(EnrichmentStages.ContextualEnrichment);
        (await vault.GetPendingEnrichmentAsync(Ct)).Select(e => e.FilepathHash).Should().Contain(entry.FilepathHash);
        (await vault.SearchAsync("coolant pressure gauge", ct: Ct)).Items.Should().NotBeEmpty("the native chunks are indexed");

        var upgraded = await vault.UpgradeAsync(_file, ct: Ct);

        upgraded.PendingEnrichment.Should().Be(EnrichmentStages.None);
        _contexts.Chunks.Should().Be(entry.ChunkCount);
        var rows = (await store.GetByDocumentIdAsync(entry.FilepathHash, Ct)).ToList();
        rows.Should().NotBeEmpty();
        rows.Should().AllSatisfy(r => r.Content.Should().StartWith("CTX "));
        (await vault.GetPendingEnrichmentAsync(Ct)).Should().BeEmpty();

        var calls = _contexts.Chunks;
        var embedded = _embedder.EmbeddedTexts;
        await vault.RefreshAsync(_file, Ct);

        _contexts.Chunks.Should().Be(calls, "a stored context for an unchanged passage of unchanged text is reused");
        _embedder.EmbeddedTexts.Should().Be(embedded);
        (await store.GetByDocumentIdAsync(entry.FilepathHash, Ct)).Should().AllSatisfy(r => r.Content.Should().StartWith("CTX "));
    }

    [Fact]
    public async Task ADeferredReMemorize_KeepsTheContextsAnUpgradeAlreadyWrote()
    {
        await using var provider = BuildStack(o => o.DeferEnrichment = true, withContexts: true);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();
        var entry = await vault.MemorizeAsync(_file, Ct);
        await vault.UpgradeAsync(_file, ct: Ct);

        var again = await vault.MemorizeAsync(_file, Ct);

        again.PendingEnrichment.Should().Be(EnrichmentStages.None);
        (await store.GetByDocumentIdAsync(entry.FilepathHash, Ct)).Should().AllSatisfy(r => r.Content.Should().StartWith("CTX "));
    }

    [Fact]
    public async Task WithTheWorker_TheUpgradeRunsAsAQueuedJob()
    {
        await using var provider = BuildStack(o => { o.DeferEnrichment = true; o.EnableBackgroundProcessing = true; }, withContexts: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, Ct);
        entry.PendingEnrichment.Should().Be(EnrichmentStages.ContextualEnrichment);

        var upgraded = await vault.UpgradeAsync(_file, waitForCompletion: true, ct: Ct);

        upgraded.PendingEnrichment.Should().Be(EnrichmentStages.None);
        _contexts.Chunks.Should().BeGreaterThan(0);
    }

    /// <summary>A host learns that an upgrade finished, with the entry as it now stands; a failing handler changes nothing.</summary>
    [Fact]
    public async Task ADirectUpgrade_RaisesEntryUpgraded_AndAThrowingHandlerDoesNotFailIt()
    {
        await using var provider = BuildStack(o => o.DeferEnrichment = true, withContexts: true);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var entry = await vault.MemorizeAsync(_file, Ct);
        var raised = new List<FluxFeed.Domain.Entities.VaultEntry>();
        vault.EntryUpgraded += (_, _) => throw new InvalidOperationException("handler bug");
        vault.EntryUpgraded += (_, e) => raised.Add(e);

        var upgraded = await vault.UpgradeAsync(_file, ct: Ct);

        upgraded.PendingEnrichment.Should().Be(EnrichmentStages.None);
        raised.Should().ContainSingle().Which.FilepathHash.Should().Be(entry.FilepathHash);
        raised[0].PendingEnrichment.Should().Be(EnrichmentStages.None);
    }

    [Fact]
    public async Task AQueuedUpgrade_RaisesEntryUpgraded_WhenTheJobCompletes()
    {
        await using var provider = BuildStack(o => { o.DeferEnrichment = true; o.EnableBackgroundProcessing = true; }, withContexts: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, Ct);
        var raised = new TaskCompletionSource<FluxFeed.Domain.Entities.VaultEntry>(TaskCreationOptions.RunContinuationsAsynchronously);
        vault.EntryUpgraded += (_, e) => raised.TrySetResult(e);

        await vault.UpgradeAsync(_file, ct: Ct);
        var upgraded = await raised.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);

        upgraded.FilepathHash.Should().Be(entry.FilepathHash);
        upgraded.PendingEnrichment.Should().Be(EnrichmentStages.None);
    }

    private ServiceProvider BuildStack(Action<FileVaultOptions>? configure = null, bool withContexts = false)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = Path.Combine(_root, "fluxindex.db");
            o.VectorDimension = _embedder.GetEmbeddingDimension();
            o.FallbackToInMemoryOnError = false;
        });
        services.AddSingleton<IEmbeddingService>(_embedder);
        if (withContexts)
        {
            services.AddSingleton<IContextualEnrichmentService>(_contexts);
        }

        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
            o.EnableBackgroundProcessing = false;
            o.Chunking.Strategy = "Paragraph";
            o.Chunking.MaxChunkSize = 64;
            o.Chunking.OverlapSize = 0;
            o.ContextualEnrichment.Enabled = withContexts;
            configure?.Invoke(o);
        });
        return services.BuildServiceProvider();
    }

    private static async Task<IAsyncDisposable> StartHostedServicesAsync(IServiceProvider provider)
    {
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        return new HostedServicesLease(hosted);
    }

    private sealed class HostedServicesLease(IReadOnlyList<IHostedService> services) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in services)
                await service.StopAsync(cts.Token);
        }
    }

    /// <summary>Writes «CTX &lt;first word&gt;» as each chunk's context and counts the chunks it was asked about.</summary>
    private sealed class CountingContextService : IContextualEnrichmentService
    {
        private int _chunks;

        public int Chunks => _chunks;

        public Task<string> GenerateContextAsync(string chunkContent, string fullDocumentText, int chunkIndex, int totalChunks,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _chunks);
            return Task.FromResult(Context(chunkContent));
        }

        public Task<IReadOnlyList<string>> GenerateContextBatchAsync(IReadOnlyList<string> chunks, string fullDocumentText,
            CancellationToken cancellationToken = default)
        {
            Interlocked.Add(ref _chunks, chunks.Count);
            return Task.FromResult<IReadOnlyList<string>>(chunks.Select(Context).ToList());
        }

        private static string Context(string chunk) => "CTX " + chunk.Split(' ', 2)[0];
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}
