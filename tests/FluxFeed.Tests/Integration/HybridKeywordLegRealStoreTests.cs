using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Application.Services.KeywordSearch;
using FluxIndex.Storage.SQLite;
using FluxIndex.Storage.SQLite.KeywordSearch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// The README's SQLite stack with a relational keyword index next to the sqlite-vec store. Hybrid must fuse over
/// that keyword index — the one the registered text analyzer shapes — rather than the store's own FTS5 table, whose
/// tokenizer never sees the analyzer. Korean compounds make the difference observable: FTS5 <c>unicode61</c> keeps
/// <c>월세공제</c> as one token, so a query for <c>월세</c> does not match it, while the CJK bigram analyzer does.
/// The deterministic embedder shares no word between that query and any document, so only the keyword leg can
/// rank the right document first.
/// </summary>
public sealed class HybridKeywordLegRealStoreTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-hybridleg-" + Guid.NewGuid().ToString("N"));
    private readonly string _docs;
    private readonly DeterministicEmbeddingService _embedder = new();

    public HybridKeywordLegRealStoreTests()
    {
        _docs = Path.Combine(_root, "docs");
        Directory.CreateDirectory(_docs);
        File.WriteAllText(Path.Combine(_docs, "rent.md"), "# 안내\n\n월세공제 신청은 연말정산 기간에 한다.\n");
        File.WriteAllText(Path.Combine(_docs, "leave.md"), "# 안내\n\n육아휴직 신청은 인사팀에 한다.\n");
        File.WriteAllText(Path.Combine(_docs, "deploy.md"), "# Deploy\n\nRun the rollback command when the health check fails.\n");
        File.WriteAllText(Path.Combine(_docs, "tax.md"), "# 안내\n\n의료비공제 서류는 병원에서 발급한다.\n");
    }

    [Fact]
    public async Task Hybrid_WithAKeywordIndex_FusesOverTheAnalyzedIndex_NotTheStoreFts()
    {
        await using var provider = BuildStack();
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var pipeline = scope.ServiceProvider.GetRequiredService<IVaultPipeline>();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        pipeline.HybridKeywordLeg.Should().Be(HybridKeywordLeg.KeywordIndex,
            "a registered keyword index is the hybrid keyword leg even when the vector store has native hybrid");

        foreach (var file in Directory.GetFiles(_docs, "*.md"))
        {
            var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
            entry.Stage.Should().Be(ProcessingStage.Memorized, because: $"{Path.GetFileName(file)}: {entry.LastError}");
        }

        var result = await vault.SearchAsync("월세", new VaultSearchOptions { SearchStrategy = VaultSearchStrategy.Hybrid, TopK = 4 }, TestContext.Current.CancellationToken);

        result.ExecutedStrategy.Should().Be(VaultSearchStrategy.Hybrid);
        result.Items.Should().NotBeEmpty();
        result.Items[0].SourcePath.Should().EndWith("rent.md",
            "only the bigram-analyzed keyword index matches 월세 inside 월세공제; the store's FTS5 does not");
    }

    private ServiceProvider BuildStack()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        var dbPath = Path.Combine(_root, "fluxindex.db");
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = dbPath;
            o.VectorDimension = _embedder.GetEmbeddingDimension();
            o.FallbackToInMemoryOnError = false;
        });
        services.AddSingleton<IEmbeddingService>(_embedder);
        services.AddSingleton<ITextAnalyzer, CjkBigramTextAnalyzer>();
        services.AddSingleton<IKeywordSearchService>(sp => new SQLiteKeywordSearchService(
            $"Data Source={dbPath}",
            sp.GetRequiredService<ILogger<SQLiteKeywordSearchService>>(),
            sp.GetRequiredService<ITextAnalyzer>()));
        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
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

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
