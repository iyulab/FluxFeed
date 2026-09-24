using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// <see cref="VaultSearchOptions.UseReranker"/> on the README's SQLite stack: retrieval over-fetches, the registered
/// <see cref="IReranker"/> orders the candidates, and the vault returns <c>TopK</c> of them. The reranker here reverses
/// the retrieval order, so a reranked result is observably different from an unreranked one.
/// </summary>
public sealed class VaultSearchRerankRealStoreTests : IDisposable
{
    private const string Query = "quarterly budget review";
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-rerank-" + Guid.NewGuid().ToString("N"));
    private readonly string _docs;
    private readonly DeterministicEmbeddingService _embedder = new();

    public VaultSearchRerankRealStoreTests()
    {
        _docs = Path.Combine(_root, "docs");
        Directory.CreateDirectory(_docs);
        for (var i = 0; i < 12; i++)
            File.WriteAllText(Path.Combine(_docs, $"note-{i:D2}.md"), $"# Note {i}\n\nquarterly budget review item{i:D2} for the finance team.\n");
    }

    [Fact]
    public async Task UseReranker_OrdersACandidatePoolOfThreeTimesTopK_AndReturnsTopK()
    {
        var reranker = new ReversingReranker();
        await using var provider = BuildStack(reranker);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await MemorizeAllAsync(vault);
        var ct = TestContext.Current.CancellationToken;

        var retrieval = await vault.SearchAsync(Query, new VaultSearchOptions { TopK = 9 }, ct);
        var reranked = await vault.SearchAsync(Query, new VaultSearchOptions { TopK = 3, UseReranker = true }, ct);

        reranker.LastCandidateCount.Should().Be(9, "the default pool is TopK * 3");
        reranked.Items.Should().HaveCount(3);
        // Reversed pool of the top 9: the 9th, 8th and 7th retrieval hits, in that order.
        reranked.Items.Select(i => i.SourcePath).Should().Equal(retrieval.Items.Skip(6).Reverse().Select(i => i.SourcePath));
        reranked.Items.Should().OnlyContain(i => i.RetrievalScore != null, "the retrieval score is kept next to the reranker's");
        reranked.Items[0].Score.Should().BeGreaterThan(reranked.Items[1].Score, "the score is the reranker's");
    }

    [Fact]
    public async Task RerankCandidateCount_SetsThePool()
    {
        var reranker = new ReversingReranker();
        await using var provider = BuildStack(reranker);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await MemorizeAllAsync(vault);

        var result = await vault.SearchAsync(Query,
            new VaultSearchOptions { TopK = 2, UseReranker = true, RerankCandidateCount = 5 }, TestContext.Current.CancellationToken);

        reranker.LastCandidateCount.Should().Be(5);
        result.Items.Should().HaveCount(2);
    }

    [Fact]
    public async Task Default_DoesNotRerank()
    {
        // Positive control for the option: with UseReranker unset a registered reranker is never called.
        var reranker = new ReversingReranker();
        await using var provider = BuildStack(reranker);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await MemorizeAllAsync(vault);

        var result = await vault.SearchAsync(Query, new VaultSearchOptions { TopK = 3 }, TestContext.Current.CancellationToken);

        reranker.Calls.Should().Be(0);
        result.Items.Should().HaveCount(3);
        result.Items.Should().OnlyContain(i => i.RetrievalScore == null);
    }

    [Fact]
    public async Task UseReranker_WithoutARegisteredReranker_Throws()
    {
        await using var provider = BuildStack(reranker: null);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var act = () => vault.SearchAsync(Query, new VaultSearchOptions { UseReranker = true }, TestContext.Current.CancellationToken);

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*IReranker*");
    }

    [Fact]
    public async Task TenantVault_FromTheFactory_GetsTheRegisteredReranker()
    {
        var reranker = new ReversingReranker();
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        AddStores(services, reranker);
        services.AddFileVaultFactory(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vaults");
            o.EnableRealTimeWatch = false;
        });
        await using var provider = services.BuildServiceProvider();
        var vault = provider.GetRequiredService<IVaultFactory>().GetOrCreate("tenant-a");
        await MemorizeAllAsync(vault);

        var result = await vault.SearchAsync(Query, new VaultSearchOptions { TopK = 2, UseReranker = true }, TestContext.Current.CancellationToken);

        reranker.Calls.Should().Be(1, "the tenant vault resolves optional services from its scope, the reranker included");
        result.Items.Should().HaveCount(2);
    }

    private async Task MemorizeAllAsync(IVault vault)
    {
        foreach (var file in Directory.GetFiles(_docs, "*.md"))
        {
            var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
            entry.Stage.Should().Be(ProcessingStage.Memorized, because: $"{Path.GetFileName(file)}: {entry.LastError}");
        }
    }

    private ServiceProvider BuildStack(IReranker? reranker)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        AddStores(services, reranker);
        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
        });
        return services.BuildServiceProvider();
    }

    private void AddStores(IServiceCollection services, IReranker? reranker)
    {
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = Path.Combine(_root, "fluxindex.db");
            o.VectorDimension = _embedder.GetEmbeddingDimension();
            o.FallbackToInMemoryOnError = false;
        });
        services.AddSingleton<IEmbeddingService>(_embedder);
        if (reranker != null)
            services.AddSingleton(reranker);
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

    /// <summary>Reverses the retrieval order and scores by the new rank (higher is better).</summary>
    private sealed class ReversingReranker : IReranker
    {
        public int Calls { get; private set; }
        public int LastCandidateCount { get; private set; }

        public Task<IEnumerable<RerankResult>> RerankAsync(
            string query, IEnumerable<RetrievalCandidate> candidates, RerankOptions? options = null, CancellationToken cancellationToken = default)
        {
            Calls++;
            var list = candidates.ToList();
            LastCandidateCount = list.Count;
            var topN = options?.TopN ?? list.Count;
            var results = list.AsEnumerable().Reverse().Take(topN).Select((c, i) => new RerankResult
            {
                Id = c.Id,
                DocumentId = c.DocumentId,
                ChunkId = c.ChunkId,
                Content = c.Content,
                InitialScore = c.InitialScore,
                InitialRank = c.InitialRank,
                Metadata = c.Metadata,
                NewRank = i + 1,
                RerankScore = 100 - i,
            });
            return Task.FromResult(results);
        }

        public RerankModelInfo GetModelInfo() => new();
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
