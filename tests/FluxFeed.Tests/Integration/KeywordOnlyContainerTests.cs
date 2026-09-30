using AwesomeAssertions;
using FluxFeed.Adapters;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Application.Services;
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// A container whose embedding service is FluxIndex's keyword-only placeholder (<see cref="NoEmbeddingService"/> — what a
/// FluxIndex context without an embedder registers since FluxIndex 0.65.0). Building a vault in it used to throw: the
/// pipeline bound the vector store to the placeholder's identity, which it refuses to name. The vault now behaves as it
/// does with no embedding service.
/// Deliberately not tagged <c>Category=Integration</c>: no external dependency, must run in CI.
/// </summary>
public sealed class KeywordOnlyContainerTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-keywordonly-" + Guid.NewGuid().ToString("N"));

    public KeywordOnlyContainerTests() => Directory.CreateDirectory(_root);

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private ServiceProvider BuildStack(IEmbeddingService embeddingService)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = Path.Combine(_root, "fluxindex.db");
            o.FallbackToInMemoryOnError = false;
        });
        services.AddSingleton(embeddingService);
        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
        });
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task AVault_CanBeBuiltAndSearched_InAKeywordOnlyContainer()
    {
        await using var provider = BuildStack(NoEmbeddingService.Instance);
        using var scope = provider.CreateScope();

        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var hits = await vault.SearchAsync("anything", ct: TestContext.Current.CancellationToken);

        hits.Items.Should().BeEmpty("a vault without an embedding service does not index vectors");
    }

    [Fact]
    public async Task FluxIndexMemorizer_SaysItNeedsAnEmbedder_InAKeywordOnlyContainer()
    {
        await using var provider = BuildStack(NoEmbeddingService.Instance);
        using var scope = provider.CreateScope();

        var act = () => scope.ServiceProvider.GetRequiredService<FluxIndexMemorizer>();

        act.Should().Throw<InvalidOperationException>().WithMessage("*needs an embedding service*keyword-only*");
    }
}
