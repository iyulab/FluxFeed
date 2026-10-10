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
/// «Documents like this one» on the README's SQLite stack: the vault compares an entry's stored chunk vectors with the
/// other entries', so the answer comes from what the vault already holds — not from a query a consumer had to invent.
/// The deterministic embedder makes closeness a matter of shared words.
/// </summary>
public sealed class SimilarEntriesRealStoreTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-similar-" + Guid.NewGuid().ToString("N"));
    private readonly string _docs;
    private readonly string _archive;
    private readonly DeterministicEmbeddingService _embedder = new();

    public SimilarEntriesRealStoreTests()
    {
        _docs = Path.Combine(_root, "docs");
        _archive = Path.Combine(_root, "archive");
        Directory.CreateDirectory(_docs);
        Directory.CreateDirectory(_archive);
        File.WriteAllText(Path.Combine(_docs, "rollback.md"), "# Rollback\n\nRun the rollback command when the health check fails.\n");
        File.WriteAllText(Path.Combine(_docs, "rollback-guide.md"), "# Rollback guide\n\nWhen the health check fails, run the rollback command and page the on-call engineer.\n");
        File.WriteAllText(Path.Combine(_docs, "recipe.md"), "# Cake\n\nMix flour, sugar and eggs, then bake the cake for forty minutes.\n");
        File.WriteAllText(Path.Combine(_archive, "rollback-old.md"), "# Old rollback\n\nThe rollback command ran when the health check failed.\n");
    }

    [Fact]
    public async Task The_closest_entry_comes_first_and_the_entry_itself_is_left_out()
    {
        await using var provider = BuildStack();
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await MemorizeAllAsync(vault);
        var ct = TestContext.Current.CancellationToken;

        var similar = await vault.FindSimilarEntriesAsync(Path.Combine(_docs, "rollback.md"), new VaultSimilarityOptions { TopK = 10 }, ct);

        similar.Select(s => Path.GetFileName(s.Entry.SourcePath)).Should().NotContain("rollback.md");
        similar.Should().HaveCount(3, "every other entry is a candidate when no scope or floor is set");
        Path.GetFileName(similar[0].Entry.SourcePath).Should().BeOneOf("rollback-guide.md", "rollback-old.md");
        Path.GetFileName(similar[^1].Entry.SourcePath).Should().Be("recipe.md", "it shares no word with the rollback note");
        similar.Select(s => s.Score).Should().BeInDescendingOrder();
    }

    [Fact]
    public async Task A_path_scope_and_a_minimum_score_narrow_the_candidates()
    {
        await using var provider = BuildStack();
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await MemorizeAllAsync(vault);
        var ct = TestContext.Current.CancellationToken;
        var source = Path.Combine(_docs, "rollback.md");

        var inArchive = await vault.FindSimilarEntriesAsync(source, new VaultSimilarityOptions { PathScope = [_archive] }, ct);
        inArchive.Select(s => Path.GetFileName(s.Entry.SourcePath)).Should().Equal("rollback-old.md");

        var all = await vault.FindSimilarEntriesAsync(source, null, ct);
        var floor = (all[0].Score + all[^1].Score) / 2;
        var aboveFloor = await vault.FindSimilarEntriesAsync(source, new VaultSimilarityOptions { MinScore = floor }, ct);
        aboveFloor.Should().NotBeEmpty("the closest entry scores above the midpoint")
            .And.HaveCountLessThan(all.Count, "and the floor leaves the unrelated one out");
        aboveFloor.Should().OnlyContain(s => s.Score >= floor);
    }

    [Fact]
    public async Task A_path_that_is_not_an_entry_is_refused()
    {
        await using var provider = BuildStack();
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var act = () => vault.FindSimilarEntriesAsync(Path.Combine(_docs, "never-memorized.md"), null, TestContext.Current.CancellationToken);

        await act.Should().ThrowAsync<ArgumentException>();
    }

    private async Task MemorizeAllAsync(IVault vault)
    {
        foreach (var file in Directory.GetFiles(_root, "*.md", SearchOption.AllDirectories))
        {
            var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
            entry.Stage.Should().Be(ProcessingStage.Memorized, because: $"{Path.GetFileName(file)}: {entry.LastError}");
        }
    }

    private ServiceProvider BuildStack()
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
