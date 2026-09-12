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
/// Chunk identity against the README's real default stack (sqlite-vec store + hosted worker), the way
/// a consumer runs it: a re-memorize keeps the chunk ids it wrote, and a re-index that fails halfway
/// leaves the previous generation whole and searchable. Both depend on the store honouring the ids the
/// pipeline chooses — a store that mints its own would either duplicate rows or leave a half-written
/// generation behind, and nothing in the in-memory doubles can show that.
/// Deliberately not tagged <c>Category=Integration</c>: no external dependency, must run in CI.
/// </summary>
public sealed class ReadmeDefaultStackChunkIdentityTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-chunkid-" + Guid.NewGuid().ToString("N"));
    private readonly string _file;
    private readonly FailableEmbeddingService _embedder = new();

    private const string Paragraphs =
        "# Field manual\n\n" +
        "The generator must be refuelled every six hours during continuous operation; log each refuel in the shift book.\n\n" +
        "Coolant pressure is read from the gauge on the north panel; a reading below two bar means the pump has lost prime.\n\n" +
        "Radio checks are performed on the hour with the base station; a missed check is escalated after fifteen minutes.\n\n" +
        "The quartermaster issues replacement filters on Tuesdays; bring the serial number of the unit being serviced.\n\n" +
        "Night shifts hand over at the marmalade board, where open work orders are pinned until they are closed.\n";

    public ReadmeDefaultStackChunkIdentityTests()
    {
        Directory.CreateDirectory(_root);
        _file = Path.Combine(_root, "field-manual.md");
        File.WriteAllText(_file, Paragraphs);
    }

    [Fact]
    public async Task ReMemorizeOfAnUnchangedFile_KeepsTheSameChunkIds()
    {
        await using var provider = BuildStack();
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();

        var first = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        first.Stage.Should().Be(ProcessingStage.Memorized, because: first.LastError);
        first.ChunkCount.Should().BeGreaterThanOrEqualTo(3, "the manual must split into several chunks");
        var firstIds = await store.GetChunkIdsByDocumentIdAsync(first.FilepathHash, TestContext.Current.CancellationToken);
        firstIds.Should().HaveCount(first.ChunkCount);

        var second = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        second.Stage.Should().Be(ProcessingStage.Memorized, because: second.LastError);
        var secondIds = await store.GetChunkIdsByDocumentIdAsync(second.FilepathHash, TestContext.Current.CancellationToken);

        secondIds.Should().BeEquivalentTo(firstIds, "an unchanged document is the same chunks under the same ids");
        secondIds.Should().HaveCount(second.ChunkCount, "one row per chunk, no second copy");
    }

    [Fact]
    public async Task ReindexThatFailsMidway_LeavesThePreviousGenerationWholeAndSearchable()
    {
        await using var provider = BuildStack(o => o.EnableAutoRetry = false);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);
        entry.ChunkCount.Should().BeGreaterThanOrEqualTo(3);
        var previousGeneration = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);

        // Change the last paragraph and let the per-chunk worker path write at least one chunk before
        // the embedder throws: a genuine half-written generation.
        await File.WriteAllTextAsync(_file,
            Paragraphs.Replace("marmalade board", "turquoise ledger", StringComparison.Ordinal),
            TestContext.Current.CancellationToken);
        _embedder.FailOnSingleCall(2);

        var act = () => vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*failed*");

        var afterFailure = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        afterFailure.Should().BeEquivalentTo(previousGeneration,
            "the rollback removes what the failed run added and leaves the previous generation's rows in place");

        var oldWording = await vault.SearchAsync("marmalade board work orders", ct: TestContext.Current.CancellationToken);
        oldWording.Items.Should().Contain(i => i.Content != null && i.Content.Contains("marmalade", StringComparison.Ordinal),
            "the previous generation still answers searches");
        var newWording = await vault.SearchAsync("turquoise ledger work orders", ct: TestContext.Current.CancellationToken);
        newWording.Items.Should().NotContain(i => i.Content != null && i.Content.Contains("turquoise", StringComparison.Ordinal),
            "nothing of the failed generation may be searchable");

        // The retry lands the new generation exactly: the changed paragraph's old row is gone.
        _embedder.Disarm();
        var retried = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        retried.Stage.Should().Be(ProcessingStage.Memorized, because: retried.LastError);
        var newGeneration = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        newGeneration.Should().HaveCount(retried.ChunkCount);
        newGeneration.Should().NotBeEquivalentTo(previousGeneration, "at least one chunk changed");
        newGeneration.Intersect(previousGeneration).Should().NotBeEmpty("unchanged paragraphs keep their ids");
        var afterRetry = await vault.SearchAsync("turquoise ledger work orders", ct: TestContext.Current.CancellationToken);
        afterRetry.Items.Should().Contain(i => i.Content != null && i.Content.Contains("turquoise", StringComparison.Ordinal));
    }

    [Fact]
    public async Task FirstMemorizeThatFailsMidway_IsRetriedFromTheStart_AndLandsTheWholeDocument()
    {
        // The worker checkpoints after every stored chunk and the rollback removes those rows. The
        // automatic retry used to resume from the checkpoint and skip the chunks the rollback had
        // removed: the retry reported success with the first paragraphs missing.
        await using var provider = BuildStack(o =>
        {
            o.EnableAutoRetry = true;
            o.RetryDelayMs = 50;
        });
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();

        // Fails once, on the second chunk of the first attempt; the retry's calls come after.
        _embedder.FailOnSingleCall(2);
        var act = () => vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*failed*");

        var entry = await WaitForStageAsync(vault, ProcessingStage.Memorized, TimeSpan.FromSeconds(30));
        entry.ChunkCount.Should().BeGreaterThanOrEqualTo(3);
        var ids = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        ids.Should().HaveCount(entry.ChunkCount, "the retry must land every chunk, including the ones the rollback removed");

        var firstParagraph = await vault.SearchAsync("generator refuelled every six hours", ct: TestContext.Current.CancellationToken);
        firstParagraph.Items.Should().Contain(i => i.Content != null && i.Content.Contains("refuelled", StringComparison.Ordinal),
            "the first chunk was rolled back after the failure and must be written again by the retry");
    }

    private async Task<FluxFeed.Domain.Entities.VaultEntry> WaitForStageAsync(IVault vault, ProcessingStage stage, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        FluxFeed.Domain.Entities.VaultEntry? entry = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            entry = await vault.GetAsync(_file, TestContext.Current.CancellationToken);
            if (entry?.Stage == stage)
                return entry;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
        throw new TimeoutException($"entry did not reach {stage} within {timeout}; last stage {entry?.Stage}, error {entry?.LastError}");
    }

    private ServiceProvider BuildStack(Action<FileVaultOptions>? configure = null)
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
            // Small chunks, split on paragraphs, so the manual is several rows and a mid-run failure
            // leaves a genuinely partial generation.
            o.Chunking.Strategy = "Paragraph";
            o.Chunking.MaxChunkSize = 64;
            o.Chunking.OverlapSize = 0;
            configure?.Invoke(o);
        });
        return services.BuildServiceProvider();
    }

    private static async Task<IAsyncDisposable> StartHostedServicesAsync(IServiceProvider provider)
    {
        var hosted = provider.GetServices<IHostedService>().ToList();
        hosted.Should().NotBeEmpty();
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
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}
