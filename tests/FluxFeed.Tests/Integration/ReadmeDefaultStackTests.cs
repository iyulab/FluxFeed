using System.Diagnostics;
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
/// Exercises the README's default stack for real — <c>AddSQLiteVecVectorStore</c> + an
/// <see cref="IEmbeddingService"/> + <c>AddFileVaultWithFluxIndex</c> — the way a consumer assembles it,
/// instead of through in-memory doubles. Two defects lived exactly in that gap: the store was never
/// bound to the embedder identity (every memorize ended in <see cref="ProcessingStage.Error"/>), and a
/// caller without a Generic Host waited forever because the hosted worker never started.
/// Deliberately not tagged <c>Category=Integration</c>: it has no external dependency and must run in CI.
/// </summary>
public sealed class ReadmeDefaultStackTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-readme-" + Guid.NewGuid().ToString("N"));
    private readonly string _docs;
    private readonly DeterministicEmbeddingService _embedder = new();

    public ReadmeDefaultStackTests()
    {
        _docs = Path.Combine(_root, "docs");
        Directory.CreateDirectory(_docs);
        File.WriteAllText(Path.Combine(_docs, "vacation-policy.md"),
            "# Vacation policy\n\nEvery employee receives twenty vacation days per year. Unused days carry over once.\n");
        File.WriteAllText(Path.Combine(_docs, "deploy-runbook.md"),
            "# Deploy runbook\n\nRun the rollback command when the health check fails after a deploy.\n");
    }

    [Fact]
    public async Task ReadmeStack_WithHostedWorkerRunning_MemorizesAndSearches()
    {
        await using var provider = BuildReadmeStack();
        await using var worker = await StartHostedServicesAsync(provider);

        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        foreach (var file in Directory.GetFiles(_docs, "*.md"))
        {
            var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
            entry.Stage.Should().Be(ProcessingStage.Memorized,
                because: $"{Path.GetFileName(file)} should index on the README stack without any manual BindIdentity call (error: {entry.LastError})");
            entry.ChunkCount.Should().BeGreaterThan(0);
        }

        var result = await vault.SearchAsync("how many vacation days per year", ct: TestContext.Current.CancellationToken);
        result.Items.Should().NotBeEmpty();
        result.Items[0].SourcePath.Should().EndWith("vacation-policy.md");
    }

    [Fact]
    public async Task ReadmeStack_BindsVectorStoreToEmbedderIdentity()
    {
        await using var provider = BuildReadmeStack();

        using var scope = provider.CreateScope();
        _ = scope.ServiceProvider.GetRequiredService<IVault>();

        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>()
            .Should().BeOfType<SQLiteVecVectorStore>().Subject;
        store.BoundIdentity.Should().NotBeNull();
        store.BoundIdentity!.Fingerprint.Should().Be(_embedder.GetIdentity().Fingerprint);
    }

    [Fact]
    public async Task ReadmeStack_WithoutHost_WaitForCompletionFailsFastWithDiagnosis()
    {
        var grace = TimeSpan.FromSeconds(1);
        await using var provider = BuildReadmeStack(o => o.WorkerStartupTimeout = grace);
        // No host: the IHostedService is registered but nothing starts it — the README's plain
        // ServiceCollection path.

        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var file = Path.Combine(_docs, "vacation-policy.md");

        var sw = Stopwatch.StartNew();
        var act = () => vault.MemorizeAsync(file, waitForCompletion: true);

        var ex = await act.Should().ThrowAsync<InvalidOperationException>();
        ex.Which.Message.Should().Contain("no worker is consuming this queue")
            .And.Contain("Generic Host")
            .And.Contain("EnableBackgroundProcessing");
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(30),
            because: "the wait must fail within the configured grace instead of hanging");
    }

    [Fact]
    public async Task ReadmeStack_WorkerStartingWithinGrace_CompletesTheWait()
    {
        await using var provider = BuildReadmeStack(o => o.WorkerStartupTimeout = TimeSpan.FromSeconds(20));

        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var file = Path.Combine(_docs, "deploy-runbook.md");

        var memorize = vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
        await Task.Delay(300, TestContext.Current.CancellationToken);
        memorize.IsCompleted.Should().BeFalse(because: "no worker has started yet");

        await using var worker = await StartHostedServicesAsync(provider);

        var entry = await memorize;
        entry.Stage.Should().Be(ProcessingStage.Memorized);
    }

    [Fact]
    public async Task ReadmeStack_HandEditedAppendText_RefreshIndexesAndCommitsIt()
    {
        // README: "use RefreshAsync after hand-editing append-text.md". Inline mode makes the refresh
        // synchronous so the assertions are deterministic.
        await using var provider = BuildReadmeStack(o => o.EnableBackgroundProcessing = false);

        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var file = Path.Combine(_docs, "vacation-policy.md");

        var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized);
        var commitsAfterMemorize = await vault.LogAsync(file, ct: TestContext.Current.CancellationToken);

        await File.WriteAllTextAsync(entry.AppendTextPath,
            "\nThe vault mascot is a purple axolotl named Zorbix.\n", TestContext.Current.CancellationToken);

        var refreshed = await vault.RefreshAsync(file, TestContext.Current.CancellationToken);
        refreshed.Stage.Should().Be(ProcessingStage.Memorized);

        var commitsAfterRefresh = await vault.LogAsync(file, ct: TestContext.Current.CancellationToken);
        commitsAfterRefresh.Count.Should().Be(commitsAfterMemorize.Count + 1,
            because: "refresh auto-commits the hand-edited vault content");

        var result = await vault.SearchAsync("purple axolotl Zorbix", ct: TestContext.Current.CancellationToken);
        result.Items.Should().NotBeEmpty();
        result.Items[0].SourcePath.Should().EndWith("vacation-policy.md");
        result.Items[0].Content.Should().Contain("Zorbix", because: "the appended text must be part of the indexed chunks");
    }

    [Fact]
    public async Task ReadmeStack_BackgroundMode_RefreshWaitForCompletion_ReturnsReindexedEntry()
    {
        // Background mode: RefreshAsync(path) only enqueues and returns the pre-refresh snapshot, which
        // is what made a consumer script read "refresh did nothing". The waitForCompletion overload
        // mirrors MemorizeAsync and returns the entry after the worker re-indexed and committed.
        await using var provider = BuildReadmeStack();
        await using var worker = await StartHostedServicesAsync(provider);

        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var file = Path.Combine(_docs, "vacation-policy.md");

        var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
        var commitsAfterMemorize = await vault.LogAsync(file, ct: TestContext.Current.CancellationToken);

        await File.WriteAllTextAsync(entry.AppendTextPath,
            "\nThe vault mascot is a purple axolotl named Zorbix.\n", TestContext.Current.CancellationToken);

        var refreshed = await vault.RefreshAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);

        refreshed.Stage.Should().Be(ProcessingStage.Memorized);
        var commitsAfterRefresh = await vault.LogAsync(file, ct: TestContext.Current.CancellationToken);
        commitsAfterRefresh.Count.Should().Be(commitsAfterMemorize.Count + 1);

        var result = await vault.SearchAsync("purple axolotl Zorbix", ct: TestContext.Current.CancellationToken);
        result.Items.Should().NotBeEmpty();
        result.Items[0].Content.Should().Contain("Zorbix");
    }

    private ServiceProvider BuildReadmeStack(Action<FileVaultOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));

        // README default stack, verbatim in shape.
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = Path.Combine(_root, "fluxindex.db");
            o.VectorDimension = _embedder.GetEmbeddingDimension();
            // A native sqlite-vec load failure must fail the test, not silently degrade to in-memory.
            o.FallbackToInMemoryOnError = false;
        });
        services.AddSingleton<IEmbeddingService>(_embedder);
        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
            configure?.Invoke(o);
        });

        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Starts every registered <see cref="IHostedService"/> the way a Generic Host would, and stops
    /// them on dispose.
    /// </summary>
    private static async Task<IAsyncDisposable> StartHostedServicesAsync(IServiceProvider provider)
    {
        var hosted = provider.GetServices<IHostedService>().ToList();
        hosted.Should().NotBeEmpty(because: "AddFileVaultWithFluxIndex registers the background worker as an IHostedService");
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
