using AwesomeAssertions;
using FluxFeed.Domain.Entities;
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
/// <see cref="IVault.EntryStateChanged"/> on the README's default stack: one change per persisted transition — success,
/// failure, removal, move — including the ones the background worker writes through its own pipeline, and nothing for a
/// rewrite that changes no state a status display follows.
/// </summary>
public sealed class EntryStateChangedTests : IDisposable
{
    private const string Text =
        "# Shift notes\n\n" +
        "The generator must be refuelled every six hours during continuous operation.\n\n" +
        "Coolant pressure is read from the gauge on the north panel.\n";

    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-entrystate-" + Guid.NewGuid().ToString("N"));
    private readonly string _file;
    private readonly FailableEmbeddingService _embedder = new();

    public EntryStateChangedTests()
    {
        Directory.CreateDirectory(_root);
        _file = Path.Combine(_root, "shift-notes.md");
        File.WriteAllText(_file, Text);
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task AMemorize_RaisesEachStage_EndingMemorized()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var changes = Collect(vault);

        var entry = await vault.MemorizeAsync(_file, Ct);

        changes.Should().NotBeEmpty();
        changes.Should().OnlyContain(c => c.FilepathHash == entry.FilepathHash && c.SourcePath == entry.SourcePath);
        changes[0].Previous.Should().BeNull("the entry did not exist before");
        changes[0].Current!.Value.Stage.Should().Be(ProcessingStage.Source);
        changes.Select(c => c.Current!.Value.Stage).Should().ContainInOrder(
            ProcessingStage.Source, ProcessingStage.Extracted, ProcessingStage.Refined, ProcessingStage.Memorized);
        var last = changes[^1].Current!.Value;
        last.ChunkCount.Should().Be(entry.ChunkCount).And.BeGreaterThan(0);
        last.SyncStatus.Should().Be(SyncStatus.InSync);
        changes.Should().OnlyContain(c => c.Previous != c.Current, "a write that changes nothing raises nothing");
    }

    [Fact]
    public async Task AFailedMemorize_RaisesTheErrorState()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var changes = Collect(vault);
        _embedder.FailEveryCall = true;

        try { await vault.MemorizeAsync(_file, Ct); }
        catch (Exception ex) when (ex is not OperationCanceledException) { }

        var last = changes.Should().NotBeEmpty().And.Subject.Last().Current!.Value;
        last.Stage.Should().Be(ProcessingStage.Error);
        last.LastError.Should().NotBeNullOrWhiteSpace();
    }

    [Fact]
    public async Task ARemoval_RaisesAChangeWithNoCurrentState()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var entry = await vault.MemorizeAsync(_file, Ct);
        var changes = Collect(vault);

        await vault.RemoveAsync(_file, Ct);

        var removal = changes.Should().ContainSingle(c => c.IsRemoval).Subject;
        removal.FilepathHash.Should().Be(entry.FilepathHash);
        removal.Previous!.Value.Stage.Should().Be(ProcessingStage.Memorized);
    }

    [Fact]
    public async Task AMove_RaisesTheNewKeyWithThePreviousOne()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var entry = await vault.MemorizeAsync(_file, Ct);
        var changes = Collect(vault);
        var destination = Path.Combine(_root, "renamed-notes.md");
        File.Move(_file, destination);

        var moved = await vault.MoveAsync(_file, destination, Ct);

        var change = changes.Should().ContainSingle(c => c.PreviousFilepathHash != null).Subject;
        change.PreviousFilepathHash.Should().Be(entry.FilepathHash);
        change.FilepathHash.Should().Be(moved.FilepathHash);
        change.SourcePath.Should().Be(moved.DestinationPath);
        change.Current.Should().NotBeNull();
    }

    [Fact]
    public async Task AScanThatFindsNothingChanged_RaisesNothing()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await vault.MemorizeAsync(_file, Ct);
        var changes = Collect(vault);

        await vault.DetectChangesAsync(Ct);
        await vault.DetectChangesAsync(_file, Ct);

        changes.Should().BeEmpty("the scan rewrites its check time, not the state");
    }

    [Fact]
    public async Task ASourceEditFoundByAScan_RaisesTheSyncChange()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        await vault.MemorizeAsync(_file, Ct);
        var changes = Collect(vault);

        await File.AppendAllTextAsync(_file, "\nRadio checks run on the hour.\n", Ct);
        await vault.DetectChangesAsync(_file, Ct);

        changes.Should().ContainSingle().Which.Current!.Value.SyncStatus.Should().Be(SyncStatus.SourceModified);
    }

    [Fact]
    public async Task AThrowingHandler_DoesNotFailTheWrite_OrOtherHandlers()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        vault.EntryStateChanged += (_, _) => throw new InvalidOperationException("handler bug");
        var changes = Collect(vault);

        var entry = await vault.MemorizeAsync(_file, Ct);

        entry.Stage.Should().Be(ProcessingStage.Memorized);
        changes.Should().NotBeEmpty();
    }

    /// <summary>
    /// The background worker writes through a pipeline of its own; a vault the host holds still hears every change.
    /// </summary>
    [Fact]
    public async Task AQueuedMemorize_ReachesTheHostsVault_ThroughTheWorkersWrites()
    {
        await using var provider = BuildStack(o => o.EnableBackgroundProcessing = true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var memorized = new TaskCompletionSource<VaultEntryStateChange>(TaskCreationOptions.RunContinuationsAsynchronously);
        vault.EntryStateChanged += (_, c) =>
        {
            if (c.Current is { Stage: ProcessingStage.Memorized })
                memorized.TrySetResult(c);
        };

        await vault.MemorizeAsync(_file, VaultJobPriority.Normal, waitForCompletion: false, Ct);
        var change = await memorized.Task.WaitAsync(TimeSpan.FromSeconds(30), Ct);

        change.Current!.Value.ChunkCount.Should().BeGreaterThan(0);
    }

    private static List<VaultEntryStateChange> Collect(IVault vault)
    {
        var changes = new List<VaultEntryStateChange>();
        var gate = new object();
        vault.EntryStateChanged += (_, c) => { lock (gate) changes.Add(c); };
        return changes;
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
            o.EnableBackgroundProcessing = false;
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
