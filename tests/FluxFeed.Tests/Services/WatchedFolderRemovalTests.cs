using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// <see cref="IVault.RemoveWatchedFolderAsync"/> with <c>removeTrackedFiles: true</c> removes the entries under the
/// folder — and only those: a sibling folder whose name starts with the same letters is not inside it.
/// </summary>
public sealed class WatchedFolderRemovalTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"WatchedFolderRemoval_{Guid.NewGuid():N}");
    private readonly IVaultQueueService _queue = Substitute.For<IVaultQueueService>();
    private readonly FileVaultOptions _options;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public WatchedFolderRemovalTests()
    {
        Directory.CreateDirectory(_dir);
        _options = new FileVaultOptions
        {
            VaultBasePath = Path.Combine(_dir, ".vault"),
            EnableRealTimeWatch = false,
            EnableBackgroundProcessing = true,
        };
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public async Task SiblingFolderSharingThePrefix_IsNotRemoved()
    {
        var inside = Track("docs", "a.md");
        var sibling = Track("docs2", "x.md");
        var vault = CreateVault();
        var folder = await vault.AddWatchedFolderAsync(Folder("docs"), ct: Ct);

        await vault.RemoveWatchedFolderAsync(folder.Id, removeTrackedFiles: true, Ct);

        RemovedPaths().Should().BeEquivalentTo([inside], "docs2/x.md is not inside docs");
        RemovedPaths().Should().NotContain(sibling);
    }

    [Fact]
    public async Task FolderAddedWithTrailingSeparator_MatchesTheSameEntries()
    {
        var inside = Track("docs", "a.md");
        Track("docs2", "x.md");
        var vault = CreateVault();
        var folder = await vault.AddWatchedFolderAsync(Folder("docs") + Path.DirectorySeparatorChar, ct: Ct);

        await vault.RemoveWatchedFolderAsync(folder.Id, removeTrackedFiles: true, Ct);

        RemovedPaths().Should().BeEquivalentTo([inside]);
    }

    [Fact]
    public async Task NestedFiles_AreRemoved_AndANestedFolderLeavesItsParentAlone()
    {
        var top = Track("docs", "a.md");
        var deep = Track(Path.Combine("docs", "sub", "deep"), "b.md");
        var vault = CreateVault();

        var nested = await vault.AddWatchedFolderAsync(Folder(Path.Combine("docs", "sub")), ct: Ct);
        await vault.RemoveWatchedFolderAsync(nested.Id, removeTrackedFiles: true, Ct);
        RemovedPaths().Should().BeEquivalentTo([deep], "removing docs/sub reaches into its subfolders but not up to docs");

        _queue.ClearReceivedCalls();
        var parent = await vault.AddWatchedFolderAsync(Folder("docs"), ct: Ct);
        await vault.RemoveWatchedFolderAsync(parent.Id, removeTrackedFiles: true, Ct);
        RemovedPaths().Should().Contain(top);
    }

    private string Folder(string relative)
    {
        var path = Path.Combine(_dir, "files", relative);
        Directory.CreateDirectory(path);
        return path;
    }

    private string Track(string folder, string fileName)
    {
        var path = Path.Combine(Folder(folder), fileName);
        File.WriteAllText(path, "content");
        VaultEntry.Create(path, _options.VaultBasePath!).SaveMetadata();
        return Path.GetFullPath(path);
    }

    private List<string> RemovedPaths() =>
        _queue.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IVaultQueueService.EnqueueRemoveAsync))
            .Select(c => (string)c.GetArguments()[1]!)
            .ToList();

    private VaultManager CreateVault()
    {
        var git = Substitute.For<IGitService>();
        var hasher = new ContentHasher();
        var storage = new VaultStorageService(NullLogger<VaultStorageService>.Instance, git, MsOptions.Create(_options));
        var pipeline = new VaultPipeline(git, hasher, storage, NullLogger<VaultPipeline>.Instance,
            extractor: null, chunker: null, vectorStore: null, embeddingService: null);
        return new VaultManager(hasher, git, pipeline, _queue, Substitute.For<IFileWatcherService>(), storage,
            NullLogger<VaultManager>.Instance, MsOptions.Create(_options));
    }
}
