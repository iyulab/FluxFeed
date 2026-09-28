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
/// Two <see cref="FileVaultOptions"/> were copied between option instances and never applied: every job was created with
/// three retries whatever <see cref="FileVaultOptions.MaxRetryCount"/> said, and every added folder was watched whatever
/// <see cref="FileVaultOptions.EnableRealTimeWatch"/> said.
/// </summary>
public sealed class FileVaultOptionsWiringTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"VaultOptionsWiring_{Guid.NewGuid():N}");

    public FileVaultOptionsWiringTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public async Task EnqueuedJobs_TakeMaxRetryCount()
    {
        using var queue = new VaultQueueService(
            NullLogger<VaultQueueService>.Instance,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _dir, MaxRetryCount = 7 }));

        var job = await queue.EnqueueMemorizeAsync("hash", Path.Combine(_dir, "a.txt"), ct: TestContext.Current.CancellationToken);

        job.MaxRetries.Should().Be(7);
    }

    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 0)]
    public async Task AddedFolders_AreWatchedOnlyWithRealTimeWatch(bool enableRealTimeWatch, int expectedStarts)
    {
        var watcher = Substitute.For<IFileWatcherService>();
        var folderPath = Path.Combine(_dir, "watched");
        Directory.CreateDirectory(folderPath);
        var vault = CreateVault(watcher, new FileVaultOptions { VaultBasePath = Path.Combine(_dir, ".vault"), EnableRealTimeWatch = enableRealTimeWatch });

        var folder = await vault.AddWatchedFolderAsync(folderPath, ct: TestContext.Current.CancellationToken);
        await vault.PauseWatchingAsync(folder.Id, TestContext.Current.CancellationToken);
        await vault.ResumeWatchingAsync(folder.Id, TestContext.Current.CancellationToken);

        await watcher.Received(expectedStarts * 2).StartWatchingAsync(Arg.Any<WatchedFolder>(), Arg.Any<CancellationToken>());
    }

    private static VaultManager CreateVault(IFileWatcherService watcher, FileVaultOptions options)
    {
        var git = Substitute.For<IGitService>();
        var hasher = new ContentHasher();
        var storage = new VaultStorageService(NullLogger<VaultStorageService>.Instance, git, MsOptions.Create(options));
        var pipeline = new VaultPipeline(git, hasher, storage, NullLogger<VaultPipeline>.Instance,
            extractor: null, chunker: null, vectorStore: null, embeddingService: null);
        var queue = Substitute.For<IVaultQueueService>();
        return new VaultManager(hasher, git, pipeline, queue, watcher, storage, NullLogger<VaultManager>.Instance, MsOptions.Create(options));
    }
}
