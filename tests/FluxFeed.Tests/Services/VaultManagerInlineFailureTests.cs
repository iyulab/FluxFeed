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
/// With background processing off, a memorize or refresh is finished by the time the call returns.
/// A failure there is terminal, so it has to reach the caller.
/// </summary>
/// <remarks>
/// <para>
/// The pipeline signals failure by returning <c>MemorizeResult.Failed(...)</c>, never by throwing.
/// The <c>waitForCompletion: true</c> path already surfaced that; the no-wait overloads logged it and
/// returned the entry anyway, so <c>await vault.MemorizeAsync(path)</c> came back looking successful
/// while nothing had been indexed. Two paths through the same terminal operation disagreed about
/// what a failure means.
/// </para>
/// <para>
/// Pinned per path rather than once, because the divergence was not a shared helper getting it wrong
/// - it was one branch having been fixed and its neighbours not.
/// </para>
/// </remarks>
public sealed class VaultManagerInlineFailureTests : IDisposable
{
    private readonly IVaultQueueService _queue = Substitute.For<IVaultQueueService>();
    private readonly IVaultPipeline _pipeline = Substitute.For<IVaultPipeline>();
    private readonly IVaultStorageService _storage = Substitute.For<IVaultStorageService>();
    private readonly IGitService _git = Substitute.For<IGitService>();
    private readonly IFileWatcherService _watcher = Substitute.For<IFileWatcherService>();
    private readonly VaultManager _vault;
    private readonly string _testDir;
    private readonly string _file;

    public VaultManagerInlineFailureTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultInlineFailureTests_" + Guid.NewGuid().ToString("N"));
        var vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(vaultDir);
        _file = Path.Combine(_testDir, "notes.md");
        File.WriteAllText(_file, "# notes\n");

        _watcher.GetAllWatchers().Returns([]);
        _storage.BasePath.Returns(vaultDir);
        _storage.EntryStorageExists(Arg.Any<VaultEntry>()).Returns(false);
        _storage.InitializeEntryAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _storage.GetStorageSizeAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(0L);

        _vault = new VaultManager(
            new ContentHasher(),
            _git,
            _pipeline,
            _queue,
            _watcher,
            _storage,
            NullLogger<VaultManager>.Instance,
            MsOptions.Create(new FileVaultOptions
            {
                VaultBasePath = vaultDir,
                EnableBackgroundProcessing = false,   // inline: the call is terminal
            }));
    }

    [Fact]
    public async Task InlineMemorize_ThatFailed_DoesNotReturnAHealthyLookingEntry()
    {
        _pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Failed("no reader for .md", TimeSpan.Zero));

        var act = async () => await _vault.MemorizeAsync(_file);

        (await act.Should().ThrowAsync<InvalidOperationException>())
            .WithMessage("*no reader for .md*");
    }

    [Fact]
    public async Task InlineMemorize_ThatSucceeded_StillReturnsTheEntry()
    {
        _pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Succeeded(3, 900, TimeSpan.Zero));

        var entry = await _vault.MemorizeAsync(_file);

        entry.Should().NotBeNull();
    }

    [Fact]
    public async Task QueuedMemorize_IsUnaffected_TheFailureIsNotTheCallersYet()
    {
        // The background branch hands the work to the queue and returns before it runs, so there is
        // no failure to surface here - that outcome belongs to the queue worker.
        var queued = new VaultManager(
            new ContentHasher(), _git, _pipeline, _queue, _watcher, _storage,
            NullLogger<VaultManager>.Instance,
            MsOptions.Create(new FileVaultOptions
            {
                VaultBasePath = _storage.BasePath,
                EnableBackgroundProcessing = true,
            }));
        _pipeline.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>())
            .Returns(MemorizeResult.Failed("should never be consulted", TimeSpan.Zero));

        var entry = await queued.MemorizeAsync(_file);

        entry.Should().NotBeNull();
        await _pipeline.DidNotReceive().MemorizeAsync(
            Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>());
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch (IOException)
        {
            // Test scratch directory; a lingering handle is not worth failing a run over.
        }
    }
}
