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
/// The queue has always ordered by priority; until 0.22.0 nothing on <see cref="IVault"/> could set it, so
/// every path enqueued at <see cref="VaultJobPriority.Normal"/> and a bulk crawl competed with a user waiting
/// on one file purely on arrival order.
///
/// <para>
/// These are wiring facts, deliberately at the seam rather than in the queue: the queue's own priority
/// ordering was never in doubt, and what was actually missing was the value reaching it. A default silently
/// reintroduced anywhere along <c>MemorizeAsync</c> → <c>MemorizeCoreAsync</c> → <c>EnqueueMemorizeAsync</c>
/// would leave every other test green.
/// </para>
/// </summary>
public class VaultManagerPriorityTests : IDisposable
{
    private readonly IVaultQueueService _queue = Substitute.For<IVaultQueueService>();
    private readonly IVaultPipeline _pipeline = Substitute.For<IVaultPipeline>();
    private readonly IVaultStorageService _storage = Substitute.For<IVaultStorageService>();
    private readonly IGitService _git = Substitute.For<IGitService>();
    private readonly IFileWatcherService _watcher = Substitute.For<IFileWatcherService>();
    private readonly VaultManager _vault;
    private readonly string _testDir;
    private readonly string _file;

    public VaultManagerPriorityTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultPriorityTests_" + Guid.NewGuid().ToString("N"));
        var vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(vaultDir);
        _file = Path.Combine(_testDir, "notes.md");
        File.WriteAllText(_file, "# notes\n");

        _watcher.GetAllWatchers().Returns([]);
        _storage.BasePath.Returns(vaultDir);
        _storage.EntryStorageExists(Arg.Any<VaultEntry>()).Returns(false);
        _storage.InitializeEntryAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _storage.GetStorageSizeAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(0L);
        _queue.EnqueueMemorizeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<VaultJobPriority>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
            .Returns(ci => VaultJob.Create(ci.ArgAt<string>(1), ci.ArgAt<string>(0), VaultJobType.Memorize, ci.ArgAt<VaultJobPriority>(2)));

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
                EnableBackgroundProcessing = true,
            }));
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch
        {
            // Best-effort cleanup.
        }
    }

    [Fact]
    public async Task MemorizeAsync_PassesTheRequestedPriorityToTheQueue()
    {
        await _vault.MemorizeAsync(_file, VaultJobPriority.High, ct: TestContext.Current.CancellationToken);

        await _queue.Received(1).EnqueueMemorizeAsync(
            Arg.Any<string>(),
            Path.GetFullPath(_file),
            VaultJobPriority.High,
            Arg.Any<string?>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task MemorizeAsync_WithoutAPriority_StillEnqueuesAtNormal()
    {
        await _vault.MemorizeAsync(_file, TestContext.Current.CancellationToken);

        await _queue.Received(1).EnqueueMemorizeAsync(
            Arg.Any<string>(),
            Path.GetFullPath(_file),
            VaultJobPriority.Normal,
            Arg.Any<string?>(),
            Arg.Any<CancellationToken>());
    }
}
