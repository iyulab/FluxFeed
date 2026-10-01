using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// A folder added with <c>autoMemorize: true</c> keeps the vault in step with it: what the watcher reports is
/// memorized, removed or moved without the consumer subscribing to anything.
/// </summary>
public sealed class WatchedFolderSyncTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), $"WatchedFolderSync_{Guid.NewGuid():N}");
    private readonly FakeWatcher _watcher = new();
    private readonly IVault _vault = Substitute.For<IVault>();

    public WatchedFolderSyncTests() => Directory.CreateDirectory(_root);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }

    private WatchedFolderSync Sync() =>
        new(_watcher, _ => ValueTask.FromResult(new VaultLease(_vault, owner: null)), NullLogger.Instance);

    private WatchedFolder Folder(WatchedFolderSync sync, bool autoMemorize = true, string[]? exclude = null)
    {
        var folder = WatchedFolder.Create(_root, autoMemorize: autoMemorize, includePatterns: ["*.md", "*.txt"], excludePatterns: exclude ?? []);
        return sync.GetOrAdd(folder);
    }

    private string File(string name, string content = "x")
    {
        var path = Path.Combine(_root, name);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        System.IO.File.WriteAllText(path, content);
        return path;
    }

    private void Tracked(string path) =>
        _vault.GetAsync(path, Arg.Any<CancellationToken>()).Returns(VaultEntry.Create(path, Path.Combine(_root, ".vault")));

    [Fact]
    public async Task A_created_or_modified_file_is_memorized()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var a = File("a.md");
        var b = File("b.md");

        _watcher.Created(folder.Id, a);
        _watcher.Modified(folder.Id, b);
        await sync.DrainAsync(Ct);

        await _vault.Received(1).MemorizeAsync(a, Arg.Any<CancellationToken>());
        await _vault.Received(1).MemorizeAsync(b, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task A_folder_added_without_autoMemorize_only_raises_events()
    {
        using var sync = Sync();
        var folder = Folder(sync, autoMemorize: false);
        var a = File("a.md");

        _watcher.Created(folder.Id, a);
        _watcher.Deleted(folder.Id, a);
        await sync.DrainAsync(Ct);

        _vault.ReceivedCalls().Should().BeEmpty();
    }

    [Fact]
    public async Task Events_of_another_vaults_folder_are_ignored()
    {
        using var sync = Sync();
        Folder(sync);

        _watcher.Created(Guid.NewGuid(), File("a.md"));
        await sync.DrainAsync(Ct);

        _vault.ReceivedCalls().Should().BeEmpty();
    }

    [Fact]
    public async Task A_deleted_file_is_removed_only_when_tracked()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var tracked = Path.Combine(_root, "tracked.md");
        var untracked = Path.Combine(_root, "untracked.md");
        Tracked(tracked);

        _watcher.Deleted(folder.Id, tracked);
        _watcher.Deleted(folder.Id, untracked);
        await sync.DrainAsync(Ct);

        await _vault.Received(1).RemoveAsync(tracked, Arg.Any<CancellationToken>());
        await _vault.DidNotReceive().RemoveAsync(untracked, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task A_renamed_tracked_file_is_moved_not_re_memorized()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var oldPath = Path.Combine(_root, "old.md");
        var newPath = File("new.md");
        Tracked(oldPath);

        _watcher.Renamed(folder.Id, oldPath, newPath);
        await sync.DrainAsync(Ct);

        await _vault.Received(1).MoveAsync(oldPath, newPath, Arg.Any<CancellationToken>());
        await _vault.DidNotReceive().MemorizeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task A_renamed_folder_moves_every_entry_under_it()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var oldDir = Path.Combine(_root, "docs");
        var newDir = Path.Combine(_root, "notes");
        Directory.CreateDirectory(newDir);
        _vault.MoveFolderAsync(oldDir, newDir, Arg.Any<CancellationToken>()).Returns(new VaultFolderMoveResult());

        _watcher.Renamed(folder.Id, oldDir, newDir);
        await sync.DrainAsync(Ct);

        await _vault.Received(1).MoveFolderAsync(oldDir, newDir, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task A_rename_to_a_name_the_patterns_reject_removes_the_entry()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var oldPath = Path.Combine(_root, "doc.md");
        var newPath = File("doc.md.bak");
        Tracked(oldPath);

        _watcher.Renamed(folder.Id, oldPath, newPath);
        await sync.DrainAsync(Ct);

        await _vault.Received(1).RemoveAsync(oldPath, Arg.Any<CancellationToken>());
        await _vault.DidNotReceive().MoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task A_rename_of_an_untracked_file_into_the_patterns_memorizes_it()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var oldPath = Path.Combine(_root, "draft.tmp");
        var newPath = File("draft.md");

        _watcher.Renamed(folder.Id, oldPath, newPath);
        await sync.DrainAsync(Ct);

        await _vault.Received(1).MemorizeAsync(newPath, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task A_move_refused_while_a_job_is_queued_is_retried()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var oldPath = Path.Combine(_root, "old.md");
        var newPath = File("new.md");
        Tracked(oldPath);
        _vault.MoveAsync(oldPath, newPath, Arg.Any<CancellationToken>())
            .Returns(
                _ => throw new InvalidOperationException("a job targets this path"),
                _ => Task.FromResult(new VaultMoveResult()));

        _watcher.Renamed(folder.Id, oldPath, newPath);
        await sync.DrainAsync(Ct);

        await _vault.Received(2).MoveAsync(oldPath, newPath, Arg.Any<CancellationToken>());
        await _vault.DidNotReceive().RemoveAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task A_failing_change_does_not_stop_the_ones_after_it()
    {
        using var sync = Sync();
        var folder = Folder(sync);
        var a = File("a.md");
        var b = File("b.md");
        _vault.MemorizeAsync(a, Arg.Any<CancellationToken>()).ThrowsAsync(new IOException("locked"));

        _watcher.Created(folder.Id, a);
        _watcher.Created(folder.Id, b);
        await sync.DrainAsync(Ct);

        await _vault.Received(1).MemorizeAsync(b, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task With_the_real_watcher_a_file_saved_several_times_is_memorized_once()
    {
        var options = new FileVaultOptions { DebounceDelayMs = 300 };
        using var watcher = new FileWatcherService(NullLogger<FileWatcherService>.Instance, MsOptions.Create(options));
        using var sync = new WatchedFolderSync(watcher, _ => ValueTask.FromResult(new VaultLease(_vault, owner: null)));
        var folder = sync.GetOrAdd(WatchedFolder.Create(_root, autoMemorize: true, includePatterns: ["*.md"]));
        await watcher.StartWatchingAsync(folder, Ct);
        var path = Path.Combine(_root, "note.md");

        for (var i = 0; i < 5; i++)
        {
            await System.IO.File.WriteAllTextAsync(path, $"revision {i}", Ct);
            await Task.Delay(30, Ct);
        }

        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (_vault.ReceivedCalls().All(c => c.GetMethodInfo().Name != nameof(IVault.MemorizeAsync)) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(100, Ct);
        }

        await Task.Delay(options.DebounceDelayMs * 3, Ct); // a second, late memorize would land in this window
        await sync.DrainAsync(Ct);

        await _vault.Received(1).MemorizeAsync(path, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Every_scope_of_the_container_vault_sees_the_same_watched_folders()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddFileVault(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
        });
        services.AddSingleton(Substitute.For<IVaultPipeline>());
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        WatchedFolder added;
        await using (var first = provider.CreateAsyncScope())
        {
            added = await first.ServiceProvider.GetRequiredService<IVault>().AddWatchedFolderAsync(_root, autoMemorize: true, ct: Ct);
        }

        await using var second = provider.CreateAsyncScope();
        var vault = second.ServiceProvider.GetRequiredService<IVault>();
        (await vault.GetAllWatchedFoldersAsync(Ct)).Should().ContainSingle(f => f.Id == added.Id);
        await vault.RemoveWatchedFolderAsync(added.Id, ct: Ct);
        (await vault.GetWatchedFolderAsync(added.Id, Ct)).Should().BeNull();
    }

    private sealed class FakeWatcher : IFileWatcherService
    {
        public event EventHandler<FileChangeEventArgs>? FileCreated;
        public event EventHandler<FileChangeEventArgs>? FileModified;
        public event EventHandler<FileChangeEventArgs>? FileDeleted;
        public event EventHandler<FileRenamedEventArgs>? FileRenamed;
        public event EventHandler<WatcherErrorEventArgs>? ErrorOccurred;

        public void Created(Guid folder, string path) => FileCreated?.Invoke(this, new FileChangeEventArgs { FolderId = folder, FilePath = path });

        public void Modified(Guid folder, string path) => FileModified?.Invoke(this, new FileChangeEventArgs { FolderId = folder, FilePath = path });

        public void Deleted(Guid folder, string path) => FileDeleted?.Invoke(this, new FileChangeEventArgs { FolderId = folder, FilePath = path });

        public void Renamed(Guid folder, string oldPath, string newPath) =>
            FileRenamed?.Invoke(this, new FileRenamedEventArgs { FolderId = folder, OldPath = oldPath, NewPath = newPath });

        public Task StartWatchingAsync(WatchedFolder folder, CancellationToken ct = default) => Task.CompletedTask;

        public Task StopWatchingAsync(Guid folderId, CancellationToken ct = default) => Task.CompletedTask;

        public Task StopAllAsync(CancellationToken ct = default) => Task.CompletedTask;

        public WatcherInfo? GetWatcherInfo(Guid folderId) => null;

        public IReadOnlyList<WatcherInfo> GetAllWatchers() => [];

        public void Dispose() => ErrorOccurred = null;
    }
}
