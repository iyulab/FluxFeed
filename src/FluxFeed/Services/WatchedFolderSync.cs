using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Channels;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FluxFeed.Services;

/// <summary>
/// The watched folders of one vault, and what keeps that vault in step with them. One instance lives as long as the
/// vault's identity does — the container's vault (every scoped <see cref="IVault"/> shares it) or one tenant vault — so a
/// folder added through one scope is still watched, listed and removable from the next.
/// </summary>
/// <remarks>
/// <para>
/// For a folder added with <c>autoMemorize: true</c>, the changes <see cref="IFileWatcherService"/> reports are applied
/// to the vault: a created or modified file is memorized (unchanged content is detected by hash and costs nothing), a
/// deleted file is removed, a renamed file is moved (<see cref="IVault.MoveAsync"/> — nothing is extracted or embedded
/// again) and a renamed folder moves every entry under it (<see cref="IVault.MoveFolderAsync"/>). A rename whose new
/// name the folder's include/exclude patterns reject removes the entry; one whose old name was never tracked memorizes
/// the file. Folders added without it only raise the watcher's events, as before.
/// </para>
/// <para>
/// Changes are applied one at a time, in the order they were reported, on a background loop; a failure is logged and
/// does not stop the loop. A move that the vault refuses because a job for that file is still queued is retried a few
/// times before falling back to remove + memorize.
/// </para>
/// </remarks>
public sealed partial class WatchedFolderSync : IDisposable
{
    private static readonly TimeSpan[] MoveRetryDelays = [TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)];

    private readonly ConcurrentDictionary<Guid, WatchedFolder> _folders = new();
    private readonly IFileWatcherService _watcher;
    private readonly Func<CancellationToken, ValueTask<VaultLease>> _openVault;
    private readonly ILogger _logger;
    private readonly Channel<Change> _changes = Channel.CreateUnbounded<Change>(new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _loop;
    private int _disposed;

    /// <summary>Creates the set for a vault reached through <paramref name="openVault"/>.</summary>
    /// <param name="watcher">The watcher whose events are applied.</param>
    /// <param name="openVault">Opens the vault the changes apply to; the lease is disposed after each change.</param>
    /// <param name="logger">Receives a line for every applied change and every failure.</param>
    internal WatchedFolderSync(IFileWatcherService watcher, Func<CancellationToken, ValueTask<VaultLease>>? openVault, ILogger? logger = null)
    {
        _watcher = watcher ?? throw new ArgumentNullException(nameof(watcher));
        _openVault = openVault!;
        _logger = logger ?? NullLogger.Instance;

        if (openVault is null)
        {
            _loop = Task.CompletedTask; // detached: holds folders, applies nothing
            _changes.Writer.TryComplete();
            return;
        }

        _watcher.FileCreated += OnFileCreated;
        _watcher.FileModified += OnFileModified;
        _watcher.FileDeleted += OnFileDeleted;
        _watcher.FileRenamed += OnFileRenamed;
        _loop = Task.Run(() => RunAsync(_stopping.Token));
    }

    /// <summary>A set with no vault behind it: it holds folders, and changes are not applied.</summary>
    internal static WatchedFolderSync Detached(IFileWatcherService watcher) => new(watcher, openVault: null);

    /// <summary>The watched folders.</summary>
    internal ICollection<WatchedFolder> Folders => _folders.Values;

    internal bool TryGet(Guid folderId, [NotNullWhen(true)] out WatchedFolder? folder) => _folders.TryGetValue(folderId, out folder);

    internal WatchedFolder GetOrAdd(WatchedFolder folder) => _folders.GetOrAdd(folder.Id, folder);

    internal WatchedFolder? FindByPath(string fullPath) =>
        _folders.Values.FirstOrDefault(f => f.Path.Equals(fullPath, StringComparison.OrdinalIgnoreCase));

    internal bool TryRemove(Guid folderId, [NotNullWhen(true)] out WatchedFolder? folder) => _folders.TryRemove(folderId, out folder);

    /// <summary>Completes when every change reported so far has been applied (tests and orderly shutdown).</summary>
    internal async Task DrainAsync(CancellationToken ct = default)
    {
        var marker = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_changes.Writer.TryWrite(new Change(ChangeKind.Barrier, Guid.Empty, string.Empty, null, marker)))
        {
            return;
        }

        await marker.Task.WaitAsync(ct).ConfigureAwait(false);
    }

    private void OnFileCreated(object? sender, FileChangeEventArgs e) => Enqueue(ChangeKind.Upsert, e.FolderId, e.FilePath);

    private void OnFileModified(object? sender, FileChangeEventArgs e) => Enqueue(ChangeKind.Upsert, e.FolderId, e.FilePath);

    private void OnFileDeleted(object? sender, FileChangeEventArgs e) => Enqueue(ChangeKind.Delete, e.FolderId, e.FilePath);

    private void OnFileRenamed(object? sender, FileRenamedEventArgs e) => Enqueue(ChangeKind.Rename, e.FolderId, e.NewPath, e.OldPath);

    private void Enqueue(ChangeKind kind, Guid folderId, string path, string? oldPath = null)
    {
        // Only this vault's folders, and only those that asked for it.
        if (!_folders.TryGetValue(folderId, out var folder) || !folder.AutoMemorize || folder.Status != WatcherStatus.Active)
        {
            return;
        }

        _changes.Writer.TryWrite(new Change(kind, folderId, path, oldPath, null));
    }

    private async Task RunAsync(CancellationToken stopping)
    {
        try
        {
            await foreach (var change in _changes.Reader.ReadAllAsync(stopping).ConfigureAwait(false))
            {
                if (change.Kind == ChangeKind.Barrier)
                {
                    change.Done!.TrySetResult();
                    continue;
                }

                try
                {
                    await ApplyAsync(change, stopping).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException || !stopping.IsCancellationRequested)
                {
                    LogChangeFailed(_logger, ex, change.Kind.ToString(), change.Path);
                }
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
        }
    }

    private async Task ApplyAsync(Change change, CancellationToken ct)
    {
        if (!_folders.TryGetValue(change.FolderId, out var folder))
        {
            return; // removed after the change was reported
        }

        await using var lease = await _openVault(ct).ConfigureAwait(false);
        var vault = lease.Vault;

        switch (change.Kind)
        {
            case ChangeKind.Upsert:
                if (File.Exists(change.Path))
                {
                    await vault.MemorizeAsync(change.Path, ct).ConfigureAwait(false);
                    LogApplied(_logger, "memorize", change.Path);
                }

                break;

            case ChangeKind.Delete:
                if (await vault.GetAsync(change.Path, ct).ConfigureAwait(false) is not null)
                {
                    await vault.RemoveAsync(change.Path, ct).ConfigureAwait(false);
                    LogApplied(_logger, "remove", change.Path);
                }

                break;

            case ChangeKind.Rename:
                await ApplyRenameAsync(vault, folder, change.OldPath!, change.Path, ct).ConfigureAwait(false);
                break;
        }
    }

    private async Task ApplyRenameAsync(IVault vault, WatchedFolder folder, string oldPath, string newPath, CancellationToken ct)
    {
        if (Directory.Exists(newPath))
        {
            var moved = await vault.MoveFolderAsync(oldPath, newPath, ct).ConfigureAwait(false);
            LogFolderMoved(_logger, oldPath, newPath, moved.Moved.Count, moved.Errors.Count);
            return;
        }

        var tracked = await vault.GetAsync(oldPath, ct).ConfigureAwait(false) is not null;
        var wanted = folder.ShouldIncludeFile(newPath);

        if (!tracked)
        {
            if (wanted && File.Exists(newPath))
            {
                await vault.MemorizeAsync(newPath, ct).ConfigureAwait(false);
                LogApplied(_logger, "memorize", newPath);
            }

            return;
        }

        if (!wanted)
        {
            await vault.RemoveAsync(oldPath, ct).ConfigureAwait(false);
            LogApplied(_logger, "remove", oldPath);
            return;
        }

        foreach (var delay in MoveRetryDelays.Append(TimeSpan.Zero))
        {
            try
            {
                await vault.MoveAsync(oldPath, newPath, ct).ConfigureAwait(false);
                LogApplied(_logger, "move", newPath);
                return;
            }
            catch (InvalidOperationException) when (delay > TimeSpan.Zero)
            {
                // A job for the file is still queued or running — the move is refused with nothing changed. Wait it out.
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
            catch (InvalidOperationException ex)
            {
                LogMoveFellBack(_logger, ex, oldPath, newPath);
                await vault.RemoveAsync(oldPath, ct).ConfigureAwait(false);
                await vault.MemorizeAsync(newPath, ct).ConfigureAwait(false);
                return;
            }
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _watcher.FileCreated -= OnFileCreated;
        _watcher.FileModified -= OnFileModified;
        _watcher.FileDeleted -= OnFileDeleted;
        _watcher.FileRenamed -= OnFileRenamed;
        _changes.Writer.TryComplete();
        _stopping.Cancel();
        try
        {
            _loop.Wait(TimeSpan.FromSeconds(5));
        }
        catch (AggregateException)
        {
        }

        _stopping.Dispose();
    }

    private enum ChangeKind
    {
        Upsert,
        Delete,
        Rename,
        Barrier,
    }

    private sealed record Change(ChangeKind Kind, Guid FolderId, string Path, string? OldPath, TaskCompletionSource? Done);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Watched folder sync: {Action} {Path}")]
    private static partial void LogApplied(ILogger logger, string action, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Watched folder sync: folder {OldPath} -> {NewPath}, {Moved} entries moved, {Errors} failed")]
    private static partial void LogFolderMoved(ILogger logger, string oldPath, string newPath, int moved, int errors);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Watched folder sync: move {OldPath} -> {NewPath} refused; removing and memorizing instead")]
    private static partial void LogMoveFellBack(ILogger logger, Exception exception, string oldPath, string newPath);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Watched folder sync: {Kind} {Path} failed")]
    private static partial void LogChangeFailed(ILogger logger, Exception exception, string kind, string path);
}

/// <summary>A vault opened for one change, and what to release afterwards (a DI scope, or nothing).</summary>
internal readonly struct VaultLease(IVault vault, IAsyncDisposable? owner) : IAsyncDisposable
{
    public IVault Vault { get; } = vault;

    public ValueTask DisposeAsync() => owner?.DisposeAsync() ?? ValueTask.CompletedTask;
}
