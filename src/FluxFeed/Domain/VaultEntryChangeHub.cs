using FluxFeed.Domain.Entities;

namespace FluxFeed.Domain;

/// <summary>
/// Receives the persisted state changes of the entries under one vault directory.
/// </summary>
internal interface IVaultEntryChangeListener
{
    void OnEntryStateChanged(VaultEntryStateChange change);
}

/// <summary>
/// Carries entry state changes from where they are persisted to the vaults that report them.
/// </summary>
/// <remarks>
/// Entry state reaches disk in exactly two places — <see cref="VaultEntry.SaveMetadata"/> (every stage, sync and error
/// transition, whichever component makes it: memorize, the queue worker, recovery, enrichment) and the removal of the entry
/// directory. Publishing from those two places, rather than from each caller, is what makes the signal complete: a new
/// write path cannot forget it. Listeners are keyed by the vault directory and held weakly, so a vault that is dropped
/// without being disposed is not kept alive by its subscription.
/// </remarks>
internal static class VaultEntryChangeHub
{
    private static readonly StringComparer s_pathComparer =
        OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;

    private static readonly Lock s_gate = new();
    private static readonly Dictionary<string, List<WeakReference<IVaultEntryChangeListener>>> s_listeners = new(s_pathComparer);

    public static void Subscribe(string vaultBasePath, IVaultEntryChangeListener listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        var key = Normalize(vaultBasePath);
        lock (s_gate)
        {
            if (!s_listeners.TryGetValue(key, out var list))
                s_listeners[key] = list = [];
            list.RemoveAll(r => !r.TryGetTarget(out _));
            list.Add(new WeakReference<IVaultEntryChangeListener>(listener));
        }
    }

    public static void Publish(string vaultBasePath, VaultEntryStateChange change)
    {
        IVaultEntryChangeListener[] targets;
        lock (s_gate)
        {
            if (!s_listeners.TryGetValue(Normalize(vaultBasePath), out var list))
                return;
            list.RemoveAll(r => !r.TryGetTarget(out _));
            targets = [.. list.Select(r => r.TryGetTarget(out var t) ? t : null).OfType<IVaultEntryChangeListener>()];
        }

        foreach (var target in targets)
        {
            try
            {
                target.OnEntryStateChanged(change);
            }
            catch (Exception)
            {
                // A listener reports its own handler failures; nothing it throws may fail the write that published.
            }
        }
    }

    private static string Normalize(string vaultBasePath)
        => Path.TrimEndingDirectorySeparator(Path.GetFullPath(vaultBasePath));
}
