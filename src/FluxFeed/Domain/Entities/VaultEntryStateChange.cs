using FluxFeed.Domain.Enums;

namespace FluxFeed.Domain.Entities;

/// <summary>
/// The part of a <see cref="VaultEntry"/> a status display follows: where the entry is in the pipeline, whether it is in sync
/// with its source, what enrichment it still waits for, how many chunks are indexed and the last error.
/// </summary>
/// <param name="Stage">Processing stage (<see cref="VaultEntry.Stage"/>).</param>
/// <param name="SyncStatus">Consistency with the source and the index (<see cref="VaultEntry.SyncStatus"/>).</param>
/// <param name="PendingEnrichment">Enrichment still to run (<see cref="VaultEntry.PendingEnrichment"/>).</param>
/// <param name="ChunkCount">Chunks in the index (<see cref="VaultEntry.ChunkCount"/>).</param>
/// <param name="LastError">The latest failure, if any (<see cref="VaultEntry.LastError"/>).</param>
public readonly record struct VaultEntryState(
    ProcessingStage Stage,
    SyncStatus SyncStatus,
    EnrichmentStages PendingEnrichment,
    int ChunkCount,
    string? LastError)
{
    /// <summary>The state of <paramref name="entry"/> as it stands.</summary>
    public static VaultEntryState Of(VaultEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return new(entry.Stage, entry.SyncStatus, entry.PendingEnrichment, entry.ChunkCount, entry.LastError);
    }
}

/// <summary>
/// One persisted change of a vault entry's <see cref="VaultEntryState"/>, raised by <c>IVault.EntryStateChanged</c>.
/// </summary>
/// <param name="FilepathHash">The entry's key (its directory under the vault).</param>
/// <param name="SourcePath">The source file the entry tracks.</param>
/// <param name="Previous">
/// The state the writer had read before this change; <c>null</c> for an entry written for the first time. When several
/// writers overlap, this is what the writing one last read, not necessarily the previous event's <see cref="Current"/>.
/// </param>
/// <param name="Current">The state now on disk; <c>null</c> when the entry was removed.</param>
/// <param name="PreviousFilepathHash">
/// Set when the entry moved to another key (<c>IVault.MoveAsync</c>): the key it had before. Nothing more is raised for
/// that key.
/// </param>
public sealed record VaultEntryStateChange(
    string FilepathHash,
    string SourcePath,
    VaultEntryState? Previous,
    VaultEntryState? Current,
    string? PreviousFilepathHash = null)
{
    /// <summary>True when the entry was removed from the vault (<see cref="Current"/> is <c>null</c>).</summary>
    public bool IsRemoval => Current is null;
}
