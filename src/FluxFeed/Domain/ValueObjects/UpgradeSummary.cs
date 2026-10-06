using FluxFeed.Domain.Enums;

namespace FluxFeed.Domain.ValueObjects;

/// <summary>
/// What an upgrade (<c>IVaultPipeline.UpgradeAsync</c>) did to one entry.
/// </summary>
/// <param name="ChunkCount">Chunks the entry has now.</param>
/// <param name="ReEmbedded">Chunks whose text changed, embedded and written again.</param>
/// <param name="Kept">Chunks identical to their stored row, left as they were (not embedded again).</param>
/// <param name="StillPending">Stages still pending after the upgrade (an image whose description failed, not yet permanently).</param>
/// <param name="CommitHash">The vault commit, or null when the commit was skipped.</param>
public sealed record UpgradeSummary(int ChunkCount, int ReEmbedded, int Kept, EnrichmentStages StillPending, string? CommitHash);
