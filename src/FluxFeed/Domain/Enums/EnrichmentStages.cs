namespace FluxFeed.Domain.Enums;

/// <summary>
/// Optional enrichment stages an entry can still be waiting for (<see cref="Entities.VaultEntry.PendingEnrichment"/>).
/// Set when <c>FileVaultOptions.DeferEnrichment</c> left a stage for a later upgrade
/// (<c>IVault.UpgradeAsync</c>); cleared when an upgrade, memorize or refresh ran it.
/// </summary>
[Flags]
public enum EnrichmentStages
{
    /// <summary>Nothing pending.</summary>
    None = 0,

    /// <summary>Images still without a description (an image enricher is registered and they have not failed permanently).</summary>
    ImageDescriptions = 1,

    /// <summary>Text chunks still without a contextual-enrichment context (a contextual enrichment service is enabled).</summary>
    ContextualEnrichment = 2,

    /// <summary>
    /// Pages still not read by a vision model: <c>FileVaultOptions.PageReading</c> selects pages, and the extraction ran
    /// without it. An upgrade re-extracts the document with page reading and re-indexes the chunks that changed.
    /// </summary>
    PageReads = 4,

    /// <summary>
    /// LLM refinement configured through <c>FileVaultOptions.LlmRefine</c> that the extraction ran without. An upgrade
    /// re-extracts the document with it and re-indexes the chunks that changed.
    /// </summary>
    LlmRefinement = 8,
}
