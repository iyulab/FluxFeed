namespace FluxFeed.Options;

/// <summary>
/// When a refresh or a sync re-extracts an entry whose source is unchanged because a newer extractor would produce
/// something else (<see cref="FileVaultOptions.Reextraction"/>). Re-extraction re-runs OCR and image
/// description, so it is opt-in.
/// </summary>
public enum ReextractionPolicy
{
    /// <summary>Never: an unchanged source keeps its extraction. Re-extract one file deliberately with <c>MemorizeAsync</c>.</summary>
    Never = 0,

    /// <summary>
    /// When the extractor's major.minor version is newer than the one that made the entry's extraction (or the entry
    /// predates recorded identities, or FluxFeed's pipeline revision moved). Patch releases do not trigger it.
    /// </summary>
    WhenExtractorMinorChanges = 1,

    /// <summary>When the extractor's version differs at all — patch releases included.</summary>
    WhenExtractorChanges = 2,
}
