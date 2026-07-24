using FluxFeed.Domain.Entities;

namespace FluxFeed.Interfaces;

/// <summary>
/// Service for managing vault storage structure.
/// Directory structure:
/// .vault/{filepath-hash}/
/// ├── meta.json          (git 추적 X)
/// ├── extracted.md       (git 추적 X) - raw extraction output
/// ├── images/            (git 추적 X)
/// │   └── manifest.json
/// └── vault/             (git 추적 O)
///     ├── .git/
///     ├── refined.md     - LLM-refined content with image descriptions
///     ├── append-text.md
///     └── qa.md
/// </summary>
public interface IVaultStorageService
{
    /// <summary>
    /// Gets the vault base path (.vault directory).
    /// </summary>
    string BasePath { get; }

    /// <summary>
    /// Initializes the vault directory structure for an entry.
    /// Creates: entry dir, vault/ subdir, .gitignore, and initializes git in vault/.
    /// </summary>
    Task InitializeEntryAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Stores raw extracted content to extracted.md (not git-tracked).
    /// </summary>
    Task StoreExtractedContentAsync(VaultEntry entry, string content, CancellationToken ct = default);

    /// <summary>
    /// Gets raw extracted content from extracted.md.
    /// </summary>
    Task<string?> GetExtractedContentAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Stores refined content to vault/refined.md.
    /// </summary>
    Task StoreRefinedContentAsync(VaultEntry entry, string content, CancellationToken ct = default);

    /// <summary>
    /// Gets refined content from vault/refined.md.
    /// </summary>
    Task<string?> GetRefinedContentAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Stores extracted images to images/ directory.
    /// </summary>
    Task StoreImagesAsync(VaultEntry entry, IEnumerable<ImageArtifact> images, CancellationToken ct = default);

    /// <summary>
    /// Gets all images from images/ directory.
    /// </summary>
    Task<IReadOnlyList<ImageArtifact>> GetImagesAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Lists the image manifest without reading image bytes — the form used to decide which images
    /// still need description. Returns an empty list when the entry has no images.
    /// </summary>
    Task<IReadOnlyList<VaultImage>> GetImageManifestAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Persists a description for a single image in the manifest, leaving the other entries and the
    /// stored image files untouched. This is what makes enrichment idempotent across re-runs: a
    /// described image is never offered to the enricher again.
    /// </summary>
    /// <returns>False when the entry has no manifest or no image with that id.</returns>
    Task<bool> SetImageDescriptionAsync(VaultEntry entry, string imageId, string description, CancellationToken ct = default);

    /// <summary>
    /// Gets all text content from vault/ directory (refined.md + append-text.md + qa.md).
    /// </summary>
    Task<VaultTextContent> GetAllVaultContentAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Stores user-appended text to vault/append-text.md.
    /// </summary>
    Task StoreAppendTextAsync(VaultEntry entry, string content, CancellationToken ct = default);

    /// <summary>
    /// Stores QA content to vault/qa.md.
    /// </summary>
    Task StoreQaContentAsync(VaultEntry entry, string content, CancellationToken ct = default);

    /// <summary>
    /// Deletes all storage for an entry.
    /// </summary>
    Task DeleteEntryStorageAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Gets total storage size for an entry.
    /// </summary>
    Task<long> GetStorageSizeAsync(VaultEntry entry, CancellationToken ct = default);

    /// <summary>
    /// Checks if entry storage exists.
    /// </summary>
    bool EntryStorageExists(VaultEntry entry);

    /// <summary>
    /// Lists all entry directories in the vault.
    /// </summary>
    IEnumerable<string> ListEntryDirectories();

    /// <summary>
    /// Creates the .gitignore file in the entry directory to exclude meta.json and images/.
    /// </summary>
    Task CreateGitignoreAsync(VaultEntry entry, CancellationToken ct = default);
}

/// <summary>
/// Combined text content from vault/ directory.
/// </summary>
public sealed class VaultTextContent
{
    /// <summary>
    /// Content from refined.md.
    /// </summary>
    public string? RefinedContent { get; init; }

    /// <summary>
    /// Content from append-text.md.
    /// </summary>
    public string? AppendText { get; init; }

    /// <summary>
    /// Content from qa.md.
    /// </summary>
    public string? QaContent { get; init; }

    /// <summary>
    /// Gets combined content for chunking/indexing.
    /// </summary>
    public string GetCombinedContent()
    {
        var parts = new List<string>();

        if (!string.IsNullOrWhiteSpace(RefinedContent))
            parts.Add(RefinedContent);

        if (!string.IsNullOrWhiteSpace(AppendText))
            parts.Add($"\n\n---\n\n## Additional Notes\n\n{AppendText}");

        if (!string.IsNullOrWhiteSpace(QaContent))
            parts.Add($"\n\n---\n\n## Q&A\n\n{QaContent}");

        return string.Join("", parts);
    }

    /// <summary>
    /// Checks if any content exists.
    /// </summary>
    public bool HasContent => !string.IsNullOrWhiteSpace(RefinedContent)
                              || !string.IsNullOrWhiteSpace(AppendText)
                              || !string.IsNullOrWhiteSpace(QaContent);
}

/// <summary>
/// Represents an extracted image.
/// </summary>
public sealed class ImageArtifact
{
    public string Id { get; init; } = string.Empty;
    public byte[] Data { get; init; } = [];
    public string ContentType { get; init; } = "application/octet-stream";

    /// <summary>
    /// Description produced by an <see cref="IVaultImageEnricher"/>, once one has succeeded for
    /// this image. Null while the image is still pending description (or when no enricher is
    /// registered).
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Alt text or caption carried by the source document, when the reader reports one.
    /// </summary>
    public string? AltText { get; init; }

    public int Width { get; init; }
    public int Height { get; init; }
}

/// <summary>
/// A stored image as recorded in the vault's image manifest, without its bytes — the listing form
/// used to decide which images still need description.
/// </summary>
public sealed class VaultImage
{
    /// <summary>Stable image identifier within the document (e.g. <c>img_000</c>).</summary>
    public required string Id { get; init; }

    /// <summary>Absolute path to the stored image file.</summary>
    public required string FilePath { get; init; }

    /// <summary>File name of the stored image, relative to the entry's images directory.</summary>
    public required string FileName { get; init; }

    public required string ContentType { get; init; }

    /// <summary>Description produced by an enricher, or null while still pending.</summary>
    public string? Description { get; init; }

    /// <summary>Alt text carried by the source document, when the reader reported one.</summary>
    public string? AltText { get; init; }

    /// <summary>True once a description has been persisted for this image.</summary>
    public bool IsDescribed => !string.IsNullOrWhiteSpace(Description);
}
