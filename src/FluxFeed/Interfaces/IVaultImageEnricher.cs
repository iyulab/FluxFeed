namespace FluxFeed.Interfaces;

/// <summary>
/// Consumer-supplied describer for images extracted from a document.
/// <para>
/// This is the one piece a consuming application owns — <b>which</b> vision model, prompt, language
/// and cost policy produce the description. Everything around it belongs to the vault pipeline:
/// when the enricher is called, which images are still pending, idempotence across re-runs, retry
/// of the images that failed, how descriptions reach the index, and how a chunk declares that it
/// came from an image.
/// </para>
/// <para>
/// Registering an implementation is optional. Without one the pipeline behaves exactly as before —
/// images are still extracted and stored, they are simply not described.
/// </para>
/// </summary>
public interface IVaultImageEnricher
{
    /// <summary>
    /// Produces a description of the requested image, suitable for indexing and retrieval.
    /// </summary>
    /// <returns>
    /// The description, or <c>null</c> to report "not this time". A null return is treated as a
    /// retryable failure: the image stays pending and is offered again on the next memorize or
    /// refresh, while the images that did succeed are persisted and never re-described. Throwing
    /// has the same effect for this image and does not abort the other images or the memorize.
    /// </returns>
    Task<string?> DescribeAsync(VaultImageDescriptionRequest request, CancellationToken ct = default);
}

/// <summary>
/// An image to describe, together with the document context the pipeline can supply for the prompt.
/// </summary>
public sealed class VaultImageDescriptionRequest
{
    /// <summary>The stored image itself.</summary>
    public required VaultImage Image { get; init; }

    /// <summary>
    /// Path of the source document this image was extracted from — useful as prompt context
    /// ("this figure comes from a quotation sheet").
    /// </summary>
    public required string SourcePath { get; init; }

    /// <summary>
    /// Full extracted text of the source document, so the enricher can build surrounding context
    /// for the prompt. Null when the document has no text layer (a scanned document — the very case
    /// where the image <em>is</em> the content).
    /// <para>
    /// Character-offset anchoring of an image inside this text is deliberately not offered: only the
    /// HTML reader reports a real position while the Office readers report 0, so an offset would
    /// silently mis-anchor for most formats.
    /// </para>
    /// </summary>
    public string? DocumentText { get; init; }
}
