using System.Security.Cryptography;
using System.Text;

namespace FluxFeed.Services;

/// <summary>
/// Derives the id a chunk is indexed under from what the chunk <em>is</em> — the document it came
/// from and the passage it carries — so that memorizing the same document twice writes the same ids
/// twice, and only the passages that actually changed get new ones.
/// </summary>
/// <remarks>
/// <para>
/// Ids are UUIDs built from a SHA-256 of <c>filepathHash | passage identity</c>, tagged as RFC 9562
/// version 8 (the classic name-based version 5 is fixed to SHA-1, which static analysis rightly
/// rejects). A UUID rather than a bare hash so that stores keyed on <c>uuid</c> (PostgreSQL, Qdrant)
/// use the id verbatim instead of deriving a second one from it.
/// </para>
/// <para>
/// A text chunk's identity is its <b>raw chunker output</b>, before contextual enrichment prepends an
/// LLM-written summary and before RAG security sanitizes it. Both transform the content under a fixed
/// id: an enrichment that phrases its summary differently on the next run is a new wording of the same
/// passage, not a new passage. Two identical passages in one document (a repeated disclaimer, a
/// heading that recurs) are told apart by their occurrence ordinal, so neither can shadow the other.
/// </para>
/// <para>
/// An image-description chunk's identity is the image, not the description: a re-captioned image is
/// the same image with new content, so its rows are updated rather than duplicated.
/// </para>
/// <para>
/// <b>Never change the namespace or the layout below.</b> Stability across processes and releases is
/// the whole point — it is what turns a re-memorize into an update and lets a downstream index skip
/// work it already did for an unchanged chunk.
/// </para>
/// </remarks>
public static class ChunkIdentity
{
    private static readonly Guid Namespace = new("7c1d0a2e-5b3f-4e8a-9d61-2f4c8b7a3e15");

    /// <summary>
    /// Id for the <paramref name="occurrence"/>-th (0-based) chunk with exactly this raw content in the
    /// document identified by <paramref name="filepathHash"/>.
    /// </summary>
    public static string ForText(string filepathHash, string content, int occurrence)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filepathHash);
        ArgumentNullException.ThrowIfNull(content);
        ArgumentOutOfRangeException.ThrowIfNegative(occurrence);

        var contentHash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(content))).ToLowerInvariant();
        return Derive($"text|{filepathHash}|{contentHash}|{occurrence}");
    }

    /// <summary>
    /// Id for the <paramref name="part"/>-th (0-based) description chunk of image <paramref name="imageId"/>
    /// in the document identified by <paramref name="filepathHash"/>.
    /// </summary>
    public static string ForImage(string filepathHash, string imageId, int part)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filepathHash);
        ArgumentException.ThrowIfNullOrWhiteSpace(imageId);
        ArgumentOutOfRangeException.ThrowIfNegative(part);

        return Derive($"image|{filepathHash}|{imageId}|{part}");
    }

    /// <summary>
    /// Ids for a document's text chunks in order, assigning occurrence ordinals to repeated content.
    /// </summary>
    public static IReadOnlyList<string> ForTexts(string filepathHash, IReadOnlyList<string> contents)
    {
        ArgumentNullException.ThrowIfNull(contents);

        var ids = new List<string>(contents.Count);
        var occurrences = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var content in contents)
        {
            occurrences.TryGetValue(content, out var seen);
            occurrences[content] = seen + 1;
            ids.Add(ForText(filepathHash, content, seen));
        }

        return ids;
    }

    private static string Derive(string name)
    {
        // Guid.ToByteArray emits the first three fields little-endian; both ends swap so the hashed
        // form and the produced value are in the RFC's byte order (same construction FluxIndex uses
        // for its storage ids, kept separate because FluxFeed owns this scheme).
        var namespaceBytes = Namespace.ToByteArray();
        SwapToBigEndian(namespaceBytes);
        var nameBytes = Encoding.UTF8.GetBytes(name);
        var input = new byte[namespaceBytes.Length + nameBytes.Length];
        namespaceBytes.CopyTo(input, 0);
        nameBytes.CopyTo(input, namespaceBytes.Length);

        var hash = SHA256.HashData(input);
        var result = new byte[16];
        Array.Copy(hash, result, 16);
        result[6] = (byte)((result[6] & 0x0F) | 0x80); // version 8
        result[8] = (byte)((result[8] & 0x3F) | 0x80); // RFC 9562 variant
        SwapToBigEndian(result);
        return new Guid(result).ToString("D");
    }

    private static void SwapToBigEndian(byte[] guidBytes)
    {
        (guidBytes[0], guidBytes[3]) = (guidBytes[3], guidBytes[0]);
        (guidBytes[1], guidBytes[2]) = (guidBytes[2], guidBytes[1]);
        (guidBytes[4], guidBytes[5]) = (guidBytes[5], guidBytes[4]);
        (guidBytes[6], guidBytes[7]) = (guidBytes[7], guidBytes[6]);
    }
}
