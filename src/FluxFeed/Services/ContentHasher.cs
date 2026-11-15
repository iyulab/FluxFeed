using System.Security.Cryptography;
using System.Text;
using FluxFeed.Domain.ValueObjects;
using FluxFeed.Interfaces;

namespace FluxFeed.Services;

/// <summary>
/// SHA256-based content hasher for file change detection.
/// </summary>
public sealed class ContentHasher : IContentHasher
{
    /// <inheritdoc />
    public async Task<ContentHash> ComputeHashAsync(string filePath, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(filePath);

        if (!File.Exists(filePath))
            throw new FileNotFoundException("File not found", filePath);

        // Shared for read, write and delete: a document is most often hashed while the program that edits it still has
        // it open for writing, and denying writers is what makes Windows refuse that file.
        await using var stream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            bufferSize: 81920,
            useAsync: true);

        return await ComputeHashAsync(stream, ct);
    }

    /// <inheritdoc />
    public async Task<ContentHash> ComputeHashAsync(Stream stream, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var hashBytes = await SHA256.HashDataAsync(stream, ct);
        return ContentHash.FromBytes(hashBytes);
    }

    /// <inheritdoc />
    public ContentHash ComputeHash(byte[] data)
    {
        ArgumentNullException.ThrowIfNull(data);

        var hashBytes = SHA256.HashData(data);
        return ContentHash.FromBytes(hashBytes);
    }

    /// <inheritdoc />
    public ContentHash ComputeHash(string content)
    {
        ArgumentNullException.ThrowIfNull(content);

        var contentBytes = Encoding.UTF8.GetBytes(content);
        return ComputeHash(contentBytes);
    }
}
