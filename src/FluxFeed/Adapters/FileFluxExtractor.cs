using System.Globalization;
using FileFlux;
using FileFlux.Core;
using FluxFeed.Services;
using Microsoft.Extensions.Logging;

namespace FluxFeed.Adapters;

/// <summary>
/// FileFlux adapter for content extraction.
/// Bridges IExtractor to FileFlux's IDocumentProcessorFactory.
/// </summary>
public sealed partial class FileFluxExtractor : IExtractor
{
    private readonly IDocumentProcessorFactory _processorFactory;
    private readonly ILogger<FileFluxExtractor> _logger;

    public FileFluxExtractor(
        IDocumentProcessorFactory processorFactory,
        ILogger<FileFluxExtractor> logger)
    {
        _processorFactory = processorFactory ?? throw new ArgumentNullException(nameof(processorFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<ExtractionResult> ExtractAsync(string sourcePath, CancellationToken ct = default)
    {
        LogExtracting(_logger, sourcePath);

        try
        {
            await using var processor = _processorFactory.Create(sourcePath);

            // Process with minimal chunking to get raw extracted text
            // FileFlux requires a strategy - use Auto with large chunk size to get full content
            var options = new ProcessingOptions
            {
                Chunking = new FileFlux.Core.ChunkingOptions
                {
                    Strategy = ChunkingStrategies.Auto,
                    MaxChunkSize = int.MaxValue // Get full content as single chunk
                }
            };

            await processor.ProcessAsync(options, ct);

            var result = processor.Result;

            // Get content from chunks (FileFlux stores extracted text in chunks)
            var content = result.Chunks?.Count > 0
                ? string.Join("\n\n", result.Chunks.Select(c => c.Content))
                : string.Empty;

            // Extract images from RawContent if available
            Dictionary<string, byte[]>? images = null;
            if (result.Raw?.Images?.Count > 0)
            {
                images = [];
                foreach (var (img, idx) in result.Raw.Images.Select((img, idx) => (img, idx)))
                {
                    if (img.Data is { Length: > 0 })
                    {
                        var baseName = !string.IsNullOrEmpty(img.Id) ? img.Id : $"img_{idx:D3}";
                        var extension = GetExtensionFromContentType(img.MimeType);
                        var key = $"{baseName}{extension}";
                        images[key] = img.Data;
                    }
                }
            }

            var hints = ProjectScalarHints(result.Raw?.Hints);
            var warnings = result.Raw?.Warnings is { Count: > 0 } w ? w.ToArray() : null;

            LogExtracted(_logger, content.Length, images?.Count ?? 0, sourcePath);
            if (hints != null || warnings != null)
            {
                LogExtractionDiagnostics(_logger, hints?.Count ?? 0, warnings?.Length ?? 0, sourcePath);
            }

            return new ExtractionResult
            {
                Content = content,
                Images = images?.Count > 0 ? images : null,
                Hints = hints,
                Warnings = warnings
            };
        }
        catch (Exception ex)
        {
            LogExtractionFailed(_logger, ex, sourcePath);
            throw;
        }
    }

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Debug, Message = "Extracting content from {SourcePath}")]
    private static partial void LogExtracting(ILogger logger, string sourcePath);

    [LoggerMessage(Level = LogLevel.Information, Message = "Extracted {ContentLength} chars and {ImageCount} images from {SourcePath}")]
    private static partial void LogExtracted(ILogger logger, int contentLength, int imageCount, string sourcePath);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to extract content from {SourcePath}")]
    private static partial void LogExtractionFailed(ILogger logger, Exception exception, string sourcePath);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Extraction reported {HintCount} hints and {WarningCount} warnings for {SourcePath}")]
    private static partial void LogExtractionDiagnostics(ILogger logger, int hintCount, int warningCount, string sourcePath);

    #endregion

    /// <summary>
    /// Projects FileFlux's <c>RawContent.Hints</c> (an untyped <c>Dictionary&lt;string, object&gt;</c>)
    /// onto the persistable string map carried by <see cref="ExtractionResult.Hints"/>.
    /// <para>
    /// Keys are passed through opaquely — FluxFeed never interprets FileFlux vocabulary. Values are
    /// filtered by <b>type</b>, not by key: only scalars survive. Reader-internal structural values
    /// (e.g. <c>PageRanges</c>, a <c>Dictionary&lt;int, (int, int)&gt;</c>) would stringify to a bare
    /// type name, so they are dropped rather than persisted as noise in meta.json. A type rule keeps
    /// this free of vocabulary drift as FileFlux adds hint keys.
    /// </para>
    /// </summary>
    private static Dictionary<string, string>? ProjectScalarHints(Dictionary<string, object>? hints)
    {
        if (hints is not { Count: > 0 })
            return null;

        Dictionary<string, string>? projected = null;
        foreach (var (key, value) in hints)
        {
            if (!TryFormatScalar(value, out var formatted))
                continue;

            projected ??= [];
            projected[key] = formatted;
        }

        return projected;
    }

    private static bool TryFormatScalar(object? value, out string formatted)
    {
        formatted = value switch
        {
            string s => s,
            bool b => b ? "true" : "false",
            byte or sbyte or short or ushort or int or uint or long or ulong
                or float or double or decimal or char
                => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
            DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
            TimeSpan ts => ts.ToString("c", CultureInfo.InvariantCulture),
            Guid g => g.ToString(),
            Enum e => e.ToString(),
            _ => string.Empty
        };

        return formatted.Length > 0 || value is string;
    }

    /// <summary>
    /// Get file extension from content type.
    /// </summary>
    private static string GetExtensionFromContentType(string? contentType) => contentType?.ToLowerInvariant() switch
    {
        "image/png" => ".png",
        "image/jpeg" => ".jpg",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "image/bmp" => ".bmp",
        "image/svg+xml" => ".svg",
        "image/tiff" => ".tiff",
        "image/x-icon" or "image/vnd.microsoft.icon" => ".ico",
        _ => ".png"  // Default to PNG for unknown types
    };
}
