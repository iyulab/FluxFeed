using FileFlux;
using FileFlux.Core;
using FluxFeed.Interfaces;
using FluxFeed.Services;
using Microsoft.Extensions.Logging;
using FileFluxChunkingOptions = FileFlux.Core.ChunkingOptions;
using VaultChunkingOptions = FluxFeed.Services.ChunkingOptions;

namespace FluxFeed.Adapters;

/// <summary>
/// FileFlux adapter for content chunking.
/// Bridges IChunker to FileFlux's chunking capabilities. The text goes to FileFlux as already-read content
/// (<see cref="IDocumentProcessorFactory.Create(RawContent)"/>) together with its source spans, so each chunk comes back
/// with the pages or time range it covers.
/// </summary>
public sealed partial class FileFluxChunker : IChunker
{
    private readonly IDocumentProcessorFactory _processorFactory;
    private readonly ILogger<FileFluxChunker> _logger;

    public FileFluxChunker(
        IDocumentProcessorFactory processorFactory,
        ILogger<FileFluxChunker> logger)
    {
        _processorFactory = processorFactory ?? throw new ArgumentNullException(nameof(processorFactory));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<IReadOnlyList<ContentChunk>> ChunkAsync(
        string content,
        IReadOnlyList<ContentSpan>? spans,
        VaultChunkingOptions options,
        CancellationToken ct = default)
    {
        LogChunking(_logger, content.Length, options.Strategy, options.MaxChunkSize);

        try
        {
            var raw = new RawContent
            {
                Text = content,
                Spans = spans is { Count: > 0 }
                    ? spans.Select(s => new SourceSpan(s.Start, s.End) { Page = s.Page, StartTime = s.StartTime, EndTime = s.EndTime }).ToList()
                    : [],
                // Stored vault content is markdown (extracted.md / refined.md).
                File = new SourceFileInfo { Name = "content.md", Extension = ".md", Size = content.Length },
            };

            await using var processor = _processorFactory.Create(raw);

            var chunkingOptions = new FileFluxChunkingOptions
            {
                Strategy = MapStrategy(options.Strategy),
                MaxChunkSize = options.MaxChunkSize,
                OverlapSize = options.OverlapSize
            };

            // Apply language if specified via CustomProperties
            if (!string.IsNullOrEmpty(options.Language))
            {
                chunkingOptions.CustomProperties["language"] = options.Language;
            }

            // The stored text was already refined (LLM refinement included) when it was extracted. A second LLM pass
            // here would pay again and, by rewriting the text, drop the source spans.
            var processingOptions = new ProcessingOptions
            {
                Chunking = chunkingOptions,
                IncludeLlmRefine = false
            };

            await processor.ProcessAsync(processingOptions, ct);

            var chunks = (processor.Result.Chunks ?? [])
                .Where(c => !string.IsNullOrWhiteSpace(c.Content))
                .Select(c => new ContentChunk(c.Content) { Location = ToLocation(c.Location) })
                .ToList();

            LogCreatedChunks(_logger, chunks.Count, content.Length);

            return chunks;
        }
        catch (Exception ex)
        {
            LogChunkingFailed(_logger, ex);
            throw;
        }
    }

    private static ContentLocation? ToLocation(SourceLocation? location)
    {
        if (location is null)
            return null;

        var result = new ContentLocation
        {
            StartPage = location.StartPage,
            EndPage = location.EndPage,
            StartTime = location.StartTime,
            EndTime = location.EndTime,
        };
        return result.IsEmpty ? null : result;
    }

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Debug, Message = "Chunking content: {ContentLength} chars, Strategy={Strategy}, MaxSize={MaxSize}")]
    private static partial void LogChunking(ILogger logger, int contentLength, string strategy, int maxSize);

    [LoggerMessage(Level = LogLevel.Information, Message = "Created {ChunkCount} chunks from {ContentLength} chars")]
    private static partial void LogCreatedChunks(ILogger logger, int chunkCount, int contentLength);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to chunk content")]
    private static partial void LogChunkingFailed(ILogger logger, Exception exception);

    #endregion

    private static string MapStrategy(string vaultStrategy) => vaultStrategy.ToLowerInvariant() switch
    {
        "auto" => ChunkingStrategies.Auto,
        "semantic" => ChunkingStrategies.Semantic,
        "paragraph" => ChunkingStrategies.Paragraph,
        "sentence" => ChunkingStrategies.Sentence,
        "token" => ChunkingStrategies.Token,
        "hierarchical" => ChunkingStrategies.Hierarchical,
        // Legacy aliases
        "intelligent" => ChunkingStrategies.Auto,
        "smart" => ChunkingStrategies.Semantic,
        "fixed" => ChunkingStrategies.Token,
        _ => ChunkingStrategies.Auto
    };
}
