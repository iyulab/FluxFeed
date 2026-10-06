using System.Globalization;
using FileFlux;
using FileFlux.Core;
using FluxFeed.Domain.ValueObjects;
using FluxFeed.Interfaces;
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

    /// <summary>
    /// <c>FileFlux</c> at the version of the FileFlux package this process loaded — the readers that made the
    /// extraction. Build metadata after <c>+</c> is not part of it.
    /// </summary>
    public ExtractionIdentity Identity { get; } = ExtractionIdentity.FromAssembly("FileFlux", typeof(IDocumentProcessorFactory).Assembly);

    ExtractionIdentity? IExtractor.Identity => Identity;

    public async Task<ExtractionResult> ExtractAsync(string sourcePath, ExtractionSettings? settings = null, CancellationToken ct = default)
    {
        LogExtracting(_logger, sourcePath);

        try
        {
            await using var processor = _processorFactory.Create(sourcePath);

            // Read and refine only (rule-based, then LLM when a refiner is registered): the vault chunks the stored
            // text later, at memorize. The refined text is what gets stored, and its spans (pages, time ranges) index
            // exactly that text. A whole-document LLM rewrite changes the text, so its spans no longer apply and are not
            // kept; a page-scoped refinement re-expresses them over its text, so they are.
            await processor.RefineAsync(cancellationToken: ct);
            await processor.LlmRefineAsync(settings?.LlmRefine, cancellationToken: ct);

            var result = processor.Result;
            var refinedText = result.Refined?.Text ?? string.Empty;
            var llmText = result.LlmRefined?.Text;
            var useLlm = !string.IsNullOrEmpty(llmText) && llmText != refinedText;
            var content = useLlm ? llmText! : refinedText;
            var sourceSpans = useLlm ? result.LlmRefined!.Spans : result.Refined?.Spans;
            var spans = sourceSpans is { Count: > 0 }
                ? sourceSpans.Select(s => new ContentSpan(s.Start, s.End) { Page = s.Page, StartTime = s.StartTime, EndTime = s.EndTime }).ToList()
                : null;

            // Extract images from RawContent if available
            List<ImageArtifact>? images = null;
            if (result.Raw?.Images?.Count > 0)
            {
                foreach (var (img, idx) in result.Raw.Images.Select((img, idx) => (img, idx)))
                {
                    if (img.Data is not { Length: > 0 })
                        continue;

                    images ??= [];
                    images.Add(new ImageArtifact
                    {
                        Id = !string.IsNullOrEmpty(img.Id) ? img.Id : $"img_{idx:D3}",
                        Data = img.Data,
                        // No format guess here: an unrecognized MIME type must surface as
                        // "we don't know" so consumers can decide, not be silently mislabeled
                        // as a format the bytes may not actually be.
                        ContentType = img.MimeType ?? "application/octet-stream",
                        // Alt text / caption when the format carries one (HTML alt, Office alt text).
                        AltText = string.IsNullOrWhiteSpace(img.Caption) ? null : img.Caption,
                        PageNumber = img.PageNumber
                    });
                }
            }

            var tables = result.Raw?.Tables is { Count: > 0 } rawTables ? ToTableArtifacts(rawTables) : null;

            var hints = WithPageRefinement(ProjectScalarHints(result.Raw?.Hints), result.LlmRefined?.Pages);
            var warnings = result.Raw?.Warnings is { Count: > 0 } w ? w.ToArray() : null;

            LogExtracted(_logger, content.Length, images?.Count ?? 0, sourcePath);
            if (hints != null || warnings != null)
            {
                LogExtractionDiagnostics(_logger, hints?.Count ?? 0, warnings?.Length ?? 0, sourcePath);
            }

            return new ExtractionResult
            {
                Content = content,
                Images = images,
                Hints = hints,
                Warnings = warnings,
                Spans = spans,
                Tables = tables
            };
        }
        catch (Exception ex)
        {
            LogExtractionFailed(_logger, ex, sourcePath);
            throw;
        }
    }

    /// <summary>
    /// FileFlux's tables as vault table artifacts, ids from their document position. The reader's tables are the ones
    /// its text carries, in the same order.
    /// </summary>
    internal static IReadOnlyList<TableArtifact> ToTableArtifacts(IReadOnlyList<FileFlux.Core.TableData> tables) =>
        tables.Select((table, index) => new TableArtifact
        {
            Id = TableArtifact.IdFor(index),
            Index = index,
            Rows = table.Cells.Select(row => (IReadOnlyList<string>)row.ToArray()).ToList(),
            HeaderRows = table.Props.TryGetValue("header_rows", out var headerRows) && headerRows is int declared
                ? declared
                : table.HasHeader ? 1 : 0,
            PageNumber = table.PageNumber > 0 ? table.PageNumber : null,
            Section = table.Props.TryGetValue("section_name", out var section) ? section?.ToString() : null,
            Caption = string.IsNullOrWhiteSpace(table.Caption) ? null : table.Caption,
            Confidence = table.Confidence,
            DetectionMethod = table.DetectionMethod.ToString(),
            MergedCells = table.MergedCells.Select(m => new TableCellSpan(m.StartRow, m.EndRow, m.StartCol, m.EndCol)).ToList(),
        }).ToList();

    #region LoggerMessage Definitions

    /// <summary>
    /// Adds a page-scoped refinement's outcome to the extraction hints: pages per outcome
    /// (<c>llm_refine_pages_refined</c> / <c>_native</c> / <c>_rejected</c> / <c>_skipped</c>) and, when any page kept its
    /// text, <c>llm_refine_rejected</c> — <c>page:reason</c> pairs, comma separated.
    /// </summary>
    internal static IReadOnlyDictionary<string, string>? WithPageRefinement(
        IReadOnlyDictionary<string, string>? hints, IReadOnlyList<PageRefinement>? pages)
    {
        if (pages is not { Count: > 0 })
            return hints;

        var merged = hints is null ? new Dictionary<string, string>() : new Dictionary<string, string>(hints);
        foreach (var outcome in Enum.GetValues<PageRefinementOutcome>())
            merged[$"llm_refine_pages_{outcome.ToString().ToLowerInvariant()}"] =
                pages.Count(p => p.Outcome == outcome).ToString(CultureInfo.InvariantCulture);
        var rejected = pages.Where(p => p.Outcome == PageRefinementOutcome.Rejected).ToList();
        if (rejected.Count > 0)
            merged["llm_refine_rejected"] = string.Join(",", rejected.Select(p => $"{p.Page.ToString(CultureInfo.InvariantCulture)}:{p.Reason}"));
        return merged;
    }

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
}
