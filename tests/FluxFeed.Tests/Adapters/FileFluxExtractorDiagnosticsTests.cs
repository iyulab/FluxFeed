using FileFlux;
using FileFlux.Core;
using FluentAssertions;
using FluxFeed.Adapters;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace FluxFeed.Tests.Adapters;

/// <summary>
/// Pins the extraction-diagnostics contract at the FileFlux adapter boundary: FileFlux's structured
/// diagnostics (<c>RawContent.Hints</c> / <c>Warnings</c>) must reach the vault pipeline instead of
/// being dropped, so a legitimate 0-chunk outcome (scanned/blank document) stays explainable.
/// </summary>
public class FileFluxExtractorDiagnosticsTests
{
    private static FileFluxExtractor CreateExtractor(RawContent raw, IReadOnlyList<DocumentChunk>? chunks = null)
    {
        var processor = Substitute.For<IDocumentProcessor>();
        var result = new ProcessingResult { Raw = raw, Chunks = chunks };
        processor.Result.Returns(result);

        var factory = Substitute.For<IDocumentProcessorFactory>();
        factory.Create(Arg.Any<string>()).Returns(processor);

        return new FileFluxExtractor(factory, NullLogger<FileFluxExtractor>.Instance);
    }

    [Fact]
    public async Task ExtractAsync_ImageOnlyPdf_PropagatesFailureReasonAndWarning()
    {
        // Arrange — what FileFlux 0.14.0 reports for an image-only/scanned PDF: no text, but a
        // structured reason rather than an exception.
        var raw = new RawContent
        {
            Text = string.Empty,
            Hints = { ["extraction_failure_reason"] = "no_text_layer", ["resource_count"] = 3 },
            Warnings = { "PDF appears to be an image-only/scanned document; OCR is required." }
        };
        var extractor = CreateExtractor(raw);

        // Act
        var result = await extractor.ExtractAsync("scan.pdf");

        // Assert
        result.Content.Should().BeEmpty();
        result.Hints.Should().NotBeNull();
        result.Hints!["extraction_failure_reason"].Should().Be("no_text_layer");
        result.Hints["resource_count"].Should().Be("3");
        result.Warnings.Should().ContainSingle()
            .Which.Should().Contain("OCR is required");
    }

    [Fact]
    public async Task ExtractAsync_BlankDocument_PropagatesBlankPageReason()
    {
        var raw = new RawContent
        {
            Text = string.Empty,
            Hints = { ["extraction_failure_reason"] = "blank_page" },
            Warnings = { "every page is blank" }
        };
        var extractor = CreateExtractor(raw);

        var result = await extractor.ExtractAsync("blank.pdf");

        result.Hints!["extraction_failure_reason"].Should().Be("blank_page");
    }

    [Fact]
    public async Task ExtractAsync_NoDiagnostics_LeavesHintsAndWarningsNull()
    {
        var raw = new RawContent { Text = "hello" };
        var extractor = CreateExtractor(raw, [new DocumentChunk { Content = "hello" }]);

        var result = await extractor.ExtractAsync("plain.txt");

        result.Content.Should().Be("hello");
        result.Hints.Should().BeNull();
        result.Warnings.Should().BeNull();
    }

    [Fact]
    public async Task ExtractAsync_ScalarHints_AreFormattedInvariantly()
    {
        var raw = new RawContent
        {
            Text = "x",
            Hints =
            {
                ["page_count"] = 12,
                ["HasTables"] = true,
                ["confidence"] = 0.75,
                ["document_title"] = "견적서"
            }
        };
        var extractor = CreateExtractor(raw, [new DocumentChunk { Content = "x" }]);

        var result = await extractor.ExtractAsync("doc.pdf");

        result.Hints.Should().BeEquivalentTo(new Dictionary<string, string>
        {
            ["page_count"] = "12",
            ["HasTables"] = "true",
            ["confidence"] = "0.75",
            ["document_title"] = "견적서"
        });
    }

    [Fact]
    public async Task ExtractAsync_ProjectsRawImagesWithIdentityAndAltText()
    {
        // The one seam where real FileFlux image data enters the vault: identity, mime type and
        // alt text must survive, and an image the reader left unnamed must still get a stable id.
        var raw = new RawContent
        {
            Text = "body",
            Images =
            {
                new ImageInfo
                {
                    Id = "chart_1",
                    Caption = "quarterly sales",
                    MimeType = "image/jpeg",
                    Data = [1, 2, 3]
                },
                new ImageInfo { Id = string.Empty, Data = [4, 5] }
            }
        };
        var extractor = CreateExtractor(raw, [new DocumentChunk { Content = "body" }]);

        var result = await extractor.ExtractAsync("report.docx");

        result.Images.Should().HaveCount(2);

        var first = result.Images![0];
        first.Id.Should().Be("chart_1");
        first.ContentType.Should().Be("image/jpeg");
        first.AltText.Should().Be("quarterly sales");
        first.Data.Should().Equal([1, 2, 3]);

        var second = result.Images[1];
        second.Id.Should().Be("img_001", "an unnamed image still needs a stable id, indexed by position");
        second.ContentType.Should().Be("image/png", "unknown mime type falls back to png");
        second.AltText.Should().BeNull("no caption means no alt text, not an empty one");
    }

    [Fact]
    public async Task ExtractAsync_SkipsImagesWithoutData()
    {
        // A reader can report an image it could not materialise (external URL, unsupported codec).
        // Storing a zero-byte artifact would give an enricher nothing to look at.
        var raw = new RawContent
        {
            Text = "body",
            Images =
            {
                new ImageInfo { Id = "external", Data = null, SourceUrl = "https://example.com/a.png" },
                new ImageInfo { Id = "real", Data = [7] }
            }
        };
        var extractor = CreateExtractor(raw, [new DocumentChunk { Content = "body" }]);

        var result = await extractor.ExtractAsync("page.html");

        result.Images.Should().ContainSingle().Which.Id.Should().Be("real");
    }

    [Fact]
    public async Task ExtractAsync_NoImages_LeavesImagesNull()
    {
        var extractor = CreateExtractor(new RawContent { Text = "body" }, [new DocumentChunk { Content = "body" }]);

        (await extractor.ExtractAsync("plain.txt")).Images.Should().BeNull();
    }

    [Fact]
    public async Task ExtractAsync_NonScalarHints_AreDropped()
    {
        // PageRanges is reader-internal structure; stringifying it would persist a bare type name
        // into every meta.json. The filter is by value type, not by key, so it never drifts as
        // FileFlux adds hint keys.
        var raw = new RawContent
        {
            Text = "x",
            Hints =
            {
                ["PageRanges"] = new Dictionary<int, (int Start, int End)> { [1] = (0, 10) },
                ["page_count"] = 1
            }
        };
        var extractor = CreateExtractor(raw, [new DocumentChunk { Content = "x" }]);

        var result = await extractor.ExtractAsync("doc.pdf");

        result.Hints.Should().ContainKey("page_count");
        result.Hints.Should().NotContainKey("PageRanges");
    }
}
