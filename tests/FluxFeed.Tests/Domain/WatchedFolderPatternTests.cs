using FluentAssertions;
using FluxFeed.Domain.Entities;
using Xunit;

namespace FluxFeed.Tests.Domain;

/// <summary>
/// Contract tests for the watcher-path pattern gate (WatchedFolder.ShouldIncludeFile).
/// FileWatcherService.OnFileEvent relies on this method to drop non-matching files
/// before any queuing happens — these tests pin that contract.
/// </summary>
public class WatchedFolderPatternTests
{
    [Theory]
    [InlineData("photo.jpg", false)]      // outside include whitelist
    [InlineData("Thumbs.db", false)]      // default exclude pattern
    [InlineData("~$report.docx", false)]  // Office lock file (default exclude)
    [InlineData("doc1.pdf", true)]
    [InlineData("notes.md", true)]
    public void ShouldIncludeFile_DefaultPatterns_FiltersNonDocumentFiles(string fileName, bool expected)
    {
        var folder = WatchedFolder.Create(Path.GetTempPath());

        folder.ShouldIncludeFile(Path.Combine(Path.GetTempPath(), fileName))
            .Should().Be(expected);
    }

    [Fact]
    public void ShouldIncludeFile_ExplicitIncludePatterns_ExcludesUnlistedExtensions()
    {
        var folder = WatchedFolder.Create(
            Path.GetTempPath(),
            includePatterns: ["*.pdf", "*.docx"]);

        folder.ShouldIncludeFile("archive.zip").Should().BeFalse();
        folder.ShouldIncludeFile("drawing.dwg").Should().BeFalse();
        folder.ShouldIncludeFile("contract.pdf").Should().BeTrue();
    }

    [Fact]
    public void SetPatterns_RecompilesRegexes_NewPatternsTakeEffect()
    {
        var folder = WatchedFolder.Create(Path.GetTempPath(), includePatterns: ["*.pdf"]);
        folder.ShouldIncludeFile("notes.md").Should().BeFalse();

        folder.SetPatterns(["*.md"], excludePatterns: null);

        folder.ShouldIncludeFile("notes.md").Should().BeTrue();
        folder.ShouldIncludeFile("contract.pdf").Should().BeFalse();
    }
}
