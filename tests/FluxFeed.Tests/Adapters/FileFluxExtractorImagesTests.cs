using AwesomeAssertions;
using FileFlux.Core;
using FluxFeed.Adapters;
using Xunit;

namespace FluxFeed.Tests.Adapters;

/// <summary>
/// FileFlux's images become vault image artifacts with every page that shows them: a picture a deck reuses on slides 1
/// and 4 keeps both, so the vault can attribute its description to each slide.
/// </summary>
public class FileFluxExtractorImagesTests
{
    [Fact]
    public void ToImageArtifacts_CarriesEveryPage_AndSkipsImagesWithoutBytes()
    {
        var artifacts = FileFluxExtractor.ToImageArtifacts(
        [
            new ImageInfo { Id = "image1.png", Data = [1, 2], MimeType = "image/png", PageNumbers = [1, 4] },
            new ImageInfo { Data = [3, 4], PageNumber = 3 },
            new ImageInfo { Id = "empty.png" },
        ]);

        artifacts.Should().NotBeNull().And.HaveCount(2);
        var reused = artifacts![0];
        reused.PageNumber.Should().Be(1);
        reused.PageNumbers.Should().Equal(1, 4);
        var single = artifacts[1];
        single.Id.Should().Be("img_001", "the position names an image the reader gave no id");
        single.PageNumbers.Should().Equal(3);
        single.ContentType.Should().Be("application/octet-stream");
    }

    [Fact]
    public void ToImageArtifacts_CarriesTheNameTheTextUses_AndLeavesItNullWhenTheReaderGivesNone()
    {
        // A Word document's Markdown refers to a picture by the media part's file name while its id is the resource id.
        var named = new ImageInfo { Id = "rId9", Data = [1], MimeType = "image/png" };
        named.Properties[FileFluxExtractor.ReferenceNameProperty] = "image1.png";
        var unnamed = new ImageInfo { Id = "img_x", Data = [2], MimeType = "image/png" };

        var artifacts = FileFluxExtractor.ToImageArtifacts([named, unnamed]);

        artifacts.Should().NotBeNull();
        artifacts![0].Id.Should().Be("rId9");
        artifacts[0].ReferenceName.Should().Be("image1.png");
        artifacts[1].ReferenceName.Should().BeNull();
    }

    [Fact]
    public void ToImageArtifacts_IsNull_WhenNoImageHasBytes() =>
        FileFluxExtractor.ToImageArtifacts([new ImageInfo { Id = "empty.png" }]).Should().BeNull();
}
