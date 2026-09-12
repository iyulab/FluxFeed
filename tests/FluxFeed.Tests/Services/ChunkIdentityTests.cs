using AwesomeAssertions;
using FluxFeed.Services;
using Xunit;

namespace FluxFeed.Tests.Services;

public sealed class ChunkIdentityTests
{
    private const string Doc = "0123456789abcdef";

    [Fact]
    public void ForText_IsDeterministic_AndAValidUuid()
    {
        var a = ChunkIdentity.ForText(Doc, "The client pays a retainer each month.", 0);
        var b = ChunkIdentity.ForText(Doc, "The client pays a retainer each month.", 0);

        a.Should().Be(b);
        Guid.TryParse(a, out var parsed).Should().BeTrue();
        a.Should().Be(parsed.ToString("D"), because: "the id is the canonical lowercase UUID form");
        // RFC 9562 version 8 / variant 10x, so a store that keys on uuid takes it verbatim.
        a[14].Should().Be('8');
        "89ab".Should().Contain(a[19].ToString());
    }

    [Fact]
    public void ForText_ChangesWithEveryInput()
    {
        var baseline = ChunkIdentity.ForText(Doc, "same passage", 0);

        ChunkIdentity.ForText("fedcba9876543210", "same passage", 0).Should().NotBe(baseline, "a different document");
        ChunkIdentity.ForText(Doc, "same passage.", 0).Should().NotBe(baseline, "a different passage");
        ChunkIdentity.ForText(Doc, "same passage", 1).Should().NotBe(baseline, "the second occurrence of the same passage");
    }

    [Fact]
    public void ForTexts_NumbersRepeatedPassages_AndKeepsDistinctOnesAtZero()
    {
        var ids = ChunkIdentity.ForTexts(Doc, ["Disclaimer.", "Section one.", "Disclaimer.", "Section two."]);

        ids.Should().HaveCount(4).And.OnlyHaveUniqueItems();
        ids[0].Should().Be(ChunkIdentity.ForText(Doc, "Disclaimer.", 0));
        ids[2].Should().Be(ChunkIdentity.ForText(Doc, "Disclaimer.", 1));
        ids[1].Should().Be(ChunkIdentity.ForText(Doc, "Section one.", 0));
        ids[3].Should().Be(ChunkIdentity.ForText(Doc, "Section two.", 0));
    }

    [Fact]
    public void ForImage_KeysOnTheImage_NotItsDescription()
    {
        var a = ChunkIdentity.ForImage(Doc, "img_000", 0);

        a.Should().Be(ChunkIdentity.ForImage(Doc, "img_000", 0));
        a.Should().NotBe(ChunkIdentity.ForImage(Doc, "img_001", 0));
        a.Should().NotBe(ChunkIdentity.ForImage(Doc, "img_000", 1));
        a.Should().NotBe(ChunkIdentity.ForText(Doc, "img_000", 0), "text and image identities do not collide");
    }

    [Fact]
    public void Inputs_AreValidated()
    {
        var text = () => ChunkIdentity.ForText(" ", "x", 0);
        text.Should().Throw<ArgumentException>();
        var occurrence = () => ChunkIdentity.ForText(Doc, "x", -1);
        occurrence.Should().Throw<ArgumentOutOfRangeException>();
        var image = () => ChunkIdentity.ForImage(Doc, "", 0);
        image.Should().Throw<ArgumentException>();
    }
}
