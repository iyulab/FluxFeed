using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.ValueObjects;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// An entry records which extractor made its extraction, and under <see cref="FileVaultOptions.Reextraction"/> a refresh
/// of an entry an older extractor made re-extracts it — so an extractor upgrade reaches documents whose source never
/// changes. Off by default; never over uncommitted vault edits.
/// </summary>
public sealed class VaultPipelineReextractionTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;
    private readonly IGitService _git;
    private readonly List<DocumentChunk> _chunks = [];
    private readonly IVectorStore _vectorStore;

    public VaultPipelineReextractionTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"VaultReextract_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_vaultDir);

        _git = Substitute.For<IGitService>();
        _git.InitAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("commit");
        GivenVaultEdits();

        _vectorStore = Substitute.For<IVectorStore>();
        _vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var batch = ((IEnumerable<DocumentChunk>)call[0]).ToList();
                _chunks.AddRange(batch);
                return Task.FromResult<IEnumerable<string>>(batch.Select(c => c.Id.ToString()).ToList());
            });
        _vectorStore.DeleteByDocumentIdAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                _chunks.RemoveAll(c => c.DocumentId == (string)call[0]);
                return Task.FromResult(true);
            });

        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            _git,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, recursive: true); } catch (IOException) { }
        }
    }

    private void GivenVaultEdits(params string[] files) =>
        _git.StatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new GitStatus { ModifiedFiles = [.. files] });

    private sealed class VersionedExtractor(string version, ExtractionResult result) : IExtractor
    {
        public int Calls { get; private set; }

        public ExtractionIdentity? Identity { get; } = new("FileFlux", version);

        public Task<ExtractionResult> ExtractAsync(string sourcePath, ExtractionSettings? settings = null, CancellationToken ct = default)
        {
            Calls++;
            return Task.FromResult(result);
        }
    }

    private static IEmbeddingService Embedder()
    {
        var embedder = Substitute.For<IEmbeddingService>();
        embedder.GenerateEmbeddingAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new[] { 0.1f, 0.2f, 0.3f }));
        embedder.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(call => Task.FromResult(((IEnumerable<string>)call[0]).Select(_ => new[] { 0.1f, 0.2f, 0.3f })));
        embedder.GetIdentity().Returns(new EmbeddingIdentity { Provider = "Test", Model = "test-embed", Dimension = 3 });
        return embedder;
    }

    private VaultPipeline Pipeline(IExtractor extractor, ReextractionPolicy policy = ReextractionPolicy.Never) => new(
        _git,
        new ContentHasher(),
        _storage,
        NullLogger<VaultPipeline>.Instance,
        options: MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir, Reextraction = policy }),
        extractor: extractor,
        vectorStore: _vectorStore,
        embeddingService: Embedder());

    private VaultEntry Entry(string name)
    {
        var path = Path.Combine(_testDir, name);
        File.WriteAllText(path, "source bytes");
        return VaultEntry.Create(path, _vaultDir);
    }

    private static ImageArtifact Image(string id, int? page = null) =>
        new() { Id = id, Data = [1, 2, 3, 4], ContentType = "image/png", PageNumber = page };

    private static ExtractionResult Old => new() { Content = "Old extraction body.", Images = [Image("page3_Im1")] };

    private static ExtractionResult New => new() { Content = "New extraction body.", Images = [Image("page3_Im1", page: 3)] };

    private static Task<string> RefinedAsync(VaultEntry entry) => File.ReadAllTextAsync(entry.RefinedMdPath, TestContext.Current.CancellationToken);

    [Fact]
    public async Task Memorize_RecordsTheExtractor_WithThePipelineRevision()
    {
        var entry = Entry("manual.pdf");

        await Pipeline(new VersionedExtractor("0.36.2", Old)).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);

        entry.ExtractedBy.Should().Be(new ExtractionIdentity("FileFlux", "0.36.2") { PipelineRevision = VaultPipeline.ExtractionPipelineRevision });
        VaultEntry.Load(entry.EntryPath, _vaultDir)!.ExtractedBy.Should().Be(entry.ExtractedBy, "it survives the metadata round trip");
    }

    [Fact]
    public async Task Refresh_UnderTheDefaultPolicy_KeepsTheOldExtraction()
    {
        var entry = Entry("manual.pdf");
        await Pipeline(new VersionedExtractor("0.35.0", Old)).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        var newer = new VersionedExtractor("0.36.0", New);

        await Pipeline(newer).RefreshAsync(entry, ct: TestContext.Current.CancellationToken);

        newer.Calls.Should().Be(0);
        entry.ExtractedBy!.Version.Should().Be("0.35.0");
        (await RefinedAsync(entry)).Should().Contain("Old extraction");
    }

    [Fact]
    public async Task Refresh_OfAnEntryAnOlderMinorMade_ReextractsIt_AndTheUpgradeReachesTheIndex()
    {
        // The consumer-visible outcome: an image that had no page gains it, and the text is the new extractor's.
        var entry = Entry("manual.pdf");
        await Pipeline(new VersionedExtractor("0.35.0", Old)).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        var newer = new VersionedExtractor("0.36.0", New);

        var result = await Pipeline(newer, ReextractionPolicy.WhenExtractorMinorChanges)
            .RefreshAsync(entry, ct: TestContext.Current.CancellationToken);

        result.Success.Should().BeTrue();
        newer.Calls.Should().Be(1);
        entry.ExtractedBy!.Version.Should().Be("0.36.0");
        (await RefinedAsync(entry)).Should().Contain("New extraction");
        (await _storage.GetImageManifestAsync(entry, TestContext.Current.CancellationToken)).Single().PageNumber.Should().Be(3);
        _chunks.Should().Contain(c => c.Content.Contains("New extraction"));
        await _git.Received().CommitAsync(entry.VaultPath, Arg.Is<string>(m => m.StartsWith("re-extract: FileFlux 0.36.0")), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Refresh_AfterAPatchRelease_DoesNotReextract_UnderTheMinorPolicy()
    {
        var entry = Entry("manual.pdf");
        await Pipeline(new VersionedExtractor("0.36.1", Old)).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        var patched = new VersionedExtractor("0.36.2", New);

        await Pipeline(patched, ReextractionPolicy.WhenExtractorMinorChanges).RefreshAsync(entry, ct: TestContext.Current.CancellationToken);
        await Pipeline(patched, ReextractionPolicy.WhenExtractorChanges).RefreshAsync(entry, ct: TestContext.Current.CancellationToken);

        patched.Calls.Should().Be(1, "only the any-change policy re-extracts on a patch release");
        entry.ExtractedBy!.Version.Should().Be("0.36.2");
    }

    [Fact]
    public async Task Refresh_OfAnEntryWithNoRecordedExtractor_Reextracts()
    {
        // Every entry extracted before identities were recorded is the case the request was about.
        var entry = Entry("manual.pdf");
        await Pipeline(new StubExtractorWithoutIdentity(Old)).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        entry.ExtractedBy.Should().BeNull();
        var current = new VersionedExtractor("0.36.2", New);

        await Pipeline(current, ReextractionPolicy.WhenExtractorMinorChanges).RefreshAsync(entry, ct: TestContext.Current.CancellationToken);

        current.Calls.Should().Be(1);
        entry.ExtractedBy.Should().NotBeNull();
    }

    [Fact]
    public async Task Refresh_OverUncommittedVaultEdits_RefreshesTheEdits_AndKeepsThem()
    {
        var entry = Entry("manual.pdf");
        await Pipeline(new VersionedExtractor("0.35.0", Old)).MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(entry.RefinedMdPath, "Edited by hand.", TestContext.Current.CancellationToken);
        GivenVaultEdits("refined.md");
        var newer = new VersionedExtractor("0.36.0", New);

        var result = await Pipeline(newer, ReextractionPolicy.WhenExtractorChanges).RefreshAsync(entry, ct: TestContext.Current.CancellationToken);

        result.Success.Should().BeTrue();
        newer.Calls.Should().Be(0);
        (await RefinedAsync(entry)).Should().Be("Edited by hand.");
        _chunks.Should().Contain(c => c.Content.Contains("Edited by hand"));
    }

    [Fact]
    public async Task Reextraction_ThatFindsFewerImages_DropsTheOthers_FromTheManifestAndTheDisk()
    {
        var entry = Entry("deck.pptx");
        await Pipeline(new VersionedExtractor("0.35.0", new ExtractionResult { Content = "Body.", Images = [Image("img_a"), Image("img_b")] }))
            .MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);
        File.Exists(Path.Combine(entry.ImagesPath, "img_b.png")).Should().BeTrue();

        await Pipeline(new VersionedExtractor("0.36.0", new ExtractionResult { Content = "Body.", Images = [Image("img_a")] }))
            .MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);

        (await _storage.GetImageManifestAsync(entry, TestContext.Current.CancellationToken)).Select(i => i.Id).Should().Equal("img_a");
        File.Exists(Path.Combine(entry.ImagesPath, "img_b.png")).Should().BeFalse();

        await Pipeline(new VersionedExtractor("0.37.0", new ExtractionResult { Content = "Body." }))
            .MemorizeAsync(entry, ct: TestContext.Current.CancellationToken);

        (await _storage.GetImageManifestAsync(entry, TestContext.Current.CancellationToken)).Should().BeEmpty(
            "a re-extraction with no images must not keep the previous extraction's");
        Directory.EnumerateFiles(entry.ImagesPath).Select(Path.GetFileName).Should().Equal("manifest.json");
    }

    private sealed class StubExtractorWithoutIdentity(ExtractionResult result) : IExtractor
    {
        public ExtractionIdentity? Identity => null;

        public Task<ExtractionResult> ExtractAsync(string sourcePath, ExtractionSettings? settings = null, CancellationToken ct = default) => Task.FromResult(result);
    }
}

public sealed class ExtractionIdentityTests
{
    private static ExtractionIdentity Id(string version, string extractor = "FileFlux", int revision = 1) =>
        new(extractor, version) { PipelineRevision = revision };

    [Theory]
    [InlineData(ReextractionPolicy.Never, "0.1.0", false)]
    [InlineData(ReextractionPolicy.WhenExtractorMinorChanges, "0.36.2", false)]
    [InlineData(ReextractionPolicy.WhenExtractorMinorChanges, "0.36.0", false)]
    [InlineData(ReextractionPolicy.WhenExtractorMinorChanges, "0.35.9", true)]
    [InlineData(ReextractionPolicy.WhenExtractorMinorChanges, "0.37.0", false)]
    [InlineData(ReextractionPolicy.WhenExtractorChanges, "0.36.1", true)]
    [InlineData(ReextractionPolicy.WhenExtractorChanges, "0.36.2", false)]
    [InlineData(ReextractionPolicy.WhenExtractorMinorChanges, "nightly", true)]
    public void Versions_CompareAtThePolicysGranularity(ReextractionPolicy policy, string recorded, bool outdated)
    {
        Id("0.36.2").IsOutdatedBy(Id(recorded), policy).Should().Be(outdated);
    }

    [Fact]
    public void AnotherExtractor_AnOlderPipelineRevision_OrNoRecord_IsOutdated()
    {
        var current = Id("0.36.2");

        current.IsOutdatedBy(Id("0.36.2", extractor: "FluxFeed.PlainText"), ReextractionPolicy.WhenExtractorMinorChanges).Should().BeTrue();
        current.IsOutdatedBy(Id("0.36.2", revision: 0), ReextractionPolicy.WhenExtractorMinorChanges).Should().BeTrue();
        current.IsOutdatedBy(null, ReextractionPolicy.WhenExtractorMinorChanges).Should().BeTrue();
        current.IsOutdatedBy(null, ReextractionPolicy.Never).Should().BeFalse();
    }

    [Fact]
    public void FromAssembly_DropsBuildMetadata()
    {
        var identity = ExtractionIdentity.FromAssembly("FluxFeed", typeof(VaultPipeline).Assembly);

        identity.Version.Should().NotContain("+").And.MatchRegex(@"^\d+\.\d+\.\d+");
    }
}
