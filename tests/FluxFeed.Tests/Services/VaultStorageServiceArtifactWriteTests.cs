using System.Text.Json;
using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Entry artifacts are rewritten whole, and two writers can overlap on one entry. A rewrite in place
/// truncates only when the file is opened, so a shorter write can leave the tail of a longer one
/// behind — a manifest in that state no longer parses, and every later attempt on the entry fails
/// until someone deletes the file. The manifest is also read, modified and written back, which loses
/// one of two overlapping updates even when each write is whole.
/// </summary>
public sealed class VaultStorageServiceArtifactWriteTests : IDisposable
{
    private readonly string _testDir;
    private readonly VaultStorageService _storage;

    public VaultStorageServiceArtifactWriteTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultArtifactWriteTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);
        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            Substitute.For<IGitService>(),
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _testDir }));
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup.
        }
    }

    public static TheoryData<string> TextArtifacts => ["extracted", "refined", "append-text", "qa"];

    [Theory]
    [MemberData(nameof(TextArtifacts))]
    public async Task Text_artifact_rewrite_overlapping_an_open_writer_leaves_one_whole_version(string artifact)
    {
        var entry = VaultEntry.Create(Path.Combine(_testDir, $"{artifact}.docx"), _testDir);
        var (path, write) = TextArtifact(entry, artifact);
        const string longer = "the earlier, considerably longer version of this artifact's content";
        const string shorter = "a shorter competing version";
        const string ours = "the version this write stores";
        await write(longer);

        // Both opens precede both flushes: an in-place rewrite cannot shrink the file under the
        // competing handle, so the longer version's tail would survive behind whichever lands last.
        var competing = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
        var competingWriter = new StreamWriter(competing);
        await competingWriter.WriteAsync(shorter);

        await write(ours);

        try
        {
            await competingWriter.DisposeAsync();
        }
        catch (IOException)
        {
            // A stale writer failing once its target has been replaced is acceptable — what matters
            // is what is left on disk.
        }

        var onDisk = await File.ReadAllTextAsync(path, TestContext.Current.CancellationToken);
        onDisk.Should().BeOneOf(ours, shorter);
    }

    [Fact]
    public async Task Manifest_rewrite_overlapping_an_open_writer_leaves_a_parseable_manifest()
    {
        var entry = VaultEntry.Create(Path.Combine(_testDir, "slides.pptx"), _testDir);
        await _storage.StoreImagesAsync(entry, Images(12), TestContext.Current.CancellationToken);

        // Opened without truncating, so the rewrite still reads the manifest it replaces; the competing
        // shorter content is flushed over the start of the file only after the rewrite has landed.
        var competing = new FileStream(
            entry.ImagesManifestPath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
        var competingWriter = new StreamWriter(competing);
        await competingWriter.WriteAsync("[]");

        await _storage.StoreImagesAsync(entry, Images(2), TestContext.Current.CancellationToken);

        try
        {
            await competingWriter.DisposeAsync();
        }
        catch (IOException)
        {
        }

        var json = await File.ReadAllTextAsync(entry.ImagesManifestPath, TestContext.Current.CancellationToken);
        var parse = () => JsonDocument.Parse(json);
        parse.Should().NotThrow("a manifest must stay readable when a concurrent writer overlaps it");
    }

    [Fact]
    public async Task A_damaged_manifest_does_not_block_the_rewrite_that_replaces_it()
    {
        // The shape a racing in-place rewrite leaves behind: a whole shorter array followed by the tail of
        // a longer one. Before, every later memorize of the entry failed reading it back.
        var entry = VaultEntry.Create(Path.Combine(_testDir, "damaged.pptx"), _testDir);
        await _storage.StoreImagesAsync(entry, Images(3), TestContext.Current.CancellationToken);
        var intact = await File.ReadAllTextAsync(entry.ImagesManifestPath, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(
            entry.ImagesManifestPath, "[]" + intact[2..], TestContext.Current.CancellationToken);

        await _storage.StoreImagesAsync(entry, Images(2), TestContext.Current.CancellationToken);

        var manifest = await _storage.GetImageManifestAsync(entry, TestContext.Current.CancellationToken);
        manifest.Select(image => image.Id).Should().BeEquivalentTo(["img-00", "img-01"]);
    }

    [Fact]
    public async Task Overlapping_description_updates_for_different_images_are_all_kept()
    {
        var entry = VaultEntry.Create(Path.Combine(_testDir, "album.pptx"), _testDir);
        var images = Images(16);
        await _storage.StoreImagesAsync(entry, images, TestContext.Current.CancellationToken);

        for (var round = 0; round < 10; round++)
        {
            await Task.WhenAll(images.Select(image => Task.Run(
                () => _storage.SetImageDescriptionAsync(entry, image.Id, $"round {round} {image.Id}"),
                TestContext.Current.CancellationToken)));

            var manifest = await _storage.GetImageManifestAsync(entry, TestContext.Current.CancellationToken);
            manifest.Select(image => image.Description).Should().BeEquivalentTo(
                images.Select(image => $"round {round} {image.Id}"),
                $"round {round}: every description written must survive the others");
        }
    }

    private (string Path, Func<string, Task> Write) TextArtifact(VaultEntry entry, string artifact) => artifact switch
    {
        "extracted" => (entry.ExtractedMdPath, content => _storage.StoreExtractedContentAsync(entry, content)),
        "refined" => (entry.RefinedMdPath, content => _storage.StoreRefinedContentAsync(entry, content)),
        "append-text" => (entry.AppendTextPath, content => _storage.StoreAppendTextAsync(entry, content)),
        "qa" => (entry.QaPath, content => _storage.StoreQaContentAsync(entry, content)),
        _ => throw new ArgumentOutOfRangeException(nameof(artifact)),
    };

    private static List<ImageArtifact> Images(int count) =>
        Enumerable.Range(0, count)
            .Select(i => new ImageArtifact
            {
                Id = $"img-{i:D2}",
                Data = [(byte)i, 1, 2, 3],
                ContentType = "image/png",
            })
            .ToList();
}
