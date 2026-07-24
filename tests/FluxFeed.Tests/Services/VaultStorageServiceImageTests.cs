using FluentAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using System.Text.Json;
using System.Text.Json.Serialization;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Tests for VaultStorageService image handling.
/// Verifies that image IDs are preserved consistently throughout the pipeline.
/// </summary>
public class VaultStorageServiceImageTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;
    private readonly IGitService _gitMock;

    public VaultStorageServiceImageTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"VaultStorageImageTests_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_testDir);
        Directory.CreateDirectory(_vaultDir);

        _gitMock = Substitute.For<IGitService>();
        _gitMock.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("abc123");

        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            _gitMock,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_testDir))
                Directory.Delete(_testDir, recursive: true);
        }
        catch { /* ignore cleanup errors */ }
    }

    [Fact]
    public async Task StoreImagesAsync_PreservesOriginalImageIds()
    {
        // Arrange
        var docPath = CreateDocument("test.txt", "Test content");
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);

        var testImageData = new byte[] { 0x89, 0x50, 0x4E, 0x47 }; // PNG header
        var images = new List<ImageArtifact>
        {
            new() { Id = "img_000", Data = testImageData, ContentType = "image/png" },
            new() { Id = "img_001", Data = testImageData, ContentType = "image/jpeg" },
            new() { Id = "img_002", Data = testImageData, ContentType = "image/png" }
        };

        // Act
        await _storage.StoreImagesAsync(entry, images, default);

        // Assert - verify file names match IDs
        var imagesDir = entry.ImagesPath;
        Directory.Exists(imagesDir).Should().BeTrue();

        File.Exists(Path.Combine(imagesDir, "img_000.png")).Should().BeTrue("First image should be named img_000.png");
        File.Exists(Path.Combine(imagesDir, "img_001.jpg")).Should().BeTrue("Second image should be named img_001.jpg");
        File.Exists(Path.Combine(imagesDir, "img_002.png")).Should().BeTrue("Third image should be named img_002.png");

        // Assert - verify manifest has correct IDs
        var manifestPath = entry.ImagesManifestPath;
        File.Exists(manifestPath).Should().BeTrue();

        var manifestJson = await File.ReadAllTextAsync(manifestPath);
        var jsonOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };
        var manifest = JsonSerializer.Deserialize<List<ImageManifestEntry>>(manifestJson, jsonOptions);

        manifest.Should().NotBeNull();
        manifest!.Count.Should().Be(3);
        manifest[0].Id.Should().Be("img_000");
        manifest[0].FileName.Should().Be("img_000.png");
        manifest[1].Id.Should().Be("img_001");
        manifest[1].FileName.Should().Be("img_001.jpg");
        manifest[2].Id.Should().Be("img_002");
        manifest[2].FileName.Should().Be("img_002.png");
    }

    [Fact]
    public async Task StoreImagesAsync_WithCustomIds_PreservesCustomIds()
    {
        // Arrange - simulate FileFlux returning custom IDs
        var docPath = CreateDocument("test2.txt", "Test content for custom IDs");
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);

        var testImageData = new byte[] { 0x89, 0x50, 0x4E, 0x47 };
        var images = new List<ImageArtifact>
        {
            new() { Id = "figure_1", Data = testImageData, ContentType = "image/png" },
            new() { Id = "diagram_main", Data = testImageData, ContentType = "image/jpeg" }
        };

        // Act
        await _storage.StoreImagesAsync(entry, images, default);

        // Assert - verify images directory exists and contains files
        var imagesDir = entry.ImagesPath;
        Directory.Exists(imagesDir).Should().BeTrue($"Images directory should exist at {imagesDir}");

        var files = Directory.GetFiles(imagesDir);
        files.Should().HaveCount(3, "Should have 2 image files and 1 manifest.json");

        // Verify custom IDs are preserved in file names
        files.Should().Contain(f => f.EndsWith("figure_1.png"), "figure_1.png should exist");
        files.Should().Contain(f => f.EndsWith("diagram_main.jpg"), "diagram_main.jpg should exist");
    }

    [Fact]
    public async Task GetImagesAsync_ReturnsCorrectIds()
    {
        // Arrange
        var docPath = CreateDocument("test.txt", "Test content");
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);

        var testImageData = new byte[] { 0x89, 0x50, 0x4E, 0x47 };
        var images = new List<ImageArtifact>
        {
            new() { Id = "img_000", Data = testImageData, ContentType = "image/png" },
            new() { Id = "img_001", Data = testImageData, ContentType = "image/jpeg" }
        };
        await _storage.StoreImagesAsync(entry, images, default);

        // Act
        var retrieved = await _storage.GetImagesAsync(entry, default);

        // Assert
        retrieved.Should().HaveCount(2);
        retrieved[0].Id.Should().Be("img_000");
        retrieved[1].Id.Should().Be("img_001");
    }

    [Fact]
    public async Task ImageIds_ConsistentBetweenStoreAndRetrieve()
    {
        // Arrange - This test verifies the fix for the index mismatch bug
        var docPath = CreateDocument("test.txt", "Test content");
        var entry = VaultEntry.Create(docPath, _vaultDir);
        await _storage.InitializeEntryAsync(entry, default);

        var testImageData = new byte[] { 0x89, 0x50, 0x4E, 0x47 };
        var originalImages = new List<ImageArtifact>
        {
            new() { Id = "img_000", Data = testImageData, ContentType = "image/png" },
            new() { Id = "img_001", Data = testImageData, ContentType = "image/jpeg" },
            new() { Id = "img_002", Data = testImageData, ContentType = "image/gif" }
        };

        // Act
        await _storage.StoreImagesAsync(entry, originalImages, default);
        var retrievedImages = await _storage.GetImagesAsync(entry, default);

        // Assert - IDs must match exactly (this was the bug: store used local index)
        for (int i = 0; i < originalImages.Count; i++)
        {
            retrievedImages[i].Id.Should().Be(originalImages[i].Id,
                $"Image at index {i} should preserve original ID '{originalImages[i].Id}'");
        }
    }

    [Fact]
    public async Task StoreImagesAsync_ReExtractionOfSameImage_KeepsItsDescription()
    {
        // A description is keyed to the picture, not to the run that extracted it. Re-memorize
        // re-extracts identical images; dropping descriptions here would make enrichment pay again
        // on every single memorize.
        var entry = VaultEntry.Create(CreateDocument("doc.docx", "body"), _vaultDir);
        var image = new ImageArtifact { Id = "img_000", Data = [1, 2, 3], ContentType = "image/png" };

        await _storage.StoreImagesAsync(entry, [image], default);
        await _storage.SetImageDescriptionAsync(entry, "img_000", "A revenue chart.", default);

        // Same image comes back from a fresh extraction, with no description of its own.
        await _storage.StoreImagesAsync(entry, [image], default);

        var manifest = await _storage.GetImageManifestAsync(entry, default);
        manifest.Should().ContainSingle().Which.Description.Should().Be("A revenue chart.");
    }

    [Fact]
    public async Task StoreImagesAsync_ImageContentChanged_DropsTheStaleDescription()
    {
        // Same id, different picture (the source document was edited) — the old description is now
        // about something that is no longer there, so it must not survive.
        var entry = VaultEntry.Create(CreateDocument("doc.docx", "body"), _vaultDir);

        await _storage.StoreImagesAsync(entry,
            [new ImageArtifact { Id = "img_000", Data = [1, 2, 3], ContentType = "image/png" }], default);
        await _storage.SetImageDescriptionAsync(entry, "img_000", "A revenue chart.", default);

        await _storage.StoreImagesAsync(entry,
            [new ImageArtifact { Id = "img_000", Data = [9, 9, 9, 9], ContentType = "image/png" }], default);

        var manifest = await _storage.GetImageManifestAsync(entry, default);
        manifest.Should().ContainSingle().Which.IsDescribed.Should().BeFalse();
    }

    [Fact]
    public async Task SetImageDescriptionAsync_UnknownImage_ReturnsFalse()
    {
        var entry = VaultEntry.Create(CreateDocument("doc.docx", "body"), _vaultDir);
        await _storage.StoreImagesAsync(entry,
            [new ImageArtifact { Id = "img_000", Data = [1], ContentType = "image/png" }], default);

        (await _storage.SetImageDescriptionAsync(entry, "img_999", "nope", default)).Should().BeFalse();
    }

    [Fact]
    public async Task GetImageManifestAsync_NoImages_ReturnsEmpty()
    {
        var entry = VaultEntry.Create(CreateDocument("plain.txt", "body"), _vaultDir);

        (await _storage.GetImageManifestAsync(entry, default)).Should().BeEmpty();
    }

    private string CreateDocument(string fileName, string content)
    {
        var path = Path.Combine(_testDir, fileName);
        File.WriteAllText(path, content);
        return path;
    }

    // Helper class to deserialize manifest
    private class ImageManifestEntry
    {
        public string Id { get; set; } = string.Empty;
        public string FileName { get; set; } = string.Empty;
        public string ContentType { get; set; } = string.Empty;
        public string? Description { get; set; }
        public int Width { get; set; }
        public int Height { get; set; }
        public long Size { get; set; }
    }
}
