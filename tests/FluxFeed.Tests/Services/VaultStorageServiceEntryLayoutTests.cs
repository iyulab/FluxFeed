using FluentAssertions;
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
/// The entry directory holds the working artifacts (metadata, extracted text, images); the tracked
/// git repository is the <c>vault/</c> subdirectory beneath it. Ignore rules only take effect inside
/// a repository, so an ignore file written at the entry level can never be read by git — and the
/// artifacts it would name are outside that repository to begin with. These tests pin the layout
/// so such a file is not reintroduced.
/// </summary>
public class VaultStorageServiceEntryLayoutTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;

    public VaultStorageServiceEntryLayoutTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"VaultEntryLayoutTests_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_testDir);
        Directory.CreateDirectory(_vaultDir);

        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            Substitute.For<IGitService>(),
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
        GC.SuppressFinalize(this);
    }

    private VaultEntry NewEntry(string fileName)
    {
        var docPath = Path.Combine(_testDir, fileName);
        File.WriteAllText(docPath, "content");
        return VaultEntry.Create(docPath, _vaultDir);
    }

    [Fact]
    public async Task InitializeEntryAsync_WritesNoIgnoreFileOutsideTheTrackedRepository()
    {
        var entry = NewEntry("document.txt");

        await _storage.InitializeEntryAsync(entry);

        File.Exists(Path.Combine(entry.EntryPath, ".gitignore")).Should().BeFalse(
            "an ignore file at the entry level sits outside the tracked repository and can never apply");
    }

    [Fact]
    public async Task InitializeEntryAsync_CreatesTheTrackedSubdirectoryAndMetadata()
    {
        var entry = NewEntry("document.txt");

        await _storage.InitializeEntryAsync(entry);

        Directory.Exists(entry.VaultPath).Should().BeTrue();
        File.Exists(entry.MetaPath).Should().BeTrue();
    }
}
