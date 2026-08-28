using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Exceptions;
using Xunit;

namespace FluxFeed.Tests.Domain;

/// <summary>
/// "The record is absent" and "the record cannot be read" call for opposite responses: the first is
/// a normal state, the second is a fault a caller may be able to repair. Reporting both as a null
/// entry makes an unreadable record disappear from every listing without a trace.
/// </summary>
public class VaultEntryRecordReadTests : IDisposable
{
    private readonly string _testDir;

    public VaultEntryRecordReadTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultEntryRecordReadTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            Directory.Delete(_testDir, recursive: true);
        }

        GC.SuppressFinalize(this);
    }

    [Fact]
    public void Load_WhenTheRecordCannotBeParsed_ThrowsNamingTheRecordPath()
    {
        // Arrange
        var entry = VaultEntry.Create(Path.Combine(_testDir, "report.md"), _testDir);
        entry.SaveMetadata();
        File.WriteAllText(entry.MetaPath, "{ this is not a record");

        // Act
        var load = () => VaultEntry.LoadByHash(entry.FilepathHash, _testDir);

        // Assert
        load.Should().Throw<VaultRecordUnreadableException>()
            .Which.RecordPath.Should().Be(entry.MetaPath);
    }

    [Fact]
    public void Load_WhenTheRecordIsAbsent_ReturnsNull()
    {
        // Arrange
        var entry = VaultEntry.Create(Path.Combine(_testDir, "never-written.md"), _testDir);

        // Act
        var loaded = VaultEntry.LoadByHash(entry.FilepathHash, _testDir);

        // Assert — a null entry means "absent", and nothing else.
        loaded.Should().BeNull();
    }
}
