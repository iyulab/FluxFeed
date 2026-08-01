using FluentAssertions;
using FluxFeed.Domain.Entities;
using Xunit;

namespace FluxFeed.Tests.Domain;

/// <summary>
/// The metadata record is rewritten on a schedule as well as on demand, so two writers can
/// overlap. An in-place rewrite only truncates when the file is opened, which lets a shorter
/// serialization leave the tail of a longer one behind and produce a record that no longer parses.
/// </summary>
public class VaultEntryMetadataWriteTests : IDisposable
{
    private readonly string _testDir;

    public VaultEntryMetadataWriteTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "VaultEntryMetadataWriteTests_" + Guid.NewGuid().ToString("N"));
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
    public void SaveMetadata_WhenAnotherWriterHoldsTheRecordOpen_LeavesItParseable()
    {
        // Arrange
        var entry = VaultEntry.Create(Path.Combine(_testDir, "report.md"), _testDir);
        entry.SaveMetadata();
        var shorterSerialization = ShortenByOneCharacter(File.ReadAllText(entry.MetaPath));

        // A competing writer opens the record and buffers a serialization one character shorter,
        // then flushes after the entry has written its own. Both opens precede both flushes, so an
        // in-place rewrite cannot shrink the file and the longer record's tail survives.
        var competing = new FileStream(
            entry.MetaPath, FileMode.Create, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete);
        var competingWriter = new StreamWriter(competing);
        competingWriter.Write(shorterSerialization);

        // Act
        entry.SaveMetadata();

        try
        {
            competingWriter.Dispose();
        }
        catch (IOException)
        {
            // A stale writer failing once its target has been replaced is an acceptable outcome —
            // what matters is the record left on disk.
        }

        // Assert
        VaultEntry.LoadByHash(entry.FilepathHash, _testDir)
            .Should().NotBeNull("a record must stay readable when a concurrent writer overlaps it");
    }

    [Fact]
    public async Task SaveMetadata_ConcurrentWritersOfDifferentLengths_NeverLeaveAnUnreadableRecord()
    {
        // Arrange
        var entry = VaultEntry.Create(Path.Combine(_testDir, "notes.md"), _testDir);
        entry.SaveMetadata();

        var shortRecord = VaultEntry.LoadByHash(entry.FilepathHash, _testDir)!;
        var longRecord = VaultEntry.LoadByHash(entry.FilepathHash, _testDir)!;
        shortRecord.MarkError("short");
        longRecord.MarkError("a considerably longer failure description than the other writer records");

        // Act + Assert
        for (var round = 0; round < 50; round++)
        {
            await Task.WhenAll(
                Task.Run(shortRecord.SaveMetadata),
                Task.Run(longRecord.SaveMetadata));

            VaultEntry.LoadByHash(entry.FilepathHash, _testDir)
                .Should().NotBeNull($"round {round} must not leave an unreadable record");
        }
    }

    [Fact]
    public void SaveMetadata_WhenTheRecordCannotBePublished_FailsLoudlyAndLeavesNoTemporaryFile()
    {
        // Arrange - a directory occupying the record's name blocks the publish permanently on every
        // platform, which is the one case the bounded retry cannot clear. (Sharing modes are not a
        // portable way to arrange this: they are advisory outside Windows, so a lock-based
        // arrangement would let the publish succeed and assert nothing.)
        var entry = VaultEntry.Create(Path.Combine(_testDir, "blocked.md"), _testDir);
        Directory.CreateDirectory(entry.EntryPath);
        Directory.CreateDirectory(entry.MetaPath);

        // Act
        var save = () => entry.SaveMetadata();

        // Assert - a write that cannot land must say so rather than disappear.
        save.Should().Throw<Exception>()
            .Which.Should().Match(ex => ex is IOException || ex is UnauthorizedAccessException);

        // Scoped to the writer's own naming: the platform matches "*.tmp" case-insensitively and
        // would also catch the scratch copies its atomic replacement leaves behind.
        Directory.GetFiles(entry.EntryPath, "meta.json.*.tmp")
            .Where(p => p.EndsWith(".tmp", StringComparison.Ordinal)).Should()
            .BeEmpty("a failed publish must not leave its temporary file behind");
    }

    [Fact]
    public void SaveMetadata_WhenAHolderSharesNothing_FailsLoudly()
    {
        // Exclusive sharing is only enforced on Windows; elsewhere it is advisory and the publish
        // would simply succeed, so this covers the platform where the arrangement means something.
        // The portable statement of the same invariant is the test above.
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var entry = VaultEntry.Create(Path.Combine(_testDir, "locked.md"), _testDir);
        entry.SaveMetadata();

        using var exclusiveHolder = new FileStream(
            entry.MetaPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);

        var save = () => entry.SaveMetadata();

        save.Should().Throw<Exception>()
            .Which.Should().Match(ex => ex is IOException || ex is UnauthorizedAccessException);

        Directory.GetFiles(entry.EntryPath, "meta.json.*.tmp")
            .Where(p => p.EndsWith(".tmp", StringComparison.Ordinal)).Should()
            .BeEmpty("a failed publish must not leave its temporary file behind");
    }

    [Fact]
    public void SaveMetadata_RemovesAbandonedReplacementLeftovers()
    {
        // Arrange - the platform's atomic replacement keeps the outgoing record under a scratch
        // name while it swaps, and abandons that name if it cannot clean up (a reader holding the
        // outgoing record open is enough). Left alone these accumulate in the entry directory,
        // each holding a stale copy of the record.
        var entry = VaultEntry.Create(Path.Combine(_testDir, "report.md"), _testDir);
        entry.SaveMetadata();

        var abandoned = Path.Combine(entry.EntryPath, "meta.json~RF1234abc.TMP");
        File.WriteAllText(abandoned, "{ stale record }");
        File.SetLastWriteTimeUtc(abandoned, DateTime.UtcNow.AddHours(-1));

        // Act
        entry.SaveMetadata();

        // Assert
        File.Exists(abandoned).Should().BeFalse(
            "a leftover no write could still be using is the writer's to clean up");
    }

    [Fact]
    public void SaveMetadata_KeepsReplacementLeftoversThatMayStillBeInFlight()
    {
        // Arrange - a leftover that was just written may belong to a replacement running right
        // now. Deleting it would destroy the only copy of the outgoing record if that replacement
        // then had to roll back, so recent ones are left alone.
        var entry = VaultEntry.Create(Path.Combine(_testDir, "report.md"), _testDir);
        entry.SaveMetadata();

        var inFlight = Path.Combine(entry.EntryPath, "meta.json~RF5678def.TMP");
        File.WriteAllText(inFlight, "{ outgoing record }");

        // Act
        entry.SaveMetadata();

        // Assert
        File.Exists(inFlight).Should().BeTrue(
            "a replacement in progress may still need to roll back onto its scratch copy");
    }

    /// <summary>
    /// Drops one indent character. Indented JSON has whitespace to spare, so the result stays
    /// valid while reproducing the one-character length difference that a timestamp's trailing
    /// fractional-second digit produces in practice.
    /// </summary>
    private static string ShortenByOneCharacter(string json)
    {
        var indentIndex = json.IndexOf("\n  ", StringComparison.Ordinal);
        indentIndex.Should().BeGreaterThan(-1, "the serialization is indented");
        return json.Remove(indentIndex + 1, 1);
    }
}
