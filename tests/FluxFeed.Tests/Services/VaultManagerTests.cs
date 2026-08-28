using AwesomeAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

public class VaultManagerTests : IDisposable
{
    private readonly ContentHasher _contentHasher;
    private readonly IGitService _gitServiceMock;
    private readonly IVaultPipeline _pipelineMock;
    private readonly IVaultQueueService _queueServiceMock;
    private readonly IFileWatcherService _fileWatcherMock;
    private readonly IVaultStorageService _storageMock;
    private readonly VaultManager _vault;
    private readonly string _testDir;
    private readonly string _vaultDir;

    public VaultManagerTests()
    {
        _contentHasher = new ContentHasher();
        _gitServiceMock = Substitute.For<IGitService>();
        _pipelineMock = Substitute.For<IVaultPipeline>();
        _queueServiceMock = Substitute.For<IVaultQueueService>();
        _fileWatcherMock = Substitute.For<IFileWatcherService>();
        _storageMock = Substitute.For<IVaultStorageService>();

        // Create test directories first
        _testDir = Path.Combine(Path.GetTempPath(), "FileVaultTests_" + Guid.NewGuid().ToString("N"));
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_testDir);
        Directory.CreateDirectory(_vaultDir);

        // Setup default mock returns
        _fileWatcherMock.GetAllWatchers().Returns([]);
        _storageMock.BasePath.Returns(_vaultDir);
        _storageMock.GetStorageSizeAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(0L);
        _storageMock.EntryStorageExists(Arg.Any<VaultEntry>()).Returns(false);
        _storageMock.InitializeEntryAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        // Setup pipeline mock to return success
        _pipelineMock.MemorizeAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>()).Returns(MemorizeResult.Succeeded(5, 1000, TimeSpan.FromSeconds(1)));
        _pipelineMock.RefreshAsync(Arg.Any<VaultEntry>(), Arg.Any<MemorizeOptions>(), Arg.Any<CancellationToken>()).Returns(MemorizeResult.Succeeded(5, 1000, TimeSpan.FromSeconds(1)));
        _pipelineMock.ExtractAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);

        // Setup queue mock
        _queueServiceMock.EnqueueMemorizeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(callInfo => { var hash = callInfo.ArgAt<string>(0); var path = callInfo.ArgAt<string>(1); return CreateTestJob(hash, path, VaultJobType.Memorize); });
        _queueServiceMock.EnqueueRefreshAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(callInfo => { var hash = callInfo.ArgAt<string>(0); var path = callInfo.ArgAt<string>(1); return CreateTestJob(hash, path, VaultJobType.Refresh); });
        _queueServiceMock.GetStatisticsAsync(Arg.Any<CancellationToken>()).Returns(new QueueStatistics());

        var options = MsOptions.Create(new FileVaultOptions
        {
            VaultBasePath = _vaultDir
        });

        var logger = NullLogger<VaultManager>.Instance;

        _vault = new VaultManager(
            _contentHasher,
            _gitServiceMock,
            _pipelineMock,
            _queueServiceMock,
            _fileWatcherMock,
            _storageMock,
            logger,
            options);
    }

    private VaultManager CreateVaultWithVaultId(string? vaultId) =>
        new(
            _contentHasher,
            _gitServiceMock,
            _pipelineMock,
            _queueServiceMock,
            _fileWatcherMock,
            _storageMock,
            NullLogger<VaultManager>.Instance,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir, VaultId = vaultId }));

    [Fact]
    public async Task PurgeAsync_TenantScoped_BulkDeletesViaPipelineWithVaultId()
    {
        _pipelineMock.PurgeVectorsAsync("tenant-x", Arg.Any<CancellationToken>())
            .Returns(7);
        var vault = CreateVaultWithVaultId("tenant-x");

        var deleted = await vault.PurgeAsync();

        deleted.Should().Be(7);
        await _pipelineMock.Received(1).PurgeVectorsAsync("tenant-x", Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task PurgeAsync_NonTenantScoped_ThrowsRatherThanGuessing()
    {
        var vault = CreateVaultWithVaultId(vaultId: null);

        var act = async () => await vault.PurgeAsync();

        await act.Should().ThrowAsync<InvalidOperationException>();
        await _pipelineMock.DidNotReceive().PurgeVectorsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            try
            {
                Directory.Delete(_testDir, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Fact]
    public async Task MemorizeAsync_NewFile_EnqueuesJob()
    {
        // Arrange
        var filePath = CreateTestFile("test.txt", "Hello, World!");

        // Act
        var result = await _vault.MemorizeAsync(filePath);

        // Assert
        result.Should().NotBeNull();
        result.SourcePath.Should().Be(Path.GetFullPath(filePath));
        await _queueServiceMock.Received(1).EnqueueMemorizeAsync(
            Arg.Any<string>(),
            Path.GetFullPath(filePath),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task MemorizeAsync_NonExistentFile_ThrowsFileNotFoundException()
    {
        // Arrange
        var nonExistentPath = Path.Combine(_testDir, "nonexistent.txt");

        // Act & Assert
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => _vault.MemorizeAsync(nonExistentPath));
    }

    // === MU-2: terminal-await overload ===

    private static VaultJob MakeTerminalJob(VaultJobStatus status, string? error = null) =>
        VaultJob.Restore(
            id: Guid.NewGuid(),
            filePath: "/tmp/x.txt",
            filepathHash: "hash",
            jobType: VaultJobType.Memorize,
            status: status,
            priority: VaultJobPriority.Normal,
            queuedAt: DateTimeOffset.UtcNow,
            startedAt: DateTimeOffset.UtcNow,
            completedAt: DateTimeOffset.UtcNow,
            retryCount: 0,
            maxRetries: 3,
            errorMessage: error,
            lastCompletedChunkIndex: -1);

    [Fact]
    public async Task MemorizeAsync_WaitForCompletion_AwaitsTerminalJob()
    {
        var filePath = CreateTestFile("wait.txt", "content");
        _queueServiceMock.WaitForJobAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>())
            .Returns(MakeTerminalJob(VaultJobStatus.Completed));

        var result = await _vault.MemorizeAsync(filePath, waitForCompletion: true);

        result.Should().NotBeNull();
        await _queueServiceMock.Received(1).EnqueueMemorizeAsync(
            Arg.Any<string>(), Path.GetFullPath(filePath), Arg.Any<CancellationToken>());
        await _queueServiceMock.Received(1).WaitForJobAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task MemorizeAsync_WaitForCompletionFalse_DoesNotAwait()
    {
        var filePath = CreateTestFile("nowait.txt", "content");

        await _vault.MemorizeAsync(filePath, waitForCompletion: false);

        await _queueServiceMock.Received(1).EnqueueMemorizeAsync(
            Arg.Any<string>(), Path.GetFullPath(filePath), Arg.Any<CancellationToken>());
        await _queueServiceMock.DidNotReceive().WaitForJobAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task MemorizeAsync_WaitForCompletion_ThrowsOnFailedJob()
    {
        var filePath = CreateTestFile("fail.txt", "content");
        _queueServiceMock.WaitForJobAsync(Arg.Any<Guid>(), Arg.Any<CancellationToken>())
            .Returns(MakeTerminalJob(VaultJobStatus.Failed, "pipeline error"));

        var act = async () => await _vault.MemorizeAsync(filePath, waitForCompletion: true);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*pipeline error*");
    }

    [Fact]
    public async Task MemorizeAsync_InitializesEntryStorage()
    {
        // Arrange
        var filePath = CreateTestFile("test.txt", "Hello, World!");

        // Act
        await _vault.MemorizeAsync(filePath);

        // Assert
        await _storageMock.Received(1).InitializeEntryAsync(
            Arg.Any<VaultEntry>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetAsync_NonExistentEntry_ReturnsNull()
    {
        // Act
        var result = await _vault.GetAsync(Path.Combine(_testDir, "nonexistent.txt"));

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task RefreshAsync_NonExistentEntry_ThrowsInvalidOperationException()
    {
        // Arrange
        var filePath = CreateTestFile("test.txt", "Hello, World!");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => _vault.RefreshAsync(filePath));
    }

    [Fact]
    public async Task RefreshAsync_ExistingEntry_EnqueuesRefreshJob()
    {
        // Arrange - refresh re-indexes refined content, so that content must exist. This test used to
        // stop at the Extracted stage, which the pipeline would then reject.
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        var entry = CreateEntryWithMetadataAtStage(filePath, ProcessingStage.Extracted);
        WriteRefinedContent(entry);

        // Act
        var result = await _vault.RefreshAsync(filePath);

        // Assert
        result.Should().NotBeNull();
        await _queueServiceMock.Received(1).EnqueueRefreshAsync(
            Arg.Any<string>(),
            Path.GetFullPath(filePath),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetAsync_ExistingEntry_ReturnsEntry()
    {
        // Arrange - Create entry with metadata on disk
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        var createdEntry = CreateEntryWithMetadata(filePath);

        // Act
        var retrieved = await _vault.GetAsync(filePath);

        // Assert
        retrieved.Should().NotBeNull();
        retrieved!.SourcePath.Should().Be(Path.GetFullPath(filePath));
    }

    [Fact]
    public async Task GetByHashAsync_ExistingEntry_ReturnsEntry()
    {
        // Arrange - Create entry with metadata on disk
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        var createdEntry = CreateEntryWithMetadata(filePath);

        // Act
        var retrieved = await _vault.GetByHashAsync(createdEntry.FilepathHash);

        // Assert
        retrieved.Should().NotBeNull();
        retrieved!.FilepathHash.Should().Be(createdEntry.FilepathHash);
    }

    [Fact]
    public async Task ListAsync_WithEntries_ReturnsAll()
    {
        // Arrange - Create entries with metadata on disk
        var file1 = CreateTestFile("file1.txt", "Content 1");
        var file2 = CreateTestFile("file2.txt", "Content 2");
        CreateEntryWithMetadata(file1);
        CreateEntryWithMetadata(file2);

        // Act
        var entries = await _vault.ListAsync();

        // Assert
        entries.Should().HaveCount(2);
    }

    [Fact]
    public async Task StatusAsync_WithEntries_ReturnsCorrectCounts()
    {
        // Arrange - Create entries with metadata on disk
        var file1 = CreateTestFile("file1.txt", "Content 1");
        var file2 = CreateTestFile("file2.txt", "Content 2");
        CreateEntryWithMetadata(file1);
        CreateEntryWithMetadata(file2);

        // Act
        var status = await _vault.StatusAsync();

        // Assert
        status.TotalEntries.Should().Be(2);
    }

    [Fact]
    public async Task DiffAsync_ExistingEntry_CallsGitService()
    {
        // Arrange - Create entry with metadata on disk
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        CreateEntryWithMetadata(filePath);
        _gitServiceMock.DiffAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("diff output");

        // Act
        var diff = await _vault.DiffAsync(filePath);

        // Assert
        diff.Should().Be("diff output");
        await _gitServiceMock.Received(1).DiffAsync(
            Arg.Any<string>(),
            Arg.Any<string>(),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task LogAsync_ExistingEntry_CallsGitService()
    {
        // Arrange - Create entry with metadata on disk
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        CreateEntryWithMetadata(filePath);
        _gitServiceMock.LogAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<CancellationToken>()).Returns([]);

        // Act
        var logs = await _vault.LogAsync(filePath);

        // Assert
        await _gitServiceMock.Received(1).LogAsync(
            Arg.Any<string>(),
            10,
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetContentAtCommitAsync_KnownCommit_CombinesTrackedFilesAtThatCommit()
    {
        // Arrange - Create entry with metadata on disk
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        CreateEntryWithMetadata(filePath);
        _gitServiceMock.ShowFileAsync(Arg.Any<string>(), "abc123", "refined.md", Arg.Any<CancellationToken>())
            .Returns("old refined content");
        _gitServiceMock.ShowFileAsync(Arg.Any<string>(), "abc123", "append-text.md", Arg.Any<CancellationToken>())
            .Returns((string?)null);
        _gitServiceMock.ShowFileAsync(Arg.Any<string>(), "abc123", "qa.md", Arg.Any<CancellationToken>())
            .Returns((string?)null);

        // Act
        var content = await _vault.GetContentAtCommitAsync(filePath, "abc123");

        // Assert
        content.Should().Be("old refined content");
    }

    [Fact]
    public async Task GetContentAtCommitAsync_UnknownCommit_ReturnsNull()
    {
        // Arrange - Create entry with metadata on disk, but the commit resolves to nothing
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        CreateEntryWithMetadata(filePath);
        _gitServiceMock.ShowFileAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns((string?)null);

        // Act
        var content = await _vault.GetContentAtCommitAsync(filePath, "deadbeef");

        // Assert
        content.Should().BeNull();
    }

    [Fact]
    public async Task GetContentAtCommitAsync_NoEntry_ReturnsNull()
    {
        // Act - no CreateEntryWithMetadata call, so GetAsync resolves to null
        var content = await _vault.GetContentAtCommitAsync(Path.Combine(_testDir, "missing.txt"), "abc123");

        // Assert
        content.Should().BeNull();
    }

    [Fact]
    public async Task GetImageManifestAsync_KnownEntry_ReturnsStorageManifest()
    {
        // Arrange - Create entry with metadata on disk
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        var entry = CreateEntryWithMetadata(filePath);
        var manifest = new List<VaultImage>
        {
            new()
            {
                Id = "img_000",
                FilePath = Path.Combine(_vaultDir, "images", "img_000.png"),
                FileName = "img_000.png",
                ContentType = "image/png",
                Description = "A chart of quarterly revenue."
            }
        };
        _storageMock.GetImageManifestAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>())
            .Returns(manifest);

        // Act
        var result = await _vault.GetImageManifestAsync(filePath);

        // Assert - the facade passes the resolved entry straight through to storage
        result.Should().BeEquivalentTo(manifest);
        await _storageMock.Received(1).GetImageManifestAsync(
            Arg.Is<VaultEntry>(e => e.FilepathHash == entry.FilepathHash),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task GetImageManifestAsync_NoEntry_ReturnsEmpty()
    {
        // Act - no CreateEntryWithMetadata call, so GetAsync resolves to null
        var result = await _vault.GetImageManifestAsync(Path.Combine(_testDir, "missing.txt"));

        // Assert
        result.Should().BeEmpty();
        await _storageMock.DidNotReceive().GetImageManifestAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task ScanFolderAsync_WithFiles_DiscoverFiles()
    {
        // Arrange
        CreateTestFile("doc1.txt", "Content 1");
        CreateTestFile("doc2.md", "Content 2");

        // Act
        var result = await _vault.ScanFolderAsync(_testDir);

        // Assert
        result.NewFilesCount.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task ScanFolderAsync_UnregisteredFolder_RespectsDefaultIncludePatterns()
    {
        // Arrange — files outside DefaultIncludePatterns must not enter DetectedChanges
        CreateTestFile("doc1.pdf", "PDF content");
        CreateTestFile("photo.jpg", "JPEG bytes");
        CreateTestFile("Thumbs.db", "thumbnail cache");

        // Act
        var result = await _vault.ScanFolderAsync(_testDir);

        // Assert
        result.DetectedChanges.Should().OnlyContain(c => c.FilePath.EndsWith("doc1.pdf"));
        result.SkippedFilesCount.Should().Be(2);
    }

    [Fact]
    public async Task ScanFolderAsync_TrackedEntryWithDeletedSource_SurfacesRemoveAction()
    {
        // Arrange — an entry that was memorized and then had its source file deleted. The forward
        // per-file loop can never observe this (Directory.EnumerateFiles only yields files that
        // still exist), so discovery has to come from a reverse pass over tracked entries instead.
        var filePath = CreateTestFile("orphaned.txt", "Hello, World!");
        CreateEntryWithMetadata(filePath);
        File.Delete(filePath);

        // Act
        var result = await _vault.ScanFolderAsync(_testDir);

        // Assert
        result.OrphanedFilesCount.Should().Be(1);
        result.DetectedChanges.Should().ContainSingle(c =>
            c.FilePath == Path.GetFullPath(filePath) && c.RecommendedAction == ChangeAction.Remove);
    }

    [Fact]
    public async Task ScanFolderAsync_TrackedEntryWithDeletedSourceOutsideFolder_IsNotSurfaced()
    {
        // Arrange — an orphan whose source lived elsewhere must not leak into a scan scoped to a
        // different folder (folder-scoped scans should not become an implicit global sweep).
        var outsideDir = Path.Combine(Path.GetTempPath(), "FileVaultTests_Outside_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(outsideDir);
        try
        {
            var outsidePath = Path.Combine(outsideDir, "elsewhere.txt");
            File.WriteAllText(outsidePath, "content");
            CreateEntryWithMetadata(outsidePath);
            File.Delete(outsidePath);

            // Act
            var result = await _vault.ScanFolderAsync(_testDir);

            // Assert
            result.OrphanedFilesCount.Should().Be(0);
        }
        finally
        {
            Directory.Delete(outsideDir, recursive: true);
        }
    }

    [Fact]
    public async Task DetectChangesAsync_NewFile_ReturnsMemorizeAction()
    {
        // Arrange
        var filePath = CreateTestFile("test.txt", "Hello, World!");

        // Act
        var result = await _vault.DetectChangesAsync(filePath);

        // Assert
        result.Should().NotBeNull();
        result.RecommendedAction.Should().Be(ChangeAction.Memorize);
    }

    [Fact]
    public async Task GetQueueStatusAsync_ReturnsQueueStatus()
    {
        // Act
        var status = await _vault.GetQueueStatusAsync();

        // Assert
        status.Should().NotBeNull();
    }

    [Fact]
    public async Task GetQueueStatusAsync_PropagatesLastProcessedAtAndAverageProcessingTime()
    {
        // Arrange — regression guard: the mapping from QueueStatistics (internal) to QueueStatus
        // (public) previously dropped LastProcessedAt and never carried AverageProcessingTimeMs at
        // all, so both always read as 0/null regardless of what the queue actually recorded.
        var lastProcessedAt = DateTimeOffset.UtcNow;
        _queueServiceMock.GetStatisticsAsync(Arg.Any<CancellationToken>()).Returns(new QueueStatistics
        {
            LastProcessedAt = lastProcessedAt,
            AverageProcessingTimeMs = 123.45,
        });

        // Act
        var status = await _vault.GetQueueStatusAsync();

        // Assert
        status.LastProcessedAt.Should().Be(lastProcessedAt);
        status.AverageProcessingTimeMs.Should().Be(123.45);
    }

    [Fact]
    public async Task RemoveAsync_ExistingEntry_RemovesFromVectorStore()
    {
        // Arrange - Create entry with metadata on disk
        var filePath = CreateTestFile("test.txt", "Hello, World!");
        CreateEntryWithMetadata(filePath);

        _pipelineMock.RemoveAsync(Arg.Any<VaultEntry>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _queueServiceMock.EnqueueRemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(callInfo => { var hash = callInfo.ArgAt<string>(0); var path = callInfo.ArgAt<string>(1); return CreateTestJob(hash, path, VaultJobType.Remove); });

        // Act
        await _vault.RemoveAsync(filePath);

        // Assert
        await _queueServiceMock.Received(1).EnqueueRemoveAsync(
            Arg.Any<string>(),
            Path.GetFullPath(filePath),
            Arg.Any<CancellationToken>());
    }

    private string CreateTestFile(string fileName, string content)
    {
        var filePath = Path.Combine(_testDir, fileName);
        File.WriteAllText(filePath, content);
        return filePath;
    }

    /// <summary>
    /// Creates a VaultEntry and saves its metadata to disk so it can be loaded by GetAsync/ListAsync.
    /// </summary>
    private VaultEntry CreateEntryWithMetadata(string filePath)
    {
        return CreateEntryWithMetadataAtStage(filePath, ProcessingStage.Source);
    }

    /// <summary>
    /// Creates a VaultEntry at a specific stage and saves its metadata to disk.
    /// </summary>
    private VaultEntry CreateEntryWithMetadataAtStage(string filePath, ProcessingStage stage)
    {
        var fullPath = Path.GetFullPath(filePath);
        var entry = VaultEntry.Create(fullPath, _vaultDir);

        // Create entry directory structure
        Directory.CreateDirectory(entry.EntryPath);
        Directory.CreateDirectory(entry.VaultPath);

        // Set the stage
        if (stage >= ProcessingStage.Extracted)
        {
            var hash = _contentHasher.ComputeHashAsync(fullPath, default).GetAwaiter().GetResult();
            entry.MarkExtracted(hash);
        }
        if (stage >= ProcessingStage.Memorized)
        {
            entry.MarkMemorized(1);
        }

        // Save metadata
        entry.SaveMetadata();

        return entry;
    }

    private static VaultJob CreateTestJob(string filepathHash, string filePath, VaultJobType jobType)
    {
        return VaultJob.Create(filepathHash, filePath, jobType);
    }

    /// <summary>
    /// Writes refined.md, the artifact a refresh re-indexes. No <see cref="ProcessingStage"/> implies
    /// it exists, so tests that need a refreshable entry have to create it explicitly.
    /// </summary>
    private static void WriteRefinedContent(VaultEntry entry)
    {
        Directory.CreateDirectory(entry.VaultPath);
        File.WriteAllText(entry.RefinedMdPath, "# refined\n\nrefined body\n");
    }

    /// <summary>
    /// Creates an entry that extracted successfully and then failed, mirroring the field shape: the
    /// failure reason is recorded, and the vault directory exists with tracked files in it.
    /// </summary>
    private VaultEntry CreateFailedEntry(string filePath, string errorMessage)
    {
        var entry = CreateEntryWithMetadataAtStage(filePath, ProcessingStage.Extracted);
        entry.MarkError(errorMessage);
        entry.SaveMetadata();
        return entry;
    }

    private void GivenModifiedVaultFiles(params string[] files) =>
        _gitServiceMock.StatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new GitStatus { ModifiedFiles = [.. files] });

    [Fact]
    public async Task RemoveAsync_BatchWithMultipleExistingEntries_QueuesEach()
    {
        // Arrange
        var path1 = CreateTestFile("batch1.txt", "content1");
        var path2 = CreateTestFile("batch2.txt", "content2");
        CreateEntryWithMetadata(path1);
        CreateEntryWithMetadata(path2);

        _queueServiceMock.EnqueueRemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => CreateTestJob(
                callInfo.ArgAt<string>(0), callInfo.ArgAt<string>(1), VaultJobType.Remove));

        // Act
        await _vault.RemoveAsync(new[] { path1, path2 });

        // Assert
        await _queueServiceMock.Received(1).EnqueueRemoveAsync(
            Arg.Any<string>(), Path.GetFullPath(path1), Arg.Any<CancellationToken>());
        await _queueServiceMock.Received(1).EnqueueRemoveAsync(
            Arg.Any<string>(), Path.GetFullPath(path2), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RemoveAsync_BatchWithMissingEntry_SkipsMissingAndRemovesFound()
    {
        // Arrange
        var existingPath = CreateTestFile("exists.txt", "content");
        var missingPath = Path.Combine(_testDir, "does-not-exist.txt");
        CreateEntryWithMetadata(existingPath);

        _queueServiceMock.EnqueueRemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(callInfo => CreateTestJob(
                callInfo.ArgAt<string>(0), callInfo.ArgAt<string>(1), VaultJobType.Remove));

        // Act
        await _vault.RemoveAsync(new[] { existingPath, missingPath });

        // Assert — only the existing path is queued
        await _queueServiceMock.Received(1).EnqueueRemoveAsync(
            Arg.Any<string>(), Path.GetFullPath(existingPath), Arg.Any<CancellationToken>());
        await _queueServiceMock.DidNotReceive().EnqueueRemoveAsync(
            Arg.Any<string>(), Path.GetFullPath(missingPath), Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task RemoveAsync_BatchWithEmptyList_DoesNothing()
    {
        // Act
        await _vault.RemoveAsync(Array.Empty<string>());

        // Assert
        await _queueServiceMock.DidNotReceive().EnqueueRemoveAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    #region SyncStatus Query Tests

    [Fact]
    public async Task ListByStatusAsync_FiltersCorrectly()
    {
        // Arrange
        var file1 = CreateTestFile("file1.txt", "Content 1");
        var file2 = CreateTestFile("file2.txt", "Content 2");
        var entry1 = CreateEntryWithMetadata(file1);
        var entry2 = CreateEntryWithMetadata(file2);
        entry2.UpdateSyncStatus(SyncStatus.SourceModified);
        entry2.SaveMetadata();

        // Act
        var sourceModifiedEntries = await _vault.ListByStatusAsync(SyncStatus.SourceModified);

        // Assert
        sourceModifiedEntries.Should().HaveCount(1);
        sourceModifiedEntries[0].SourcePath.Should().Be(Path.GetFullPath(file2));
    }

    [Fact]
    public async Task GetPendingRemovalsAsync_ReturnsAllRemovalStates()
    {
        // Arrange
        var file1 = CreateTestFile("file1.txt", "Content 1");
        var file2 = CreateTestFile("file2.txt", "Content 2");
        var file3 = CreateTestFile("file3.txt", "Content 3");

        var entry1 = CreateEntryWithMetadata(file1);
        entry1.MarkSourceDeleted();
        entry1.SaveMetadata();

        var entry2 = CreateEntryWithMetadata(file2);
        entry2.MarkRemovalPending();
        entry2.SaveMetadata();

        var entry3 = CreateEntryWithMetadata(file3);
        entry3.MarkRemovalPartial("Vector");
        entry3.SaveMetadata();

        // Act
        var pendingRemovals = await _vault.GetPendingRemovalsAsync();

        // Assert
        pendingRemovals.Should().HaveCount(3);
    }

    [Fact]
    public async Task GetErrorEntriesAsync_ReturnsErrorEntries()
    {
        // Arrange
        var file1 = CreateTestFile("file1.txt", "Content 1");
        var file2 = CreateTestFile("file2.txt", "Content 2");

        var entry1 = CreateEntryWithMetadata(file1);
        entry1.MarkSyncError("Test error");
        entry1.SaveMetadata();

        CreateEntryWithMetadata(file2);

        // Act
        var errorEntries = await _vault.GetErrorEntriesAsync();

        // Assert
        errorEntries.Should().HaveCount(1);
        errorEntries[0].SyncStatus.Should().Be(SyncStatus.Error);
        errorEntries[0].LastError.Should().Be("Test error");
    }

    [Fact]
    public async Task GetErrorEntriesAsync_IncludesPipelineErrors()
    {
        // Arrange — pipeline error (Stage=Error via MarkError)
        var file1 = CreateTestFile("pipeline-fail.txt", "Content");
        var entry1 = CreateEntryWithMetadata(file1);
        entry1.MarkError("Embedding timeout");
        entry1.SaveMetadata();

        // Arrange — sync error (SyncStatus=Error via MarkSyncError)
        var file2 = CreateTestFile("sync-fail.txt", "Content");
        var entry2 = CreateEntryWithMetadata(file2);
        entry2.MarkSyncError("Removal failed");
        entry2.SaveMetadata();

        // Arrange — healthy entry
        var file3 = CreateTestFile("healthy.txt", "Content");
        CreateEntryWithMetadata(file3);

        // Act
        var errors = await _vault.GetErrorEntriesAsync();

        // Assert — both pipeline and sync errors detected
        errors.Should().HaveCount(2);
        errors.Should().Contain(e => e.Stage == ProcessingStage.Error);
        errors.Should().Contain(e => e.SyncStatus == SyncStatus.Error);
    }

    [Fact]
    public async Task GetEntriesNeedingSyncAsync_ReturnsModifiedEntries()
    {
        // Arrange
        var file1 = CreateTestFile("file1.txt", "Content 1");
        var file2 = CreateTestFile("file2.txt", "Content 2");
        var file3 = CreateTestFile("file3.txt", "Content 3");

        var entry1 = CreateEntryWithMetadata(file1);
        entry1.UpdateSyncStatus(SyncStatus.SourceModified);
        entry1.SaveMetadata();

        var entry2 = CreateEntryWithMetadata(file2);
        entry2.UpdateSyncStatus(SyncStatus.VaultModified);
        entry2.SaveMetadata();

        CreateEntryWithMetadata(file3); // InSync

        // Act
        var needingSync = await _vault.GetEntriesNeedingSyncAsync();

        // Assert
        needingSync.Should().HaveCount(2);
    }

    [Fact]
    public async Task StatusAsync_IncludesSyncStatusCounts()
    {
        // Arrange
        var file1 = CreateTestFile("file1.txt", "Content 1");
        var file2 = CreateTestFile("file2.txt", "Content 2");
        var file3 = CreateTestFile("file3.txt", "Content 3");

        var entry1 = CreateEntryWithMetadata(file1);
        // entry1 remains InSync

        var entry2 = CreateEntryWithMetadata(file2);
        entry2.UpdateSyncStatus(SyncStatus.SourceModified);
        entry2.SaveMetadata();

        var entry3 = CreateEntryWithMetadata(file3);
        entry3.MarkSyncError("Error");
        entry3.SaveMetadata();

        // Act
        var status = await _vault.StatusAsync();

        // Assert
        status.TotalEntries.Should().Be(3);
        status.InSyncCount.Should().BeGreaterThanOrEqualTo(0);
        status.SourceModifiedCount.Should().BeGreaterThanOrEqualTo(0);
        status.ErrorCount.Should().Be(1);
    }

    [Fact]
    public async Task DetectChangesAsync_UpdatesEntrySyncStatus()
    {
        // Arrange
        var filePath = CreateTestFile("test.txt", "Original content");
        // Use Extracted stage so that SourceContentHash is set
        var entry = CreateEntryWithMetadataAtStage(filePath, ProcessingStage.Extracted);
        entry.MarkInSync();
        entry.SaveMetadata();

        // Modify the file
        File.WriteAllText(filePath, "Modified content that is different");

        // Setup git service to return no vault changes
        _gitServiceMock.StatusAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(new GitStatus { ModifiedFiles = [] });

        // Act
        var changes = await _vault.DetectChangesAsync(filePath);

        // Assert
        changes.SourceChanged.Should().BeTrue();
        changes.RecommendedAction.Should().Be(ChangeAction.Memorize);

        // Verify entry was updated
        var reloaded = await _vault.GetAsync(filePath);
        reloaded!.SyncStatus.Should().Be(SyncStatus.SourceModified);
    }

    #endregion

    #region SearchAsync strategy carrier (ISSUE-161)

    [Fact]
    public async Task SearchAsync_HybridRequest_WhenPipelineExecutesHybrid_SurfacesHybrid()
    {
        var file = CreateTestFile("strategy-hybrid.txt", "content");
        CreateEntryWithMetadataAtStage(file, ProcessingStage.Memorized);

        _pipelineMock
            .SearchAsync(Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<VaultSearchStrategy>(), Arg.Any<CancellationToken>())
            .Returns(new VaultPipelineSearchResponse([], VaultSearchStrategy.Hybrid));

        var result = await _vault.SearchAsync("query", new VaultSearchOptions { SearchStrategy = VaultSearchStrategy.Hybrid }, CancellationToken.None);

        result.RequestedStrategy.Should().Be(VaultSearchStrategy.Hybrid);
        result.ExecutedStrategy.Should().Be(VaultSearchStrategy.Hybrid);

        // The requested strategy must be forwarded to the pipeline (carrier wired through).
        await _pipelineMock.Received().SearchAsync(
            Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<int>(), Arg.Any<float>(),
            VaultSearchStrategy.Hybrid, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task SearchAsync_HybridRequest_WhenPipelineDegradesToVector_SurfacesVectorTruthfully()
    {
        var file = CreateTestFile("strategy-degrade.txt", "content");
        CreateEntryWithMetadataAtStage(file, ProcessingStage.Memorized);

        // Pipeline reports it actually ran vector (no IHybridSearchService registered downstream).
        _pipelineMock
            .SearchAsync(Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<VaultSearchStrategy>(), Arg.Any<CancellationToken>())
            .Returns(new VaultPipelineSearchResponse([], VaultSearchStrategy.Vector));

        var result = await _vault.SearchAsync("query", new VaultSearchOptions { SearchStrategy = VaultSearchStrategy.Hybrid }, CancellationToken.None);

        // No silent mismatch: requested=Hybrid but executed=Vector is reported honestly.
        result.RequestedStrategy.Should().Be(VaultSearchStrategy.Hybrid);
        result.ExecutedStrategy.Should().Be(VaultSearchStrategy.Vector);
    }

    #endregion

    #region SearchAsync cancellation propagation (ISSUE-163)

    [Fact]
    public async Task SearchAsync_WhenPipelineThrowsOCE_AndCallerTokenCancelled_PropagatesCancellation()
    {
        // Arrange - one memorized entry so SearchAsync reaches the pipeline
        var file = CreateTestFile("search-cancel.txt", "content");
        CreateEntryWithMetadataAtStage(file, ProcessingStage.Memorized);

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        _pipelineMock
            .SearchAsync(Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<VaultSearchStrategy>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException(cts.Token));

        // Act
        var act = async () => await _vault.SearchAsync("query", VaultSearchOptions.All(), cts.Token);

        // Assert - cancellation surfaces, NOT laundered into a silent error result
        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task SearchAsync_WhenOCE_ButCallerTokenNotCancelled_ReturnsErrorResult()
    {
        // Arrange - OCE not tied to the caller's token (e.g. an internal timeout)
        var file = CreateTestFile("search-internal-oce.txt", "content");
        CreateEntryWithMetadataAtStage(file, ProcessingStage.Memorized);

        _pipelineMock
            .SearchAsync(Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<VaultSearchStrategy>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());

        // Act - caller token is live; the when-filter must NOT rethrow
        var result = await _vault.SearchAsync("query", VaultSearchOptions.All(), CancellationToken.None);

        // Assert - genuine failure path still produces an error result, no throw
        result.Should().NotBeNull();
        result.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task SearchAsync_WhenPipelineThrowsGenericException_ReturnsErrorResult()
    {
        // Arrange
        var file = CreateTestFile("search-generic-error.txt", "content");
        CreateEntryWithMetadataAtStage(file, ProcessingStage.Memorized);

        _pipelineMock
            .SearchAsync(Arg.Any<string>(), Arg.Any<IEnumerable<string>?>(), Arg.Any<int>(), Arg.Any<float>(), Arg.Any<VaultSearchStrategy>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("boom"));

        // Act
        var result = await _vault.SearchAsync("query", VaultSearchOptions.All(), CancellationToken.None);

        // Assert - broad catch still converts real failures into an error result
        result.Should().NotBeNull();
        result.IsSuccess.Should().BeFalse();
        result.ErrorMessage.Should().Contain("boom");
    }

    #endregion

    #region Refresh feasibility

    // A refresh that the pipeline is guaranteed to reject must never be recommended: the job is
    // requeued on every scan, never succeeds, and each attempt replaces the entry's error with the
    // rejection — destroying the original diagnosis.

    [Fact]
    public async Task DetectChangesAsync_VaultModifiedWithoutRefinedContent_RecommendsMemorize()
    {
        // Arrange - a memorize that finds no indexable content skips refine, so the entry reaches
        // Memorized with no refined.md. Nothing about the stage reveals that.
        var filePath = CreateTestFile("no-content.pdf", "scanned, no text layer");
        var entry = CreateEntryWithMetadataAtStage(filePath, ProcessingStage.Memorized);
        entry.RefinedExists.Should().BeFalse("this is the state the empty-content path leaves behind");
        GivenModifiedVaultFiles("append-text.md");

        // Act
        var change = await _vault.DetectChangesAsync(filePath);

        // Assert
        change.VaultChanged.Should().BeTrue();
        change.RecommendedAction.Should().Be(ChangeAction.Memorize,
            "memorize re-extracts, which is what the rejection message prescribes");
    }

    [Fact]
    public async Task DetectChangesAsync_VaultModifiedWithRefinedContent_RecommendsRefresh()
    {
        // Arrange - the control: when a refresh can succeed, it is still the right recommendation.
        var filePath = CreateTestFile("refreshable.txt", "body");
        var entry = CreateEntryWithMetadataAtStage(filePath, ProcessingStage.Memorized);
        WriteRefinedContent(entry);
        GivenModifiedVaultFiles("qa.md");

        // Act
        var change = await _vault.DetectChangesAsync(filePath);

        // Assert
        change.RecommendedAction.Should().Be(ChangeAction.Refresh);
    }

    [Fact]
    public async Task DetectChangesAsync_VaultModifiedOnFailedEntry_RecommendsMemorizeSoItCanRecover()
    {
        // Arrange - Error sorts above every progress stage, so an ordered comparison reads a failed
        // entry as the most advanced one. It has no refined content and cannot be refreshed.
        var filePath = CreateTestFile("failed.pdf", "broken");
        CreateFailedEntry(filePath, "Extraction failed: ... [extraction_error_kind=PdfParse]");
        GivenModifiedVaultFiles("append-text.md");

        // Act
        var change = await _vault.DetectChangesAsync(filePath);

        // Assert
        change.VaultChanged.Should().BeTrue("a failed entry can still own modified vault files");
        change.RecommendedAction.Should().Be(ChangeAction.Memorize);
        change.RecommendedAction.Should().NotBe(ChangeAction.Refresh,
            "refresh would fail forever without ever recovering");
    }

    [Fact]
    public async Task DetectChangesAsync_FailedEntry_ReportsTheOriginalCauseAlongsideTheLatest()
    {
        // Arrange - the first failure carries the extractor's diagnosis; a later one usually reports a
        // downstream consequence. Consumers need the former to diagnose without the source file.
        var filePath = CreateTestFile("first-error.pdf", "broken");
        var entry = CreateFailedEntry(filePath, "Extraction failed: ... [extraction_error_kind=PdfParse]");
        entry.MarkError("No refined content found at ... Run memorize first.");
        entry.SaveMetadata();
        GivenModifiedVaultFiles();

        // Act
        var change = await _vault.DetectChangesAsync(filePath);

        // Assert
        change.FirstError.Should().Contain("extraction_error_kind=PdfParse");
        change.LastError.Should().Contain("Run memorize first");
    }

    [Fact]
    public async Task RefreshAsync_EntryWithoutRefinedContent_ThrowsAndSaysToMemorize()
    {
        // Arrange - the guard used to accept anything at or past Extracted, which the pipeline then
        // rejected. Guard and pipeline now enforce the same precondition.
        var filePath = CreateTestFile("guard.pdf", "scanned");
        CreateEntryWithMetadataAtStage(filePath, ProcessingStage.Memorized);

        // Act
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => _vault.RefreshAsync(filePath));

        // Assert
        ex.Message.Should().Contain("Run memorize first");
        await _queueServiceMock.DidNotReceive().EnqueueRefreshAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    #endregion

    #region Unreadable records

    [Fact]
    public async Task ListAsync_WhenOneRecordIsUnreadable_StillReturnsTheReadableEntries()
    {
        // Arrange
        var readable = CreateTestFile("readable.md", "content");
        CreateEntryWithMetadataAtStage(readable, ProcessingStage.Memorized);
        CorruptRecordFor(CreateTestFile("unreadable.md", "content"));

        // Act
        var entries = await _vault.ListAsync();

        // Assert - one damaged record must not take the rest of the listing with it.
        entries.Should().ContainSingle()
            .Which.SourcePath.Should().Be(Path.GetFullPath(readable));
    }

    [Fact]
    public async Task ListUnreadableAsync_ReportsTheEntryDirectoryThatCannotBeRead()
    {
        // Arrange
        CreateEntryWithMetadataAtStage(CreateTestFile("readable.md", "content"), ProcessingStage.Memorized);
        var damagedEntryPath = CorruptRecordFor(CreateTestFile("unreadable.md", "content"));

        // Act
        var unreadable = await _vault.ListUnreadableAsync();

        // Assert - a caller can only offer repair for what it has been told about.
        unreadable.Should().ContainSingle().Which.Should().Be(damagedEntryPath);
    }

    [Fact]
    public async Task MemorizeAsync_WhenTheExistingRecordIsUnreadable_RebuildsItInsteadOfFailing()
    {
        // Arrange - rewriting the record is what memorize is about to do anyway, so an unreadable
        // one is repairable here rather than fatal.
        var filePath = CreateTestFile("rebuild.md", "content");
        CorruptRecordFor(filePath);

        // Act
        var entry = await _vault.MemorizeAsync(filePath);

        // Assert
        entry.SourcePath.Should().Be(Path.GetFullPath(filePath));
        VaultEntry.LoadByHash(entry.FilepathHash, _vaultDir).Should().NotBeNull();
    }

    [Fact]
    public async Task ListAsync_AndListUnreadableAsync_TogetherAccountForEveryEntry()
    {
        // Arrange - a caller building a repair view compares the two lists, so an entry must appear
        // in exactly one of them. Anything skipped by both is invisible again.
        CreateEntryWithMetadataAtStage(CreateTestFile("first.md", "content"), ProcessingStage.Memorized);
        CreateEntryWithMetadataAtStage(CreateTestFile("second.md", "content"), ProcessingStage.Memorized);
        CorruptRecordFor(CreateTestFile("damaged.md", "content"));

        // Act
        var readable = await _vault.ListAsync();
        var unreadable = await _vault.ListUnreadableAsync();

        // Assert
        (readable.Count + unreadable.Count).Should().Be(Directory.GetDirectories(_vaultDir).Length);
    }

    private string CorruptRecordFor(string filePath)
    {
        var entry = VaultEntry.Create(filePath, _vaultDir);
        entry.SaveMetadata();
        File.WriteAllText(entry.MetaPath, "{ this is not a record");
        return entry.EntryPath;
    }

    #endregion
}
