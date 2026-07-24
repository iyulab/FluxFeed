using FluentAssertions;
using FluxFeed.Domain.Entities;
using FluxFeed.Domain.Enums;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Services;

/// <summary>
/// Pins the end-to-end path of extraction diagnostics through the vault pipeline: an extractor that
/// reports "no text layer" must leave that reason readable on the persisted entry, including on the
/// 0-chunk short-circuit where the pipeline never reaches chunking.
/// </summary>
public class VaultPipelineExtractionDiagnosticsTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;
    private readonly IGitService _git;

    public VaultPipelineExtractionDiagnosticsTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"VaultDiag_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_vaultDir);

        _git = Substitute.For<IGitService>();
        _git.InitAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("commit");

        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance,
            _git,
            MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, recursive: true); } catch (IOException) { }
        }
    }

    private VaultPipeline CreatePipeline(IExtractor extractor) => new(
        _git,
        new ContentHasher(),
        _storage,
        NullLogger<VaultPipeline>.Instance,
        options: MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }),
        extractor: extractor);

    private sealed class StubExtractor(ExtractionResult result) : IExtractor
    {
        public Task<ExtractionResult> ExtractAsync(string sourcePath, CancellationToken ct = default)
            => Task.FromResult(result);
    }

    private string CreateSourceFile(string name, string content = "binary-ish")
    {
        var path = Path.Combine(_testDir, name);
        File.WriteAllText(path, content);
        return path;
    }

    [Fact]
    public async Task MemorizeAsync_ScannedDocumentWithNoText_PersistsReasonOnZeroChunkEntry()
    {
        // Arrange — FileFlux 0.14.0's non-exception path: extraction succeeds with no text and a
        // structured reason. Previously the entry read as Memorized/0 chunks with no explanation.
        var sourcePath = CreateSourceFile("scan.pdf");
        var entry = VaultEntry.Create(sourcePath, _vaultDir);
        var pipeline = CreatePipeline(new StubExtractor(new ExtractionResult
        {
            Content = string.Empty,
            Hints = new Dictionary<string, string> { ["extraction_failure_reason"] = "no_text_layer" },
            Warnings = ["PDF appears to be an image-only/scanned document; OCR is required."]
        }));

        // Act
        var result = await pipeline.MemorizeAsync(entry);

        // Assert — user-visible outcome: the consumer can tell "scanned, needs OCR" from "empty file".
        result.Success.Should().BeTrue();
        result.ChunkCount.Should().Be(0);

        var reloaded = VaultEntry.Load(entry.EntryPath, _vaultDir);
        reloaded.Should().NotBeNull();
        reloaded!.Stage.Should().Be(ProcessingStage.Memorized);
        reloaded.ExtractionHints.Should().NotBeNull();
        reloaded.ExtractionHints!["extraction_failure_reason"].Should().Be("no_text_layer");
        reloaded.ExtractionWarnings.Should().ContainSingle()
            .Which.Should().Contain("OCR is required");
    }

    [Fact]
    public async Task MemorizeAsync_TextDocumentWithoutDiagnostics_LeavesDiagnosticsNull()
    {
        var sourcePath = CreateSourceFile("plain.txt", "hello world");
        var entry = VaultEntry.Create(sourcePath, _vaultDir);
        var pipeline = CreatePipeline(new StubExtractor(new ExtractionResult { Content = "hello world" }));

        await pipeline.MemorizeAsync(entry);

        var reloaded = VaultEntry.Load(entry.EntryPath, _vaultDir);
        reloaded!.ExtractionHints.Should().BeNull();
        reloaded.ExtractionWarnings.Should().BeNull();
    }

    [Fact]
    public async Task ExtractAsync_ReExtractionWithoutDiagnostics_ClearsStaleReason()
    {
        // The source is replaced by a text-bearing revision: the old "no_text_layer" must not linger.
        var sourcePath = CreateSourceFile("replaced.pdf");
        var entry = VaultEntry.Create(sourcePath, _vaultDir);
        await _storage.InitializeEntryAsync(entry);

        await CreatePipeline(new StubExtractor(new ExtractionResult
        {
            Content = string.Empty,
            Hints = new Dictionary<string, string> { ["extraction_failure_reason"] = "no_text_layer" }
        })).ExtractAsync(entry);

        entry.ExtractionHints.Should().NotBeNull();

        await CreatePipeline(new StubExtractor(new ExtractionResult { Content = "now has text" }))
            .ExtractAsync(entry);

        entry.ExtractionHints.Should().BeNull();
        VaultEntry.Load(entry.EntryPath, _vaultDir)!.ExtractionHints.Should().BeNull();
    }
}
