using AwesomeAssertions;
using FileFlux.Core;
using FluxFeed.Adapters;
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
/// The vault keeps a document's tables as structured rows (one artifact per entry, rebuilt on re-extraction), and a
/// chunk holding table rows says which table it is, which piece and which rows — so a consumer can cite a table and,
/// if it chooses, read its rows instead of re-parsing chunk text.
/// </summary>
public sealed class VaultTablesTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _vaultDir;
    private readonly VaultStorageService _storage;
    private readonly IGitService _git;

    public VaultTablesTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), $"VaultTables_{Guid.NewGuid():N}");
        _vaultDir = Path.Combine(_testDir, ".vault");
        Directory.CreateDirectory(_vaultDir);
        _git = Substitute.For<IGitService>();
        _git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("commit");
        _storage = new VaultStorageService(
            NullLogger<VaultStorageService>.Instance, _git, MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }));
    }

    public void Dispose()
    {
        if (Directory.Exists(_testDir))
        {
            try { Directory.Delete(_testDir, recursive: true); } catch (IOException) { }
        }
    }

    private static TableArtifact Table(int index, string section = "Plan") => new()
    {
        Id = TableArtifact.IdFor(index),
        Index = index,
        Rows = [["Group", "Done"], ["Banks", "930"]],
        HeaderRows = 1,
        PageNumber = 2,
        Section = section,
        MergedCells = [new TableCellSpan(1, 2, 0, 0)],
    };

    private sealed class StubExtractor(ExtractionResult result) : IExtractor
    {
        public Task<ExtractionResult> ExtractAsync(string sourcePath, CancellationToken ct = default) => Task.FromResult(result);

        public FluxFeed.Domain.ValueObjects.ExtractionIdentity? Identity => null;
    }

    [Fact]
    public async Task Extraction_StoresTheTables_AndAReextractionWithoutTablesRemovesThem()
    {
        var source = Path.Combine(_testDir, "budget.xlsx");
        await File.WriteAllTextAsync(source, "x", TestContext.Current.CancellationToken);
        var entry = VaultEntry.Create(source, _vaultDir);

        await Pipeline(new ExtractionResult { Content = "| Group | Done |", Tables = [Table(0), Table(1, "Summary")] })
            .ExtractAsync(entry, TestContext.Current.CancellationToken);

        var stored = await _storage.GetTablesAsync(entry, TestContext.Current.CancellationToken);
        stored.Select(t => t.Id).Should().Equal("t000", "t001");
        stored[1].Section.Should().Be("Summary");
        stored[0].Columns.Should().Equal("Group", "Done");
        stored[0].MergedCells.Should().ContainSingle().Which.Should().Be(new TableCellSpan(1, 2, 0, 0));

        await Pipeline(new ExtractionResult { Content = "no tables any more" }).ExtractAsync(entry, TestContext.Current.CancellationToken);

        (await _storage.GetTablesAsync(entry, TestContext.Current.CancellationToken)).Should().BeEmpty();
        File.Exists(entry.ExtractedTablesPath).Should().BeFalse();
    }

    private VaultPipeline Pipeline(ExtractionResult result) => new(
        _git, new ContentHasher(), _storage, NullLogger<VaultPipeline>.Instance,
        options: MsOptions.Create(new FileVaultOptions { VaultBasePath = _vaultDir }),
        extractor: new StubExtractor(result));

    [Fact]
    public void TablePiece_TiedToItsStoredTable_CarriesTheTableKeys()
    {
        var chunk = new ContentChunk("| Group | Done |\n| --- | --- |\n| Banks | 930 |")
        {
            Location = new ContentLocation { StartPage = 2, EndPage = 2 },
            Table = new ContentTablePiece(TableIndex: 1, Piece: 2, Pieces: 3, RowStart: 10, RowEnd: 19),
        };

        var metadata = VaultPipeline.TextChunkMetadata(chunk, [Table(0), Table(1, "Summary")])!;

        metadata["chunk_kind"].Should().Be(VaultPipeline.TableChunkKind);
        metadata[VaultPipeline.TableIdMetadataKey].Should().Be("t001");
        metadata["table_piece"].Should().Be(2);
        metadata["table_pieces"].Should().Be(3);
        metadata["table_row_start"].Should().Be(10);
        metadata["table_row_end"].Should().Be(19);
        metadata["table_columns"].Should().Be("Group | Done");
        metadata["table_section"].Should().Be("Summary");
        metadata[VaultPipeline.PageNumberMetadataKey].Should().Be(2);
    }

    [Fact]
    public void TablePiece_WhenTheStoredTablesDoNotLineUp_IsMarkedButNotTiedToATable()
    {
        var chunk = new ContentChunk("| a | b |\n| --- | --- |") { Table = new ContentTablePiece(0, 1, 1, 0, 0) };

        var metadata = VaultPipeline.TextChunkMetadata(chunk, tables: null)!;

        metadata["chunk_kind"].Should().Be(VaultPipeline.TableChunkKind);
        metadata.Should().NotContainKey(VaultPipeline.TableIdMetadataKey);
    }

    [Fact]
    public void ProseChunk_HasNoTableKeys()
    {
        VaultPipeline.TextChunkMetadata(new ContentChunk("Just prose."), [Table(0)]).Should().BeNull();
    }

    [Fact]
    public void Extractor_MapsFileFluxTables_WithPositionIdsHeaderRowsAndSheet()
    {
        var table = new TableData
        {
            Cells = [["Basic", "", "Contact"], ["No", "Name", "Mail"], ["1", "Kim", "k@x"]],
            PageNumber = 1,
            Confidence = 0.5,
            DetectionMethod = TableDetectionMethod.Heuristic,
            MergedCells = [new MergedCell { StartRow = 0, EndRow = 0, StartCol = 0, EndCol = 1, Content = "Basic" }],
        };
        table.Props["header_rows"] = 2;
        table.Props["section_name"] = "Contacts";

        var artifact = FileFluxExtractor.ToTableArtifacts([new TableData { Cells = [["x"]] }, table])[1];

        artifact.Id.Should().Be("t001");
        artifact.HeaderRows.Should().Be(2);
        artifact.Columns.Should().Equal("No", "Name", "Mail");
        artifact.Section.Should().Be("Contacts");
        artifact.Confidence.Should().Be(0.5);
        artifact.DetectionMethod.Should().Be("Heuristic");
        artifact.MergedCells.Should().Equal(new TableCellSpan(0, 0, 0, 1));
    }

    [Fact]
    public void Chunker_ReadsTheTableAwareChunkKeys()
    {
        var piece = FileFluxChunker.ToTablePiece(new Dictionary<string, object>
        {
            ["table"] = true, ["table_index"] = 2, ["table_piece"] = 1, ["table_pieces"] = 4, ["table_row_start"] = 0, ["table_row_end"] = 24,
        });

        piece.Should().Be(new ContentTablePiece(2, 1, 4, 0, 24));
        FileFluxChunker.ToTablePiece(new Dictionary<string, object> { ["HierarchyLevel"] = 1 }).Should().BeNull();
    }
}
