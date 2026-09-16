using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Storage.SQLite;
using FluxIndex.Storage.SQLite.KeywordSearch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// Chunk identity against the README's real default stack (sqlite-vec store + hosted worker), the way
/// a consumer runs it: a re-memorize keeps the chunk ids it wrote, and a re-index that fails halfway
/// leaves the previous generation whole and searchable. Both depend on the store honouring the ids the
/// pipeline chooses — a store that mints its own would either duplicate rows or leave a half-written
/// generation behind, and nothing in the in-memory doubles can show that.
/// Deliberately not tagged <c>Category=Integration</c>: no external dependency, must run in CI.
/// </summary>
public sealed class ReadmeDefaultStackChunkIdentityTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-chunkid-" + Guid.NewGuid().ToString("N"));
    private readonly string _file;
    private readonly FailableEmbeddingService _embedder = new();

    private const string Paragraphs =
        "# Field manual\n\n" +
        "The generator must be refuelled every six hours during continuous operation; log each refuel in the shift book.\n\n" +
        "Coolant pressure is read from the gauge on the north panel; a reading below two bar means the pump has lost prime.\n\n" +
        "Radio checks are performed on the hour with the base station; a missed check is escalated after fifteen minutes.\n\n" +
        "The quartermaster issues replacement filters on Tuesdays; bring the serial number of the unit being serviced.\n\n" +
        "Night shifts hand over at the marmalade board, where open work orders are pinned until they are closed.\n";

    public ReadmeDefaultStackChunkIdentityTests()
    {
        Directory.CreateDirectory(_root);
        _file = Path.Combine(_root, "field-manual.md");
        File.WriteAllText(_file, Paragraphs);
    }

    [Fact]
    public async Task ReMemorizeOfAnUnchangedFile_KeepsTheSameChunkIds()
    {
        await using var provider = BuildStack();
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();

        var first = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        first.Stage.Should().Be(ProcessingStage.Memorized, because: first.LastError);
        first.ChunkCount.Should().BeGreaterThanOrEqualTo(3, "the manual must split into several chunks");
        var firstIds = await store.GetChunkIdsByDocumentIdAsync(first.FilepathHash, TestContext.Current.CancellationToken);
        firstIds.Should().HaveCount(first.ChunkCount);

        var second = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        second.Stage.Should().Be(ProcessingStage.Memorized, because: second.LastError);
        var secondIds = await store.GetChunkIdsByDocumentIdAsync(second.FilepathHash, TestContext.Current.CancellationToken);

        secondIds.Should().BeEquivalentTo(firstIds, "an unchanged document is the same chunks under the same ids");
        secondIds.Should().HaveCount(second.ChunkCount, "one row per chunk, no second copy");
    }

    [Fact]
    public async Task ReindexOfAChangedFile_ReplacesTheKeywordGenerationToo()
    {
        // The keyword leg has never been measured on a real store: the unit double shares ids with
        // the vector double by construction, and the facts above register no keyword service.
        await using var provider = BuildStack(withKeywordIndex: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();
        var keyword = scope.ServiceProvider.GetRequiredService<IKeywordSearchService>();

        var first = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        first.Stage.Should().Be(ProcessingStage.Memorized, because: first.LastError);
        var firstKeywordIds = await keyword.GetChunkIdsByDocumentIdAsync(first.FilepathHash, TestContext.Current.CancellationToken);
        firstKeywordIds.Should().BeEquivalentTo(
            await store.GetChunkIdsByDocumentIdAsync(first.FilepathHash, TestContext.Current.CancellationToken),
            "both legs are written under the ids the pipeline derives");

        await File.WriteAllTextAsync(_file,
            Paragraphs.Replace("marmalade board", "turquoise ledger", StringComparison.Ordinal),
            TestContext.Current.CancellationToken);
        var second = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        second.Stage.Should().Be(ProcessingStage.Memorized, because: second.LastError);

        var vectorIds = await store.GetChunkIdsByDocumentIdAsync(second.FilepathHash, TestContext.Current.CancellationToken);
        var keywordIds = await keyword.GetChunkIdsByDocumentIdAsync(second.FilepathHash, TestContext.Current.CancellationToken);
        keywordIds.Should().BeEquivalentTo(vectorIds, "one keyword row per chunk of the current generation, none of the previous one");
        keywordIds.Should().HaveCount(second.ChunkCount);

        var oldWording = await keyword.SearchAsync("marmalade", cancellationToken: TestContext.Current.CancellationToken);
        oldWording.Should().BeEmpty("the previous generation's passage must not answer keyword searches");
        var newWording = await keyword.SearchAsync("turquoise", cancellationToken: TestContext.Current.CancellationToken);
        newWording.Should().ContainSingle(r => r.Chunk.Content.Contains("turquoise", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ReindexOfADocumentWithKeywordRowsUnderForeignIds_RemovesThoseRowsAsWell()
    {
        // A vault written before the stores honoured caller ids holds keyword rows whose ids match
        // nothing in the vector store. Measured on such a vault: 167 keyword rows, 0 ids in common,
        // one more stale generation per re-index. The swap must enumerate the keyword leg's own ids,
        // not delete on it with the vector store's.
        await using var provider = BuildStack(withKeywordIndex: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();
        var keyword = scope.ServiceProvider.GetRequiredService<IKeywordSearchService>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);

        var legacyIds = new[] { Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
        await keyword.IndexChunksAsync(
        [
            new DocumentChunk { Id = legacyIds[0], DocumentId = entry.FilepathHash, ChunkIndex = 0, Content = "The generator must be refuelled every six hours (previous generation).", TokenCount = 10 },
            new DocumentChunk { Id = legacyIds[1], DocumentId = entry.FilepathHash, ChunkIndex = 1, Content = "Night shifts hand over at the marmalade board (previous generation).", TokenCount = 10 }
        ], TestContext.Current.CancellationToken);
        (await keyword.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken))
            .Should().Contain(legacyIds, "the legacy rows are in place before the re-index");

        await File.WriteAllTextAsync(_file,
            Paragraphs.Replace("marmalade board", "turquoise ledger", StringComparison.Ordinal),
            TestContext.Current.CancellationToken);
        var reindexed = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        reindexed.Stage.Should().Be(ProcessingStage.Memorized, because: reindexed.LastError);

        var vectorIds = await store.GetChunkIdsByDocumentIdAsync(reindexed.FilepathHash, TestContext.Current.CancellationToken);
        var keywordIds = await keyword.GetChunkIdsByDocumentIdAsync(reindexed.FilepathHash, TestContext.Current.CancellationToken);
        keywordIds.Should().NotContain(legacyIds, "a successful re-index supersedes every keyword row the new generation did not write");
        keywordIds.Should().BeEquivalentTo(vectorIds);
    }

    [Fact]
    public async Task Status_ReportsEachIndexLegsRowsAndTheEntriesWhereTheyDisagree()
    {
        // Nothing on the public surface showed the accumulation: the entry store counts chunks, not
        // rows, and the two legs were never compared. The status must make the drift visible before
        // a re-index and show it gone after one.
        await using var provider = BuildStack(withKeywordIndex: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var keyword = scope.ServiceProvider.GetRequiredService<IKeywordSearchService>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);

        var clean = await vault.AuditIndexAsync(TestContext.Current.CancellationToken);
        clean.IndexedChunkCount.Should().Be(entry.ChunkCount);
        clean.VectorRowCount.Should().Be(entry.ChunkCount);
        clean.KeywordRowCount.Should().Be(entry.ChunkCount);
        clean.MismatchedEntryCount.Should().Be(0);

        await keyword.IndexChunksAsync(
            [new DocumentChunk { Id = Guid.NewGuid().ToString(), DocumentId = entry.FilepathHash, ChunkIndex = 0, Content = "Radio checks on the hour (previous generation).", TokenCount = 5 }],
            TestContext.Current.CancellationToken);

        var drifted = await vault.AuditIndexAsync(TestContext.Current.CancellationToken);
        drifted.VectorRowCount.Should().Be(entry.ChunkCount);
        drifted.KeywordRowCount.Should().Be(entry.ChunkCount + 1, "the keyword leg carries a row the vector leg does not");
        drifted.MismatchedEntryCount.Should().Be(1);

        await File.WriteAllTextAsync(_file,
            Paragraphs.Replace("marmalade board", "turquoise ledger", StringComparison.Ordinal),
            TestContext.Current.CancellationToken);
        var reindexed = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        reindexed.Stage.Should().Be(ProcessingStage.Memorized, because: reindexed.LastError);

        var healed = await vault.AuditIndexAsync(TestContext.Current.CancellationToken);
        healed.KeywordRowCount.Should().Be(healed.VectorRowCount);
        healed.MismatchedEntryCount.Should().Be(0);
    }

    [Fact]
    public async Task RepairKeywordIndex_RebuildsADriftedKeywordLegFromTheVectorLeg_WithoutReembedding()
    {
        // A document indexed before the stores honoured caller ids and never re-indexed since keeps its
        // stale keyword rows forever. Re-memorizing it pays for every embedding again; the vector leg
        // already holds exactly the rows the keyword leg should.
        await using var provider = BuildStack(withKeywordIndex: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();
        var keyword = scope.ServiceProvider.GetRequiredService<IKeywordSearchService>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);

        // The legacy shape: the keyword leg holds the document only under ids the vector store never held.
        var vectorIds = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        await keyword.DeleteChunksAsync(vectorIds, TestContext.Current.CancellationToken);
        await keyword.IndexChunksAsync(
            [new DocumentChunk { Id = Guid.NewGuid().ToString(), DocumentId = entry.FilepathHash, ChunkIndex = 0, Content = "Coolant pressure gauge on the south wall (stale wording).", TokenCount = 8 }],
            TestContext.Current.CancellationToken);
        (await vault.AuditIndexAsync(TestContext.Current.CancellationToken)).MismatchedEntryCount.Should().Be(1);
        var embeddedBefore = _embedder.EmbeddedTexts;

        var result = await vault.RepairKeywordIndexAsync(TestContext.Current.CancellationToken);

        result.EntriesChecked.Should().Be(1);
        result.EntriesRepaired.Should().Be(1);
        result.KeywordRowsWritten.Should().Be(vectorIds.Count);
        result.KeywordRowsRemoved.Should().Be(1);
        _embedder.EmbeddedTexts.Should().Be(embeddedBefore, "the keyword leg is rebuilt from rows the vector leg already holds");

        var healed = await vault.AuditIndexAsync(TestContext.Current.CancellationToken);
        healed.MismatchedEntryCount.Should().Be(0);
        (await keyword.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken))
            .Should().BeEquivalentTo(vectorIds);

        var keywordOnly = new VaultSearchOptions { SearchStrategy = VaultSearchStrategy.Keyword, TopK = 10 };
        var current = await vault.SearchAsync("marmalade", keywordOnly, TestContext.Current.CancellationToken);
        current.Items.Should().Contain(i => i.Content != null && i.Content.Contains("marmalade", StringComparison.Ordinal),
            "the rebuilt keyword leg answers with the document's current wording");
        // Scoped to the file: the scope is a metadata filter on the keyword leg, so the rebuilt rows must carry
        // the filterable metadata the pipeline wrote, not just the text.
        var scoped = await vault.SearchAsync(
            "marmalade",
            new VaultSearchOptions { SearchStrategy = VaultSearchStrategy.Keyword, TopK = 10, PathScope = [_file] },
            TestContext.Current.CancellationToken);
        scoped.Items.Should().Contain(i => i.Content != null && i.Content.Contains("marmalade", StringComparison.Ordinal),
            "a file-scoped keyword search must still find the document after its keyword leg is rebuilt");
        scoped.ExecutedStrategy.Should().Be(VaultSearchStrategy.Keyword);

        var stale = await vault.SearchAsync("south wall", keywordOnly, TestContext.Current.CancellationToken);
        stale.Items.Should().NotContain(i => i.Content != null && i.Content.Contains("stale wording", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RepairKeywordIndex_All_RewritesAnEntryWhoseLegsAgree_RestoringTheFieldsFromTheVectorLeg()
    {
        // After a text-analyzer or keyword-field change every keyword row is present under the right
        // id and every one is written the old way. The id comparison sees nothing to do; re-memorizing
        // pays for extraction and embedding again. All copies the vector rows across once more -
        // metadata included, which is what the keyword fields (file name, title) are read from.
        await using var provider = BuildStack(withKeywordIndex: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();
        var keyword = scope.ServiceProvider.GetRequiredService<IKeywordSearchService>();

        // A file whose name shares no token with its body, so a hit on it can only come from the file-name field.
        var file = Path.Combine(_root, "zephyr-ledger.md");
        File.WriteAllText(file, Paragraphs);
        var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);

        var byFileName = new VaultSearchOptions { SearchStrategy = VaultSearchStrategy.Keyword, TopK = 10 };
        (await vault.SearchAsync("zephyr", byFileName, TestContext.Current.CancellationToken)).Items
            .Should().NotBeEmpty("fixture premise: the file name is a keyword field on a freshly indexed entry");

        // The old-configuration shape: the same rows under the same ids, written without the fields.
        var vectorIds = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        var bodiesOnly = (await store.GetByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken))
            .Select(c => new DocumentChunk { Id = c.Id, DocumentId = c.DocumentId, ChunkIndex = c.ChunkIndex, Content = c.Content, TokenCount = c.TokenCount })
            .ToList();
        await keyword.DeleteChunksAsync(vectorIds, TestContext.Current.CancellationToken);
        await keyword.IndexChunksAsync(bodiesOnly, TestContext.Current.CancellationToken);
        (await vault.SearchAsync("zephyr", byFileName, TestContext.Current.CancellationToken)).Items
            .Should().BeEmpty("fixture premise: rows written without metadata carry no file-name field");
        (await vault.AuditIndexAsync(TestContext.Current.CancellationToken)).MismatchedEntryCount
            .Should().Be(0, "fixture premise: the legs agree, which is exactly why the mismatch-only repair cannot help");
        var embeddedBefore = _embedder.EmbeddedTexts;

        var untouched = await vault.RepairKeywordIndexAsync(TestContext.Current.CancellationToken);
        untouched.EntriesRepaired.Should().Be(0, "the mismatch-only scope leaves agreeing legs alone");

        var result = await vault.RepairKeywordIndexAsync(KeywordIndexRepairScope.All, TestContext.Current.CancellationToken);

        result.EntriesChecked.Should().Be(1);
        result.EntriesRepaired.Should().Be(1);
        result.KeywordRowsWritten.Should().Be(vectorIds.Count);
        result.KeywordRowsRemoved.Should().Be(0, "every row was already present under the right id");
        _embedder.EmbeddedTexts.Should().Be(embeddedBefore, "the keyword leg is rebuilt from rows the vector leg already holds");
        (await vault.SearchAsync("zephyr", byFileName, TestContext.Current.CancellationToken)).Items
            .Should().NotBeEmpty("the rebuilt rows carry the metadata the vector store returned, so the file-name field is back");
    }

    [Fact]
    public async Task RepairKeywordIndex_LeavesEntriesWhoseLegsAgreeAlone()
    {
        await using var provider = BuildStack(withKeywordIndex: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);

        var result = await vault.RepairKeywordIndexAsync(TestContext.Current.CancellationToken);

        result.EntriesChecked.Should().Be(1);
        result.EntriesRepaired.Should().Be(0);
        result.KeywordRowsWritten.Should().Be(0);
        result.KeywordRowsRemoved.Should().Be(0);
    }

    [Fact]
    public async Task RepairKeywordIndex_WithoutAKeywordIndex_SaysSoInsteadOfReportingNothingToRepair()
    {
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var repair = () => vault.RepairKeywordIndexAsync(TestContext.Current.CancellationToken);

        await repair.Should().ThrowAsync<InvalidOperationException>().WithMessage("*keyword index*");
    }

    [Fact]
    public async Task Status_WithoutAKeywordIndex_ReportsThatLegAsAbsentNotEmpty()
    {
        await using var provider = BuildStack();
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);

        var status = await vault.AuditIndexAsync(TestContext.Current.CancellationToken);
        status.VectorRowCount.Should().Be(entry.ChunkCount);
        status.KeywordRowCount.Should().BeNull("no keyword index is registered — a zero would read as an empty index");
        status.MismatchedEntryCount.Should().Be(0, "one leg cannot disagree with itself");
    }

    [Fact]
    public async Task ReindexThatFailsMidway_LeavesThePreviousGenerationWholeAndSearchable()
    {
        await using var provider = BuildStack(o => o.EnableAutoRetry = false, withKeywordIndex: true);
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();
        var keyword = scope.ServiceProvider.GetRequiredService<IKeywordSearchService>();

        var entry = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);
        entry.ChunkCount.Should().BeGreaterThanOrEqualTo(3);
        var previousGeneration = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);

        // A keyword row from before the stores honoured caller ids: part of the previous generation
        // on that leg, under an id the vector store never held. The rollback must leave it too.
        var legacyId = Guid.NewGuid().ToString();
        await keyword.IndexChunksAsync(
            [new DocumentChunk { Id = legacyId, DocumentId = entry.FilepathHash, ChunkIndex = 0, Content = "Coolant pressure gauge (previous generation).", TokenCount = 5 }],
            TestContext.Current.CancellationToken);
        var previousKeywordGeneration = await keyword.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        previousKeywordGeneration.Should().Contain(legacyId);

        // Change the last paragraph and let the per-chunk worker path write at least one chunk before
        // the embedder throws: a genuine half-written generation.
        await File.WriteAllTextAsync(_file,
            Paragraphs.Replace("marmalade board", "turquoise ledger", StringComparison.Ordinal),
            TestContext.Current.CancellationToken);
        _embedder.FailOnSingleCall(2);

        var act = () => vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*failed*");

        var afterFailure = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        afterFailure.Should().BeEquivalentTo(previousGeneration,
            "the rollback removes what the failed run added and leaves the previous generation's rows in place");
        (await keyword.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken))
            .Should().BeEquivalentTo(previousKeywordGeneration,
                "the rollback deletes attempted minus previous: the keyword leg's previous generation, legacy ids included, is untouched");

        var oldWording = await vault.SearchAsync("marmalade board work orders", ct: TestContext.Current.CancellationToken);
        oldWording.Items.Should().Contain(i => i.Content != null && i.Content.Contains("marmalade", StringComparison.Ordinal),
            "the previous generation still answers searches");
        var newWording = await vault.SearchAsync("turquoise ledger work orders", ct: TestContext.Current.CancellationToken);
        newWording.Items.Should().NotContain(i => i.Content != null && i.Content.Contains("turquoise", StringComparison.Ordinal),
            "nothing of the failed generation may be searchable");

        // The retry lands the new generation exactly: the changed paragraph's old row is gone.
        _embedder.Disarm();
        var retried = await vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        retried.Stage.Should().Be(ProcessingStage.Memorized, because: retried.LastError);
        var newGeneration = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        newGeneration.Should().HaveCount(retried.ChunkCount);
        newGeneration.Should().NotBeEquivalentTo(previousGeneration, "at least one chunk changed");
        newGeneration.Intersect(previousGeneration).Should().NotBeEmpty("unchanged paragraphs keep their ids");
        var afterRetry = await vault.SearchAsync("turquoise ledger work orders", ct: TestContext.Current.CancellationToken);
        afterRetry.Items.Should().Contain(i => i.Content != null && i.Content.Contains("turquoise", StringComparison.Ordinal));
        (await keyword.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken))
            .Should().BeEquivalentTo(newGeneration, "the successful retry supersedes the keyword leg's previous generation, legacy ids included");
    }

    [Fact]
    public async Task FirstMemorizeThatFailsMidway_IsRetriedFromTheStart_AndLandsTheWholeDocument()
    {
        // The worker checkpoints after every stored chunk and the rollback removes those rows. The
        // automatic retry used to resume from the checkpoint and skip the chunks the rollback had
        // removed: the retry reported success with the first paragraphs missing.
        await using var provider = BuildStack(o =>
        {
            o.EnableAutoRetry = true;
            o.RetryDelayMs = 50;
        });
        await using var worker = await StartHostedServicesAsync(provider);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();
        var store = scope.ServiceProvider.GetRequiredService<IVectorStore>();

        // Fails once, on the second chunk of the first attempt; the retry's calls come after.
        _embedder.FailOnSingleCall(2);
        var act = () => vault.MemorizeAsync(_file, waitForCompletion: true, TestContext.Current.CancellationToken);
        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*failed*");

        var entry = await WaitForStageAsync(vault, ProcessingStage.Memorized, TimeSpan.FromSeconds(30));
        entry.ChunkCount.Should().BeGreaterThanOrEqualTo(3);
        var ids = await store.GetChunkIdsByDocumentIdAsync(entry.FilepathHash, TestContext.Current.CancellationToken);
        ids.Should().HaveCount(entry.ChunkCount, "the retry must land every chunk, including the ones the rollback removed");

        var firstParagraph = await vault.SearchAsync("generator refuelled every six hours", ct: TestContext.Current.CancellationToken);
        firstParagraph.Items.Should().Contain(i => i.Content != null && i.Content.Contains("refuelled", StringComparison.Ordinal),
            "the first chunk was rolled back after the failure and must be written again by the retry");
    }

    private async Task<FluxFeed.Domain.Entities.VaultEntry> WaitForStageAsync(IVault vault, ProcessingStage stage, TimeSpan timeout)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        FluxFeed.Domain.Entities.VaultEntry? entry = null;
        while (DateTimeOffset.UtcNow < deadline)
        {
            entry = await vault.GetAsync(_file, TestContext.Current.CancellationToken);
            if (entry?.Stage == stage)
                return entry;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }
        throw new TimeoutException($"entry did not reach {stage} within {timeout}; last stage {entry?.Stage}, error {entry?.LastError}");
    }

    private ServiceProvider BuildStack(Action<FileVaultOptions>? configure = null, bool withKeywordIndex = false)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        var databasePath = Path.Combine(_root, "fluxindex.db");
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = databasePath;
            o.VectorDimension = _embedder.GetEmbeddingDimension();
            o.FallbackToInMemoryOnError = false;
        });
        if (withKeywordIndex)
        {
            // The README's keyword leg on the same database, the way FluxIndexContext wires it.
            services.AddSingleton<IKeywordSearchService>(sp => new SQLiteKeywordSearchService(
                $"Data Source={databasePath}",
                sp.GetRequiredService<ILogger<SQLiteKeywordSearchService>>()));
        }
        services.AddSingleton<IEmbeddingService>(_embedder);
        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
            // Small chunks, split on paragraphs, so the manual is several rows and a mid-run failure
            // leaves a genuinely partial generation.
            o.Chunking.Strategy = "Paragraph";
            o.Chunking.MaxChunkSize = 64;
            o.Chunking.OverlapSize = 0;
            configure?.Invoke(o);
        });
        return services.BuildServiceProvider();
    }

    private static async Task<IAsyncDisposable> StartHostedServicesAsync(IServiceProvider provider)
    {
        var hosted = provider.GetServices<IHostedService>().ToList();
        hosted.Should().NotBeEmpty();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        return new HostedServicesLease(hosted);
    }

    private sealed class HostedServicesLease(IReadOnlyList<IHostedService> services) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in services)
                await service.StopAsync(cts.Token);
        }
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); }
        catch (IOException) { }
        catch (UnauthorizedAccessException) { }
    }
}
