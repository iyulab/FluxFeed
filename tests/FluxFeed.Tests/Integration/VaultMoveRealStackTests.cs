using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxFeed.Services;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using FluxIndex.Storage.SQLite;
using FluxIndex.Storage.SQLite.KeywordSearch;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// Moving a tracked file on the README stack (sqlite-vec + relational keyword index + git-backed vault): the entry, its
/// history and every index row follow the file to its new path, and nothing is embedded again.
/// </summary>
public sealed class VaultMoveRealStackTests : IDisposable
{
    private const string Report = """
        # Quarterly report

        The harbor project finished ahead of schedule and under budget.

        ## Staffing

        Two engineers joined the lighthouse team in March.

        ## Risks

        Supply of copper cable remains the main risk for the next quarter.
        """;

    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-move-" + Guid.NewGuid().ToString("N"));
    private readonly CountingEmbeddingService _embedder = new(new DeterministicEmbeddingService());

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    private string WriteFile(string relativePath, string content)
    {
        var path = Path.Combine(_root, "files", relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, content);
        return path;
    }

    private static string MoveOnDisk(string from, string to)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(to)!);
        File.Move(from, to);
        return to;
    }

    [Fact]
    public async Task Move_ReKeysEveryLeg_KeepsHistory_AndEmbedsNothing()
    {
        await using var stack = await Stack.StartAsync(Path.Combine(_root, "stack"), _embedder);
        var vault = stack.Vault;
        var from = WriteFile(Path.Combine("inbox", "report.md"), Report);
        var memorized = await vault.MemorizeAsync(from, waitForCompletion: true, Ct);
        memorized.Stage.Should().Be(ProcessingStage.Memorized, memorized.LastError);
        var embeddingsBefore = _embedder.Calls;
        embeddingsBefore.Should().BePositive("the control: memorizing embeds, so a move that embedded would show up here");
        var historyBefore = await vault.LogAsync(from, 50, Ct);
        historyBefore.Should().NotBeEmpty("the memorize committed the refined content");

        var to = MoveOnDisk(from, Path.Combine(_root, "files", "archive", "2026", "renamed-report.md"));
        var result = await vault.MoveAsync(from, to, Ct);

        _embedder.Calls.Should().Be(embeddingsBefore, "a move re-keys the stored vectors instead of embedding again");
        result.IndexRekeyed.Should().BeTrue();
        result.VectorChunksMoved.Should().Be(memorized.ChunkCount).And.BePositive();
        result.KeywordChunksMoved.Should().Be(memorized.ChunkCount);
        result.FilepathHash.Should().Be(FilepathHasher.ComputeHash(to));

        (await vault.GetAsync(from, Ct)).Should().BeNull();
        var moved = await vault.GetAsync(to, Ct);
        moved.Should().NotBeNull();
        moved!.Stage.Should().Be(ProcessingStage.Memorized);
        moved.ChunkCount.Should().Be(memorized.ChunkCount);
        moved.Id.Should().Be(memorized.Id);
        (await vault.LogAsync(to, 50, Ct)).Select(c => c.Hash).Should().Equal(historyBefore.Select(c => c.Hash),
            "the entry's git repository moved with it");

        foreach (var strategy in new[] { VaultSearchStrategy.Vector, VaultSearchStrategy.Keyword, VaultSearchStrategy.Hybrid })
        {
            var inNewFolder = await vault.SearchAsync("copper cable risk",
                new VaultSearchOptions { SearchStrategy = strategy, TopK = 10, PathScope = [Path.GetDirectoryName(to)!] }, Ct);
            inNewFolder.Items.Should().NotBeEmpty($"{strategy} finds the moved chunks under the new path");
            inNewFolder.Items.Should().OnlyContain(i => i.SourcePath == to);

            var inOldFolder = await vault.SearchAsync("copper cable risk",
                new VaultSearchOptions { SearchStrategy = strategy, TopK = 10, PathScope = [Path.GetDirectoryName(from)!] }, Ct);
            inOldFolder.Items.Should().BeEmpty($"{strategy} finds nothing left under the old path");
        }

        var chunks = (await stack.VectorStore.GetByDocumentIdAsync(result.FilepathHash, Ct)).ToList();
        chunks.Should().HaveCount(memorized.ChunkCount);
        chunks.Should().OnlyContain(c =>
            Text(c.Metadata, "source_path") == to
            && Text(c.Metadata, "file_name") == "renamed-report.md"
            && Text(c.Metadata, "filepath_hash") == result.FilepathHash
            && Text(c.Metadata, "document_id") == result.FilepathHash);
        (await stack.VectorStore.GetChunkIdsByDocumentIdAsync(result.PreviousFilepathHash, Ct)).Should().BeEmpty();
        (await stack.KeywordIndex.GetChunkIdsByDocumentIdAsync(result.PreviousFilepathHash, Ct)).Should().BeEmpty();
    }

    // The map is right only if the moved chunks carry exactly the ids a memorize at the new path writes: then the next
    // memorize of that file is an update, not a second generation.
    [Fact]
    public async Task Move_GivesTheChunksTheIdsAFreshMemorizeAtTheNewPathWrites()
    {
        var from = WriteFile(Path.Combine("inbox", "report.md"), Report);
        var to = Path.Combine(_root, "files", "archive", "report.md");

        await using (var stack = await Stack.StartAsync(Path.Combine(_root, "moved"), _embedder))
        {
            (await stack.Vault.MemorizeAsync(from, waitForCompletion: true, Ct)).Stage.Should().Be(ProcessingStage.Memorized);
            MoveOnDisk(from, to);
            await stack.Vault.MoveAsync(from, to, Ct);
            var movedIds = await stack.VectorStore.GetChunkIdsByDocumentIdAsync(FilepathHasher.ComputeHash(to), Ct);
            var movedKeywordIds = await stack.KeywordIndex.GetChunkIdsByDocumentIdAsync(FilepathHasher.ComputeHash(to), Ct);

            await using var fresh = await Stack.StartAsync(Path.Combine(_root, "fresh"), new CountingEmbeddingService(new DeterministicEmbeddingService()));
            (await fresh.Vault.MemorizeAsync(to, waitForCompletion: true, Ct)).Stage.Should().Be(ProcessingStage.Memorized);
            var freshIds = await fresh.VectorStore.GetChunkIdsByDocumentIdAsync(FilepathHasher.ComputeHash(to), Ct);

            freshIds.Should().NotBeEmpty();
            movedIds.Should().BeEquivalentTo(freshIds);
            movedKeywordIds.Should().BeEquivalentTo(freshIds);
        }
    }

    [Fact]
    public async Task Move_WithAChunkWhoseIdCannotBeReproduced_Throws_AndChangesNothing()
    {
        await using var stack = await Stack.StartAsync(Path.Combine(_root, "stack"), _embedder);
        var from = WriteFile(Path.Combine("inbox", "report.md"), Report);
        var memorized = await stack.Vault.MemorizeAsync(from, waitForCompletion: true, Ct);
        var oldHash = memorized.FilepathHash;
        // A row under this document whose id does not follow from its content - as a store-minted id would.
        await stack.VectorStore.StoreAsync(new DocumentChunk
        {
            Id = Guid.NewGuid().ToString(),
            DocumentId = oldHash,
            ChunkIndex = 99,
            TotalChunks = 100,
            Content = "legacy row",
            Embedding = await _embedder.GenerateEmbeddingAsync("legacy row", Ct)
        }, Ct);
        var idsBefore = await stack.VectorStore.GetChunkIdsByDocumentIdAsync(oldHash, Ct);
        var to = MoveOnDisk(from, Path.Combine(_root, "files", "archive", "report.md"));

        var act = () => stack.Vault.MoveAsync(from, to, Ct);

        (await act.Should().ThrowAsync<InvalidOperationException>()).WithMessage("*cannot be re-keyed*");
        (await stack.Vault.GetAsync(from, Ct)).Should().NotBeNull("the entry directory was moved back");
        (await stack.Vault.GetAsync(to, Ct)).Should().BeNull();
        Directory.Exists(Path.Combine(stack.VaultBasePath, FilepathHasher.ComputeHash(to))).Should().BeFalse();
        (await stack.VectorStore.GetChunkIdsByDocumentIdAsync(oldHash, Ct)).Should().BeEquivalentTo(idsBefore);
        (await stack.Vault.LogAsync(from, 50, Ct)).Should().NotBeEmpty();
    }

    [Fact]
    public async Task MoveFolder_MovesEveryEntryUnderThePrefix_ReportsTheOneThatFailed_AndLeavesASiblingFolderAlone()
    {
        await using var stack = await Stack.StartAsync(Path.Combine(_root, "stack"), _embedder);
        var a = WriteFile(Path.Combine("docs", "a.md"), "# Alpha\n\nThe alpha note mentions the harbor.\n");
        var b = WriteFile(Path.Combine("docs", "sub", "b.md"), "# Bravo\n\nThe bravo note mentions the lighthouse.\n");
        var sibling = WriteFile(Path.Combine("docs2", "c.md"), "# Charlie\n\nThe charlie note mentions copper.\n");
        foreach (var path in new[] { a, b, sibling })
            (await stack.Vault.MemorizeAsync(path, waitForCompletion: true, Ct)).Stage.Should().Be(ProcessingStage.Memorized);
        var embeddingsBefore = _embedder.Calls;

        // b cannot be re-keyed: it holds a row whose id does not follow from its content.
        await stack.VectorStore.StoreAsync(new DocumentChunk
        {
            Id = Guid.NewGuid().ToString(),
            DocumentId = FilepathHasher.ComputeHash(b),
            ChunkIndex = 9,
            TotalChunks = 10,
            Content = "legacy row",
            Embedding = await _embedder.GenerateEmbeddingAsync("legacy row", Ct)
        }, Ct);
        embeddingsBefore = _embedder.Calls;

        var docs = Path.Combine(_root, "files", "docs");
        var moved = Path.Combine(_root, "files", "moved");
        Directory.Move(docs, moved);
        var result = await stack.Vault.MoveFolderAsync(docs, moved, Ct);

        result.IsSuccess.Should().BeFalse();
        result.Moved.Select(m => m.DestinationPath).Should().Equal(Path.Combine(moved, "a.md"));
        var error = result.Errors.Should().ContainSingle().Subject;
        error.SourcePath.Should().Be(b);
        error.DestinationPath.Should().Be(Path.Combine(moved, "sub", "b.md"));
        error.Exception.Should().BeOfType<InvalidOperationException>();
        (await stack.Vault.GetAsync(Path.Combine(moved, "a.md"), Ct)).Should().NotBeNull();
        (await stack.Vault.GetAsync(b, Ct)).Should().NotBeNull("the entry that failed stays tracked under its old path");
        (await stack.Vault.GetAsync(sibling, Ct)).Should().NotBeNull("docs2 is not under docs");
        _embedder.Calls.Should().Be(embeddingsBefore);
    }

    [Fact]
    public async Task Move_OntoATrackedPath_Throws_AndChangesNothing()
    {
        await using var stack = await Stack.StartAsync(Path.Combine(_root, "stack"), _embedder);
        var a = WriteFile("a.md", "# Alpha\n\nAlpha text about the harbor.\n");
        var b = WriteFile("b.md", "# Bravo\n\nBravo text about the lighthouse.\n");
        await stack.Vault.MemorizeAsync(a, waitForCompletion: true, Ct);
        await stack.Vault.MemorizeAsync(b, waitForCompletion: true, Ct);

        var act = () => stack.Vault.MoveAsync(a, b, Ct);

        await act.Should().ThrowAsync<InvalidOperationException>();
        (await stack.Vault.GetAsync(a, Ct)).Should().NotBeNull();
        (await stack.VectorStore.GetChunkIdsByDocumentIdAsync(FilepathHasher.ComputeHash(a), Ct)).Should().NotBeEmpty();
    }

    [Fact]
    public async Task Move_OfAnUntrackedPath_ThrowsKeyNotFound()
    {
        await using var stack = await Stack.StartAsync(Path.Combine(_root, "stack"), _embedder);

        var act = () => stack.Vault.MoveAsync(Path.Combine(_root, "nowhere.md"), Path.Combine(_root, "elsewhere.md"), Ct);

        await act.Should().ThrowAsync<KeyNotFoundException>();
    }

    private static string? Text(IReadOnlyDictionary<string, object>? metadata, string key) =>
        metadata != null && metadata.TryGetValue(key, out var value) ? value switch
        {
            System.Text.Json.JsonElement { ValueKind: System.Text.Json.JsonValueKind.String } json => json.GetString(),
            _ => value?.ToString()
        } : null;

    /// <summary>One README stack: sqlite-vec vectors, a relational keyword index in the same database, a git-backed vault.</summary>
    private sealed class Stack : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;
        private readonly IReadOnlyList<IHostedService> _hosted;
        private readonly IServiceScope _scope;

        private Stack(ServiceProvider provider, IReadOnlyList<IHostedService> hosted, string vaultBasePath)
        {
            _provider = provider;
            _hosted = hosted;
            _scope = provider.CreateScope();
            VaultBasePath = vaultBasePath;
        }

        public string VaultBasePath { get; }
        public IVault Vault => _scope.ServiceProvider.GetRequiredService<IVault>();
        public IVectorStore VectorStore => _scope.ServiceProvider.GetRequiredService<IVectorStore>();
        public IKeywordSearchService KeywordIndex => _scope.ServiceProvider.GetRequiredService<IKeywordSearchService>();

        public static async Task<Stack> StartAsync(string root, IEmbeddingService embedder)
        {
            Directory.CreateDirectory(root);
            var vaultBasePath = Path.Combine(root, ".vault");
            var services = new ServiceCollection();
            services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
            services.AddSQLiteVecVectorStore(o =>
            {
                o.DatabasePath = Path.Combine(root, "fluxindex.db");
                o.VectorDimension = embedder.GetEmbeddingDimension();
                o.FallbackToInMemoryOnError = false;
            });
            services.AddSingleton(embedder);
            services.AddSQLiteKeywordSearch();
            services.AddFileVaultWithFluxIndex(o =>
            {
                o.VaultBasePath = vaultBasePath;
                o.EnableRealTimeWatch = false;
            });
            var provider = services.BuildServiceProvider();
            var hosted = provider.GetServices<IHostedService>().ToList();
            foreach (var service in hosted)
                await service.StartAsync(CancellationToken.None);
            return new Stack(provider, hosted, vaultBasePath);
        }

        public async ValueTask DisposeAsync()
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in _hosted)
                await service.StopAsync(cts.Token);
            _scope.Dispose();
            await _provider.DisposeAsync();
        }
    }

    /// <summary>Counts every embedding request, single or batched — what a move must not make.</summary>
    private sealed class CountingEmbeddingService(IEmbeddingService inner) : IEmbeddingService
    {
        private int _calls;

        public int Calls => Volatile.Read(ref _calls);

        public Task<float[]> GenerateEmbeddingAsync(string text, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _calls);
            return inner.GenerateEmbeddingAsync(text, ct);
        }

        public Task<IEnumerable<float[]>> GenerateEmbeddingsBatchAsync(IEnumerable<string> texts, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _calls);
            return inner.GenerateEmbeddingsBatchAsync(texts, ct);
        }

        public int GetEmbeddingDimension() => inner.GetEmbeddingDimension();
        public string GetModelName() => inner.GetModelName();
        public int GetMaxTokens() => inner.GetMaxTokens();
        public EmbeddingIdentity GetIdentity() => inner.GetIdentity();
        public Task<int> CountTokensAsync(string text, CancellationToken ct = default) => inner.CountTokensAsync(text, ct);
    }
}
