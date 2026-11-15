using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxFeed.Services;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// A document is most often asked about while the program that edits it still has it open, and on Windows an editor
/// holds its file open for writing. Memorize used to open the source asking to deny writers - first for the content
/// hash, then to read the text - and was refused with «being used by another process» exactly then.
/// </summary>
public sealed class FilesHeldOpenForWritingTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-heldopen-" + Guid.NewGuid().ToString("N"));
    private readonly DeterministicEmbeddingService _embedder = new();

    public FilesHeldOpenForWritingTests() => Directory.CreateDirectory(Path.Combine(_root, "docs"));

    // No extractor is registered, so the text is read by the pipeline's own fallback: both of FluxFeed's reads of the source
    // (the content hash, then the text) are exercised here. The read the registered extractor makes is that package's own.
    [Fact]
    public async Task Memorize_IndexesAFile_AnEditorHoldsOpenForWriting_OnTheFallbackReadPath()
    {
        var path = Path.Combine(_root, "docs", "plan.md");
        File.WriteAllText(path, "# Plan\n\nThe rollback command restores the previous release.\n");
        await using var provider = BuildStack();
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        using var editor = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        var entry = await vault.MemorizeAsync(path, waitForCompletion: true, TestContext.Current.CancellationToken);

        entry.Stage.Should().Be(ProcessingStage.Memorized, because: entry.LastError);
    }

    [Fact]
    public async Task TheContentHash_OfAHeldOpenFile_IsTheHashOfItsContent()
    {
        var path = Path.Combine(_root, "docs", "held.md");
        File.WriteAllText(path, "# Held\n");
        var hasher = new ContentHasher();
        var closed = await hasher.ComputeHashAsync(path, TestContext.Current.CancellationToken);

        using var editor = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
        var held = await hasher.ComputeHashAsync(path, TestContext.Current.CancellationToken);

        held.Should().Be(closed);
    }

    private ServiceProvider BuildStack()
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = Path.Combine(_root, "fluxindex.db");
            o.VectorDimension = _embedder.GetEmbeddingDimension();
            o.FallbackToInMemoryOnError = false;
        });
        services.AddSingleton<IEmbeddingService>(_embedder);
        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
            o.EnableBackgroundProcessing = false;
        });
        services.RemoveAll<IExtractor>();
        return services.BuildServiceProvider();
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
