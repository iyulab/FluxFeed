using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// <see cref="FileVaultOptions.MaxFileSizeMB"/> on the README stack: a file over the limit is skipped by a folder scan and
/// fails memorize permanently with the size in the message. The option was declared (default 100 MB, documented as
/// «larger files will be skipped») and read by nothing, so every file of any size was extracted.
/// </summary>
public sealed class MaxFileSizeRealStackTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-maxsize-" + Guid.NewGuid().ToString("N"));
    private readonly string _docs;
    private readonly DeterministicEmbeddingService _embedder = new();

    public MaxFileSizeRealStackTests()
    {
        _docs = Path.Combine(_root, "docs");
        Directory.CreateDirectory(_docs);
        File.WriteAllText(Path.Combine(_docs, "small.md"), "# Small\n\nA short note about the rollback command.\n");
        // Just over 1 MB of text.
        File.WriteAllText(Path.Combine(_docs, "large.md"), "# Large\n\n" + string.Concat(Enumerable.Repeat("filler line of text\n", 60_000)));
    }

    [Fact]
    public async Task MemorizeOverTheLimit_FailsWithTheSize_AndTheSmallFileStillIndexes()
    {
        await using var provider = BuildStack(maxFileSizeMB: 1);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var small = await vault.MemorizeAsync(Path.Combine(_docs, "small.md"), waitForCompletion: true, TestContext.Current.CancellationToken);
        small.Stage.Should().Be(ProcessingStage.Memorized, because: small.LastError);

        var act = async () => await vault.MemorizeAsync(Path.Combine(_docs, "large.md"), waitForCompletion: true, TestContext.Current.CancellationToken);
        (await act.Should().ThrowAsync<Exception>()).Which.Message.Should().Contain("MaxFileSizeMB");
    }

    [Fact]
    public async Task ScanOverTheLimit_SkipsTheLargeFile()
    {
        await using var provider = BuildStack(maxFileSizeMB: 1);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var scan = await vault.ScanFolderAsync(_docs, TestContext.Current.CancellationToken);

        scan.SkippedFilesCount.Should().Be(1);
        scan.DetectedChanges.Select(c => Path.GetFileName(c.FilePath)).Should().Equal(["small.md"]);
    }

    [Fact]
    public async Task ZeroLimit_MeansNoLimit()
    {
        await using var provider = BuildStack(maxFileSizeMB: 0);
        using var scope = provider.CreateScope();
        var vault = scope.ServiceProvider.GetRequiredService<IVault>();

        var scan = await vault.ScanFolderAsync(_docs, TestContext.Current.CancellationToken);

        scan.SkippedFilesCount.Should().Be(0);
    }

    private ServiceProvider BuildStack(int maxFileSizeMB)
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
            o.MaxFileSizeMB = maxFileSizeMB;
        });
        return services.BuildServiceProvider();
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
