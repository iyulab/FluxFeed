using AwesomeAssertions;
using FluxFeed.Adapters;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.Entities;
using FluxIndex.Core.Domain.ValueObjects;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace FluxFeed.Tests.Adapters;

/// <summary>
/// <see cref="FluxIndexMemorizer"/> writes vectors through <see cref="IVectorStore.StoreBatchAsync"/> itself, so it records on
/// each the vector-space revision it was computed in — a store counts a vector without one as outside every space.
/// </summary>
public sealed class FluxIndexMemorizerVectorSpaceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"FluxFeedMemorizer_{Guid.NewGuid():N}");

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    [Fact]
    public async Task MemorizeFromVault_RecordsOnEveryStoredVector_TheRevisionItWasComputedIn()
    {
        var stored = new List<DocumentChunk>();
        var vectorStore = Substitute.For<IVectorStore>();
        vectorStore.StoreBatchAsync(Arg.Any<IEnumerable<DocumentChunk>>(), Arg.Any<CancellationToken>())
            .Returns(ci =>
            {
                var chunks = ((IEnumerable<DocumentChunk>)ci[0]).ToList();
                stored.AddRange(chunks);
                return Task.FromResult<IEnumerable<string>>(chunks.Select(c => c.Id).ToList());
            });
        var embedder = Substitute.For<IEmbeddingService>();
        embedder.GenerateEmbeddingsBatchAsync(Arg.Any<IEnumerable<string>>(), Arg.Any<CancellationToken>())
            .Returns(ci => Task.FromResult<IEnumerable<float[]>>(((IEnumerable<string>)ci[0]).Select(_ => new[] { 0.1f, 0.2f }).ToList()));
        embedder.GetIdentity().Returns(new EmbeddingIdentity { Provider = "Test", Model = "test", Dimension = 2, VectorSpaceRevision = "rev-1" });
        var memorizer = new FluxIndexMemorizer(
            vectorStore, embedder, Substitute.For<IVaultStorageService>(), NullLogger<FluxIndexMemorizer>.Instance);
        Directory.CreateDirectory(_dir);
        var source = Path.Combine(_dir, "notes.txt");
        await File.WriteAllTextAsync(source, "x", TestContext.Current.CancellationToken);

        await memorizer.MemorizeFromVaultAsync(
            VaultEntry.Create(source, Path.Combine(_dir, ".vault")), ["first passage", "second passage"], TestContext.Current.CancellationToken);

        stored.Should().HaveCount(2);
        stored.Should().OnlyContain(c => c.VectorSpaceRevision == "rev-1");
    }
}
