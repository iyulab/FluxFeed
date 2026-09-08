using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.ValueObjects;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// Deterministic, dependency-free <see cref="IEmbeddingService"/> for integration tests.
/// Hash-based embeddings give reproducible vectors so search assertions are stable across runs.
/// </summary>
internal sealed class DeterministicEmbeddingService : IEmbeddingService
{
    public const int Dimension = 128;
    private const int MaxTokens = 8192;

    public Task<float[]> GenerateEmbeddingAsync(string text, CancellationToken ct = default)
    {
        var embedding = new float[Dimension];
        var words = text.ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries);

        foreach (var word in words)
        {
            var hash = word.GetHashCode();
            for (int i = 0; i < Dimension; i++)
            {
                embedding[i] += MathF.Sin(hash * (i + 1) * 0.1f) * 0.1f;
            }
        }

        var norm = MathF.Sqrt(embedding.Sum(x => x * x));
        if (norm > 0)
        {
            for (int i = 0; i < Dimension; i++)
                embedding[i] /= norm;
        }

        return Task.FromResult(embedding);
    }

    public async Task<IEnumerable<float[]>> GenerateEmbeddingsBatchAsync(IEnumerable<string> texts, CancellationToken ct = default)
    {
        var results = new List<float[]>();
        foreach (var text in texts)
        {
            results.Add(await GenerateEmbeddingAsync(text, ct));
        }
        return results;
    }

    public int GetEmbeddingDimension() => Dimension;
    public string GetModelName() => "InMemoryTestEmbedding";
    public int GetMaxTokens() => MaxTokens;

    public EmbeddingIdentity GetIdentity() => new()
    {
        Provider = "InMemory",
        Model = GetModelName(),
        Dimension = Dimension
    };

    public Task<int> CountTokensAsync(string text, CancellationToken ct = default)
    {
        // Simple approximation: ~4 characters per token
        return Task.FromResult(text.Length / 4);
    }
}
