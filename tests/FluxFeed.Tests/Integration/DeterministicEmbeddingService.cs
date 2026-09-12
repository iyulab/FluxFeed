using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.ValueObjects;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// Deterministic, dependency-free <see cref="IEmbeddingService"/> for integration tests.
/// </summary>
/// <remarks>
/// <para>
/// Bag-of-words: every term adds one count to the slot it always hashes to, and the vector is then
/// normalised. Two properties follow, and the search assertions in this project rely on both:
/// every vector is non-negative, so cosine similarity is never negative and a <c>minScore</c> of
/// zero never filters a stored chunk out; and two texts that share a term always score strictly
/// above zero.
/// </para>
/// <para>
/// The hash is FNV-1a over the string's code units, not <see cref="string.GetHashCode()"/>. .NET
/// randomises string hash codes per process, so an embedding built on <c>GetHashCode()</c> is
/// deterministic only within one <c>dotnet test</c> invocation: the same text embeds differently
/// in every run, and a cosine that is 0.42 in one process is negative in the next. That was the
/// whole history of the "flaky" search assertions here — five sightings across CI, Publish and
/// local runs whose only difference was the process, not the load or the test order.
/// </para>
/// </remarks>
internal sealed class DeterministicEmbeddingService : IEmbeddingService
{
    public const int Dimension = 128;
    private const int MaxTokens = 8192;

    private static readonly char[] TrimmedPunctuation = ['.', ',', ';', ':', '!', '?', '(', ')', '[', ']', '{', '}', '"', '\'', '`'];

    public Task<float[]> GenerateEmbeddingAsync(string text, CancellationToken ct = default)
    {
        var embedding = new float[Dimension];

        foreach (var word in Tokenize(text))
        {
            embedding[(int)(StableHash(word) % Dimension)] += 1f;
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

    /// <summary>
    /// Lower-cased terms split on any whitespace (a newline is a separator too — splitting on the
    /// space character alone glued the last word of one line to the first of the next), with
    /// surrounding punctuation removed so that "manipulation." and "manipulation" are one term.
    /// </summary>
    private static IEnumerable<string> Tokenize(string text) =>
        text.ToLowerInvariant()
            .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries)
            .Select(w => w.Trim(TrimmedPunctuation))
            .Where(w => w.Length > 0);

    /// <summary>FNV-1a, 32-bit: stable across processes, runtimes and machines.</summary>
    private static uint StableHash(string s)
    {
        unchecked
        {
            uint hash = 2166136261;
            foreach (var c in s)
            {
                hash ^= c;
                hash *= 16777619;
            }
            return hash;
        }
    }
}
