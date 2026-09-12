using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Core.Domain.ValueObjects;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// <see cref="DeterministicEmbeddingService"/> that can be armed to throw on the N-th single-chunk
/// embedding call, so a real-store test can produce a genuinely half-written generation on the hosted
/// worker's per-chunk path and watch the rollback undo it.
/// </summary>
internal sealed class FailableEmbeddingService : IEmbeddingService
{
    private readonly DeterministicEmbeddingService _inner = new();
    private int? _failOnCall;
    private int _calls;

    /// <summary>Throw on the <paramref name="call"/>-th (1-based) single embedding call from now.</summary>
    public void FailOnSingleCall(int call)
    {
        _calls = 0;
        _failOnCall = call;
    }

    public void Disarm() => _failOnCall = null;

    public Task<float[]> GenerateEmbeddingAsync(string text, CancellationToken ct = default)
    {
        _calls++;
        if (_calls == _failOnCall)
            throw new InvalidOperationException("embedding provider rejected a chunk");
        return _inner.GenerateEmbeddingAsync(text, ct);
    }

    public Task<IEnumerable<float[]>> GenerateEmbeddingsBatchAsync(IEnumerable<string> texts, CancellationToken ct = default)
        => _inner.GenerateEmbeddingsBatchAsync(texts, ct);

    public int GetEmbeddingDimension() => _inner.GetEmbeddingDimension();
    public string GetModelName() => _inner.GetModelName();
    public int GetMaxTokens() => _inner.GetMaxTokens();
    public EmbeddingIdentity GetIdentity() => _inner.GetIdentity();
    public Task<int> CountTokensAsync(string text, CancellationToken ct = default) => _inner.CountTokensAsync(text, ct);
}
