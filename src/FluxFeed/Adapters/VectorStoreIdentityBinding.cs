using FluxIndex.Core.Application.Interfaces;

namespace FluxFeed.Adapters;

/// <summary>
/// Binds an <see cref="IVectorStore"/> to the identity of the <see cref="IEmbeddingService"/> it will
/// be used with. Stores that derive their physical layout from the embedding identity (for example
/// the sqlite-vec store names its vector table by the embedding fingerprint) refuse every operation
/// until <see cref="IVectorStore.BindIdentity"/> has been called; FluxFeed owns both halves of the
/// pair, so it performs the binding at the point where the pair is assembled rather than leaving it
/// to the consumer.
/// </summary>
internal static class VectorStoreIdentityBinding
{
    /// <summary>
    /// Binds <paramref name="vectorStore"/> to <paramref name="embeddingService"/>'s identity when
    /// both are present. Binding is idempotent for the same identity; a store already bound to a
    /// different identity throws (<c>EmbeddingModelMismatchException</c>), which is the correct
    /// outcome — the consumer swapped embedders over an existing index — and is left to propagate.
    /// </summary>
    public static void EnsureBound(IVectorStore? vectorStore, IEmbeddingService? embeddingService)
    {
        if (vectorStore is null || embeddingService is null)
            return;

        vectorStore.BindIdentity(embeddingService.GetIdentity());
    }
}
