using System.Collections.Concurrent;

namespace FluxFeed.Services;

/// <summary>
/// Remembers which documents <see cref="VaultPipeline"/> has already examined for the
/// <c>document_id</c> scope tag, so the one-time migration of chunks that predate the tag reads each
/// document once per process rather than once per pipeline instance.
/// </summary>
/// <remarks>
/// The pipeline is registered scoped — it may depend on a scoped vector store — so anything it
/// remembers in a field lasts one scope: for a web or MCP host, one request. Registered as a
/// singleton by <c>AddFileVault</c>; a pipeline constructed without one keeps a private instance,
/// which is the per-instance behaviour.
/// </remarks>
public sealed class VaultScopeTagBackfillState
{
    private readonly ConcurrentDictionary<string, byte> _checkedDocuments = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Claims a document for examination; false when it was already examined.</summary>
    internal bool TryBegin(string documentId) => _checkedDocuments.TryAdd(documentId, 0);

    /// <summary>Forgets a document whose migration did not complete, so the next search retries it.</summary>
    internal void Forget(string documentId) => _checkedDocuments.TryRemove(documentId, out _);
}
