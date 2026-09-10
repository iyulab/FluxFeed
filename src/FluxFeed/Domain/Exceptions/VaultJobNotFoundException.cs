namespace FluxFeed.Domain.Exceptions;

/// <summary>
/// Thrown when an operation names a job the queue does not hold.
/// </summary>
/// <remarks>
/// Distinct from a job that exists and cannot be acted on. An operator console showing a stale list
/// needs to refresh; one showing a job that is simply not in a retryable state needs to say why.
/// Both used to arrive as the same <c>false</c>.
/// </remarks>
public sealed class VaultJobNotFoundException : Exception
{
    public VaultJobNotFoundException(Guid jobId)
        : base($"Vault job not found: {jobId}")
    {
        JobId = jobId;
    }

    /// <summary>
    /// The job that was named.
    /// </summary>
    public Guid JobId { get; }
}
