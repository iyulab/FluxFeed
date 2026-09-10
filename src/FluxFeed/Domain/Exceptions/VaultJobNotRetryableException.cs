namespace FluxFeed.Domain.Exceptions;

/// <summary>
/// Why a job the queue holds cannot be run again on request.
/// </summary>
public enum VaultRetryRefusal
{
    /// <summary>
    /// The job is not in a failed state — it is queued, running, already re-queued by someone else,
    /// or cancelled. Nothing is wrong; the caller is looking at a stale view.
    /// </summary>
    NotFailed = 1,

    /// <summary>
    /// The job failed for a reason no attempt can change: no reader handles it, the archive is
    /// corrupt, the document is encrypted. Running it again produces the same failure and spends an
    /// attempt doing so.
    /// </summary>
    /// <remarks>
    /// This is the case that previously read as success. A permanent failure does not consume retry
    /// budget — the worker stops before the auto-retry branch — so the ordinary retry path saw a
    /// failed job with attempts to spare and re-queued it, and the operator saw a button that
    /// appeared to work.
    /// </remarks>
    PermanentFailure = 2
}

/// <summary>
/// Thrown when a job exists but the queue will not run it again.
/// </summary>
/// <remarks>
/// Budget exhaustion is deliberately absent from <see cref="VaultRetryRefusal"/>: an operator asking
/// for a job to be run again is not subject to the automatic retry budget, which exists to stop the
/// worker looping unattended. Exhausting it is precisely the condition under which someone reaches
/// for the button, so refusing there would mean the call only ever succeeded when it was not needed.
/// </remarks>
public sealed class VaultJobNotRetryableException : Exception
{
    public VaultJobNotRetryableException(Guid jobId, VaultRetryRefusal reason, string? detail = null)
        : base(BuildMessage(jobId, reason, detail))
    {
        JobId = jobId;
        Reason = reason;
    }

    /// <summary>
    /// The job that was named.
    /// </summary>
    public Guid JobId { get; }

    /// <summary>
    /// Which condition refused it. Callers exposing this over HTTP branch on this rather than on the
    /// message text.
    /// </summary>
    public VaultRetryRefusal Reason { get; }

    private static string BuildMessage(Guid jobId, VaultRetryRefusal reason, string? detail)
    {
        var head = reason switch
        {
            VaultRetryRefusal.NotFailed =>
                $"Vault job {jobId} is not in a failed state and cannot be run again.",
            VaultRetryRefusal.PermanentFailure =>
                $"Vault job {jobId} failed for a reason no attempt can change; running it again would fail identically.",
            _ => $"Vault job {jobId} cannot be run again."
        };

        return string.IsNullOrWhiteSpace(detail) ? head : $"{head} {detail}";
    }
}
