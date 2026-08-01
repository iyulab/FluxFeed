namespace FluxFeed.Domain.Exceptions;

/// <summary>
/// Thrown when an entry's metadata record exists but cannot be read back.
/// </summary>
/// <remarks>
/// This is deliberately distinct from an absent record. An absent record is a normal state — the
/// entry simply does not exist. An unreadable one is a fault the caller may be able to repair, and
/// reporting it as "absent" makes the entry vanish from listings with no trace of why.
/// </remarks>
public sealed class VaultRecordUnreadableException : Exception
{
    public VaultRecordUnreadableException(string recordPath, Exception? innerException = null)
        : base($"Vault record cannot be read: {recordPath}", innerException)
    {
        RecordPath = recordPath;
    }

    /// <summary>
    /// Full path of the record that could not be read.
    /// </summary>
    public string RecordPath { get; }
}
