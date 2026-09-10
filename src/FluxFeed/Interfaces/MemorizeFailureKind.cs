namespace FluxFeed.Interfaces;

/// <summary>
/// Whether a failed memorize is worth attempting again.
/// </summary>
public enum MemorizeFailureKind
{
    /// <summary>
    /// Not recognised. Treated as retryable, so an unfamiliar failure keeps the behaviour it had
    /// before this classification existed rather than being silently dropped.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Expected to succeed on a later attempt - a provider timeout, a transport error, a file held
    /// open by another process.
    /// </summary>
    Transient = 1,

    /// <summary>
    /// Will fail identically however many times it is attempted - the file is gone, no reader
    /// handles the extension, the archive is corrupt. Retrying only occupies the queue.
    /// </summary>
    Permanent = 2
}

/// <summary>
/// Decides whether a failure is deterministic, from the exception type that produced it.
/// </summary>
/// <remarks>
/// <para>
/// This lives in the library rather than in each consumer because it is a fact about the pipeline,
/// not a policy: whether "no reader for this extension" can ever succeed on a second attempt does
/// not depend on who is calling. What <em>is</em> policy - how many attempts, how long to back off -
/// stays in <c>FileVaultOptions</c> where it already was.
/// </para>
/// <para>
/// Classification is by exception type name rather than by parsing the message. The pipeline
/// collapses a failure into a result and the caller re-wraps it, so the message that finally arrives
/// is several layers of prose away from the cause; the type name is not, and it does not change
/// wording between releases.
/// </para>
/// <para>
/// The list is deliberately conservative in one direction only: an unrecognised failure is
/// <see cref="MemorizeFailureKind.Unknown"/> and is retried. Being wrong about
/// <see cref="MemorizeFailureKind.Permanent"/> loses a recoverable job, which is worse than
/// retrying something that was never going to succeed.
/// </para>
/// </remarks>
public static class MemorizeFailureClassifier
{
    private static readonly HashSet<string> PermanentTypes = new(StringComparer.Ordinal)
    {
        nameof(FileNotFoundException),
        nameof(DirectoryNotFoundException),
        nameof(NotSupportedException),
        nameof(InvalidDataException),
        nameof(ArgumentException),
        nameof(ArgumentNullException),
        nameof(ArgumentOutOfRangeException),
        nameof(FormatException),
        nameof(PathTooLongException),
        // An encrypted document is the clearest permanent failure the pipeline sees: no attempt
        // supplies a password. Without this entry it classified as Unknown and was therefore
        // retried, which is exactly what a consumer measured - three attempts per protected file,
        // every time. The type is referenced rather than spelled as a string because FluxFeed
        // consumes FileFlux directly, so a rename cannot silently un-classify it.
        nameof(FileFlux.Core.EncryptedDocumentException)
    };

    private static readonly HashSet<string> TransientTypes = new(StringComparer.Ordinal)
    {
        nameof(HttpRequestException),
        nameof(TimeoutException),
        nameof(TaskCanceledException),
        // Kept after the more specific IO types above: matching is by exact name, so
        // FileNotFoundException does not fall in here through inheritance.
        nameof(IOException),
        "SocketException",
        "HttpIOException"
    };

    /// <summary>
    /// Classifies a failure from the name of the exception type that caused it.
    /// </summary>
    /// <param name="exceptionTypeName">
    /// An exception type name such as <c>TimeoutException</c>. <c>null</c> or unrecognised yields
    /// <see cref="MemorizeFailureKind.Unknown"/>.
    /// </param>
    public static MemorizeFailureKind Classify(string? exceptionTypeName)
    {
        if (string.IsNullOrEmpty(exceptionTypeName))
            return MemorizeFailureKind.Unknown;

        if (PermanentTypes.Contains(exceptionTypeName))
            return MemorizeFailureKind.Permanent;

        return TransientTypes.Contains(exceptionTypeName)
            ? MemorizeFailureKind.Transient
            : MemorizeFailureKind.Unknown;
    }
}
