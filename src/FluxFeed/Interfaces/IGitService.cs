namespace FluxFeed.Interfaces;

/// <summary>
/// Service for Git operations on vault entries.
/// Each vault entry ({hash}/) has its own Git repository.
/// </summary>
public interface IGitService
{
    /// <summary>
    /// Indicates whether the git CLI is available on this system.
    /// When false, all git operations degrade gracefully to no-ops.
    /// </summary>
    bool IsAvailable { get; }

    /// <summary>
    /// Initializes a new Git repository in the vault entry directory.
    /// </summary>
    Task InitAsync(string vaultPath, CancellationToken ct = default);

    /// <summary>
    /// Stages all changes in the vault entry.
    /// </summary>
    Task StageAllAsync(string vaultPath, CancellationToken ct = default);

    /// <summary>
    /// Creates a commit with the given message.
    /// </summary>
    /// <returns>The commit hash if a commit was created, null if no changes to commit.</returns>
    Task<string?> CommitAsync(string vaultPath, string message, CancellationToken ct = default);

    /// <summary>
    /// Gets the diff between the working tree and the index/HEAD for a specific file or all files —
    /// i.e. uncommitted changes only. Callers that auto-commit after every write (as
    /// <see cref="FluxFeed.Services.VaultManager"/>'s Memorize/Refresh path does) will normally see an
    /// empty diff here, since the working tree is already clean by the time this runs — use
    /// <see cref="DiffLastChangeAsync"/> to see what the most recent commit actually changed.
    /// </summary>
    Task<string> DiffAsync(string vaultPath, string? filePath = null, CancellationToken ct = default);

    /// <summary>
    /// Gets the diff between HEAD and the commit before it (or, on a repository with only one
    /// commit, between HEAD and the empty tree) for a specific file or all files — "what did the
    /// most recent commit change", the question <see cref="DiffAsync"/> can't answer once a caller
    /// has already committed. Returns an empty string when git is unavailable, the directory isn't a
    /// repository, or HEAD itself doesn't exist yet (zero commits).
    /// </summary>
    Task<string> DiffLastChangeAsync(string vaultPath, string? filePath = null, CancellationToken ct = default);

    /// <summary>
    /// Gets the status of the repository.
    /// </summary>
    Task<GitStatus> StatusAsync(string vaultPath, CancellationToken ct = default);

    /// <summary>
    /// Gets the commit log.
    /// </summary>
    Task<IReadOnlyList<GitCommit>> LogAsync(string vaultPath, int maxCount = 10, CancellationToken ct = default);

    /// <summary>
    /// Checks out a file from a specific commit.
    /// </summary>
    Task CheckoutFileAsync(string vaultPath, string filePath, string commitish, CancellationToken ct = default);

    /// <summary>
    /// Reads a tracked file's content as it existed at a specific commit, without touching the
    /// working tree. Returns null when the file did not exist in that commit, the commit is
    /// unknown, or git is unavailable — distinct from an empty string, which means the file existed
    /// and was empty.
    /// </summary>
    Task<string?> ShowFileAsync(string vaultPath, string commitish, string filePath, CancellationToken ct = default);

    /// <summary>
    /// Checks if the directory is a Git repository.
    /// </summary>
    bool IsGitRepository(string vaultPath);
}

/// <summary>
/// Git repository status.
/// </summary>
public sealed class GitStatus
{
    public bool HasChanges { get; init; }
    public IReadOnlyList<string> ModifiedFiles { get; init; } = [];
    public IReadOnlyList<string> AddedFiles { get; init; } = [];
    public IReadOnlyList<string> DeletedFiles { get; init; } = [];
}

/// <summary>
/// Git commit information.
/// </summary>
public sealed class GitCommit
{
    public string Hash { get; init; } = "";
    public string ShortHash => Hash.Length > 7 ? Hash[..7] : Hash;
    public string Message { get; init; } = "";
    public DateTimeOffset Timestamp { get; init; }
}
