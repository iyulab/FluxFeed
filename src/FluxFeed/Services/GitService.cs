using System.Diagnostics;
using System.Text;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FluxFeed.Services;

/// <summary>
/// Git operations service using CLI commands (<see cref="FileVaultOptions.GitExecutablePath"/>).
/// When git cannot be started the service fails fast with an actionable exception; with
/// <see cref="FileVaultOptions.AllowMissingGit"/> it degrades instead — all operations become no-ops
/// and the vault keeps no history.
/// </summary>
public sealed partial class GitService : IGitService
{
    private readonly ILogger<GitService> _logger;
    private readonly string _gitExecutable;
    private readonly bool _allowMissingGit;
    private bool? _isAvailable;

    public GitService(ILogger<GitService> logger, IOptions<FileVaultOptions>? options = null)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        var opts = options?.Value ?? new FileVaultOptions();
        _gitExecutable = string.IsNullOrWhiteSpace(opts.GitExecutablePath) ? "git" : opts.GitExecutablePath;
        _allowMissingGit = opts.AllowMissingGit;
    }

    /// <inheritdoc />
    public bool IsAvailable => CheckGitAvailable();

    public async Task InitAsync(string vaultPath, CancellationToken ct = default)
    {
        if (!CheckGitAvailable()) return;

        if (IsGitRepository(vaultPath))
        {
            LogRepoAlreadyExists(_logger, vaultPath);
            return;
        }

        Directory.CreateDirectory(vaultPath);
        await RunGitAsync(vaultPath, "init", ct);

        // Configure user for this repo
        await RunGitAsync(vaultPath, "config user.email \"fluxindex@local\"", ct);
        await RunGitAsync(vaultPath, "config user.name \"FluxIndex Vault\"", ct);

        LogInitializedRepo(_logger, vaultPath);
    }

    public async Task StageAllAsync(string vaultPath, CancellationToken ct = default)
    {
        if (!CheckGitAvailable()) return;

        await RunGitAsync(vaultPath, "add -A", ct);
    }

    public async Task<string?> CommitAsync(string vaultPath, string message, CancellationToken ct = default)
    {
        if (!CheckGitAvailable()) return null;

        // Check if there are changes to commit
        var status = await StatusAsync(vaultPath, ct);
        if (!status.HasChanges)
        {
            LogNoChangesToCommit(_logger, vaultPath);
            return null;
        }

        await StageAllAsync(vaultPath, ct);

        // Escape message for command line
        var escapedMessage = message.Replace("\"", "\\\"");
        await RunGitAsync(vaultPath, $"commit -m \"{escapedMessage}\"", ct);

        // Get the commit hash
        var commitHash = await RunGitAsync(vaultPath, "rev-parse HEAD", ct);

        LogCommitted(_logger, vaultPath, message, commitHash[..7]);
        return commitHash;
    }

    public async Task<string> DiffAsync(string vaultPath, string? filePath = null, CancellationToken ct = default)
    {
        if (!CheckGitAvailable()) return string.Empty;

        var args = string.IsNullOrEmpty(filePath) ? "diff" : $"diff -- \"{filePath}\"";
        return await RunGitAsync(vaultPath, args, ct);
    }

    // The SHA of git's well-known empty tree object. Identical in every git repository (it's the
    // hash of an empty tree, not of any repository-specific content) — diffing against it is git's
    // standard trick for "what did this commit add", including for a repo's very first commit, where
    // HEAD~1 doesn't exist.
    private const string EmptyTreeSha = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

    public async Task<string> DiffLastChangeAsync(string vaultPath, string? filePath = null, CancellationToken ct = default)
    {
        if (!CheckGitAvailable() || !IsGitRepository(vaultPath)) return string.Empty;

        var pathArg = string.IsNullOrEmpty(filePath) ? "" : $" -- \"{filePath}\"";

        // Probe the revisions before diffing. `--verify --quiet` exits non-zero without writing to
        // stderr, so a missing revision is a plain branch here — not a logged "git command failed"
        // that consumers read as vault corruption (a fresh entry with 0 or 1 commits is normal).
        var (_, headExit) = await RunGitWithExitCodeAsync(vaultPath, "rev-parse --verify --quiet HEAD", ct);
        if (headExit != 0)
            return string.Empty; // no commits yet — nothing changed

        var (_, parentExit) = await RunGitWithExitCodeAsync(vaultPath, "rev-parse --verify --quiet HEAD~1", ct);
        // First commit: diff against git's empty tree so the result reads as "everything was added".
        var baseRevision = parentExit == 0 ? "HEAD~1" : EmptyTreeSha;

        var (output, exitCode) = await RunGitWithExitCodeAsync(vaultPath, $"diff {baseRevision} HEAD{pathArg}", ct);
        return exitCode == 0 ? output : string.Empty;
    }

    public async Task<GitStatus> StatusAsync(string vaultPath, CancellationToken ct = default)
    {
        if (!CheckGitAvailable() || !IsGitRepository(vaultPath))
        {
            return new GitStatus { HasChanges = false };
        }

        var output = await RunGitAsync(vaultPath, "status --porcelain", ct);

        var modified = new List<string>();
        var added = new List<string>();
        var deleted = new List<string>();

        foreach (var line in output.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            if (line.Length < 3) continue;

            var status = line[..2];
            var file = line[3..].Trim();

            if (status.Contains('M'))
                modified.Add(file);
            else if (status.Contains('A') || status.Contains('?'))
                added.Add(file);
            else if (status.Contains('D'))
                deleted.Add(file);
        }

        return new GitStatus
        {
            HasChanges = modified.Count > 0 || added.Count > 0 || deleted.Count > 0,
            ModifiedFiles = modified,
            AddedFiles = added,
            DeletedFiles = deleted
        };
    }

    public async Task<IReadOnlyList<GitCommit>> LogAsync(string vaultPath, int maxCount = 10, CancellationToken ct = default)
    {
        if (!CheckGitAvailable() || !IsGitRepository(vaultPath))
        {
            return [];
        }

        var output = await RunGitAsync(vaultPath, $"log --format=\"%H|%s|%aI\" -n {maxCount}", ct);

        var commits = new List<GitCommit>();
        foreach (var line in output.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            var parts = line.Split('|', 3);
            if (parts.Length >= 3)
            {
                commits.Add(new GitCommit
                {
                    Hash = parts[0],
                    Message = parts[1],
                    Timestamp = DateTimeOffset.TryParse(parts[2], out var ts) ? ts : DateTimeOffset.MinValue
                });
            }
        }

        return commits;
    }

    public async Task CheckoutFileAsync(string vaultPath, string filePath, string commitish, CancellationToken ct = default)
    {
        if (!CheckGitAvailable()) return;

        await RunGitAsync(vaultPath, $"checkout {commitish} -- \"{filePath}\"", ct);
        LogCheckedOut(_logger, filePath, commitish);
    }

    public async Task<string?> ShowFileAsync(string vaultPath, string commitish, string filePath, CancellationToken ct = default)
    {
        if (!CheckGitAvailable() || !IsGitRepository(vaultPath)) return null;

        var (output, exitCode) = await RunGitWithExitCodeAsync(vaultPath, $"show {commitish}:\"{filePath}\"", ct);
        return exitCode == 0 ? output : null;
    }

    public bool IsGitRepository(string vaultPath)
    {
        return Directory.Exists(Path.Combine(vaultPath, ".git"));
    }

    private bool CheckGitAvailable()
    {
        if (_isAvailable.HasValue) return _isAvailable.Value;

        try
        {
            var psi = new ProcessStartInfo(_gitExecutable, "--version")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var process = Process.Start(psi);
            process?.WaitForExit(5000);
            _isAvailable = process?.ExitCode == 0;
        }
        catch
        {
            _isAvailable = false;
        }

        if (!_isAvailable.Value)
        {
            if (!_allowMissingGit)
            {
                throw new InvalidOperationException(
                    $"Git executable '{_gitExecutable}' could not be started. FluxFeed keeps vault history through the git CLI " +
                    "(DiffAsync, LogAsync, GetContentAtCommitAsync). Install git and make sure it is on PATH, point " +
                    "FileVaultOptions.GitExecutablePath at it, or set FileVaultOptions.AllowMissingGit = true to run with a " +
                    "history-less vault.");
            }

            LogGitNotAvailable(_logger, _gitExecutable);
        }

        return _isAvailable.Value;
    }

    private async Task<string> RunGitAsync(string workingDirectory, string arguments, CancellationToken ct)
    {
        var (output, _) = await RunGitWithExitCodeAsync(workingDirectory, arguments, ct);
        return output;
    }

    private async Task<(string Output, int ExitCode)> RunGitWithExitCodeAsync(string workingDirectory, string arguments, CancellationToken ct)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = _gitExecutable,
            Arguments = arguments,
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
            StandardOutputEncoding = Encoding.UTF8,
            StandardErrorEncoding = Encoding.UTF8
        };

        using var process = new Process { StartInfo = startInfo };

        var output = new StringBuilder();
        var error = new StringBuilder();

        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data != null) output.AppendLine(e.Data);
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data != null) error.AppendLine(e.Data);
        };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        await process.WaitForExitAsync(ct);

        if (process.ExitCode != 0 && error.Length > 0)
        {
            var errorMessage = error.ToString().Trim();
            // Ignore "nothing to commit" type messages
            if (!errorMessage.Contains("nothing to commit") &&
                !errorMessage.Contains("Already on"))
            {
                LogGitCommandFailed(_logger, arguments, errorMessage);
            }
        }

        return (output.ToString().Trim(), process.ExitCode);
    }

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Debug, Message = "Git repository already exists at {Path}")]
    private static partial void LogRepoAlreadyExists(ILogger logger, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Initialized Git repository at {Path}")]
    private static partial void LogInitializedRepo(ILogger logger, string path);

    [LoggerMessage(Level = LogLevel.Debug, Message = "No changes to commit at {Path}")]
    private static partial void LogNoChangesToCommit(ILogger logger, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Committed changes at {Path}: {Message} ({Hash})")]
    private static partial void LogCommitted(ILogger logger, string path, string message, string hash);

    [LoggerMessage(Level = LogLevel.Information, Message = "Checked out {File} from {Commit}")]
    private static partial void LogCheckedOut(ILogger logger, string file, string commit);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Git command failed: git {Args} -> {Error}")]
    private static partial void LogGitCommandFailed(ILogger logger, string args, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Git executable {GitExecutable} could not be started; running with a history-less vault because FileVaultOptions.AllowMissingGit is set. Diff, log and commit are disabled.")]
    private static partial void LogGitNotAvailable(ILogger logger, string gitExecutable);

    #endregion
}
