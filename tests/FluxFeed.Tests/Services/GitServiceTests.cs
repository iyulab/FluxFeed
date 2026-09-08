using AwesomeAssertions;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace FluxFeed.Tests.Services;

public class GitServiceTests : IDisposable
{
    private readonly GitService _git;
    private readonly RecordingLogger _log = new();
    private readonly string _repoDir;

    public GitServiceTests()
    {
        _git = new GitService(_log);
        _repoDir = Path.Combine(Path.GetTempPath(), "GitServiceTests_" + Guid.NewGuid().ToString("N"));
    }

    [Fact]
    public async Task DiffLastChangeAsync_NoCommitsYet_ReturnsEmptyWithoutWarnings()
    {
        // A fresh entry whose memorize failed before its first commit is a normal state, not a
        // damaged repository: no "git command failed" must leak into the consumer's log.
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);

        var diff = await _git.DiffLastChangeAsync(_repoDir, ct: TestContext.Current.CancellationToken);

        diff.Should().BeEmpty();
        _log.Warnings.Should().BeEmpty();
    }

    [Fact]
    public async Task DiffLastChangeAsync_FirstCommit_DoesNotLogWarnings()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v1 content", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);

        var diff = await _git.DiffLastChangeAsync(_repoDir, ct: TestContext.Current.CancellationToken);

        diff.Should().Contain("+v1 content");
        _log.Warnings.Should().BeEmpty(because: "HEAD~1 is probed, not tried-and-failed");
    }

    /// <summary>Captures warnings so tests can assert that a normal path stays silent.</summary>
    private sealed class RecordingLogger : Microsoft.Extensions.Logging.ILogger<GitService>
    {
        public List<string> Warnings { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId,
            TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= Microsoft.Extensions.Logging.LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }

    public void Dispose()
    {
        if (Directory.Exists(_repoDir))
        {
            try { Directory.Delete(_repoDir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task ShowFileAsync_FileTrackedAtCommit_ReturnsThatCommitsContent()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v1 content", TestContext.Current.CancellationToken);
        var firstCommit = await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);
        firstCommit.Should().NotBeNull();

        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v2 content", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v2", TestContext.Current.CancellationToken);

        var atFirstCommit = await _git.ShowFileAsync(_repoDir, firstCommit!, "refined.md", TestContext.Current.CancellationToken);

        atFirstCommit.Should().Be("v1 content");
    }

    [Fact]
    public async Task ShowFileAsync_FileNotYetPresentAtCommit_ReturnsNull()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v1 content", TestContext.Current.CancellationToken);
        var firstCommit = await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);
        firstCommit.Should().NotBeNull();

        // append-text.md doesn't exist until a later commit
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "append-text.md"), "notes", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "add notes", TestContext.Current.CancellationToken);

        var atFirstCommit = await _git.ShowFileAsync(_repoDir, firstCommit!, "append-text.md", TestContext.Current.CancellationToken);

        atFirstCommit.Should().BeNull();
    }

    [Fact]
    public async Task ShowFileAsync_UnknownCommit_ReturnsNull()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v1 content", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);

        var result = await _git.ShowFileAsync(_repoDir, "0000000000000000000000000000000000dead", "refined.md", TestContext.Current.CancellationToken);

        result.Should().BeNull();
    }

    // FluxFeed docket #177: DiffAsync compares the working tree against the index/HEAD, so a caller
    // that auto-commits after every write (as VaultManager's Memorize/RefreshAsync does) sees an
    // empty diff almost every time -- the working tree is already clean by the time it checks.
    // DiffLastChangeAsync answers the actually-useful question ("what did the last commit change")
    // by comparing HEAD against its own parent instead.

    [Fact]
    public async Task DiffAsync_AfterCommit_ReturnsEmpty_WorkingTreeAlreadyClean()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v1 content", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);

        var diff = await _git.DiffAsync(_repoDir, ct: TestContext.Current.CancellationToken);

        diff.Should().BeEmpty();
    }

    [Fact]
    public async Task DiffLastChangeAsync_SecondCommit_ShowsWhatChanged()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v1 content", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);

        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v2 content", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v2", TestContext.Current.CancellationToken);

        var diff = await _git.DiffLastChangeAsync(_repoDir, ct: TestContext.Current.CancellationToken);

        diff.Should().Contain("-v1 content").And.Contain("+v2 content");
    }

    [Fact]
    public async Task DiffLastChangeAsync_FirstCommit_DiffsAgainstEmptyTree_NotEmpty()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "v1 content", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);

        // HEAD~1 doesn't exist yet -- must fall back to the empty-tree comparison rather than fail.
        var diff = await _git.DiffLastChangeAsync(_repoDir, ct: TestContext.Current.CancellationToken);

        diff.Should().Contain("+v1 content");
    }

    [Fact]
    public async Task DiffLastChangeAsync_ScopedToFile_OnlyShowsThatFilesChange()
    {
        await _git.InitAsync(_repoDir, TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "unrelated v1", TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "append-text.md"), "notes v1", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v1", TestContext.Current.CancellationToken);

        await File.WriteAllTextAsync(Path.Combine(_repoDir, "refined.md"), "unrelated v2", TestContext.Current.CancellationToken);
        await File.WriteAllTextAsync(Path.Combine(_repoDir, "append-text.md"), "notes v2", TestContext.Current.CancellationToken);
        await _git.CommitAsync(_repoDir, "v2", TestContext.Current.CancellationToken);

        var diff = await _git.DiffLastChangeAsync(_repoDir, "append-text.md", TestContext.Current.CancellationToken);

        diff.Should().Contain("notes v2").And.NotContain("unrelated");
    }
}
