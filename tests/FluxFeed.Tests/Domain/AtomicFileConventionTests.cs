using System.Text.RegularExpressions;
using AwesomeAssertions;
using Xunit;

namespace FluxFeed.Tests.Domain;

/// <summary>
/// Every file a vault entry keeps is written through <c>AtomicFile</c>. The in-place rewrite it replaces
/// corrupted entries twice — first the metadata record, then the image manifest, after the first fix had
/// been scoped to the one file it was found in. This pins the rule to the source instead of to memory.
/// </summary>
public sealed partial class AtomicFileConventionTests
{
    /// <summary>
    /// Writes that are not entry artifacts. Each is listed with the reason it may write in place.
    /// </summary>
    private static readonly Dictionary<string, string> Allowed = new(StringComparer.Ordinal)
    {
        // None today. (The chunker's temporary input file went away when it began handing FileFlux the text directly.)
    };

    [Fact]
    public void Source_writes_whole_files_only_through_AtomicFile()
    {
        var sourceRoot = Path.Combine(FindRepositoryRoot(), "src", "FluxFeed");

        var offenders = Directory.EnumerateFiles(sourceRoot, "*.cs", SearchOption.AllDirectories)
            .Where(path => !IsBuildOutput(path))
            .Select(path => (Relative: Path.GetRelativePath(sourceRoot, path).Replace('\\', '/'), Text: File.ReadAllText(path)))
            .Where(file => file.Relative != "Domain/AtomicFile.cs" && !Allowed.ContainsKey(file.Relative))
            .Where(file => InPlaceWrite().IsMatch(file.Text))
            .Select(file => file.Relative)
            .ToList();

        offenders.Should().BeEmpty(
            "whole-file writes must go through AtomicFile so an overlapping writer cannot leave a blended file; " +
            "add a reasoned entry to Allowed only for a file nothing else reads or rewrites");
    }

    [Fact]
    public void Allowed_entries_still_write_in_place()
    {
        // An allowance that no longer applies is a hole waiting for the next write to slip through.
        var sourceRoot = Path.Combine(FindRepositoryRoot(), "src", "FluxFeed");

        foreach (var relative in Allowed.Keys)
        {
            var text = File.ReadAllText(Path.Combine(sourceRoot, relative));
            InPlaceWrite().IsMatch(text).Should().BeTrue($"{relative} is allowed to write in place but no longer does");
        }
    }

    [GeneratedRegex(@"\bFile\.(WriteAllText|WriteAllBytes|WriteAllLines|AppendAllText)(Async)?\s*\(")]
    private static partial Regex InPlaceWrite();

    private static bool IsBuildOutput(string path)
    {
        var normalized = path.Replace('\\', '/');
        return normalized.Contains("/bin/", StringComparison.Ordinal) || normalized.Contains("/obj/", StringComparison.Ordinal);
    }

    private static string FindRepositoryRoot()
    {
        // Walk up from the test output rather than using the compile-time source path, which a
        // deterministic CI build maps away.
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory != null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "FluxFeed.slnx")))
                return directory.FullName;
        }

        throw new InvalidOperationException("FluxFeed.slnx not found above the test output directory.");
    }
}
