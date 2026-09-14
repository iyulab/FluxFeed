namespace FluxFeed.Domain;

/// <summary>
/// Whole-file writes that readers and overlapping writers can only see as the old file or the new one.
/// </summary>
/// <remarks>
/// <para>
/// Writing in place truncates only when the file is opened, so two overlapping writers can leave the tail
/// of the longer content behind a shorter one — a JSON record in that state no longer parses, and every
/// later reader fails on it until someone deletes the file. Writing to a private temporary file and moving
/// it into place makes each write all-or-nothing: overlapping writers can only decide which version wins,
/// never blend two, and an interrupted write cannot leave a half-written file on disk.
/// </para>
/// <para>
/// Every file a vault entry keeps goes through here. The temporary file is created in a caller-chosen
/// scratch directory on the same volume — the entry directory — rather than next to the destination,
/// because some destinations live inside the entry's git working tree, which is staged with
/// <c>git add -A</c>.
/// </para>
/// <para>
/// Readers open with <see cref="FileShare.Delete"/> (<see cref="ReadAllTextAsync"/>) so that a replacement
/// can proceed underneath them.
/// </para>
/// </remarks>
internal static class AtomicFile
{
    /// <summary>
    /// Number of attempts to move a freshly written file into place. A sharing violation here is always
    /// momentary — another writer publishing its own version, or an external holder such as an indexer or
    /// backup agent — so a short bounded retry is enough.
    /// </summary>
    private const int PublishAttempts = 10;

    /// <summary>
    /// How long a replacement leftover is treated as possibly in flight. A replacement completes in
    /// microseconds, so anything older has been abandoned; the margin only has to exceed a stalled swap,
    /// not a stalled process.
    /// </summary>
    private static readonly TimeSpan AbandonedReplacementAge = TimeSpan.FromMinutes(1);

    public static void WriteAllText(string path, string content, string scratchDirectory)
    {
        var tempPath = PrepareTempPath(path, scratchDirectory);
        try
        {
            using (var fs = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            using (var writer = new StreamWriter(fs))
            {
                writer.Write(content);
            }

            for (var attempt = 1; ; attempt++)
            {
                if (TryPublish(tempPath, path, attempt))
                    return;

                Thread.Sleep(attempt);
            }
        }
        finally
        {
            Cleanup(tempPath, path);
        }
    }

    public static async Task WriteAllTextAsync(
        string path, string content, string scratchDirectory, CancellationToken ct = default)
    {
        var tempPath = PrepareTempPath(path, scratchDirectory);
        try
        {
            await using (var fs = new FileStream(
                tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true))
            await using (var writer = new StreamWriter(fs))
            {
                await writer.WriteAsync(content.AsMemory(), ct);
            }

            await PublishAsync(tempPath, path, ct);
        }
        finally
        {
            Cleanup(tempPath, path);
        }
    }

    public static async Task WriteAllBytesAsync(
        string path, byte[] content, string scratchDirectory, CancellationToken ct = default)
    {
        var tempPath = PrepareTempPath(path, scratchDirectory);
        try
        {
            await using (var fs = new FileStream(
                tempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true))
            {
                await fs.WriteAsync(content, ct);
            }

            await PublishAsync(tempPath, path, ct);
        }
        finally
        {
            Cleanup(tempPath, path);
        }
    }

    /// <summary>
    /// Reads a file that may be replaced concurrently, without blocking the replacement.
    /// </summary>
    public static async Task<string> ReadAllTextAsync(string path, CancellationToken ct = default)
    {
        await using var fs = OpenForSharedRead(path);
        using var reader = new StreamReader(fs);
        return await reader.ReadToEndAsync(ct);
    }

    /// <inheritdoc cref="ReadAllTextAsync"/>
    public static async Task<byte[]> ReadAllBytesAsync(string path, CancellationToken ct = default)
    {
        await using var fs = OpenForSharedRead(path);
        using var buffer = new MemoryStream();
        await fs.CopyToAsync(buffer, ct);
        return buffer.ToArray();
    }

    private static FileStream OpenForSharedRead(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, bufferSize: 4096, useAsync: true);

    private static string PrepareTempPath(string path, string scratchDirectory)
    {
        Directory.CreateDirectory(scratchDirectory);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        return Path.Combine(scratchDirectory, $"{Path.GetFileName(path)}.{Guid.NewGuid():N}.tmp");
    }

    private static async Task PublishAsync(string tempPath, string path, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            if (TryPublish(tempPath, path, attempt))
                return;

            await Task.Delay(attempt, ct);
        }
    }

    /// <summary>
    /// Moves a freshly written file over the live one. Returns false for a momentary sharing violation
    /// while attempts remain; the last attempt's failure propagates.
    /// </summary>
    private static bool TryPublish(string tempPath, string path, int attempt)
    {
        try
        {
            if (File.Exists(path))
            {
                // Replace tolerates a destination that is still open elsewhere, which a plain overwriting
                // move does not — readers hold the file open with FileShare.Delete precisely so a rewrite
                // can proceed underneath them.
                File.Replace(tempPath, path, destinationBackupFileName: null);
            }
            else
            {
                // First write: there is nothing to replace. A concurrent writer may still win the race to
                // create it, which surfaces as an IOException and is retried.
                File.Move(tempPath, path);
            }

            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException && attempt < PublishAttempts)
        {
            return false;
        }
    }

    private static void Cleanup(string tempPath, string path)
    {
        DeleteIfPresent(tempPath);
        SweepAbandonedReplacements(path);
    }

    /// <summary>
    /// Removes scratch copies the platform's atomic replacement left behind.
    /// </summary>
    /// <remarks>
    /// The replacement keeps the outgoing file under a scratch name next to the destination while it swaps,
    /// and abandons that name when it cannot clean up — a reader holding the outgoing file open is enough.
    /// Without this the directory grows a stale copy per abandoned swap.
    ///
    /// Only leftovers old enough that no swap could still be using them are removed: a replacement that
    /// fails partway rolls back onto its scratch copy, so deleting a live one would destroy the file it was
    /// protecting. The scratch name is a platform detail, so a platform that names them differently simply
    /// finds nothing to sweep — nothing depends on the match.
    /// </remarks>
    private static void SweepAbandonedReplacements(string path)
    {
        try
        {
            var directory = Path.GetDirectoryName(path);
            if (directory is null || !Directory.Exists(directory))
                return;

            var cutoff = DateTime.UtcNow - AbandonedReplacementAge;
            foreach (var leftover in Directory.EnumerateFiles(directory, $"{Path.GetFileName(path)}~RF*.TMP"))
            {
                if (File.GetLastWriteTimeUtc(leftover) < cutoff)
                {
                    DeleteIfPresent(leftover);
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Housekeeping must never fail the write it rode in on.
        }
    }

    /// <summary>
    /// Removes a leftover temporary file without masking the outcome of the write that produced it.
    /// </summary>
    private static void DeleteIfPresent(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        try
        {
            File.Delete(path);
        }
        catch (IOException)
        {
            // A stray temporary file is not worth replacing the caller's exception with.
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
