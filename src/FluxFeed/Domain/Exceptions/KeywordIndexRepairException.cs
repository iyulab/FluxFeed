using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;

namespace FluxFeed.Domain.Exceptions;

/// <summary>
/// One entry whose keyword leg could not be rewritten during a keyword-index repair.
/// </summary>
/// <param name="SourcePath">The entry's source path — pass it back to retry this entry alone.</param>
/// <param name="FilepathHash">The entry's document id in the index.</param>
/// <param name="Error">What failed.</param>
public sealed record KeywordIndexRepairFailure(string SourcePath, string FilepathHash, Exception Error);

/// <summary>
/// Thrown by a keyword-index repair when one or more entries failed. The repair does not stop at the first failure:
/// every other entry was rewritten, and <see cref="Result"/> counts them. <see cref="Failures"/> names the entries
/// that were not — each still holds its previous keyword rows (a repair writes before it removes) — so they can be
/// retried alone.
/// </summary>
public sealed class KeywordIndexRepairException : Exception
{
    /// <summary>Creates the exception from the entries that failed and the counts of the run.</summary>
    public KeywordIndexRepairException(KeywordIndexRepairResult result, IReadOnlyList<KeywordIndexRepairFailure> failures)
        : base(BuildMessage(result, failures), failures.Count > 0 ? failures[0].Error : null)
    {
        Result = result;
        Failures = failures;
    }

    /// <summary>The counts of the run, over the entries that succeeded.</summary>
    public KeywordIndexRepairResult Result { get; }

    /// <summary>The entries that were not rewritten, in the order they were attempted.</summary>
    public IReadOnlyList<KeywordIndexRepairFailure> Failures { get; }

    private static string BuildMessage(KeywordIndexRepairResult result, IReadOnlyList<KeywordIndexRepairFailure> failures)
    {
        var first = failures.Count > 0 ? $" First: {failures[0].SourcePath}: {failures[0].Error.Message}" : string.Empty;
        return $"Keyword index repair failed for {failures.Count} of {result.EntriesChecked} entries; " +
               $"{result.EntriesRepaired} other entries were rewritten. The failed entries keep their previous keyword rows " +
               $"and can be retried alone (Failures lists their source paths).{first}";
    }
}
