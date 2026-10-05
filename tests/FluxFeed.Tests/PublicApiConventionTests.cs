using Iyu.Conventions.Testing;
using Xunit;

namespace FluxFeed.Tests;

/// <summary>
/// The public surface follows the two API rules of the ecosystem: every public async method takes a
/// <see cref="CancellationToken"/>, and failure is reported by an exception rather than by a returned object carrying a
/// success flag and an error. The scans are <c>Iyu.Conventions.Testing</c>'s, over the same assemblies as the
/// operational-language scan.
/// </summary>
/// <remarks>
/// The rosters are the methods that break a rule today. Shrink them; never grow them silently. A change to a listed
/// method's parameters changes its entry, which is a roster change on purpose.
/// </remarks>
public class PublicApiConventionTests
{
    // Disposal follows IAsyncDisposable.DisposeAsync, which takes no token: tearing a vault down is not a
    // cancellable operation (a half-disposed vault would leak its worker and its store handles).
    private static readonly string[] KnownUncancellable =
    [
        "FluxFeed.Interfaces.IVaultFactory.DisposeAllAsync()",
        "FluxFeed.Interfaces.IVaultFactory.DisposeAsync(String)",
        "FluxFeed.Interfaces.IVaultFactory.DisposeAsync(String, Boolean)",
    ];

    // Each of these returns a report, not a failure channel. A failure of the operation itself throws; what the object
    // carries is per-item outcome data the caller acts on item by item.
    private static readonly string[] KnownResultReturns =
    [
        // One entry per moved/not-moved file; the entries that failed stay tracked under their old path.
        "FluxFeed.Interfaces.IVault.MoveFolderAsync(String, String, CancellationToken)",
        // Counts of what a scan queued, with the files it could not read; IsSuccess is ErrorCount == 0.
        "FluxFeed.Interfaces.IVault.SyncAsync(CancellationToken)",
        "FluxFeed.Interfaces.IVault.SyncAsync(VaultJobPriority, CancellationToken)",
        // The queue worker's job outcome: it is recorded on the entry (stage, error, exception type) and the job
        // retried or parked, so a failed document is state, not an exception for the worker to unwind.
        "FluxFeed.Interfaces.IVaultPipeline.MemorizeAsync(VaultEntry, MemorizeOptions, CancellationToken)",
        "FluxFeed.Interfaces.IVaultPipeline.RefreshAsync(VaultEntry, MemorizeOptions, CancellationToken)",
    ];

    [Fact]
    public void PublicAsyncMethods_TakeACancellationToken() =>
        AsyncCancellation.Scan(OptionsReachabilityRosterTests.Libraries).ShouldMatchRoster(KnownUncancellable);

    [Fact]
    public void PublicMethods_DoNotReturnResultObjects() =>
        ResultReturns.Scan(OptionsReachabilityRosterTests.Libraries).ShouldMatchRoster(KnownResultReturns);

    // Positive control: an empty roster would also pass if the scan saw no public method at all.
    [Fact]
    public void Scan_SeesThePublicSurface() =>
        Assert.True(ResultReturns.Scan(OptionsReachabilityRosterTests.Libraries).MembersRead > 0, "the scan read too few public methods");
}
