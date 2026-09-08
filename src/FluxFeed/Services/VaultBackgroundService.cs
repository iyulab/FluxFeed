using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace FluxFeed.Services;

/// <summary>
/// Hosted adapter that runs a <see cref="VaultQueueWorker"/> over the container-registered
/// <see cref="IVaultQueueService"/> for the application's lifetime. Jobs are processed through the
/// scoped <see cref="IVaultPipeline"/> (one DI scope per job). Tenant vaults created by
/// <see cref="VaultFactory"/> run their own worker; this service only consumes the container queue.
/// </summary>
public sealed class VaultBackgroundService : BackgroundService
{
    private readonly VaultQueueWorker _worker;

    public VaultBackgroundService(
        ILogger<VaultBackgroundService> logger,
        IVaultQueueService queueService,
        IServiceScopeFactory scopeFactory,
        IVaultStorageService storage,
        IOptions<FileVaultOptions> options)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _worker = new VaultQueueWorker(
            logger,
            queueService ?? throw new ArgumentNullException(nameof(queueService)),
            VaultQueueWorker.ForScopedPipeline(scopeFactory ?? throw new ArgumentNullException(nameof(scopeFactory))),
            storage ?? throw new ArgumentNullException(nameof(storage)),
            options?.Value ?? new FileVaultOptions());
    }

    /// <inheritdoc />
    protected override Task ExecuteAsync(CancellationToken stoppingToken) => _worker.RunAsync(stoppingToken);

    /// <summary>
    /// Recovers entries that are in partial removal state from previous runs.
    /// Should be called during startup after RecoverStuckJobsAsync.
    /// </summary>
    public Task RecoverPartialRemovalsAsync(CancellationToken ct) => _worker.RecoverPartialRemovalsAsync(ct);

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // Wait for active jobs to complete (bounded), then stop the loop.
        await _worker.WaitForActiveJobsAsync(cancellationToken);
        await base.StopAsync(cancellationToken);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _worker.Dispose();
        base.Dispose();
    }
}
