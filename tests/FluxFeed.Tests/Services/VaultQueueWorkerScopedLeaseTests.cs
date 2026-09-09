using AwesomeAssertions;
using FluxFeed.Interfaces;
using FluxFeed.Services;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace FluxFeed.Tests.Services;

/// <summary>
/// The per-job DI scope the queue worker leases must be disposed asynchronously. A scoped service that only
/// implements <see cref="IAsyncDisposable"/> (an LLM adapter behind the contextual-enrichment port, for
/// example) makes <c>IServiceScope.Dispose()</c> throw, and before this fix every memorize job failed
/// <em>after</em> doing its work with "type only implements IAsyncDisposable" (ecosystem E2E, 2026-09-09).
/// </summary>
public sealed class VaultQueueWorkerScopedLeaseTests
{
    private sealed class AsyncOnlyDisposable : IAsyncDisposable
    {
        public bool Disposed { get; private set; }
        public ValueTask DisposeAsync() { Disposed = true; return ValueTask.CompletedTask; }
    }

    private static ServiceProvider BuildProvider(out Func<AsyncOnlyDisposable?> lastResolved)
    {
        AsyncOnlyDisposable? last = null;
        var services = new ServiceCollection();
        services.AddScoped(_ => last = new AsyncOnlyDisposable());
        services.AddScoped<IVaultPipeline>(sp =>
        {
            _ = sp.GetRequiredService<AsyncOnlyDisposable>();   // the pipeline depends on the async-only service
            return Substitute.For<IVaultPipeline>();
        });
        lastResolved = () => last;
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task ScopedLease_DisposeAsync_DisposesTheScope_EvenWithAsyncOnlyServices()
    {
        await using var provider = BuildProvider(out var last);
        var lease = VaultQueueWorker.ForScopedPipeline(provider.GetRequiredService<IServiceScopeFactory>())();
        lease.Pipeline.Should().NotBeNull();
        last().Should().NotBeNull();

        var act = async () => { await using (lease) { } };

        await act.Should().NotThrowAsync();
        last()!.Disposed.Should().BeTrue("the scope's async disposal must reach the service");
    }

    [Fact]
    public async Task ScopedLease_SynchronousDispose_IsWhatUsedToBreak()
    {
        // Pins the failure mode the worker no longer triggers: the sync path is still exposed through
        // IDisposable for callers that have no async context, and it throws for async-only services.
        await using var provider = BuildProvider(out _);
        var lease = VaultQueueWorker.ForScopedPipeline(provider.GetRequiredService<IServiceScopeFactory>())();

        var act = () => lease.Dispose();

        act.Should().Throw<InvalidOperationException>().WithMessage("*only implements IAsyncDisposable*");
    }

    [Fact]
    public async Task SharedLease_DisposeAsync_DoesNotDisposeTheSharedPipeline()
    {
        var pipeline = Substitute.For<IVaultPipeline, IAsyncDisposable>();
        var lease = VaultQueueWorker.ForSharedPipeline(pipeline)();

        await using (lease) { }

        await ((IAsyncDisposable)pipeline).DidNotReceive().DisposeAsync();
    }
}
