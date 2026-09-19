using System.Reflection;
using AwesomeAssertions;
using FluxFeed.Domain.Enums;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxIndex.Core.Application.Interfaces;
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Integration;

/// <summary>
/// The scope-tag backfill reads each document once to see whether its chunks predate the tag. The pipeline is
/// registered scoped, so a host that opens a scope per request gets a new pipeline per search; what the backfill
/// remembers has to outlive it, or every search re-reads every document of the vault. Resolved through the
/// library's own registration on purpose — a pipeline constructed by hand is one instance and cannot show this.
/// </summary>
public sealed class ScopeTagBackfillAcrossScopesTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "fluxfeed-backfillscope-" + Guid.NewGuid().ToString("N"));
    private readonly DeterministicEmbeddingService _embedder = new();

    [Fact]
    public async Task SearchesFromFreshScopes_ReadEachDocumentOnce_NotOncePerSearch()
    {
        var docs = Path.Combine(_root, "docs");
        Directory.CreateDirectory(docs);
        const int documentCount = 5;
        for (var i = 0; i < documentCount; i++)
            File.WriteAllText(Path.Combine(docs, $"note-{i}.md"), $"# Note {i}\n\nquarterly budget review item{i} for the finance team.\n");

        var calls = new CallCounter();
        await using var provider = BuildStack(calls);
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);
        try
        {
            using (var scope = provider.CreateScope())
            {
                var vault = scope.ServiceProvider.GetRequiredService<IVault>();
                foreach (var file in Directory.GetFiles(docs, "*.md"))
                {
                    var entry = await vault.MemorizeAsync(file, waitForCompletion: true, TestContext.Current.CancellationToken);
                    entry.Stage.Should().Be(ProcessingStage.Memorized, because: $"{Path.GetFileName(file)}: {entry.LastError}");
                }
            }

            var before = calls.Count(nameof(IVectorStore.GetByDocumentIdAsync));
            const int searches = 4;
            for (var i = 0; i < searches; i++)
            {
                using var scope = provider.CreateScope();
                var vault = scope.ServiceProvider.GetRequiredService<IVault>();
                var result = await vault.SearchAsync("quarterly budget review", ct: TestContext.Current.CancellationToken);
                result.Items.Should().NotBeEmpty();
            }

            var reads = calls.Count(nameof(IVectorStore.GetByDocumentIdAsync)) - before;
            reads.Should().BeLessThanOrEqualTo(documentCount,
                $"{searches} searches from {searches} scopes examine each of the {documentCount} documents once in total, not once per search");
        }
        finally
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            foreach (var service in hosted)
                await service.StopAsync(cts.Token);
        }
    }

    private ServiceProvider BuildStack(CallCounter calls)
    {
        IServiceCollection services = new ServiceCollection();
        services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
        services.AddSQLiteVecVectorStore(o =>
        {
            o.DatabasePath = Path.Combine(_root, "fluxindex.db");
            o.VectorDimension = _embedder.GetEmbeddingDimension();
            o.FallbackToInMemoryOnError = false;
        });
        services.AddSingleton<IEmbeddingService>(_embedder);

        // Count calls on whatever store the package registered, keeping its lifetime.
        var registered = services.Last(d => d.ServiceType == typeof(IVectorStore));
        services.Remove(registered);
        services.Add(ServiceDescriptor.Describe(
            typeof(IVectorStore),
            sp => CountingProxy.Wrap(CreateFrom(registered, sp), calls),
            registered.Lifetime));

        services.AddFileVaultWithFluxIndex(o =>
        {
            o.VaultBasePath = Path.Combine(_root, ".vault");
            o.EnableRealTimeWatch = false;
        });
        return services.BuildServiceProvider();
    }

    private static IVectorStore CreateFrom(ServiceDescriptor descriptor, IServiceProvider sp)
        => (IVectorStore)(descriptor.ImplementationInstance
            ?? descriptor.ImplementationFactory?.Invoke(sp)
            ?? ActivatorUtilities.CreateInstance(sp, descriptor.ImplementationType!));

    private sealed class CallCounter
    {
        private readonly System.Collections.Concurrent.ConcurrentDictionary<string, int> _counts = new();
        public void Hit(string method) => _counts.AddOrUpdate(method, 1, (_, n) => n + 1);
        public int Count(string method) => _counts.GetValueOrDefault(method);
    }

    public class CountingProxy : DispatchProxy
    {
        private IVectorStore _inner = null!;
        private CallCounter _calls = null!;

        internal static IVectorStore Wrap(IVectorStore inner, object calls)
        {
            var proxy = Create<IVectorStore, CountingProxy>();
            var self = (CountingProxy)(object)proxy;
            self._inner = inner;
            self._calls = (CallCounter)calls;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            _calls.Hit(targetMethod!.Name);
            return targetMethod.Invoke(_inner, args);
        }
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}
