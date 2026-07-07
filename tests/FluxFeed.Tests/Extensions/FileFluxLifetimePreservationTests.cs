using System.Linq;
using FileFlux;
using FileFlux.Core;
using FluentAssertions;
using FluxFeed.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace FluxFeed.Tests.Extensions;

/// <summary>
/// Regression guard for the defect where FluxFeed's FileVault entry points re-registered FileFlux
/// unconditionally (default Scoped), silently overriding a consumer's prior AddFileFlux(Singleton)
/// and reintroducing a captive-dependency error.
/// Issue: ISSUE-FluxFeed-20260707-194622-addfilevault-filefluxlifetime-override (AIMS 2nd-consumer report).
/// </summary>
public class FileFluxLifetimePreservationTests
{
    private sealed class ReaderFactoryConsumer
    {
        // A Singleton service that captures the scoped/singleton IDocumentReaderFactory —
        // mirrors FileFlux's own FluxDocumentProcessor capture that AIMS hit.
        public ReaderFactoryConsumer(IDocumentReaderFactory factory) => Factory = factory;

        public IDocumentReaderFactory Factory { get; }
    }

    private static ServiceLifetime LifetimeOf<TService>(IServiceCollection services)
    {
        var descriptors = services.Where(d => d.ServiceType == typeof(TService)).ToList();
        descriptors.Should().ContainSingle(
            "FluxFeed must not append a duplicate {0} descriptor when the consumer already registered FileFlux",
            typeof(TService).Name);
        return descriptors[0].Lifetime;
    }

    [Fact]
    public void AddFileVaultWithFileFlux_WhenFileFluxPreRegisteredSingleton_PreservesSingletonLifetime()
    {
        var services = new ServiceCollection();
        services.AddFileFlux(ServiceLifetime.Singleton);

        services.AddFileVaultWithFileFlux();

        LifetimeOf<IDocumentReaderFactory>(services).Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddFileVaultWithFluxIndex_WhenFileFluxPreRegisteredSingleton_PreservesSingletonLifetime()
    {
        var services = new ServiceCollection();
        services.AddFileFlux(ServiceLifetime.Singleton);

        services.AddFileVaultWithFluxIndex();

        LifetimeOf<IDocumentReaderFactory>(services).Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddFileVaultFactoryWithFileFlux_WhenFileFluxPreRegisteredSingleton_PreservesSingletonLifetime()
    {
        var services = new ServiceCollection();
        services.AddFileFlux(ServiceLifetime.Singleton);

        services.AddFileVaultFactoryWithFileFlux();

        LifetimeOf<IDocumentReaderFactory>(services).Should().Be(ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddFileVaultWithFileFlux_WhenFileFluxNotPreRegistered_RegistersScopedByDefault()
    {
        var services = new ServiceCollection();

        services.AddFileVaultWithFileFlux();

        // Default behavior is unchanged (Scoped) — pure regression guard.
        LifetimeOf<IDocumentReaderFactory>(services).Should().Be(ServiceLifetime.Scoped);
    }

    [Fact]
    public void AddFileVaultWithFileFlux_WhenFileFluxPreRegisteredSingleton_SingletonConsumerIsScopeValid()
    {
        // Faithful reproduction of the AIMS captive: a Singleton that consumes IDocumentReaderFactory
        // throws under ValidateOnBuild when FluxFeed downgrades the factory to Scoped. With the guard
        // the consumer's Singleton lifetime is preserved and the graph is scope-valid.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddFileFlux(ServiceLifetime.Singleton);
        services.AddFileVaultWithFileFlux();
        services.AddSingleton<ReaderFactoryConsumer>();

        var act = () => services.BuildServiceProvider(
            new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true }).Dispose();

        act.Should().NotThrow();
    }
}
