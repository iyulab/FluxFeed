using AwesomeAssertions;
using FluxIndex.Core.Application.Interfaces;
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit;

namespace FluxFeed.Tests.Services;

/// <summary>
/// The factory is a singleton; the document-processing services a vault uses (extractor, chunker,
/// GraphRAG, ...) are registered scoped. A singleton that receives scoped services through its
/// constructor is a captive dependency: the container refuses it under scope validation (the
/// ASP.NET Core Development default), and without validation one scoped instance is silently shared
/// by every tenant for the life of the process. Each vault must resolve those services from a scope
/// of its own and release them when the tenant is disposed.
/// </summary>
public sealed class VaultFactoryScopeTests : IDisposable
{
    private readonly string _basePath = Path.Combine(Path.GetTempPath(), $"FileVaultFactoryScope_{Guid.NewGuid():N}");

    public VaultFactoryScopeTests() => Directory.CreateDirectory(_basePath);

    public void Dispose()
    {
        try { if (Directory.Exists(_basePath)) Directory.Delete(_basePath, recursive: true); }
        catch { /* ignore cleanup errors */ }
    }

    private static IGitService FakeGit()
    {
        var git = Substitute.For<IGitService>();
        git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("commit-hash");
        return git;
    }

    [Fact]
    public void FactoryPath_IsScopeValid_WhenTheProcessingServicesAreScoped()
    {
        // AddFileVaultFactoryWithFileFlux registers IExtractor/IChunker scoped; the factory must not
        // consume them from its own (singleton) constructor.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddFileVaultFactoryWithFileFlux(o => o.VaultBasePath = _basePath);

        var act = () =>
        {
            using var provider = services.BuildServiceProvider(
                new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
            provider.GetRequiredService<IVaultFactory>();
        };

        act.Should().NotThrow();
    }

    [Fact]
    public async Task EachTenant_GetsItsOwnScopedServices_ReleasedWhenTheTenantIsDisposed()
    {
        var created = 0;
        var extractors = new List<IExtractor>();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(FakeGit());
        services.AddFileVaultFactory(o => { o.VaultBasePath = _basePath; o.EnableBackgroundProcessing = false; });
        services.AddScoped<IGraphRAGService>(_ => { created++; return Substitute.For<IGraphRAGService>(); });
        services.AddScoped<IExtractor>(_ =>
        {
            var extractor = Substitute.For<IExtractor, IDisposable>();
            extractors.Add(extractor);
            return extractor;
        });

        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });
        var factory = provider.GetRequiredService<IVaultFactory>();

        factory.GetOrCreate("tenant-a");
        factory.GetOrCreate("tenant-b");

        created.Should().Be(2, "two tenants must not share one scoped GraphRAG service");
        extractors.Should().HaveCount(2);

        await factory.DisposeAsync("tenant-a");

        ((IDisposable)extractors[0]).Received(1).Dispose();
        ((IDisposable)extractors[1]).DidNotReceive().Dispose();
    }
}
