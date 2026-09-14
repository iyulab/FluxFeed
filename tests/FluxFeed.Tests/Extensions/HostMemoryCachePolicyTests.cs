using AwesomeAssertions;
using FluxFeed.Extensions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Xunit;

namespace FluxFeed.Tests.Extensions;

/// <summary>
/// The FileVault entry points pull FileFlux into the host container, so whatever FileFlux registers
/// becomes the host's. FileFlux before 0.23.8 set <c>SizeLimit</c> on the shared
/// <see cref="IMemoryCache"/>, and every entry stored without a <c>Size</c> then threw — including
/// FluxIndex's GraphRAG community summaries, which failed the whole memorize. Guarded at this surface
/// because it is the one consumers actually call.
/// </summary>
public class HostMemoryCachePolicyTests
{
    [Fact]
    public void AddFileVaultWithFluxIndex_LeavesTheHostCacheUnlimited()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMemoryCache();
        services.AddFileVaultWithFluxIndex();

        using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IMemoryCache>();

        provider.GetRequiredService<IOptions<MemoryCacheOptions>>().Value.SizeLimit.Should().BeNull();

        var store = () => cache.Set("graphrag-summary:community-1", "summary", new MemoryCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1),
        });
        store.Should().NotThrow();
    }
}
