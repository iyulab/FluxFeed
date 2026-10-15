using System.Reflection;
using System.Text.RegularExpressions;
using Iyu.Conventions.Testing.Snippets;
using Xunit;

namespace FluxFeed.Tests;

/// <summary>
/// Compiles every C# block in README.md against the current assemblies. A compiler checks the receiver, the arguments,
/// the return types a block goes on to use, and the namespaces it needs.
/// </summary>
/// <remarks>
/// Each block is compiled as a top-level program by <see cref="SnippetCompiler"/>: its own usings, the common usings and
/// the usings the Quick Start states, and the stand-ins below when the block uses the name without declaring it — values
/// and types a reader already has from the surrounding text (the vault, a file path, a logger, their own embedder), not
/// part of what the block shows.
/// </remarks>
public partial class ReadmeSnippetCompileTests
{
    // A block that is deliberately not a program (a signature sketch, pseudocode), by the heading it sits under, with the
    // reason. Shrink this, never grow it silently.
    private static readonly SnippetRoster Fragments = new(new Dictionary<string, string>(StringComparer.Ordinal));

    // The implicit usings of an SDK console project (ImplicitUsings=enable).
    private static readonly string[] CommonUsings =
        ["System", "System.Collections.Generic", "System.IO", "System.Linq", "System.Threading", "System.Threading.Tasks"];

    // The README reads as one guide: its Quick Start states these usings once, and every later block assumes them.
    // ReadmeUsings_AreTheOnesTheQuickStartStates keeps this list and the README's statement the same.
    private static readonly string[] ReadmeUsings =
    [
        "FluxFeed.Extensions", "FluxFeed.Interfaces", "FluxIndex.Core.Application.Interfaces", "FluxIndex.Storage.SQLite",
        "Microsoft.Extensions.DependencyInjection", "Microsoft.Extensions.Hosting",
    ];

    private static readonly Dictionary<string, string> StandIns = new(StringComparer.Ordinal)
    {
        ["services"] = "IServiceCollection services = new ServiceCollection();",
        ["embedder"] = "IEmbeddingService embedder = null!;",
        ["vault"] = "IVault vault = null!;",
        ["path"] = "string path = \"\";",
        ["ct"] = "CancellationToken ct = default;",
        // A typed logger is an ILogger too; the hand-built pipeline takes ILogger<VaultPipeline>.
        ["logger"] = "Microsoft.Extensions.Logging.ILogger<FluxFeed.Services.VaultPipeline> logger = null!;",
        ["configuration"] = "Microsoft.Extensions.Configuration.IConfiguration configuration = null!;",
        ["factory"] = "IVaultFactory factory = null!;",
        // The Quick Start's host, which later blocks keep using.
        ["host"] = "IHost host = null!;",
        ["queue"] = "IVaultQueueService queue = null!;",
        ["hash"] = "string hash = \"\";",
        ["vaultBasePath"] = "string vaultBasePath = \"\";",
        // The option snippets are the body of an AddFileVault*(o => { ... }) callback.
        ["o"] = "FluxFeed.Options.FileVaultOptions o = new();",
        // Hand-constructing the pipeline: the collaborators a reader has already built.
        ["git"] = "IGitService git = null!;",
        ["hasher"] = "IContentHasher hasher = null!;",
        ["storage"] = "IVaultStorageService storage = null!;",
        ["vectorStore"] = "FluxIndex.Core.Application.Interfaces.IVectorStore vectorStore = null!;",
        ["embeddingService"] = "IEmbeddingService embeddingService = null!;",

        // Types the reader brings (their own embedder, vision model, enrichment port).
        ["MyEmbeddingService"] = """
            public class MyEmbeddingService : FluxIndex.Core.Application.Interfaces.IEmbeddingService
            {
                public Task<float[]> GenerateEmbeddingAsync(string text, CancellationToken cancellationToken = default) => throw new NotImplementedException();
                public Task<IEnumerable<float[]>> GenerateEmbeddingsBatchAsync(IEnumerable<string> texts, CancellationToken cancellationToken = default) => throw new NotImplementedException();
                public int GetEmbeddingDimension() => 384;
                public string GetModelName() => "";
                public FluxIndex.Core.Domain.ValueObjects.EmbeddingIdentity GetIdentity() => throw new NotImplementedException();
            }
            """,
        ["MyVisionModel"] = """
            public class MyVisionModel
            {
                public Task<string?> CaptionAsync(string imagePath, string? documentText, CancellationToken ct) => Task.FromResult<string?>(null);
            }
            """,
        ["MyContextualEnrichment"] = """
            public abstract class MyContextualEnrichment : FluxIndex.Core.Application.Interfaces.IContextualEnrichmentService
            {
                public abstract Task<string> GenerateContextAsync(string chunkContent, string fullDocumentText, int chunkIndex, int totalChunks, CancellationToken cancellationToken = default);
                public abstract Task<IReadOnlyList<string>> GenerateContextBatchAsync(IReadOnlyList<string> chunks, string fullDocumentText, CancellationToken cancellationToken = default);
            }
            """,
    };

    private static readonly IReadOnlyList<Microsoft.CodeAnalysis.MetadataReference> References = SnippetCompiler.ReferencesFor(
        [.. new[]
        {
            "FluxFeed", "FluxIndex.Core", "FluxIndex.Storage.SQLite", "FluxGuard.Remote", "FileFlux",
            "Microsoft.Extensions.DependencyInjection", "Microsoft.Extensions.DependencyInjection.Abstractions",
            "Microsoft.Extensions.Hosting", "Microsoft.Extensions.Hosting.Abstractions",
            "Microsoft.Extensions.Configuration.Abstractions", "Microsoft.Extensions.Options.ConfigurationExtensions",
            "Microsoft.Extensions.Logging.Abstractions",
        }.Select(Assembly.Load)]);

    private static readonly SnippetCompiler Compiler = new(References)
    {
        Usings = [.. CommonUsings, .. ReadmeUsings],
        StandIns = StandIns,
    };

    private static readonly IReadOnlyList<DocSnippet> Snippets = DocSnippets.Read(Path.Combine(RepoRoot(), "README.md"), "README.md");

    public static TheoryData<string> Blocks() => new(Snippets.Select(s => s.Key));

    [Theory]
    [MemberData(nameof(Blocks))]
    public void ReadmeBlock_Compiles(string key)
    {
        var snippet = Snippets.Single(s => s.Key == key);
        if (!Fragments.IsFragment(snippet))
            Compiler.ShouldCompile(snippet);
    }

    [Fact]
    public void EveryReadmeBlock_IsFoundAndFragmentsNameRealHeadings() => Fragments.ShouldMatch([.. Snippets], atLeast: 15);

    [Fact]
    public void ReadmeUsings_AreTheOnesTheQuickStartStates()
    {
        var quickStart = Snippets[0];
        var stated = quickStart.Code.Split('\n')
            .Select(l => UsingDirective().Match(l))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value);

        Assert.Equal(ReadmeUsings, stated);
    }

    /// <summary>Positive control: the compiler rejects a call the library does not have.</summary>
    [Fact]
    public void Compile_RejectsAMethodTheLibraryDoesNotHave()
    {
        var errors = Compiler.Compile("""
            services.AddFileVaultThatDoesNotExist();
            """);

        Assert.NotEmpty(errors);
    }

    /// <summary>Positive control: a stand-in does not hide a member the library does not have.</summary>
    [Fact]
    public void Compile_RejectsAnOptionTheLibraryDoesNotHave()
    {
        Assert.Empty(Compiler.Compile("o.MaxInFlightPerGroup = 2;"));
        Assert.NotEmpty(Compiler.Compile("o.MaxInFlightPerGroupThatDoesNotExist = 2;"));
    }

    [GeneratedRegex(@"^using ([\w.]+);")]
    private static partial Regex UsingDirective();

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "FluxFeed.slnx")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("FluxFeed.slnx not found above the test output directory");
    }
}
