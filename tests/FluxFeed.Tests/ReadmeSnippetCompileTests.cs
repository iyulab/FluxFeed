using System.Collections.Immutable;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Xunit;

namespace FluxFeed.Tests;

/// <summary>
/// Compiles every <c>```csharp</c> block in README.md against the current assemblies. A compiler checks the receiver,
/// the arguments, the return types a block goes on to use, and the namespaces it needs.
/// </summary>
/// <remarks>
/// A block is compiled as a top-level program: its <c>using</c> lines are hoisted, the common usings below and the
/// usings the Quick Start states are added, and the stand-ins below are declared when the block uses the name without
/// declaring it — values a reader already has from the surrounding text (the vault, a file path, a logger), not part of
/// what the block shows.
/// </remarks>
public class ReadmeSnippetCompileTests
{
    // A block that is deliberately not a program (a signature sketch, pseudocode) is listed here by the heading it sits
    // under, with the reason. Shrink this, never grow it silently.
    private static readonly Dictionary<string, string> Fragments = new(StringComparer.Ordinal);

    // The implicit usings of an SDK console project (ImplicitUsings=enable).
    private const string CommonUsings = """
        using System;
        using System.Collections.Generic;
        using System.IO;
        using System.Linq;
        using System.Threading;
        using System.Threading.Tasks;
        """;

    // The README reads as one guide: its Quick Start states these usings once, and every later block assumes them.
    // ReadmeUsings_AreTheOnesTheQuickStartStates keeps this list and the README's statement the same.
    private static readonly string[] ReadmeUsings =
    [
        "FluxFeed.Extensions", "FluxFeed.Interfaces", "FluxIndex.Core.Application.Interfaces", "FluxIndex.Storage.SQLite",
        "Microsoft.Extensions.DependencyInjection", "Microsoft.Extensions.Hosting",
    ];

    private static readonly (string Name, string Declaration)[] StandIns =
    [
        ("services", "IServiceCollection services = new ServiceCollection();"),
        ("embedder", "IEmbeddingService embedder = null!;"),
        ("vault", "IVault vault = null!;"),
        ("path", "string path = \"\";"),
        ("ct", "CancellationToken ct = default;"),
        // A typed logger is an ILogger too; the hand-built pipeline takes ILogger<VaultPipeline>.
        ("logger", "Microsoft.Extensions.Logging.ILogger<FluxFeed.Services.VaultPipeline> logger = null!;"),
        ("configuration", "Microsoft.Extensions.Configuration.IConfiguration configuration = null!;"),
        ("factory", "IVaultFactory factory = null!;"),
        ("queue", "IVaultQueueService queue = null!;"),
        ("hash", "string hash = \"\";"),
        ("vaultBasePath", "string vaultBasePath = \"\";"),
        // The option snippets are the body of an AddFileVault*(o => { ... }) callback.
        ("o", "FluxFeed.Options.FileVaultOptions o = new();"),
        // Hand-constructing the pipeline: the collaborators a reader has already built.
        ("git", "IGitService git = null!;"),
        ("hasher", "IContentHasher hasher = null!;"),
        ("storage", "IVaultStorageService storage = null!;"),
        ("vectorStore", "FluxIndex.Core.Application.Interfaces.IVectorStore vectorStore = null!;"),
        ("embeddingService", "IEmbeddingService embeddingService = null!;"),
    ];

    // Types the reader brings (their own embedder, vision model, enrichment port). Appended after the body: a top-level
    // program declares its types after its statements.
    private static readonly (string Name, string Declaration)[] TypeStandIns =
    [
        ("MyEmbeddingService", """
            public class MyEmbeddingService : FluxIndex.Core.Application.Interfaces.IEmbeddingService
            {
                public Task<float[]> GenerateEmbeddingAsync(string text, CancellationToken cancellationToken = default) => throw new NotImplementedException();
                public Task<IEnumerable<float[]>> GenerateEmbeddingsBatchAsync(IEnumerable<string> texts, CancellationToken cancellationToken = default) => throw new NotImplementedException();
                public int GetEmbeddingDimension() => 384;
                public string GetModelName() => "";
                public int GetMaxTokens() => 512;
                public Task<int> CountTokensAsync(string text, CancellationToken cancellationToken = default) => throw new NotImplementedException();
                public FluxIndex.Core.Domain.ValueObjects.EmbeddingIdentity GetIdentity() => throw new NotImplementedException();
            }
            """),
        ("MyVisionModel", """
            public class MyVisionModel
            {
                public Task<string?> CaptionAsync(string imagePath, string? documentText, CancellationToken ct) => Task.FromResult<string?>(null);
            }
            """),
        ("MyContextualEnrichment", """
            public abstract class MyContextualEnrichment : FluxIndex.Core.Application.Interfaces.IContextualEnrichmentService
            {
                public abstract Task<string> GenerateContextAsync(string chunkContent, string fullDocumentText, int chunkIndex, int totalChunks, CancellationToken cancellationToken = default);
                public abstract Task<IReadOnlyList<string>> GenerateContextBatchAsync(IReadOnlyList<string> chunks, string fullDocumentText, CancellationToken cancellationToken = default);
            }
            """),
    ];

    private static readonly string[] AssembliesToLoad =
    [
        "FluxFeed", "FluxIndex.Core", "FluxIndex.Storage.SQLite", "FluxGuard.Remote", "FileFlux",
        "Microsoft.Extensions.DependencyInjection", "Microsoft.Extensions.DependencyInjection.Abstractions",
        "Microsoft.Extensions.Hosting", "Microsoft.Extensions.Hosting.Abstractions",
        "Microsoft.Extensions.Configuration.Abstractions", "Microsoft.Extensions.Options.ConfigurationExtensions",
        "Microsoft.Extensions.Logging.Abstractions",
    ];

    public static TheoryData<string> Blocks()
    {
        var data = new TheoryData<string>();
        foreach (var block in ReadBlocks())
            data.Add(block.Key);
        return data;
    }

    [Theory]
    [MemberData(nameof(Blocks))]
    public void ReadmeBlock_Compiles(string key)
    {
        var block = ReadBlocks().Single(b => b.Key == key);
        if (Fragments.ContainsKey(block.Heading))
            return;

        var errors = Compile(block.Code);

        Assert.True(errors.IsEmpty,
            $"README block {key} does not compile against the current API:\n" +
            string.Join("\n", errors.Select(e => e.ToString())) + "\n--- source ---\n" + Program(block.Code));
    }

    [Fact]
    public void EveryReadmeBlock_IsFoundAndFragmentsNameRealHeadings()
    {
        var blocks = ReadBlocks();
        Assert.True(blocks.Count >= 15, $"expected the README's C# blocks, found {blocks.Count}");
        Assert.All(Fragments.Keys, heading => Assert.Contains(blocks, b => b.Heading == heading));
    }

    [Fact]
    public void ReadmeUsings_AreTheOnesTheQuickStartStates()
    {
        var quickStart = ReadBlocks().First();
        var stated = quickStart.Code.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n')
            .Select(l => Regex.Match(l, @"^using ([\w.]+);"))
            .Where(m => m.Success)
            .Select(m => m.Groups[1].Value);

        Assert.Equal(ReadmeUsings, stated);
    }

    /// <summary>Positive control: the compiler rejects a call the library does not have.</summary>
    [Fact]
    public void Compile_RejectsAMethodTheLibraryDoesNotHave()
    {
        var errors = Compile("""
            services.AddFileVaultThatDoesNotExist();
            """);

        Assert.NotEmpty(errors);
    }

    /// <summary>Positive control: a stand-in does not hide a member the library does not have.</summary>
    [Fact]
    public void Compile_RejectsAnOptionTheLibraryDoesNotHave()
    {
        Assert.Empty(Compile("o.MaxInFlightPerGroup = 2;"));
        Assert.NotEmpty(Compile("o.MaxInFlightPerGroupThatDoesNotExist = 2;"));
    }

    private sealed record Block(string Key, string Heading, string Code);

    private static List<Block> ReadBlocks()
    {
        var lines = File.ReadAllText(Path.Combine(RepoRoot(), "README.md")).Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var blocks = new List<Block>();
        var heading = "(top)";
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].StartsWith('#'))
                heading = lines[i].TrimStart('#').Trim();
            if (lines[i].Trim() != "```csharp")
                continue;

            var start = i + 1;
            var code = new StringBuilder();
            for (i++; i < lines.Length && lines[i].Trim() != "```"; i++)
                code.AppendLine(lines[i]);
            blocks.Add(new Block($"README.md line {start}: {heading}", heading, code.ToString()));
        }

        return blocks;
    }

    private static string Program(string code)
    {
        var lines = code.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        // "using X;   // what it is for" is a directive too: the README annotates its usings.
        static string Code(string l) => l.Split("//", 2)[0].TrimEnd();
        bool IsUsingDirective(string l) =>
            l.StartsWith("using ", StringComparison.Ordinal) && Code(l).EndsWith(';') && !l.StartsWith("using var ", StringComparison.Ordinal)
            && !l.StartsWith("using (", StringComparison.Ordinal);

        var body = string.Join("\n", lines.Where(l => !IsUsingDirective(l)));
        var standIns = StandIns
            .Where(s => Regex.IsMatch(body, $@"\b{s.Name}\b")
                        && !Regex.IsMatch(body, $@"\b(var|[A-Z][\w<>?,\s]*)\s+{s.Name}\s*[=;]")
                        // A lambda parameter of the same name (o => ...) is the block's own.
                        && !Regex.IsMatch(body, $@"\b{s.Name}\s*=>"))
            .Select(s => s.Declaration);

        var typeStandIns = TypeStandIns
            .Where(s => Regex.IsMatch(body, $@"\b{s.Name}\b") && !Regex.IsMatch(body, $@"\bclass\s+{s.Name}\b"))
            .Select(s => s.Declaration);

        // A block that states a using the README already assumes must not get it twice.
        var stated = lines.Where(IsUsingDirective).Select(Code).ToHashSet(StringComparer.Ordinal);
        var assumed = ReadmeUsings.Select(n => $"using {n};").Where(u => !stated.Contains(u));

        return string.Join("\n", stated) + "\n" + CommonUsings + "\n" + string.Join("\n", assumed) + "\n"
               + string.Join("\n", standIns) + "\n" + body + "\n" + string.Join("\n", typeStandIns);
    }

    private static ImmutableArray<Diagnostic> Compile(string code)
    {
        var tree = CSharpSyntaxTree.ParseText(Program(code), new CSharpParseOptions(LanguageVersion.Latest));
        var compilation = CSharpCompilation.Create(
            "ReadmeSnippet", [tree], References(),
            new CSharpCompilationOptions(
                // A block that only declares types (a service, an enricher) is a library, not a program.
                tree.GetRoot().DescendantNodes().OfType<Microsoft.CodeAnalysis.CSharp.Syntax.GlobalStatementSyntax>().Any()
                    ? OutputKind.ConsoleApplication
                    : OutputKind.DynamicallyLinkedLibrary,
                nullableContextOptions: NullableContextOptions.Enable));
        return compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToImmutableArray();
    }

    private static List<MetadataReference> References()
    {
        foreach (var name in AssembliesToLoad)
            Assembly.Load(name);

        var paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") is string trusted)
            paths.UnionWith(trusted.Split(Path.PathSeparator).Where(p => p.Length > 0));
        paths.UnionWith(AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && a.Location.Length > 0)
            .Select(a => a.Location));
        return paths.Select(p => (MetadataReference)MetadataReference.CreateFromFile(p)).ToList();
    }

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "FluxFeed.slnx")))
            dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("FluxFeed.slnx not found above the test output directory");
    }
}
