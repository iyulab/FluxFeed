using AwesomeAssertions;
using FileFlux;
using FileFlux.Core;
using FluxFeed.Adapters;
using FluxFeed.Domain.Entities;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using FluxFeed.Services;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
using MsOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Tests.Adapters;

/// <summary>
/// <see cref="FileVaultOptions.LlmRefine"/> reaches FileFlux's LLM refinement, and a page-scoped refinement keeps the
/// entry's page spans and reports its per-page outcome in the extraction hints.
/// </summary>
public class FileFluxExtractorLlmRefineTests
{
    private static (FileFluxExtractor Extractor, IDocumentProcessor Processor) Create(
        string refinedText, IReadOnlyList<SourceSpan> refinedSpans, LlmRefinedContent? llm)
    {
        var processor = Substitute.For<IDocumentProcessor>();
        processor.Result.Returns(new ProcessingResult
        {
            Raw = new RawContent { Text = refinedText },
            Refined = new RefinedContent { Text = refinedText, Spans = refinedSpans },
            LlmRefined = llm,
        });
        var factory = Substitute.For<IDocumentProcessorFactory>();
        factory.Create(Arg.Any<string>()).Returns(processor);
        return (new FileFluxExtractor(factory, NullLogger<FileFluxExtractor>.Instance), processor);
    }

    private static readonly SourceSpan[] TwoPages = [new(0, 5) { Page = 1 }, new(7, 12) { Page = 2 }];

    [Fact]
    public async Task TheVaultsRefineOptions_ReachFileFlux()
    {
        var options = new LlmRefineOptions { Scope = LlmRefineScope.Pages };
        var (extractor, processor) = Create("alpha\n\nbravo", TwoPages, llm: null);

        await extractor.ExtractAsync("doc.pdf", new ExtractionSettings { LlmRefine = options }, TestContext.Current.CancellationToken);

        await processor.Received(1).LlmRefineAsync(options, Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task TheVaultsPageReading_ReachesFileFluxExtraction_BeforeRefinement()
    {
        var pageReading = new PageReadingOptions { SelectPages = q => !q.HasTextLayer };
        var (extractor, processor) = Create("alpha\n\nbravo", TwoPages, llm: null);

        await extractor.ExtractAsync("doc.pdf", new ExtractionSettings { PageReading = pageReading }, TestContext.Current.CancellationToken);

        Received.InOrder(() =>
        {
            processor.ExtractAsync(Arg.Is<ExtractOptions>(o => o.PageReading == pageReading), Arg.Any<CancellationToken>());
            processor.RefineAsync(Arg.Any<RefineOptions?>(), Arg.Any<CancellationToken>());
        });
    }

    [Fact]
    public async Task APageScopedRefinement_KeepsItsSpans_AndReportsThePages()
    {
        var llm = new LlmRefinedContent
        {
            Text = "ALPHA\n\nbravo",
            Spans = [new SourceSpan(0, 5) { Page = 1 }, new SourceSpan(7, 12) { Page = 2 }],
            Pages =
            [
                new PageRefinement(1) { Outcome = PageRefinementOutcome.Refined, TokenCoverage = 1.0, NumbersMatched = true },
                new PageRefinement(2) { Outcome = PageRefinementOutcome.Rejected, Reason = PageRefinement.LowCoverage },
            ],
        };
        var (extractor, _) = Create("alpha\n\nbravo", TwoPages, llm);

        var result = await extractor.ExtractAsync("doc.pdf", ct: TestContext.Current.CancellationToken);

        result.Content.Should().Be("ALPHA\n\nbravo");
        result.Spans.Should().NotBeNull();
        result.Spans!.Select(s => s.Page).Should().Equal(1, 2);
        result.Hints!["llm_refine_pages_refined"].Should().Be("1");
        result.Hints["llm_refine_pages_rejected"].Should().Be("1");
        result.Hints["llm_refine_pages_skipped"].Should().Be("0");
        result.Hints["llm_refine_rejected"].Should().Be("2:low_coverage");
    }

    [Fact]
    public async Task AWholeDocumentRewrite_StillDropsTheSpans()
    {
        var (extractor, _) = Create("alpha\n\nbravo", TwoPages, new LlmRefinedContent { Text = "rewritten" });

        var result = await extractor.ExtractAsync("doc.pdf", ct: TestContext.Current.CancellationToken);

        result.Content.Should().Be("rewritten");
        result.Spans.Should().BeNull();
        result.Hints.Should().BeNull();
    }

    [Fact]
    public async Task ThePipeline_PassesItsOptionsToTheExtractor()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"LlmRefinePass_{Guid.NewGuid():N}");
        var vault = Path.Combine(dir, ".vault");
        Directory.CreateDirectory(vault);
        try
        {
            var git = Substitute.For<IGitService>();
            git.CommitAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns("commit");
            var vaultOptions = new FileVaultOptions { VaultBasePath = vault, LlmRefine = new LlmRefineOptions { Scope = LlmRefineScope.Pages } };
            var storage = new VaultStorageService(NullLogger<VaultStorageService>.Instance, git, MsOptions.Create(vaultOptions));
            var extractor = new RecordingExtractor();
            var pipeline = new VaultPipeline(git, new ContentHasher(), storage, NullLogger<VaultPipeline>.Instance,
                options: MsOptions.Create(vaultOptions), extractor: extractor);
            var source = Path.Combine(dir, "doc.txt");
            await File.WriteAllTextAsync(source, "x", TestContext.Current.CancellationToken);

            await pipeline.ExtractAsync(VaultEntry.Create(source, vault), TestContext.Current.CancellationToken);

            extractor.Settings.Should().NotBeNull();
            extractor.Settings!.LlmRefine.Should().BeSameAs(vaultOptions.LlmRefine);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch (IOException) { }
        }
    }

    private sealed class RecordingExtractor : IExtractor
    {
        public ExtractionSettings? Settings { get; private set; }

        public Task<ExtractionResult> ExtractAsync(string sourcePath, ExtractionSettings? settings = null, CancellationToken ct = default)
        {
            Settings = settings;
            return Task.FromResult(new ExtractionResult { Content = "text" });
        }

        public FluxFeed.Domain.ValueObjects.ExtractionIdentity? Identity => null;
    }
}
