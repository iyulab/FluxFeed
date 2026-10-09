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

    /// <summary>A deferring vault asks for no LLM refinement at all — not the default whole-document rewrite.</summary>
    [Fact]
    public async Task SkipLlmRefine_RunsNoRefinement()
    {
        var (extractor, processor) = Create("alpha\n\nbravo", TwoPages, llm: null);

        await extractor.ExtractAsync("doc.pdf", new ExtractionSettings { LlmRefine = new LlmRefineOptions { Scope = LlmRefineScope.Pages }, SkipLlmRefine = true },
            TestContext.Current.CancellationToken);

        await processor.DidNotReceive().LlmRefineAsync(Arg.Any<LlmRefineOptions?>(), Arg.Any<CancellationToken>());
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
    public async Task TheVaultsSlideRendering_ReachesFileFluxExtraction()
    {
        var slideRendering = new SlideRenderingOptions { SelectSlides = SlideRenderingOptions.DrawnSlides };
        var (extractor, processor) = Create("alpha\n\nbravo", TwoPages, llm: null);

        await extractor.ExtractAsync("deck.pptx", new ExtractionSettings { SlideRendering = slideRendering }, TestContext.Current.CancellationToken);

        await processor.Received(1).ExtractAsync(Arg.Is<ExtractOptions>(o => o.SlideRendering == slideRendering), Arg.Any<CancellationToken>());
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
                new PageRefinement(1) { Outcome = PageRefinementOutcome.Refined, NativeCoverage = 1.0, NumbersMatched = true },
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
        result.Hints.Should().NotContainKey("llm_refine_native_reasons");
        result.Hints.Should().NotContainKey("llm_refine_pass_failures");
    }

    /// <summary>
    /// «0 refined, 3 native» is not one outcome: the model may have kept the pages, never been asked, or failed every time
    /// it was asked. The hints and the log line say which, per page.
    /// </summary>
    [Fact]
    public async Task NativePages_SayWhyTheModelDidNotChangeThem_AndWhichPassFailed()
    {
        var truncated = new LlmRefinementPass("RestoreSentences") { Outcome = LlmRefinementPassOutcome.Failed, Reason = LlmRefinementPass.Truncated };
        var tooSmall = new LlmRefinementPass("CorrectOcrErrors") { Outcome = LlmRefinementPassOutcome.Failed, Reason = LlmRefinementPass.ContextTooSmall };
        const string text = "alpha\n\nbravo\n\ncharlie";
        var llm = new LlmRefinedContent
        {
            Text = text,
            Spans = [new SourceSpan(0, 5) { Page = 1 }, new SourceSpan(7, 12) { Page = 2 }, new SourceSpan(14, 21) { Page = 3 }],
            Pages =
            [
                new PageRefinement(1) { Outcome = PageRefinementOutcome.Native, Reason = PageRefinement.PassesFailed, Passes = [truncated, tooSmall] },
                new PageRefinement(2) { Outcome = PageRefinementOutcome.Native, Reason = PageRefinement.NoPassNeeded },
                new PageRefinement(3) { Outcome = PageRefinementOutcome.Native },
            ],
        };
        var logger = new ListLogger();
        var processor = Substitute.For<IDocumentProcessor>();
        processor.Result.Returns(new ProcessingResult
        {
            Raw = new RawContent { Text = text },
            Refined = new RefinedContent { Text = text, Spans = llm.Spans },
            LlmRefined = llm,
        });
        var factory = Substitute.For<IDocumentProcessorFactory>();
        factory.Create(Arg.Any<string>()).Returns(processor);

        var result = await new FileFluxExtractor(factory, logger).ExtractAsync("doc.pdf", ct: TestContext.Current.CancellationToken);

        result.Hints!["llm_refine_pages_native"].Should().Be("3");
        result.Hints["llm_refine_native_reasons"].Should().Be("1:passes_failed,2:no_pass_needed");
        result.Hints["llm_refine_pass_failures"].Should().Be("1:RestoreSentences:truncated,1:CorrectOcrErrors:context_too_small");
        logger.Messages.Should().ContainSingle(m => m.StartsWith("Page refinement of doc.pdf", StringComparison.Ordinal))
            .Which.Should().Be("Page refinement of doc.pdf: 0 refined, 3 native, 0 rejected, 0 skipped; page reasons: "
                + "1:passes_failed,2:no_pass_needed; failed passes: 1:RestoreSentences:truncated,1:CorrectOcrErrors:context_too_small");
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

    private sealed class ListLogger : Microsoft.Extensions.Logging.ILogger<FileFluxExtractor>
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state,
            Exception? exception, Func<TState, Exception?, string> formatter) => Messages.Add(formatter(state, exception));
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
