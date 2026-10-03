using System.Globalization;
using System.Reflection;
using FluxFeed.Options;

namespace FluxFeed.Domain.ValueObjects;

/// <summary>
/// Which extractor produced an entry's extraction artifacts (<c>extracted.md</c>, the content spans, the image
/// manifest), so a later, better extractor can be told apart from the one that made what the vault holds.
/// </summary>
/// <remarks>
/// <para>
/// The extractor reports <see cref="Extractor"/> and <see cref="Version"/>
/// (<see cref="FluxFeed.Services.IExtractor.Identity"/>); the vault adds <see cref="PipelineRevision"/>, which changes when
/// FluxFeed itself changes what it derives from an extraction (the spans file, the image manifest's fields).
/// </para>
/// <para>
/// <see cref="FluxFeed.Domain.Entities.VaultEntry.ExtractedBy"/> records it at every extraction. <see cref="IsOutdatedBy"/> answers whether the
/// current extractor would produce something newer, at the granularity a <see cref="ReextractionPolicy"/> asks for.
/// </para>
/// </remarks>
public sealed record ExtractionIdentity
{
    /// <summary>Creates an identity for <paramref name="extractor"/> at <paramref name="version"/>.</summary>
    /// <exception cref="ArgumentException">Either value is blank.</exception>
    public ExtractionIdentity(string extractor, string version)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(extractor);
        ArgumentException.ThrowIfNullOrWhiteSpace(version);
        Extractor = extractor.Trim();
        Version = version.Trim();
    }

    /// <summary>The extractor's name, e.g. <c>FileFlux</c>.</summary>
    public string Extractor { get; }

    /// <summary>The extractor's version, e.g. <c>0.36.2</c> (build metadata after <c>+</c> is not part of it).</summary>
    public string Version { get; }

    /// <summary>
    /// FluxFeed's revision of what it derives from an extraction. Set by the vault, not by the extractor; 0 for an
    /// identity an extractor reports.
    /// </summary>
    public int PipelineRevision { get; init; }

    /// <summary>
    /// The identity of an assembly's informational version (build metadata after <c>+</c> removed), falling back to
    /// its assembly version.
    /// </summary>
    public static ExtractionIdentity FromAssembly(string extractor, Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        var informational = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        var version = string.IsNullOrWhiteSpace(informational)
            ? assembly.GetName().Version?.ToString(3) ?? "0.0.0"
            : informational.Split('+')[0];
        return new ExtractionIdentity(extractor, version);
    }

    /// <summary>
    /// Whether an extraction recorded as <paramref name="recorded"/> is older than what this identity would produce,
    /// under <paramref name="policy"/>.
    /// </summary>
    /// <remarks>
    /// <list type="bullet">
    /// <item><description><see cref="ReextractionPolicy.Never"/>: never.</description></item>
    /// <item><description>An extraction with no recorded identity (made before identities were recorded) is outdated.</description></item>
    /// <item><description>Another extractor, or a lower <see cref="PipelineRevision"/>, is outdated.</description></item>
    /// <item><description><see cref="ReextractionPolicy.WhenExtractorMinorChanges"/>: a lower major.minor version.
    /// A patch release (a fix or a dependency re-pin) does not re-extract.</description></item>
    /// <item><description><see cref="ReextractionPolicy.WhenExtractorChanges"/>: any other version.</description></item>
    /// </list>
    /// A version that does not parse as numbers compares as text: any difference counts at either granularity.
    /// </remarks>
    public bool IsOutdatedBy(ExtractionIdentity? recorded, ReextractionPolicy policy) => IsOutdated(recorded, this, policy);

    internal static bool IsOutdated(ExtractionIdentity? recorded, ExtractionIdentity? current, ReextractionPolicy policy)
    {
        if (policy == ReextractionPolicy.Never || current is null)
            return false;
        if (recorded is null)
            return true;
        if (!string.Equals(recorded.Extractor, current.Extractor, StringComparison.OrdinalIgnoreCase))
            return true;
        if (recorded.PipelineRevision < current.PipelineRevision)
            return true;
        if (string.Equals(recorded.Version, current.Version, StringComparison.OrdinalIgnoreCase))
            return false;

        if (policy == ReextractionPolicy.WhenExtractorChanges)
            return true;

        // WhenExtractorMinorChanges
        return TryParseMajorMinor(recorded.Version, out var was) && TryParseMajorMinor(current.Version, out var now)
            ? was.CompareTo(now) < 0
            : true;
    }

    private static bool TryParseMajorMinor(string version, out (int Major, int Minor) value)
    {
        value = default;
        var core = version.Split('-', '+')[0].Split('.');
        if (core.Length < 2
            || !int.TryParse(core[0], NumberStyles.None, CultureInfo.InvariantCulture, out var major)
            || !int.TryParse(core[1], NumberStyles.None, CultureInfo.InvariantCulture, out var minor))
            return false;
        value = (major, minor);
        return true;
    }

    /// <inheritdoc/>
    public override string ToString() =>
        PipelineRevision == 0 ? $"{Extractor} {Version}" : $"{Extractor} {Version} (pipeline r{PipelineRevision})";
}
