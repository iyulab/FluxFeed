using FluxGuard.Remote.RAG;
using System.Collections.Concurrent;
using FluxIndex.Core.Application.Interfaces;
using FluxFeed.Interfaces;
using FluxFeed.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MicrosoftOptions = Microsoft.Extensions.Options.Options;

namespace FluxFeed.Services;

/// <summary>
/// Factory for creating tenant-scoped IVault instances.
///
/// Architecture:
/// - Shared services (stateless singletons): IContentHasher, IGitService, IFileWatcherService
/// - Per-vault processing services, resolved from a service scope the vault owns: IExtractor,
///   IChunker, IVectorStore, IEmbeddingService, IHybridSearchService, IGraphRAGService,
///   IKeywordSearchService, IVaultImageEnricher, IRAGSecurityPipeline, IContextualEnrichmentService
/// - Tenant-scoped services built here: IVaultStorageService, IVaultQueueService, IVaultPipeline, IVault
///
/// The factory is a singleton and the processing services are commonly registered scoped (FileFlux
/// adapters, FluxIndex's GraphRAG and stores), so it must not receive them through its constructor:
/// the container refuses that under scope validation, and without validation one scoped instance
/// would be shared by every tenant for the life of the process. Each vault opens its own scope at
/// creation and disposes it with the tenant, so scoped services live exactly as long as the vault.
/// </summary>
public sealed partial class VaultFactory : IVaultFactory
{
    private readonly ConcurrentDictionary<string, VaultContext> _vaults = new();
    private readonly IServiceProvider _serviceProvider;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;
    private readonly FileVaultOptions _defaultOptions;
    private readonly object _createLock = new();
    private bool _disposed;

    // Shared services (stateless, can be reused across all tenants)
    private readonly IContentHasher _sharedHasher;
    private readonly IGitService _sharedGitService;
    private readonly IFileWatcherService _sharedFileWatcher;

    public VaultFactory(
        IServiceProvider serviceProvider,
        ILoggerFactory loggerFactory,
        IOptions<FileVaultOptions> options,
        IContentHasher hasher,
        IGitService gitService,
        IFileWatcherService fileWatcher)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = _loggerFactory.CreateLogger<VaultFactory>();
        _defaultOptions = options?.Value ?? new FileVaultOptions();

        // Store shared services
        _sharedHasher = hasher ?? throw new ArgumentNullException(nameof(hasher));
        _sharedGitService = gitService ?? throw new ArgumentNullException(nameof(gitService));
        _sharedFileWatcher = fileWatcher ?? throw new ArgumentNullException(nameof(fileWatcher));
    }

    public IVault GetOrCreate(string tenantId)
    {
        return GetOrCreate(tenantId, null);
    }

    public IVault GetOrCreate(string tenantId, Action<FileVaultOptions>? configureOptions)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(tenantId);

        if (_vaults.TryGetValue(tenantId, out var existingContext))
        {
            return existingContext.Vault;
        }

        lock (_createLock)
        {
            // Double-check after acquiring lock
            if (_vaults.TryGetValue(tenantId, out existingContext))
            {
                return existingContext.Vault;
            }

            var context = CreateVaultContext(tenantId, configureOptions);
            _vaults[tenantId] = context;

            LogCreatedVault(_logger, tenantId, context.VaultBasePath);

            return context.Vault;
        }
    }

    public VaultContext? GetContext(string tenantId)
    {
        _vaults.TryGetValue(tenantId, out var context);
        return context;
    }

    public bool Exists(string tenantId)
    {
        if (_vaults.ContainsKey(tenantId))
            return true;

        // Check if vault directory exists on disk
        var vaultPath = GetTenantVaultPath(tenantId);
        return Directory.Exists(vaultPath);
    }

    public IEnumerable<string> DiscoverTenants()
    {
        var basePath = _defaultOptions.VaultBasePath
            ?? Path.Combine(Directory.GetCurrentDirectory(), _defaultOptions.VaultDirectoryName);

        if (!Directory.Exists(basePath))
            yield break;

        foreach (var dir in Directory.GetDirectories(basePath))
        {
            var tenantId = Path.GetFileName(dir);
            var vaultPath = Path.Combine(dir, _defaultOptions.VaultDirectoryName);

            // Check for vault marker (queue.db or any entry directories)
            if (Directory.Exists(vaultPath) || File.Exists(Path.Combine(dir, "queue.db")))
            {
                yield return tenantId;
            }
        }
    }

    public IReadOnlyCollection<string> GetActiveTenants()
    {
        return _vaults.Keys.ToList().AsReadOnly();
    }

    public IEnumerable<VaultContext> GetAllContexts() => _vaults.Values;

    public async Task DisposeAsync(string tenantId)
    {
        if (_vaults.TryRemove(tenantId, out var context))
        {
            await DisposeContextAsync(context);

            LogDisposedVault(_logger, tenantId);
        }
    }

    public async Task DisposeAsync(string tenantId, bool purgeVectors)
    {
        if (_vaults.TryRemove(tenantId, out var context))
        {
            if (purgeVectors)
            {
                // Bulk-remove this tenant's vectors from the shared store before tearing down,
                // so they don't outlive the vault (single filtered delete — no per-entry loop).
                await context.Vault.PurgeAsync();
            }

            await DisposeContextAsync(context);

            LogDisposedVault(_logger, tenantId);
        }
    }

    public async Task DisposeAllAsync()
    {
        var contexts = _vaults.Values.ToList();
        _vaults.Clear();

        foreach (var context in contexts)
        {
            await DisposeContextAsync(context);
        }
    }

    private VaultContext CreateVaultContext(string tenantId, Action<FileVaultOptions>? configureOptions)
    {
        // Clone default options and apply tenant-specific configuration
        var tenantOptions = CloneOptions(_defaultOptions);
        tenantOptions.VaultBasePath = GetTenantVaultPath(tenantId);
        tenantOptions.VaultId = tenantId;
        configureOptions?.Invoke(tenantOptions);

        // Ensure directory exists
        Directory.CreateDirectory(tenantOptions.VaultBasePath);

        var optionsWrapper = MicrosoftOptions.Create(tenantOptions);

        // The vault's processing services come from a scope the vault owns (disposed with the
        // tenant), so scoped registrations are honoured instead of captured by this singleton.
        var scope = _serviceProvider.CreateScope();
        var scoped = scope.ServiceProvider;

        // Create tenant-specific storage service
        var storageLogger = _loggerFactory.CreateLogger<VaultStorageService>();
        var storage = new VaultStorageService(storageLogger, _sharedGitService, optionsWrapper);

        // Create tenant-specific queue service
        var queueLogger = _loggerFactory.CreateLogger<VaultQueueService>();
        var queue = new VaultQueueService(queueLogger, optionsWrapper);

        // Create tenant-specific pipeline. The tenant-specific pieces (storage, options, logger) and
        // the shared services this factory holds are passed in; every other constructor parameter -
        // the optional processing services - is resolved from the tenant's scope by the container,
        // exactly as it would be for the non-factory registration. Listing them by hand here is how
        // a newly added optional service went missing from every tenant vault while the plain
        // registration had it: the constructor is the one place that knows what the pipeline takes.
        var pipelineLogger = _loggerFactory.CreateLogger<VaultPipeline>();
        var pipeline = ActivatorUtilities.CreateInstance<VaultPipeline>(
            scoped,
            _sharedGitService,
            _sharedHasher,
            storage,
            pipelineLogger,
            optionsWrapper);

        // Create VaultManager with mixed shared/tenant-specific services
        var managerLogger = _loggerFactory.CreateLogger<VaultManager>();
        var vault = new VaultManager(
            _sharedHasher,
            _sharedGitService,
            pipeline,
            queue,
            _sharedFileWatcher,
            storage,
            managerLogger,
            optionsWrapper);

        // A tenant queue is consumed by its own worker; the hosted VaultBackgroundService only
        // consumes the container queue. Without this, background-mode tenant jobs sat Queued forever.
        VaultQueueWorker? worker = null;
        if (tenantOptions.EnableBackgroundProcessing)
        {
            worker = new VaultQueueWorker(
                _loggerFactory.CreateLogger<VaultQueueWorker>(),
                queue,
                VaultQueueWorker.ForSharedPipeline(pipeline),
                storage,
                tenantOptions);
            worker.Start();
        }

        return new VaultContext
        {
            TenantId = tenantId,
            VaultBasePath = tenantOptions.VaultBasePath,
            Vault = vault,
            QueueService = queue,
            StorageService = storage,
            Pipeline = pipeline,
            Options = tenantOptions,
            Worker = worker,
            Scope = scope
        };
    }

    private string GetTenantVaultPath(string tenantId)
    {
        var basePath = _defaultOptions.VaultBasePath
            ?? Path.Combine(Directory.GetCurrentDirectory(), _defaultOptions.VaultDirectoryName);

        return Path.Combine(basePath, tenantId, _defaultOptions.VaultDirectoryName);
    }

    private static FileVaultOptions CloneOptions(FileVaultOptions source)
    {
        return new FileVaultOptions
        {
            VaultDirectoryName = source.VaultDirectoryName,
            VaultBasePath = source.VaultBasePath,
            MaxFileSizeMB = source.MaxFileSizeMB,
            EnableRealTimeWatch = source.EnableRealTimeWatch,
            DebounceDelayMs = source.DebounceDelayMs,
            WatcherBufferSize = source.WatcherBufferSize,
            VersionRetentionCount = source.VersionRetentionCount,
            AutoCleanupOrphans = source.AutoCleanupOrphans,
            DefaultIncludePatterns = [.. source.DefaultIncludePatterns],
            DefaultExcludePatterns = [.. source.DefaultExcludePatterns],
            AdditionalTextExtensions = new HashSet<string>(source.AdditionalTextExtensions, source.AdditionalTextExtensions.Comparer),
            MaxImageEnrichmentAttempts = source.MaxImageEnrichmentAttempts,
            MaxConcurrentProcessing = source.MaxConcurrentProcessing,
            QueuePollingIntervalMs = source.QueuePollingIntervalMs,
            EnableAutoRetry = source.EnableAutoRetry,
            MaxInFlightPerGroup = source.MaxInFlightPerGroup,
            QueueGroupKey = source.QueueGroupKey,
            MaxRetryCount = source.MaxRetryCount,
            RetryDelayMs = source.RetryDelayMs,
            EnableBackgroundProcessing = source.EnableBackgroundProcessing,
            WorkerStartupTimeout = source.WorkerStartupTimeout,
            GitExecutablePath = source.GitExecutablePath,
            AllowMissingGit = source.AllowMissingGit,
            ContextualEnrichment = new ContextualEnrichmentDefaults
            {
                Enabled = source.ContextualEnrichment.Enabled,
                ContinueOnError = source.ContextualEnrichment.ContinueOnError
            },
            Chunking = new ChunkingDefaults
            {
                MaxChunkSize = source.Chunking.MaxChunkSize,
                OverlapSize = source.Chunking.OverlapSize,
                Strategy = source.Chunking.Strategy,
                Language = source.Chunking.Language,
                FormatStrategies = new Dictionary<string, string>(source.Chunking.FormatStrategies, source.Chunking.FormatStrategies.Comparer)
            }
        };
    }

    private static async Task DisposeContextAsync(VaultContext context)
    {
        // Stop the tenant's worker first (waits for in-flight jobs), then close the queue it consumed.
        if (context.Worker is { } worker)
        {
            await worker.DisposeAsync();
        }

        // Dispose queue service (closes SQLite connection)
        if (context.QueueService is IDisposable disposableQueue)
        {
            disposableQueue.Dispose();
        }

        // Release the vault's scoped services last — the pipeline and worker above used them.
        if (context.Scope is IAsyncDisposable asyncScope)
        {
            await asyncScope.DisposeAsync();
        }
        else
        {
            context.Scope?.Dispose();
        }

        // Give a moment for any pending operations
        await Task.Delay(100);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Synchronously dispose all contexts
        foreach (var context in _vaults.Values)
        {
            context.Worker?.Dispose();
            if (context.QueueService is IDisposable disposable)
            {
                disposable.Dispose();
            }
            context.Scope?.Dispose();
        }

        _vaults.Clear();
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        await DisposeAllAsync();
    }

    #region LoggerMessage Definitions

    [LoggerMessage(Level = LogLevel.Information, Message = "Created vault for tenant {TenantId} at {Path}")]
    private static partial void LogCreatedVault(ILogger logger, string tenantId, string path);

    [LoggerMessage(Level = LogLevel.Information, Message = "Disposed vault for tenant {TenantId}")]
    private static partial void LogDisposedVault(ILogger logger, string tenantId);

    #endregion
}
