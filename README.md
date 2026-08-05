# FluxFeed

> Document ingestion pipeline for .NET — track files, extract and chunk them, feed them to a vector index.

[![CI](https://github.com/iyulab/FluxFeed/actions/workflows/ci.yml/badge.svg)](https://github.com/iyulab/FluxFeed/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/FluxFeed.svg)](https://www.nuget.org/packages/FluxFeed)
[![.NET 10](https://img.shields.io/badge/.NET-10.0-purple)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## Overview

FluxFeed turns a folder of documents into an always-current search index. It watches the files,
extracts and chunks them with [FileFlux](https://github.com/iyulab/FileFlux), and indexes the chunks
into [FluxIndex](https://github.com/iyulab/FluxIndex) — then keeps the index in step as files change,
move, or disappear.

FluxFeed owns ingestion only. Embedding, retrieval and ranking belong to FluxIndex, and the
dependency is one-way: FluxFeed → FluxIndex.

Each tracked file gets a **vault entry** — a small git-backed directory holding the extracted text
plus any notes you add by hand. Re-indexing is therefore cheap and auditable: you can diff a
document's extracted content, see its commit history, and edit it without touching the original.

## Features

- **File-source vault** — per-file git-tracked directory (`refined.md`, `append-text.md`, `qa.md`)
- **Change detection** — content hash for source changes, git status for vault edits
- **Folder watching** — real-time `FileSystemWatcher` with debounce and glob include/exclude patterns
- **Background queue** — bounded concurrency, automatic retry, pause/resume, SQLite-persisted
- **Multi-tenant** — isolated vaults via `IVaultFactory`, with single-call vector purge per tenant
- **Extraction diagnostics** — a legitimate zero-chunk result (scanned PDF, blank page) says so
- **Damage-aware records** — records are swapped in atomically, and an unreadable one is reported rather than dropped from listings
- **Image enrichment** — plug in a vision model and extracted images become indexed content
- **Hybrid-ready** — chunks are written to the keyword index alongside the vector store when one is registered

## Installation

```bash
dotnet add package FluxFeed
```

## Quick Start

```csharp
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxIndex.Core.Application.Interfaces;  // IEmbeddingService
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// 1. FluxIndex side — a vector store and an embedding service must be registered.
services.AddSQLiteVecVectorStore(o =>
{
    o.DatabasePath = "fluxindex.db";
    o.VectorDimension = 1536;
});
services.AddSingleton<IEmbeddingService, MyEmbeddingService>();  // bring your own

// 2. FluxFeed side — vault + FileFlux extraction/chunking + FluxIndex indexing.
services.AddFileVaultWithFluxIndex(o => o.VaultBasePath = "./data/.vault");

var provider = services.BuildServiceProvider();

using var scope = provider.CreateScope();
var vault = scope.ServiceProvider.GetRequiredService<IVault>();

// Index a file and wait for the pipeline to finish.
var entry = await vault.MemorizeAsync("./docs/handbook.pdf", waitForCompletion: true);
Console.WriteLine($"{entry.Stage} · {entry.ChunkCount} chunks");

// Watch a folder; new and changed files are queued automatically.
await vault.AddWatchedFolderAsync("./docs", autoMemorize: true);

// Search, optionally scoped to a path.
var results = await vault.SearchAsync("vacation policy", VaultSearchOptions.ForFolder("./docs"));
```

`IVault` is scoped — resolve it from a scope, or inject it into a scoped service, rather than from
the root provider.

### Registration entry points

| Method | Registers |
|---|---|
| `AddFileVault` | Vault, queue, watcher only — bring your own `IExtractor`/`IChunker` |
| `AddFileVaultWithFileFlux` | The above + FileFlux extraction and chunking |
| `AddFileVaultWithFluxIndex` | The above + FluxIndex indexing (recommended default) |
| `AddFileVaultFactory*` | Same three, but tenant-scoped via `IVaultFactory` instead of a single `IVault` |

FileFlux services are registered only if you have not registered them yourself, so a prior
`AddFileFlux(ServiceLifetime.Singleton)` keeps its lifetime.

## How it works

An entry moves through four stages, or lands on `Error`:

```
Source → Extracted → Refined → Memorized
```

`Refined` is skipped when there is nothing to refine, and `Stale` marks an entry whose vectors went
missing — the integrity check sets it, and re-memorizing restores search.

`MemorizeAsync` runs the whole pipeline (re-extracting the source). `RefreshAsync` re-indexes the
refined content without re-extracting — use it after hand-editing `append-text.md` or `qa.md`.

Each entry lives under the vault base path, keyed by a hash of its absolute file path:

```
.vault/{filepath-hash}/
├── meta.json          (not git-tracked)
├── images/            (not git-tracked)
│   └── manifest.json
└── vault/             (git-tracked)
    ├── refined.md     extracted + refined content
    ├── append-text.md your additions
    └── qa.md          your Q&A
```

Because `vault/` is a git repository, `DiffAsync` and `LogAsync` report exactly what changed and when.

Note what is *not* in that repository: the entry record, the raw extracted text and the images are
work products sitting above it. They are outside the repository rather than ignored by it — which is
why there is no ignore file at the entry level, where git would never read one.

> **Breaking in 0.8.0** — `VaultEntry.GitignorePath` and `IVaultStorageService.CreateGitignoreAsync`
> were removed for that reason. There is no replacement; the file they produced had no effect under
> any condition, so calls to them can simply be deleted.

## File selection patterns

`FileVaultOptions.DefaultIncludePatterns` / `DefaultExcludePatterns` apply to **discovery paths only**:

| Path | Patterns applied |
|---|---|
| `ScanFolderAsync` / `SyncAsync` | Yes — non-matching files are skipped before change detection |
| Folder-watcher events | Yes — per-folder patterns override the defaults |
| Explicit `MemorizeAsync` / `RefreshAsync` | No — an explicit call is an explicit intent; silently skipping it would hide a caller's mistake |

Exclusion wins over inclusion, and an empty include list means "include everything".

## Diagnostics

A document that yields zero chunks is not necessarily a failure, and a failure is not necessarily
described by its most recent error. Both cases are reported explicitly rather than left to inference.

```csharp
var entry = await vault.MemorizeAsync(path, waitForCompletion: true);

if (entry.ExtractionHints?.TryGetValue("extraction_failure_reason", out var reason) == true)
{
    // "no_text_layer" (image-only/scanned) | "blank_page" (empty document)
    // entry.ExtractionWarnings carries the human-readable explanation.
}
```

- **Extraction hints are an opaque pass-through.** Keys and values are the extractor's own vocabulary
  (FileFlux `RawContent.Hints` / `Warnings`); FluxFeed persists them to `meta.json` without
  interpreting them. Only scalar values are stored, so new hints flow through without drift.
  They always describe the *latest* extraction, and are cleared when one reports none.
- **`FirstError` vs `LastError`.** `FirstError` is the failure that started the current episode and
  usually carries the extractor's diagnosis; `LastError` is the most recent one. A retry failing for
  its own reason overwrites the latter but not the former. Both clear on a successful stage or reset,
  and neither is cleared by sync-status transitions — so use `Stage`/`SyncStatus`, not
  `FirstError != null`, to decide whether an entry is currently broken.
- **Refresh has a precondition.** It needs refined content to exist, which `ProcessingStage` does not
  imply — a memorize with nothing to index skips the refine step, so a `Memorized` entry legitimately
  may have none. `RefreshAsync` rejects those, and `DetectChangesAsync` recommends `Memorize`
  instead, since re-extraction lets a failed or empty entry recover on its own.

## Damaged records

An entry record (`meta.json`) is written to a scratch file and swapped into place. Concurrent writers
therefore only decide which record wins — they never interleave — and an interrupted write never
leaves half a record behind.

An unreadable record is distinguished from an absent one:

```csharp
// absent → null; present but unreadable → VaultRecordUnreadableException
var entry = VaultEntry.LoadByHash(hash, vaultBasePath);

// entry directories missing from the listing, exposed so they can be repaired
IReadOnlyList<string> damaged = await vault.ListUnreadableAsync();
```

- `ListAsync()` skips unreadable records but logs a warning. To display those entries or offer to
  repair them, use the paths returned by `ListUnreadableAsync()`.
- The two listings split on the same signal, so an entry appears in exactly one of them and their
  counts sum to the total. Any other IO error propagates rather than being swallowed — a listing that
  quietly gets shorter is the failure this reporting exists to eliminate.
- The swap sets the outgoing record aside under a scratch name. When the platform cannot clear that
  scratch file — likely enough while a reader holds the record open — it stays in the entry directory
  and the next write removes it, but only once it is old enough that no in-flight swap still needs it
  to roll back to. One may briefly be visible right after a concurrent write; they do not accumulate.
- Paths that rewrite the record anyway (memorize, refresh) report an unreadable record and then
  recreate it rather than failing, which would strand the entry permanently. A recreated record
  starts with no history.

## Optional integrations

Each of these is enabled by registering a service. Register nothing and the pipeline behaves as if
the feature did not exist.

### Image enrichment — `IVaultImageEnricher`

Images extracted from documents are always stored. Register a describer and those descriptions get
indexed too, which is what makes scanned or diagram-only documents searchable at all.

```csharp
public sealed class VisionEnricher : IVaultImageEnricher
{
    public async Task<string?> DescribeAsync(VaultImageDescriptionRequest request, CancellationToken ct)
        => await _vision.CaptionAsync(request.Image.FilePath, request.DocumentText, ct);
        // returning null means "not this time" — the pipeline retries that image on the next run
}

services.AddSingleton<IVaultImageEnricher, VisionEnricher>();
```

Descriptions are persisted per image, so re-memorizing does not re-describe images that already
succeeded, and one image's failure aborts neither the others nor the memorize. Each description is
indexed as its own chunk tagged `chunk_kind="image_description"` with `image_id` / `image_file`
metadata — no markers are injected into the document text.

Descriptions go through the same chunker as the body, so `MaxChunkSize` bounds them too and a long
description becomes several chunks that each carry the same `image_id` / `image_file`. Returning a
long description is therefore safe: it cannot push the document's embedding request past the model's
context window.

### Keyword index — `IKeywordSearchService`

When one is registered, every chunk written to the vector store is written to the keyword index as
well, and `RemoveAsync` deletes from both. Without it the keyword index stays empty and hybrid search
degenerates to vector-only. Check `IVaultPipeline.SupportsKeywordIndex` to confirm the wiring —
it is on the interface, so holding the pipeline as `IVaultPipeline` is enough (`SupportsGraphRAG`
reports the GraphRAG leg the same way).

### Hybrid search — `IHybridSearchService`

`VaultSearchOptions.SearchStrategy = VaultSearchStrategy.Hybrid` is honored only when this service is
registered. Otherwise the query runs as vector search and says so via
`VaultSearchResult.ExecutedStrategy` — compare it against `RequestedStrategy` rather than assuming
the request was honored.

## Multi-tenant

`AddFileVaultFactoryWithFluxIndex` swaps the single `IVault` for an `IVaultFactory`. Each tenant gets
its own `.vault/` directory and processing queue, while stateless services and the vector store are
shared.

```csharp
services.AddFileVaultFactoryWithFluxIndex(o => o.VaultBasePath = "./data");

var vault = factory.GetOrCreate("tenant-a");
await vault.MemorizeAsync(path);

// Deleting a tenant: one filtered delete removes all of its vectors, no per-entry loop.
await factory.DisposeAsync("tenant-a", purgeVectors: true);
```

Chunks are tagged with a `vault_id` metadata field, which is what makes the bulk purge
(`IVault.PurgeAsync`) possible.

## Configuration

`FileVaultOptions` (bindable from the `FileVault` configuration section):

| Option | Default | Description |
|---|---|---|
| `VaultBasePath` | `null` | Vault root. When null, `.vault` next to each source file |
| `VaultId` | `null` | Tenant id; set by `IVaultFactory`. Required for `PurgeAsync` |
| `MaxFileSizeMB` | `100` | Larger files are skipped |
| `EnableRealTimeWatch` | `true` | Folder watching |
| `DebounceDelayMs` | `500` | Merge window for rapid change events |
| `EnableBackgroundProcessing` | `true` | Background queue; when false the service idles |
| `MaxConcurrentProcessing` | `4` | Concurrent file operations |
| `EnableAutoRetry` / `MaxRetryCount` / `RetryDelayMs` | `true` / `3` / `5000` | Retry policy |
| `AutoCleanupOrphans` | `false` | Remove entries whose source file is gone, during sync |
| `Chunking.MaxChunkSize` / `OverlapSize` / `Strategy` | `1024` / `128` / `Intelligent` | Chunking defaults, with per-extension overrides via `Chunking.FormatStrategies` |
| `DefaultIncludePatterns` / `DefaultExcludePatterns` | common document / temp-file globs | See [File selection patterns](#file-selection-patterns) |

## Requirements

- .NET 10.0
- FluxIndex.Core 0.17.0+
- FileFlux 0.16.0+ — the source of the structured extraction diagnostics described above

## License

MIT — see [LICENSE](LICENSE).
