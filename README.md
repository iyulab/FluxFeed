# FluxFeed

> Document ingestion pipeline for .NET — track files, extract and chunk them, feed them to a vector index.

[![CI](https://github.com/iyulab/FluxFeed/actions/workflows/ci.yml/badge.svg)](https://github.com/iyulab/FluxFeed/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/FluxFeed.svg)](https://www.nuget.org/packages/FluxFeed)
[![.NET 10](https://img.shields.io/badge/.NET-10.0-purple)](https://dotnet.microsoft.com/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

## Overview

FluxFeed turns a folder of documents into a search index. It extracts and chunks the files with
[FileFlux](https://github.com/iyulab/FileFlux), indexes the chunks into
[FluxIndex](https://github.com/iyulab/FluxIndex), and brings the index back in step as files change, move, or
disappear — as they happen in folders you watch with `autoMemorize: true`, or when you run a sync (see
[Watching folders](#watching-folders)).

FluxFeed owns ingestion only. Embedding, retrieval and ranking belong to FluxIndex, and the
dependency is one-way: FluxFeed → FluxIndex.

Each tracked file gets a **vault entry** — a small git-backed directory holding the extracted text
plus any notes you add by hand. Re-indexing is therefore cheap and auditable: you can diff a
document's extracted content, see its commit history, and edit it without touching the original.

## Features

- **File-source vault** — per-file git-tracked directory (`refined.md`, `append-text.md`, `qa.md`)
- **Change detection** — content hash for source changes, git status for vault edits
- **Folder watching** — `AddWatchedFolderAsync(path, autoMemorize: true)` keeps the vault in step with a folder:
  created and changed files are memorized (debounced), deleted ones removed, renamed files and folders moved without
  re-embedding; include/exclude glob patterns — see [Watching folders](#watching-folders)
- **Background queue** — bounded concurrency, automatic retry, operator requeue, pause/resume, SQLite-persisted
- **Multi-tenant** — isolated vaults via `IVaultFactory`, with single-call vector purge per tenant
- **Extraction diagnostics** — a legitimate zero-chunk result (scanned PDF, blank page) says so
- **Source locations** — a hit carries the page (`pageNumber`) or the stretch of a recording (`ff_start_seconds`) it came from — see [Where a hit came from](#where-a-hit-came-from)
- **Damage-aware records** — records are swapped in atomically, and an unreadable one is reported rather than dropped from listings
- **Image enrichment** — plug in a vision model and extracted images become indexed content
- **Hybrid-ready** — chunks are written to the keyword index alongside the vector store when one is registered
- **Move and rename without re-embedding** — `IVault.MoveAsync(sourcePath, destinationPath)` and
  `MoveFolderAsync(sourceFolder, destinationFolder)` move a tracked file's entry, git history and index rows (vector,
  keyword, GraphRAG) to its new path; always available — see [Moving and renaming files](#moving-and-renaming-files--moveasync)
- **Reranking** — `VaultSearchOptions.UseReranker` orders an over-fetched candidate pool with the registered FluxIndex
  `IReranker` (opt-in; see [Reranked search](#reranked-search--vaultsearchoptionsusereranker))

## Installation

```bash
dotnet add package FluxFeed
```

FluxFeed indexes into whatever FluxIndex vector store you register; it does not ship one. The examples below
use the SQLite store, which is a separate package:

```bash
dotnet add package FluxIndex.Storage.SQLite   # the vector store the examples use (AddSQLiteVecVectorStore)
```

### Requirements

- **.NET 10**, and a registered FluxIndex vector store + `IEmbeddingService` (see Quick Start). The package
  pulls in `FluxIndex.Core` and `FileFlux` (the source of the extraction diagnostics described below) at the
  versions it was built against.
- **git 2.x on PATH.** Vault history (`DiffAsync`, `LogAsync`, `GetContentAtCommitAsync`) runs the git CLI.
  If git is installed elsewhere, set `FileVaultOptions.GitExecutablePath`; if you deliberately want a
  history-less vault, set `FileVaultOptions.AllowMissingGit = true`. Without either, the first vault
  operation fails with a message that says exactly this instead of silently creating a vault with no history.
- **A Generic Host** (`Microsoft.Extensions.Hosting`) for background processing — the queue worker is an
  `IHostedService`. Console apps without a host set `EnableBackgroundProcessing = false` (see below).

## Quick Start

```csharp
using FluxFeed.Extensions;
using FluxFeed.Interfaces;
using FluxIndex.Core.Application.Interfaces;  // IEmbeddingService
using FluxIndex.Storage.SQLite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

// 1. FluxIndex side — a vector store and an embedding service must be registered.
IEmbeddingService embedder = new MyEmbeddingService();   // bring your own, or a FluxIndex.Providers.* service
builder.Services.AddSingleton<IEmbeddingService>(embedder);
builder.Services.AddSQLiteVecVectorStore(o =>
{
    o.DatabasePath = "fluxindex.db";
    o.VectorDimension = embedder.GetEmbeddingDimension();  // the store's dimension is the embedder's
});

// 2. FluxFeed side — vault + FileFlux extraction/chunking + FluxIndex indexing.
builder.Services.AddFileVaultWithFluxIndex(o => o.VaultBasePath = "./data/.vault");

using var host = builder.Build();
await host.StartAsync();   // starts the background queue worker

using (var scope = host.Services.CreateScope())
{
    var vault = scope.ServiceProvider.GetRequiredService<IVault>();

    // Index a file and wait for the pipeline to finish.
    var entry = await vault.MemorizeAsync("./docs/handbook.pdf", waitForCompletion: true);
    Console.WriteLine($"{entry.Stage} · {entry.ChunkCount} chunks");

    // Watch a folder: from now on its changes reach the index by themselves.
    // SyncAsync catches up on what changed while nothing was watching.
    await vault.AddWatchedFolderAsync("./docs", autoMemorize: true);
    await vault.SyncAsync();

    // Search, optionally scoped to a path.
    var results = await vault.SearchAsync("vacation policy", VaultSearchOptions.ForFolder("./docs"));
}

await host.StopAsync();
```

FluxFeed binds the vector store to the embedder's identity for you when it builds the vault pipeline (the
first `IVault` you resolve, not host start) — there is no `BindIdentity` call to make. What the identity
does is the store's business: the sqlite-vec store names its vector table by the embedder's fingerprint, so
a different embedder over the same database gets a vector table of its own rather than mixing vectors
(re-memorize to fill it), and binding one store instance to two different embedders throws
`EmbeddingModelMismatchException`.

`IVault` is scoped — resolve it from a scope, or inject it into a scoped service, rather than from
the root provider.

### Without a host

The background worker is an `IHostedService`; a plain `ServiceCollection` never starts it. Either
start the hosted services yourself, or process inline:

```csharp
var services = new ServiceCollection();
services.AddSingleton<IEmbeddingService>(embedder);
services.AddSQLiteVecVectorStore(o => { o.DatabasePath = "fluxindex.db"; o.VectorDimension = embedder.GetEmbeddingDimension(); });
services.AddFileVaultWithFluxIndex(o =>
{
    o.VaultBasePath = "./data/.vault";
    o.EnableBackgroundProcessing = false;   // MemorizeAsync/RefreshAsync run inline and return the terminal entry
});

using var provider = services.BuildServiceProvider();
using var scope = provider.CreateScope();
var vault = scope.ServiceProvider.GetRequiredService<IVault>();
var entry = await vault.MemorizeAsync("./docs/handbook.pdf", waitForCompletion: true);
```

If background processing is left on without a host, `MemorizeAsync(..., waitForCompletion: true)` does
not hang: after `WorkerStartupTimeout` (5 s) it throws an `InvalidOperationException` that names both
fixes above.

### Registration entry points

| Method | Registers |
|---|---|
| `AddFileVault` | Vault, queue, watcher only — bring your own `IExtractor`/`IChunker` |
| `AddFileVaultWithFileFlux` | The above + FileFlux extraction and chunking |
| `AddFileVaultWithFluxIndex` | The above + FluxIndex indexing (recommended default) |
| `AddFileVaultFactory*` | Same three, but tenant-scoped via `IVaultFactory` instead of a single `IVault` |

Indexing into FluxIndex happens in the vault pipeline whenever an `IVectorStore` and an `IEmbeddingService`
are registered, whichever of these entry points you call. `AddFileVaultWithFluxIndex` adds one thing on top of `AddFileVaultWithFileFlux`: a
`FluxIndexMemorizer` (Scoped) for memorizing chunks outside the pipeline in your own flows; the pipeline does not use it.

Other public entry points:

- `AddFileVaultWithPipeline` — same registrations as `AddFileVault`; the name documents that you bring your own
  `IExtractor` / `IChunker` (and a vector store + embedder for indexing).
- `UseFileVaultHasher<T>()` / `UseFileVaultGitService<T>()` — replace the content hasher (`IContentHasher`) or the
  git service (`IGitService`); both are registered as Singleton.
- `UseFileVaultPipeline<T>()` — replace `IVaultPipeline`. Note that it registers your pipeline as **Singleton**,
  while the default pipeline is Scoped.
- `FluxIndexContextBuilderExtensions.UseFileVault(...)` — calls `AddFileVault` from a builder that exposes
  `ConfigureServices(Action<IServiceCollection>)`.

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

### When the extractor gets better — re-extraction

Each extraction records which extractor made it (`entry.ExtractedBy`, e.g. `FileFlux 0.36.2` with FluxFeed's pipeline
revision). An unchanged source is never re-extracted by default, because re-extraction re-runs OCR and image
description. To let an upgrade reach existing documents, opt in:

```csharp
using FluxFeed.Options;   // ReextractionPolicy

services.AddFileVaultWithFluxIndex(o => o.Reextraction = ReextractionPolicy.WhenExtractorMinorChanges);

var status = await vault.StatusAsync();
// status.CurrentExtraction       — what an extraction made now would record
// status.OutdatedExtractionCount — entries the policy would re-extract (with the policy off: any difference)
```

With the policy on, `RefreshAsync` (and a queued refresh) of an outdated entry re-extracts it, and `DetectChangesAsync` /
`SyncAsync` recommend Memorize for it — except when its vault has uncommitted edits, which are refreshed instead of
overwritten. `WhenExtractorMinorChanges` ignores patch releases; `WhenExtractorChanges` does not. Entries extracted
before identities were recorded count as outdated. To re-extract one file regardless of the policy, call
`MemorizeAsync` for it.
With background processing on, both only enqueue a job and return the entry as it was; pass
`waitForCompletion: true` (`MemorizeAsync(path, waitForCompletion: true)`,
`RefreshAsync(path, waitForCompletion: true)`) to get the re-indexed entry — and its new commit — back.

### Where a hit came from

With the FileFlux extractor, each text chunk records where it came from in its source, as search-hit metadata:
`pageNumber` / `ff_start_page` / `ff_end_page` for paginated documents (PDF, …) and `ff_start_seconds` /
`ff_end_seconds` for recordings (with `AddLMSupplyTranscriber`). The keys are `VaultPipeline.PageNumberMetadataKey`
and its siblings. The locations are recorded at extraction (`extracted.spans.json`) and apply while `refined.md` is
unchanged; after a hand edit the chunks are indexed without a location (and a warning is logged) until the next
re-extraction. A custom `IChunker` receives the spans as `ChunkAsync(content, spans, options, ct)` and returns
`ContentChunk`s with a `Location`.

### Re-indexing: chunk identity, swap, rollback

Every chunk is indexed under an id derived from what it is — the entry's path hash plus the passage
(`ChunkIdentity.ForText`), or the image it describes (`ChunkIdentity.ForImage`) — not a fresh GUID per
run. Memorizing an unchanged file therefore rewrites the same rows in place; editing one paragraph
replaces that paragraph's rows and leaves the rest untouched. The stored id is the one you read back
from the vector store, so anything keyed on chunk ids (GraphRAG entity provenance, your own
bookkeeping) survives a re-index. Contextual enrichment and RAG sanitizing change a chunk's stored
text, not its id: identity is taken from the chunker's raw output. Two identical passages in one
document get distinct ids (by occurrence), so neither shadows the other.

Re-indexing is a swap. The previous generation's rows are identified first, the new generation is
written, and only the previous rows this run did not write again are deleted. With GraphRAG registered, the graph
leg forgets those chunks too (`IGraphRAGService.ForgetChunksAsync`): their communities, entity links and relationships
go, and so do a removed document's. If indexing fails halfway
— an embedding error on one chunk — the rows this run added are removed and the previous generation is
left whole: the entry lands on `Error`, but it keeps answering searches with its last good index
(`VaultEntry.IsSearchable`), and the job's resume checkpoint is rewound to where the run started so a
retry cannot skip rows the rollback removed. Vaults indexed before 0.26.0 hold GUID ids; their first
re-index after upgrading finds nothing in common and replaces everything, exactly as before — no
migration.

The previous generation is enumerated on every leg — the vector store and, when one is registered,
the keyword index (`IKeywordSearchService.GetChunkIdsByDocumentIdAsync`, FluxIndex.Core 0.39.0) — and
the union is what the swap supersedes. Before 0.27.0 only the vector store was asked, on the
assumption that both legs key their rows identically; keyword rows written before the SQLite stores
honoured caller ids (FluxIndex 0.36.2) never matched, so each re-index left the previous keyword
generation searchable beside the new one. An ordinary re-index now removes those rows along with the
rest of the previous generation; a document that is never re-indexed keeps them. Since 0.28.0 the
superseded keyword rows are removed with one `IKeywordSearchService.DeleteChunksAsync` call
(FluxIndex.Core 0.40.0) rather than one call per chunk.

Each entry lives under the vault base path, keyed by a hash of its absolute file path:

```
.vault/{filepath-hash}/
├── meta.json          (not git-tracked)
├── images/            (not git-tracked)
│   └── manifest.json
└── vault/             (git-tracked)
    ├── refined.md     extracted + refined content
    ├── append-text.md your additions  (create it yourself, then RefreshAsync)
    └── qa.md          your Q&A         (same)
```

Memorize writes `refined.md` only; `append-text.md` and `qa.md` are yours to create next to it. Their
content is indexed together with `refined.md` on the next `RefreshAsync`, which also commits them.

Because `vault/` is a git repository, `DiffAsync` and `LogAsync` report exactly what changed and when.
`DiffAsync` compares the working tree against HEAD — since Memorize/RefreshAsync auto-commit after
every successful update, the working tree is normally clean by the time a caller checks, so `DiffAsync`
usually returns an empty string. Use `DiffLastChangeAsync` instead to see what the most recent commit
actually changed (HEAD vs. its parent, or vs. the empty tree on the entry's first commit).
`GetContentAtCommitAsync(filePath, commitHash)` reads the combined content (`refined.md` +
`append-text.md` + `qa.md`) as it existed at a specific commit from that history — read-only, it does
not touch the working tree. Returns `null` when the commit is unknown. To actually roll an entry back,
write the returned content through the entry's normal update path (e.g. `RefreshAsync` after
overwriting `append-text.md`/`qa.md`) — the library hands back the past content, the caller decides
how to apply it.

Note what is *not* in that repository: the entry record, the raw extracted text and the images are
work products sitting above it. They are outside the repository rather than ignored by it — which is
why there is no ignore file at the entry level, where git would never read one.

> **Breaking in 0.8.0** — `VaultEntry.GitignorePath` and `IVaultStorageService.CreateGitignoreAsync`
> were removed for that reason. There is no replacement; the file they produced had no effect under
> any condition, so calls to them can simply be deleted.

### Moving and renaming files — `MoveAsync`

An entry and its chunk ids derive from the file's path, so a moved file would otherwise mean `RemoveAsync` plus
`MemorizeAsync`: extraction, chunking and embedding all over again, with the document missing from search in between.
`MoveAsync` re-keys what is already there instead (0.39.0):

```csharp
// The file has already moved on disk; the entry, its history and its index rows follow it.
VaultMoveResult moved = await vault.MoveAsync("docs/draft.md", "docs/final/report.md", ct);
Console.WriteLine($"{moved.VectorChunksMoved} chunks re-keyed, none re-embedded");

// A whole folder is one move per entry; an entry that cannot move is reported and the rest still move.
VaultFolderMoveResult folder = await vault.MoveFolderAsync("docs/2025", "archive/2025", ct);
foreach (var error in folder.Errors)
    Console.WriteLine($"{error.SourcePath}: {error.ErrorMessage}");
```

- The entry directory moves with its git repository, so `LogAsync` at the new path shows the commits made before.
- Every chunk gets the id a memorize at the new path would give it (`ChunkIdentity`), on the vector store, the keyword
  index and the GraphRAG graph, with `source_path`, `file_name`, `filepath_hash` and `document_id` rewritten. The
  vectors are kept as stored (FluxIndex `ReassignDocumentAsync`), so the next memorize of the file is an update.
- Everything is checked before anything is written. A chunk whose id no longer follows from its stored text — rewritten
  by the RAG security pipeline, or indexed before 0.26.0 — or a keyword row with no vector row makes the move throw
  with nothing changed; `RepairKeywordIndexAsync` fixes the second, removing and memorizing the entry the first. A
  destination that is already tracked, an entry being removed, or a queued or running job for either path also throws.
  If a later index leg fails, the earlier ones and the directory are moved back.
- It records a move that already happened: it neither moves the file nor checks that it exists. It runs at once,
  not through the queue.
- A rename that only changes letter case names the same entry (paths are compared case-insensitively); only the
  recorded path changes and `VaultMoveResult.IndexRekeyed` is `false`.
- In a folder watched with `autoMemorize: true` a rename is followed for you with `MoveAsync` (a folder rename with
  `MoveFolderAsync`) — see [Watching folders](#watching-folders).

## Watching folders

`AddWatchedFolderAsync(path, autoMemorize: true)` keeps the vault in step with the folder while the process runs:

| In the folder | In the vault |
|---|---|
| a file is created or saved (debounced by `DebounceDelayMs` — one save, one memorize) | `MemorizeAsync` (unchanged content is detected by hash and costs nothing) |
| a tracked file is deleted | `RemoveAsync` |
| a tracked file is renamed | `MoveAsync` — nothing is extracted or embedded again |
| a folder is renamed | `MoveFolderAsync` |
| a file is renamed to a name the patterns reject / from one they rejected | `RemoveAsync` / `MemorizeAsync` |

```csharp
var folder = await vault.AddWatchedFolderAsync(
    "./docs", autoMemorize: true, includePatterns: ["*.md", "*.pdf"], excludePatterns: ["~$*", "*.tmp"]);
```

- Needs `FileVaultOptions.EnableRealTimeWatch` (the default). Changes are applied one at a time in the order they
  happened, on a background loop; a failure is logged and the next change still applies. A move the vault refuses
  because a job for that file is still queued is retried, then falls back to remove + memorize.
- The folder belongs to the vault, not to the scope you added it from: every scope of the container's `IVault` sees
  and can pause, resume or remove it, and under `IVaultFactory` each tenant has its own folders. Folders are not
  persisted — add them again at start-up, then `SyncAsync` to catch up on what changed while the process was down.
- A rename out of a watched folder arrives as a delete, and into one as a create.
- Without `autoMemorize` (the default) a folder is registered for `SyncAsync` / `ScanFolderAsync` and the watcher only
  raises its events (`IFileWatcherService.FileCreated` / `FileModified` / `FileDeleted` / `FileRenamed`, each with the
  folder's `FolderId`) for your own handling.
- A `VaultManager` you construct yourself (outside `AddFileVault` / `IVaultFactory`) keeps its folders to itself and
  does not apply changes.
- `RemoveWatchedFolderAsync(folderId, removeTrackedFiles: true)` removes the entries inside the folder at any depth;
  a sibling that shares its leading characters (`docs2` for `docs`) is not inside it.

## File selection patterns

`FileVaultOptions.DefaultIncludePatterns` / `DefaultExcludePatterns` apply to **discovery paths only**:

| Path | Patterns applied |
|---|---|
| `ScanFolderAsync` / `SyncAsync` | Yes — non-matching files are skipped before change detection |
| Folder-watcher events | Yes for created, modified and deleted files — per-folder patterns override the defaults; renames are raised unfiltered |
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

### Status, change detection, index audit — four calls, four costs

A status read, a change sweep, an index audit and a disk measurement are different questions with different costs,
so each is its own call:

| Call | Answers | Cost |
|---|---|---|
| `StatusAsync()` | entry counts by stage and sync status, queue, watchers, `IndexedChunkCount` (what the records claim) | listing the entries — no git, no index query, no write |
| `DetectChangesAsync()` | which entries' sources changed, were deleted, or have modified vault files; **persists** each entry's `SyncStatus` | a source hash and a `git status` process per entry — tens of ms per entry |
| `AuditIndexAsync()` | what each index leg holds for the searchable entries | one id enumeration per leg per entry — a round trip per entry on a remote store |
| `GetStorageSizeAsync()` | bytes on disk under the entry directories | a directory walk per entry |

`GetEntriesNeedingSyncAsync()` and `ListByStatusAsync()` read the persisted `SyncStatus`; it is refreshed by
`DetectChangesAsync()`, `SyncAsync()` and folder scans — not by `StatusAsync()`. Entries being removed are left out of
`TotalEntries`, the stage counts and `OrphanedCount`; `RemovalPendingCount` and `RemovalPartialCount` report them.

### Index legs — `AuditIndexAsync`

The entry store counts chunks; it says nothing about rows. `VaultIndexAudit` therefore reports, for the
searchable entries, what each index leg actually holds: `IndexedChunkCount` (what the entries claim),
`VectorRowCount` and `KeywordRowCount` (what the legs hold — `null` for a leg that is not registered,
never zero), and `MismatchedEntryCount`, the entries whose two legs hold different id sets. The
counts are taken per entry through each leg's own id enumeration, so they are scoped to this vault
even on a store shared with others. A mismatch is the drift a re-index of that entry removes (see
*Re-indexing*); the same numbers before and after are how you tell the re-index did.

`MismatchedEntryCount` compares **id sets**, not row counts: on a vault indexed before FluxIndex 0.36.2 every entry's
keyword ids differ from its vector ids, so every entry counts as mismatched even where the two legs happen to hold the same
number of rows. Expect it to be much larger than a per-document row-count comparison on the same database, and to fall by one
per re-indexed entry.

An entry that is never re-indexed keeps its mismatch. `RepairKeywordIndexAsync` rebuilds the keyword rows of every mismatched
entry from the rows the vector store already holds — nothing is re-embedded — and leaves entries whose legs agree alone. Run it
once after upgrading such a vault, with the queue paused:

```csharp
await vault.PauseQueueAsync();
var repair = await vault.RepairKeywordIndexAsync();   // EntriesChecked, EntriesRepaired, KeywordRowsWritten, KeywordRowsRemoved
await vault.ResumeQueueAsync();
```

It throws when no keyword index is registered rather than reporting nothing to repair.

Switching the text analyzer (`ITextAnalyzer`) or the keyword field set (`KeywordFieldOptions`) is a different
shape: every keyword row is still present under the right id, only written the old way, so the mismatch-only
repair finds nothing to do and a full re-memorize re-extracts and re-embeds a corpus that only needs its keyword
rows rewritten. `KeywordIndexRepairScope.All` rewrites every searchable entry from the vector leg — chunk metadata
included, so the fields the new configuration reads are populated — without re-embedding:

```csharp
await vault.PauseQueueAsync();
var rebuilt = await vault.RepairKeywordIndexAsync(KeywordIndexRepairScope.All);   // EntriesRepaired == every searchable entry
await vault.ResumeQueueAsync();
```

A repair does not stop at an entry that fails (a timeout on a large index, say): every other entry is rewritten, and
the run ends with a `KeywordIndexRepairException` that carries the counts (`Result`) and the entries that were not
rewritten (`Failures` — source path and error). A failed entry keeps its previous keyword rows. Retry just those:

```csharp
using FluxFeed.Domain.Exceptions;

try
{
    await vault.RepairKeywordIndexAsync(KeywordIndexRepairScope.All);
}
catch (KeywordIndexRepairException ex)
{
    var paths = ex.Failures.Select(f => f.SourcePath).ToList();
    await vault.RepairKeywordIndexAsync(paths, KeywordIndexRepairScope.All);
}
```

```csharp
using Microsoft.Extensions.Logging;

var audit = await vault.AuditIndexAsync();
if (audit.MismatchedEntryCount > 0)
    logger.LogWarning("{Count} entries have keyword rows the vector store never keyed", audit.MismatchedEntryCount);
```

### Queue fairness

One `IVaultQueueService` shared across vaults is the default registration, and until 0.23.0 nothing
stopped one owner's backlog from occupying the whole worker pool: everyone else waited behind it in
arrival order, however small their work was.

Jobs carry an optional **group key** — `QueueGroupKey`, defaulting to the vault's own `VaultId`, so a
multi-tenant setup built on `VaultFactory` gets this without wiring anything. `MaxInFlightPerGroup`
(default `1`) is how many of one group's jobs may run at once.

The cap is **work-conserving**: it binds only while *another* group has work queued. A vault alone on
the queue still uses the full `MaxConcurrentProcessing`, so nothing is paid for having fairness on —
and the moment a second vault enqueues, the first is held to its share and the newcomer is dequeued
ahead of the backlog that arrived before it.

```csharp
o.MaxInFlightPerGroup = 2;      // each owner may hold two slots while others wait
o.MaxInFlightPerGroup = 0;      // off: arrival order and priority only
o.QueueGroupKey = "team-a";     // group several vaults together
```

What it deliberately does not do: no preemption (a job already running is never interrupted, so one
long document still holds its slot for as long as it takes — fairness is about the *other* slots), and
no per-group weights. Ungrouped jobs are never capped, and priority still orders whatever is eligible.

- **Deterministic failures are not retried** (since 0.23.0). Whether a failure can succeed on a later
  attempt is decided from the exception type behind it, not from the message: a missing file, an
  extension no reader handles, or a corrupt archive fails identically every time, and each attempt
  holds the queue head for as long as the first did. A failure the library does not recognise stays
  retryable, so narrowing this cannot silently drop a recoverable job. `MemorizeResult.FailureKind`
  exposes the judgment (`Permanent` / `Transient` / `Unknown`) if you want to act on it yourself.
- **A failed memorize is reported as failed** (fixed in 0.23.0). `MemorizeAsync`/`RefreshAsync` signal
  failure by *returning* `MemorizeResult.Failed(...)`, not by throwing. Before 0.23.0 the queue worker
  discarded that result and marked the job completed, so a document that failed to index still raised
  `completedCount` and never appeared in `failedCount`. If you built a workaround that re-checks
  entries the queue claims are done, it is no longer needed. The same release makes the **inline**
  paths agree: with `EnableBackgroundProcessing = false` the call is terminal, so a failed
  `MemorizeAsync`/`RefreshAsync` now throws on every overload rather than only on
  `waitForCompletion: true`. The queued path is unchanged — that failure belongs to the worker.
- **Auto-retry actually runs now** (fixed in 0.23.0). `CanRetry` requires a job in `Failed` state, but
  the worker held a snapshot left in `Processing` by dequeue, so the condition was never true and
  `EnableAutoRetry` / `MaxRetryCount` / `RetryDelayMs` did nothing on that path. If your deployment
  appeared to never retry, this is why.
- **An operator can put a failed job back in the queue** (since 0.24.0). `RetryAsync` enforces the
  automatic retry budget, which is right for the worker deciding whether to keep going unattended and
  wrong for a person: the jobs someone reaches for a retry button over are precisely the ones that
  have used the budget up, so that call succeeded only when it was not needed. `RequeueAsync` (on
  `IVaultQueueService` — see [Observing the queue](#observing-the-queue) for how to reach it) is the
  operator's path — it clears `RetryCount` and re-queues, and it throws rather than returning a bool,
  because the ways it can decline call for different answers. `VaultJobNotFoundException` means the
  list is stale; `VaultJobNotRetryableException` carries a `VaultRetryRefusal` of `NotFailed` (someone
  already dealt with it) or `PermanentFailure` (running it again would fail identically). Those map
  onto 404 / 409 / 409 directly.

  The `PermanentFailure` case is new information, not a new restriction. A permanent failure never
  spends retry budget — the worker stops before the auto-retry branch — so before 0.24.0 the ordinary
  retry path saw a failed job with attempts to spare and re-queued a password-protected document
  quite happily, and the operator saw a button that appeared to work. The classification is now
  written to the job row so the question can still be answered later; a job that failed before the
  column existed reads as unclassified, and unclassified is allowed through.
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
using FluxFeed.Domain.Entities;   // VaultEntry

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
services.AddSingleton<IVaultImageEnricher, VisionEnricher>();

// MyVisionModel stands for whatever vision client you use.
public sealed class VisionEnricher(MyVisionModel vision) : IVaultImageEnricher
{
    public Task<string?> DescribeAsync(VaultImageDescriptionRequest request, CancellationToken ct = default)
        => vision.CaptionAsync(request.Image.FilePath, request.DocumentText, ct);
        // returning null means "not this time" — the pipeline retries that image on the next run
}
```

Descriptions are persisted per image, so re-memorizing does not re-describe images that already
succeeded, and one image's failure aborts neither the others nor the memorize. Each description is
indexed as its own chunk tagged `chunk_kind="image_description"` with `image_id` / `image_file`
metadata — no markers are injected into the document text. When the reader reports the page an image is
on (PDF), the chunk also carries the page keys text chunks use (`pageNumber` / `ff_start_page` /
`ff_end_page`); an entry memorized before 0.42.0 gains them on its next memorize, without describing
the image again.

Descriptions go through the same chunker as the body, so `MaxChunkSize` bounds them too and a long
description becomes several chunks that each carry the same `image_id` / `image_file`. Returning a
long description is therefore safe: it cannot push the document's embedding request past the model's
context window.

An image that keeps failing (unsupported format, corrupt data — a `null` return or a thrown
exception, either counts) is not retried forever. Once it has failed
`FileVaultOptions.MaxImageEnrichmentAttempts` times (default 3), it is marked permanently failed and
the pipeline stops offering it to the enricher — this survives a process restart, since the attempt
count is persisted in the image manifest, not held in memory. A later success (e.g. after you fix the
enricher) clears the failure record for that image.

If your enricher reads only some image types (a vision model: PNG, JPEG, WebP), list them in
`FileVaultOptions.ImageEnrichmentContentTypes`: an image of any other type (TIFF, JPEG XR, EMF/WMF, SVG, BMP) is then not
offered to it and is recorded as a permanent failure with the reason `unsupported_content_type:<type>`.

Read back what the enricher wrote — or why an image is still pending — with
`GetImageManifestAsync(filePath)`. Each `VaultImage` carries its `Description` when one has been
persisted, or `LastEnrichmentFailure` (reason, attempt count, whether it is now permanent) while none
has. Returns an empty list when the entry doesn't exist or has no images.

### Tables

A document's tables are kept as structured rows beside its text: `GetTablesAsync(filePath)` returns them in
document order as `TableArtifact` — `Id` (`t000`, `t001`, … from the table's position; pair it with the entry for a
key across documents), `Rows`, `HeaderRows`, `Columns`, `PageNumber` (for a workbook, the sheet's position), `Section`
(the sheet name), `Caption`, `MergedCells`, `Confidence` and `DetectionMethod` (a PDF table is inferred from page
layout and says so). They are rebuilt on re-extraction and removed with the entry. Supplied by FileFlux 0.41.0+
readers (PDF, Word, PowerPoint, Excel, HWP).

A chunk holding table rows is tagged `chunk_kind="table"` with `table_piece` / `table_pieces` and
`table_row_start` / `table_row_end` (FluxCurator 0.11.0+ keeps a table whole, or splits it between rows with the header
repeated). When the chunked text holds exactly the tables extraction stored, the chunk also carries `table_id`,
`table_columns`, `table_section`, `table_caption` and `table_page` from its table — so a hit can be cited as a table
and its rows read with `GetTablesAsync`.

### Keyword index — `IKeywordSearchService`

When one is registered, every chunk written to the vector store is written to the keyword index as
well, and `RemoveAsync` deletes from both. Without it the keyword index stays empty and hybrid search
degenerates to vector-only. Check `IVaultPipeline.SupportsKeywordIndex` to confirm the wiring —
it is on the interface, so holding the pipeline as `IVaultPipeline` is enough (`SupportsGraphRAG`
reports the GraphRAG leg the same way).

On the SQLite stack, `services.AddSQLiteKeywordSearch()` (`FluxIndex.Storage.SQLite` 0.45.0) registers one in the
vector store's database and picks up a registered `ITextAnalyzer` and `KeywordFieldOptions`;
`AddPostgreSQLKeywordSearch(connectionString)` does the same for PostgreSQL.

### Hybrid search — over the keyword index, or store-native

`VaultSearchOptions.SearchStrategy = VaultSearchStrategy.Hybrid` fuses the vector leg with a keyword leg, chosen
once from what is registered — `IVaultPipeline.HybridKeywordLeg` reports it and the pipeline logs it once:

| Registered | Keyword leg | |
|---|---|---|
| `IKeywordSearchService` | `KeywordIndex` | the same index the `Keyword` strategy searches and ingestion writes, fused through the registered `IHybridSearchService` or the stock `HybridSearchService` — a registered `ITextAnalyzer` and `KeywordFieldOptions` apply to hybrid too |
| no keyword service, vector store with `INativeHybridSearch` | `Native` | the store fuses over keyword rows it wrote itself (`FluxIndex.Storage.SQLite`'s sqlite-vec store: FTS5, its own tokenizer) |
| no keyword service, no native store, an `IHybridSearchService` | `KeywordIndex` | fused by that service over whatever keyword index it was built with — FluxFeed's ingestion does not write to it, and `Keyword` requests still run as vector search |
| none of these | `None` | runs as vector search |

A `PathScope` reaches both legs before fusion either way, so a scoped request gets the fused ranking of the
in-scope chunks. When hybrid is not available the query runs as vector search and says so via
`VaultSearchResult.ExecutedStrategy` — compare it against `RequestedStrategy` rather than assuming the request was
honored.

The legs are fused by weighted reciprocal rank, so **hybrid scores are usually rank-sized** — about 0.016 for a chunk
both legs rank first, never near 1. On the keyword-index leg the stock `HybridSearchService` picks the weights from the
query's length (one or two terms: vector 0.3 / keyword 0.7; three to five: 0.6 / 0.4; longer: 0.8 / 0.2), and a query
containing one of the whole words `API`, `HTTP`, `JSON`, `SQL`, `AI` or `ML` is fused by weighted sum of normalized
scores instead — not rank-sized. The native leg uses the store's own weighting. FluxFeed does not expose these weights
yet.

`VaultSearchOptions.MinScore` under `Hybrid` is a **similarity floor on the vector leg, applied before fusion** —
the same meaning it has for `Vector`, so one similarity-sized value works whichever of the two runs. It is never
compared with the fused score. A chunk the keyword leg matches is fused in whatever its similarity.

### Keyword-only search — `VaultSearchStrategy.Keyword`

`VaultSearchOptions.SearchStrategy = VaultSearchStrategy.Keyword` runs pure BM25 through the same
`IKeywordSearchService` the keyword index above writes to — no query embedding, no vector search.
It degrades to vector the same way `Hybrid` does when no `IKeywordSearchService` is registered
(reported via `ExecutedStrategy`). Use this over `Hybrid` with a zero vector weight when you actually
want keyword-only: a weighted hybrid request still generates a query embedding and runs a vector
search it then discards.

### Reranked search — `VaultSearchOptions.UseReranker`

Register a FluxIndex `IReranker` (`AddLMSupplyReranker`, `AddOpenAICompatibleReranker`, or your own) and ask for it
per search:

```csharp
var results = await vault.SearchAsync("vacation policy", new VaultSearchOptions
{
    TopK = 5,
    UseReranker = true,          // retrieval fetches TopK * 3 (or RerankCandidateCount), the reranker orders them
    RerankCandidateCount = 30,   // optional pool size, never below TopK
});
```

The semantics are FluxIndex `SearchOptions.UseReranker`'s. Each item's `Score` is the reranker's (on its own scale —
logits for some models, so not necessarily 0 to 1) and `RetrievalScore` keeps the strategy's score; `MinScore` filters
on the retrieval score before reranking. `UseReranker` without a registered reranker throws
`InvalidOperationException` rather than returning unreranked results.

### RAG security — `FluxGuard.Remote.RAG.IRAGSecurityPipeline`

When one is registered, `MemorizeAsync`/`RefreshAsync` validate every chunk through it before
indexing — a chunk the pipeline suggests blocking (RAG poisoning / indirect prompt injection) is
dropped from the batch entirely, one it suggests sanitizing has its content replaced. Off by
default; nothing changes without it.

The usual way is to register it in the container, next to the vault registration:

```csharp
using FluxGuard.Remote.RAG;

services.AddSingleton<IRAGSecurityPipeline, IndirectInjectionDetector>();
```

Constructing the pipeline by hand works the same way:

```csharp
using FluxFeed.Services;
using FluxGuard.Remote.RAG;

var pipeline = new VaultPipeline(
    git, hasher, storage, logger,
    vectorStore: vectorStore,
    embeddingService: embeddingService,
    ragSecurityPipeline: new IndirectInjectionDetector());
```

### Contextual enrichment — `FluxIndex.Core.Application.Interfaces.IContextualEnrichmentService`

Opt-in. Before a document's text chunks are embedded and keyword-indexed, each one gets a short LLM-written
context — where it sits in its document — prepended (Anthropic's "contextual retrieval"). The same enriched text is
what gets stored, embedded and keyword-indexed, so retrieval and display agree; the context alone is also kept in
chunk metadata (`context_summary`) and the step is recorded as `enrichment=contextual`. A port that succeeds but
returns a blank context leaves that chunk's text as it was, tags it `enrichment=empty` and logs a warning — so "the
model said nothing" is never mistaken for "enrichment is off". Image-description chunks
are not enriched. A refresh or re-memorize reuses a chunk's stored context when its passage and the document text are
unchanged (recorded as `context_doc_hash`), and asks the port only for the rest.

The port is FluxIndex.Core's own `IContextualEnrichmentService` (`GenerateContextBatchAsync(chunks, fullDocumentText)`
→ one context per chunk, in order). FluxFeed does not depend on any particular LLM library for it; the FluxImprover-backed
implementation ships in `FluxIndex.Integrations.FluxImprover`, or implement the two methods yourself. With that
implementation, `services.AddContextualEnrichmentWrapper(new ContextualEnrichmentOptions { … })` sets the generation
options every chunk uses (output budget, temperature, `Thinking` — FluxImprover asks the model not to reason by
default), and a context the model cut off at the output budget is dropped rather than stored, so that chunk is
tagged `enrichment=empty` too.

Two things are required — registering the port alone does nothing, so a container that already has an enrichment
service for other reasons never pays one LLM call per chunk by accident:

```csharp
services.AddScoped<IContextualEnrichmentService, MyContextualEnrichment>();   // FluxIndex.Core port
services.AddFileVaultWithFluxIndex(options =>
{
    options.ContextualEnrichment.Enabled = true;          // default false
    options.ContextualEnrichment.ContinueOnError = true;  // default: warn + index plain chunks tagged enrichment=failed
});
```

Cost is whatever the port spends — typically one generation call per chunk with the whole document in the prompt,
so budget it per document size. With `ContinueOnError = false` an enrichment failure fails the memorize instead of
degrading; a port that returns the wrong number of contexts is always a failure (never a silent misalignment).

### Staged indexing — searchable first, enriched later

Every index pass embeds and writes only the chunks whose stored row differs (text, position or metadata); unchanged
chunks keep their vectors. With `options.DeferEnrichment = true` a memorize or refresh also skips the slow LLM stages:
the file is searchable on its native chunks, and page reads (`PageReading`), LLM refinement (`LlmRefine`, when set),
image descriptions and contextual enrichment wait. The entry records what is left in `VaultEntry.PendingEnrichment`
(`PageReads`, `LlmRefinement`, `ImageDescriptions`, `ContextualEnrichment`). An upgrade extracts the document again for
the first two and re-embeds only the chunks whose text changed; a scan with no text layer waits for it instead of being
indexed as empty. When to run them is the host's call:

```csharp
using FluxFeed.Domain.Entities;   // VaultJobPriority

foreach (var pending in await vault.GetPendingEnrichmentAsync())
    await vault.UpgradeAsync(pending.SourcePath, VaultJobPriority.Low, waitForCompletion: true);
```

`UpgradeAsync` runs the pending stages without re-extracting and re-embeds only the chunks whose text changed; with
background processing it is a queued job (`VaultJobType.Upgrade`) like a refresh.

## Multi-tenant

`AddFileVaultFactoryWithFluxIndex` swaps the single `IVault` for an `IVaultFactory`. Each tenant gets
its own `.vault/` directory, processing queue **and queue worker** (started by the factory, stopped when
the tenant is disposed - no host involvement). The processing services a vault uses (extractor, chunker,
vector store, embedder, GraphRAG, ...) are resolved from a service scope the vault owns and released
when the tenant is disposed, so scoped registrations are honoured per tenant — the factory itself
holds only the stateless singletons (hasher, git, file watcher) and is valid under scope validation.

```csharp
services.AddFileVaultFactoryWithFluxIndex(o => o.VaultBasePath = "./data");

// factory: the IVaultFactory resolved from the container (it is a singleton).
var vault = factory.GetOrCreate("tenant-a");
await vault.MemorizeAsync(path);

// Deleting a tenant: one filtered delete per backend removes all of its chunks, no per-entry loop.
await factory.DisposeAsync("tenant-a", purgeVectors: true);
```

Chunks are tagged with a `vault_id` metadata field, which is what makes the bulk purge
(`IVault.PurgeAsync`) possible.

> **Changed in 0.10.0** — the purge now clears the **keyword index too**. Before this it removed the
> vectors, logged a warning, and returned success while the tenant's text stayed searchable through
> keyword and hybrid search. Requires `FluxIndex.Core` 0.25.0+, which added the tag-scoped bulk
> delete this needs. The returned count is the number of chunks, not a sum across backends.

## Configuration

`FileVaultOptions` (bindable from the `FileVault` configuration section):

```csharp
using FluxFeed.Options;

services.Configure<FileVaultOptions>(configuration.GetSection(FileVaultOptions.SectionName));
```

| Option | Default | Description |
|---|---|---|
| `VaultBasePath` | `null` | Vault root. When null, one vault at `<working directory>/<VaultDirectoryName>` (`.vault` by default) — the process working directory, not next to each source file. Set it explicitly in services and hosts |
| `VaultDirectoryName` | `.vault` | Directory name used for the default vault root above, and for each tenant's vault under `IVaultFactory` |
| `VaultId` | `null` | Tenant id; set by `IVaultFactory`. Required for `PurgeAsync`. Also the GraphRAG graph partition — vaults sharing one graph store keep separate entities and communities |
| `MaxFileSizeMB` | `100` | A folder scan skips larger files; memorizing one fails permanently with the size in the message. `0` = no limit |
| `EnableRealTimeWatch` | `true` | Folder watching |
| `DebounceDelayMs` | `500` | Merge window for rapid change events |
| `WatcherBufferSize` | `65536` | `FileSystemWatcher` internal buffer, in bytes; larger misses fewer events |
| `EnableBackgroundProcessing` | `true` | Background queue; when false the service idles |
| `GitExecutablePath` | `git` | Git CLI used for vault history; set an explicit path when git is not on PATH |
| `AllowMissingGit` | `false` | When true, a missing git CLI degrades to a history-less vault (one warning) instead of failing the first vault operation |
| `WorkerStartupTimeout` | `5s` | How long `MemorizeAsync(..., waitForCompletion: true)` tolerates the absence of a running queue worker before throwing. The worker is an `IHostedService`, so without a Generic Host (or `EnableBackgroundProcessing = false`) the wait fails fast with the fix in its message instead of hanging |
| `MaxConcurrentProcessing` | `4` | Concurrent file operations. Jobs for **different** files run in parallel up to this limit; jobs for the **same** file never do (see below) |
| `MaxInFlightPerGroup` / `QueueGroupKey` | `1` / `null` (falls back to `VaultId`) | Fair share of a shared queue — see [Queue fairness](#queue-fairness) |
| `EnableAutoRetry` / `MaxRetryCount` / `RetryDelayMs` | `true` / `3` / `5000` | Retry policy — how many attempts and how long to wait. **Whether an attempt can help is not configurable**: a deterministic failure is never retried (see below) |
| `AutoCleanupOrphans` | `false` | Remove entries whose source file is gone, during sync |
| `Chunking.MaxChunkSize` / `OverlapSize` / `Strategy` | `1024` / `128` / `Intelligent` | Chunking defaults, with per-extension overrides via `Chunking.FormatStrategies` |
| `Chunking.Language` | `null` | Chunking language; null = auto-detect |
| `LlmRefine` | `null` | How FileFlux's LLM refinement runs at extraction, when an `ILlmRefiner` is registered (none = no LLM pass). `null` = one whole-document rewrite, after which the entry has no page spans. `new LlmRefineOptions { Scope = LlmRefineScope.Pages, SelectPages = … }` refines page by page, keeps an output only if it keeps the page's text (`MinNativeCoverage` — each word, however spaced) and numbers, and keeps the page spans; the per-page outcome arrives as extraction hints `llm_refine_pages_refined` / `_native` / `_rejected` / `_skipped`, `llm_refine_rejected` (`page:reason,…`), `llm_refine_native_reasons` (`page:no_pass_needed` — no model call — or `page:passes_failed`) and `llm_refine_pass_failures` (`page:pass:reason`, e.g. `3:RestoreSentences:truncated`), and as one Information log line per document |
| `PageReading` | `null` | Which PDF pages FileFlux renders and reads through its registered `IImageToTextService` at extraction (`new PageReadingOptions { SelectPages = q => !q.HasTextLayer && q.ImageCoverage > 0.5 }` for scans). A read replaces a page's text only where the page could not be read, never from an incomplete render; outcomes arrive as extraction hints `page_reads` / `page_reads_replaced`. An image on a page read this way (`VaultImage.ReadAsPage`, a scan) is stored but not offered to the image enricher. Changing it does not re-extract stored entries — refresh with re-extraction |
| `AdditionalTextExtensions` | empty | Extra extensions (lowercase, leading dot, e.g. `.myext`) read as plain text by the fallback extraction used when no `IExtractor` is registered (e.g. `AddFileVault` without FileFlux) |
| `DefaultIncludePatterns` / `DefaultExcludePatterns` | common document / temp-file globs | See [File selection patterns](#file-selection-patterns) |

The background worker (`VaultBackgroundService`) holds a lease from `IVaultQueueService.RegisterWorker()` while it consumes the queue; that lease is
how `WaitForJobAsync` tells "a worker is busy" from "nobody will ever process this job". A custom `IVaultQueueService` implementation should return a
real lease from `RegisterWorker()` (the interface default is a no-op lease, which disables the check).

`RecoverStuckJobsAsync()` returns jobs left `Processing` by work that is no longer running to the queue (the worker calls it on startup).
It never resets a job this process dequeued and has not yet reported, so a host that runs its own dequeue loop may also call it periodically
without the running job being handed out a second time. The queue knows its own process only: **one `queue.db` is consumed by one process**.

### Same-file work is serialized for you

A vault's git repository lives per **entry** (`VaultEntry.VaultPath` = `<EntryPath>/vault`), not per FileVault. Two jobs for two files
therefore commit into two different repositories and are safe to run together — that is the parallelism `MaxConcurrentProcessing` buys.
Two jobs for one file are not: they would write one working tree and race one `index.lock`.

The queue handles this itself, so **a consumer does not need a per-file lock of its own**:

- `DequeueAsync` skips any entry that already has a job in flight, and hands that job out as soon as the running one finishes.
- Enqueuing work that is already **queued** for the same file and type merges into the waiting job rather than adding a second row —
  the caller awaits the job that already exists. A more urgent request raises that job's priority instead of being demoted into it.
  A job already **processing** is not merged into: it read the file as it was, so a later request needs its own run.

### Priority

`MemorizeAsync` / `RefreshAsync` / `SyncAsync` take an optional `VaultJobPriority`. The queue has always ordered by priority; until 0.22.0
nothing on `IVault` could set it, so a bulk crawl and a user waiting on one file competed purely on arrival order.

```csharp
using FluxFeed.Domain.Entities;   // VaultJobPriority

await vault.SyncAsync(VaultJobPriority.Low, ct);                              // background crawl, yields
await vault.MemorizeAsync(path, VaultJobPriority.High, waitForCompletion: true, ct);   // user is waiting
```

### Observing the queue

`GetStatisticsAsync`, `GetJobsAsync` and `RequeueAsync` are on `IVaultQueueService`, not `IVault`. For a single vault,
resolve `IVaultQueueService` from the container; for tenants, use the tenant's own queue,
`IVaultFactory.GetContext(tenantId)?.QueueService` (null until the tenant has been created with `GetOrCreate`).

`GetStatisticsAsync()` answers two different questions, and conflating them is a reported source of false alarms:

| Field | Question it answers |
|---|---|
| `LastSucceededAt` | Is the queue *getting anywhere*? Moves only on a completed job. |
| `LastAttemptedAt` | Is the queue *alive*? Moves when any job starts or finishes, whatever the result. |

`LastSucceededAt` was called `LastProcessedAt` before 0.22.0 while only ever reflecting successes — a worker that was running steadily and
failing every job left it frozen and read as stopped. Note that `ProcessingCount` is a point-in-time count and is legitimately `0` between
jobs; it is the pair (`ProcessingCount` + a fresh `LastAttemptedAt`) that says "between jobs" rather than "stopped".

`GetJobsAsync` orders and pages in SQL:

```csharp
using FluxFeed.Domain.Entities;   // VaultJobStatus

// the latest 50 failures, not the oldest 50
var recent = await queue.GetJobsAsync(VaultJobStatus.Failed, limit: 50, newestFirst: true, ct: ct);
// second page
var next = await queue.GetJobsAsync(VaultJobStatus.Failed, limit: 50, offset: 50, newestFirst: true, ct: ct);
```

`newestFirst` sorts by `queued_at` alone — priority decides what runs next, not what is most recent. The default order is unchanged
(priority, then oldest first).

## Changelog

Version history, including breaking changes, is in [CHANGELOG.md](CHANGELOG.md).

## License

MIT — see [LICENSE](LICENSE).
