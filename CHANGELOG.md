# Changelog

All notable changes to FluxFeed are documented here.
Follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) conventions. FluxFeed is pre-1.0, so a minor
version may contain breaking changes; they are listed under **Breaking**.

Releases before 0.28.0 predate this file — see the git history.

## [0.48.0] - Unreleased

### Added
- **`FileVaultOptions.PageReading` — scanned and damaged PDF pages read by a vision model at extraction.** The options
  reach FileFlux's page reading (FileFlux 0.47.0, `ExtractOptions.PageReading`): selected pages are rendered and read
  through FileFlux's registered `IImageToTextService`, and a read replaces a page's text only where the page could not be
  read. The outcome is in the extraction hints (`page_reads`, `page_reads_replaced`); a failed read is an extraction
  warning. Tenants made by `IVaultFactory` carry the setting. Off by default.

### Changed
- The FileFlux extractor extracts with the vault's extraction options before refining (it let refinement extract with
  none). `ExtractionSettings` gains `PageReading`.

### Dependencies
- FileFlux 0.47.0.

---

---

## [Unreleased]

---

## [0.47.2] - 2026-10-07

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.46.0 -> 0.46.1, `FluxIndex.Core` 0.80.7 -> 0.80.8, `FluxIndex.Storage.SQLite` 0.80.7 -> 0.80.8. No source changes.

---

## [0.47.1] - 2026-10-07

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.45.0 -> 0.46.0, `FluxIndex.Core` 0.80.6 -> 0.80.7, `FluxIndex.Storage.SQLite` 0.80.6 -> 0.80.7. No source changes.

---

## [0.47.0] - 2026-10-06

### Added
- **Staged indexing.** `FileVaultOptions.DeferEnrichment` makes a file searchable on its native chunks first: memorize
  and refresh skip image descriptions and contextual enrichment and record what is left in
  `VaultEntry.PendingEnrichment`. `IVault.GetPendingEnrichmentAsync()` lists those entries, and
  `IVault.UpgradeAsync(path, priority, waitForCompletion)` (a queued `VaultJobType.Upgrade` job with background
  processing) runs the pending stages without re-extracting. Off by default.
- **An index pass embeds only what changed.** A chunk whose stored row is identical (text, position, metadata) is kept
  as it is instead of being embedded and written again — a refresh of unchanged content embeds nothing. Not with
  GraphRAG enabled (the graph is rebuilt from the written chunks).
- **Stored contexts are reused.** Contextual enrichment asks the port only for chunks without a stored context for the
  same passage of the same document text (`context_doc_hash` in chunk metadata).

### Fixed
- **A vault with background processing off chunks with its configured settings.** Direct memorize/refresh/upgrade calls
  used the `MemorizeOptions` defaults (1024 / 128 / `Auto`) instead of `FileVaultOptions.Chunking`, as the queue worker
  passes them; a refresh that re-extracted an outdated entry did the same.

### Changed
- **Breaking**: `IVault` gains `GetPendingEnrichmentAsync` and `UpgradeAsync`, `IVaultQueueService` gains
  `EnqueueUpgradeAsync`, and `IVaultPipeline` gains `UpgradeAsync`. Migration: an implementation or hand-written double
  of these interfaces adds the members (a test double can throw `NotImplementedException`).
- Re-pinned sibling package(s) `FileFlux` 0.44.0 -> 0.45.0.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.80.5 -> 0.80.6, `FluxIndex.Storage.SQLite` 0.80.5 -> 0.80.6.

---

## [0.46.1] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.43.0 -> 0.44.0, `FluxIndex.Core` 0.80.4 -> 0.80.5, `FluxIndex.Storage.SQLite` 0.80.4 -> 0.80.5. No source changes.

---

## [0.46.0] - 2026-10-06

### Added
- **`FileVaultOptions.LlmRefine` — page-scoped, checked LLM refinement for vault entries.** The options reach FileFlux's
  LLM refinement at extraction (it still runs only when an `ILlmRefiner` is registered). With
  `Scope = LlmRefineScope.Pages` each selected page is refined alone and kept only if it keeps the page's words and
  numbers. The entry keeps its page spans, so chunks keep their pages. The per-page outcome is in the extraction
  hints (`llm_refine_pages_refined` / `_native` / `_rejected` / `_skipped`, `llm_refine_rejected`). Tenants made by
  `IVaultFactory` carry the setting.

### Changed
- **Breaking**: `IExtractor.ExtractAsync(string sourcePath, ExtractionSettings? settings = null, CancellationToken ct = default)`
  replaces `ExtractAsync(string, CancellationToken)`; the vault passes its settings. Migration: an implementation adds
  the `settings` parameter (and may ignore it); a caller that passed the token positionally writes `ct: token`; an
  NSubstitute double configures `ExtractAsync(Arg.Any<string>(), Arg.Any<ExtractionSettings?>(), Arg.Any<CancellationToken>())`.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.80.3 -> 0.80.4, `FluxIndex.Storage.SQLite` 0.80.3 -> 0.80.4.

### Dependencies
- FileFlux 0.43.0.

---

## [0.45.2] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.41.1 -> 0.42.0, `FluxIndex.Core` 0.80.2 -> 0.80.3, `FluxIndex.Storage.SQLite` 0.80.2 -> 0.80.3. No source changes.

---

## [0.45.1] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.41.0 -> 0.41.1, `FluxIndex.Core` 0.80.1 -> 0.80.2, `FluxIndex.Storage.SQLite` 0.80.1 -> 0.80.2. No source changes.

---

## [0.45.0] - 2026-10-06

### Added
- **Tables are kept as structured rows.** Extraction stores the document's tables (from FileFlux `RawContent.Tables`)
  beside the text, and `IVault.GetTablesAsync(filePath)` returns them as `TableArtifact` — position-based `Id`
  (`t000`, …), `Rows`, `HeaderRows`, `Columns`, page or sheet, `Section`, `Caption`, `MergedCells`, `Confidence`,
  `DetectionMethod`. Rebuilt on re-extraction, removed with the entry. `ExtractionResult.Tables` carries them from any
  `IExtractor`. The extraction pipeline revision is raised, so entries extracted before read as outdated and pick the
  tables up on refresh.
- **Table chunks say what they are.** A chunk holding table rows is tagged `chunk_kind="table"` with
  `table_piece`/`table_pieces` and `table_row_start`/`table_row_end`, and — when the chunked text holds exactly the
  stored tables — `table_id`, `table_columns`, `table_section`, `table_caption`, `table_page`. `ContentChunk.Table`
  (`ContentTablePiece`) carries the piece from any `IChunker`.
- **Images the enricher cannot read are no longer offered to it.** `FileVaultOptions.ImageEnrichmentContentTypes`
  declares the content types the registered `IVaultImageEnricher` reads (a vision model: PNG, JPEG, WebP); an image of
  another type (TIFF, JPEG XR, EMF/WMF, SVG, BMP) is recorded at once as a permanent failure with the reason
  `unsupported_content_type:<type>`, so it is counted apart from enricher failures and costs no call. Null (default)
  offers every image, as before. Converting those images to a readable raster is not done yet.

### Changed
- **Breaking** — `IVault` gains `GetTablesAsync`, and `IVaultStorageService` gains `StoreTablesAsync`/`GetTablesAsync`.
  Migration: an `IVault` or `IVaultStorageService` implementation (a test double, for instance) adds them — returning
  an empty list is enough for a double.
- Re-pinned sibling package(s) `FileFlux` 0.40.0 -> 0.41.0.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.80.0 -> 0.80.1, `FluxIndex.Storage.SQLite` 0.80.0 -> 0.80.1.

---

## [0.44.5] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.79.1 -> 0.80.0, `FluxIndex.Storage.SQLite` 0.79.1 -> 0.80.0. No source changes.

---

## [0.44.4] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.79.0 -> 0.79.1, `FluxIndex.Storage.SQLite` 0.79.0 -> 0.79.1. No source changes.

---

## [0.44.3] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.78.0 -> 0.79.0, `FluxIndex.Storage.SQLite` 0.78.0 -> 0.79.0. No source changes.

---

## [0.44.2] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.39.1 -> 0.40.0, `FluxGuard.Remote` 0.20.0 -> 0.21.0, `FluxIndex.Core` 0.77.1 -> 0.78.0, `FluxIndex.Storage.SQLite` 0.77.1 -> 0.78.0. No source changes.

---

## [0.44.1] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.39.0 -> 0.39.1, `FluxIndex.Core` 0.77.0 -> 0.77.1, `FluxIndex.Storage.SQLite` 0.77.0 -> 0.77.1. No source changes.

---

## [0.44.0] - 2026-10-06

### Fixed
- **Stopping the host while a document is being memorized no longer marks that document as failed.** The memorize and
  refresh pipelines caught the cancellation like any error: the entry went to `Error`, the queue recorded a failed
  attempt (counted toward the retry limit and classified by exception type), and an interrupted indexing run was
  reported as an indexing failure. A cancelled run still rolls back what it half-wrote, then lets the cancellation
  through; the job stays in progress and is resumed on the next start. Creating an entry's git repository no longer
  treats a cancellation as a non-fatal git error.

### Changed
- **`IVault.SearchAsync` throws when the search cannot run.** Before, every failure of the index, the embedding service or
  the pipeline was caught and returned as a result with `IsSuccess = false` and an `ErrorMessage`, which a caller that
  only read `Items` saw as "nothing matched". Now the exception the pipeline raised reaches the caller. An empty result
  means only that nothing matched.
  **Breaking**: `VaultSearchResult.IsSuccess`, `VaultSearchResult.ErrorMessage` and `VaultSearchResult.Error(...)` are
  removed. Replace `if (!result.IsSuccess)` with a `try`/`catch` around the call.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.76.1 -> 0.77.0, `FluxIndex.Storage.SQLite` 0.76.1 -> 0.77.0.

---

## [0.43.10] - 2026-10-06

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.38.3 -> 0.39.0, `FluxIndex.Core` 0.76.0 -> 0.76.1, `FluxIndex.Storage.SQLite` 0.76.0 -> 0.76.1. No source changes.

---

## [0.43.9] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.75.1 -> 0.76.0, `FluxIndex.Storage.SQLite` 0.75.1 -> 0.76.0. No source changes.

---

## [0.43.8] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.75.0 -> 0.75.1, `FluxIndex.Storage.SQLite` 0.75.0 -> 0.75.1. No source changes.

---

## [0.43.7] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.74.2 -> 0.75.0, `FluxIndex.Storage.SQLite` 0.74.2 -> 0.75.0. No source changes.

---

## [0.43.6] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.38.2 -> 0.38.3, `FluxGuard.Remote` 0.19.1 -> 0.20.0, `FluxIndex.Core` 0.74.1 -> 0.74.2, `FluxIndex.Storage.SQLite` 0.74.1 -> 0.74.2. No source changes.

---

## [0.43.5] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.38.1 -> 0.38.2, `FluxIndex.Core` 0.74.0 -> 0.74.1, `FluxIndex.Storage.SQLite` 0.74.0 -> 0.74.1. No source changes.

---

## [0.43.4] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.73.8 -> 0.74.0, `FluxIndex.Storage.SQLite` 0.73.8 -> 0.74.0. No source changes.

---

## [0.43.3] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.37.2 -> 0.38.1, `FluxIndex.Core` 0.73.6 -> 0.73.8, `FluxIndex.Storage.SQLite` 0.73.6 -> 0.73.8. No source changes.

---

## [0.43.2] - 2026-10-05

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.37.1 -> 0.37.2, `FluxGuard.Remote` 0.19.0 -> 0.19.1, `FluxIndex.Core` 0.73.5 -> 0.73.6, `FluxIndex.Storage.SQLite` 0.73.5 -> 0.73.6. No source changes.

---

## [0.43.1] - 2026-10-04

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.37.0 -> 0.37.1, `FluxIndex.Core` 0.73.4 -> 0.73.5, `FluxIndex.Storage.SQLite` 0.73.4 -> 0.73.5. No source changes.

---

## [0.43.0] - 2026-10-04

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.36.2 -> 0.37.0.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.73.3 -> 0.73.4, `FluxIndex.Storage.SQLite` 0.73.3 -> 0.73.4.

### Added
- **An extractor upgrade can reach documents whose source never changes: `FileVaultOptions.Reextraction`.** Every
  extraction now records which extractor made it (`VaultEntry.ExtractedBy` — e.g. `FileFlux 0.36.2`, plus FluxFeed's
  pipeline revision). With `Reextraction = WhenExtractorMinorChanges` (or `WhenExtractorChanges`, patch releases
  included), a refresh of an entry an older extractor made re-extracts it, and change detection/sync recommends
  Memorize for it — except over uncommitted vault edits, which are refreshed as before. Entries extracted before this
  release have no record and count as outdated. Default `Never`: nothing re-extracts unless you opt in.
- **`VaultStatus.CurrentExtraction` / `OutdatedExtractionCount`**: how many entries an opt-in would re-extract, read
  before deciding. `ChangeDetectionResult.ExtractionOutdated` per file. `ExtractionIdentity.IsOutdatedBy` for your own
  comparison. To re-extract one file regardless of policy, call `MemorizeAsync` (it always re-extracts).
- **Breaking (custom extractors only):** `IExtractor` gains `ExtractionIdentity? Identity { get; }`. Return `null` if
  your extractor cannot say; its entries are then never judged outdated.

### Fixed
- **A re-extraction that finds fewer images no longer keeps the previous extraction's.** The image manifest was
  rewritten only when the new extraction had images, and image files the new extraction did not produce stayed on
  disk. The manifest now always describes the latest extraction, and files it does not name are removed.

---

## [0.42.8] - 2026-10-04

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.36.1 -> 0.36.2. No source changes.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.73.2 -> 0.73.3, `FluxIndex.Storage.SQLite` 0.73.2 -> 0.73.3.

---

## [0.42.7] - 2026-10-04

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.36.0 -> 0.36.1. No source changes.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.73.1 -> 0.73.2, `FluxIndex.Storage.SQLite` 0.73.1 -> 0.73.2.

---

## [0.42.6] - 2026-10-03

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.35.0 -> 0.36.0, `FluxIndex.Core` 0.73.0 -> 0.73.1, `FluxIndex.Storage.SQLite` 0.73.0 -> 0.73.1. No source changes.

---

## [0.42.5] - 2026-10-03

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.34.3 -> 0.35.0, `FluxIndex.Core` 0.72.1 -> 0.73.0, `FluxIndex.Storage.SQLite` 0.72.1 -> 0.73.0. No source changes.

---

## [0.42.4] - 2026-10-03

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.34.2 -> 0.34.3, `FluxIndex.Core` 0.71.3 -> 0.72.1, `FluxIndex.Storage.SQLite` 0.71.3 -> 0.72.1. No source changes.

---

## [0.42.3] - 2026-10-03

### Changed
- Re-pinned sibling package(s) `FluxGuard.Remote` 0.18.2 -> 0.19.0, `FluxIndex.Core` 0.71.2 -> 0.71.3, `FluxIndex.Storage.SQLite` 0.71.2 -> 0.71.3. No source changes.

---

## [0.42.2] - 2026-10-02

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.34.1 -> 0.34.2, `FluxIndex.Core` 0.71.1 -> 0.71.2, `FluxIndex.Storage.SQLite` 0.71.1 -> 0.71.2. No source changes.

---

## [0.42.1] - 2026-10-02

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.34.0 -> 0.34.1, `FluxGuard.Remote` 0.18.1 -> 0.18.2, `FluxIndex.Core` 0.70.0 -> 0.71.1, `FluxIndex.Storage.SQLite` 0.70.0 -> 0.71.1. No source changes.

---

## [0.42.0] - 2026-10-02

### Fixed
- **Image-description chunks carry the page the image is on.** A text chunk from a PDF says where it sits with
  `pageNumber` / `ff_start_page` / `ff_end_page`; an image-description chunk had none of them, so every image read as
  "page unknown" in a citation or a page-scoped evaluation. It now carries the same three keys (start = end) when the
  page is known. An entry memorized before this release gains them on its next memorize — the image's description is
  carried forward, so the enricher is not called again; a refresh does not re-extract and leaves them absent.

### Added
- `ImageArtifact.PageNumber` and `VaultImage.PageNumber` (recorded in the image manifest).

### Dependencies
- FileFlux 0.33.12 → 0.34.0 (`ImageInfo.PageNumber`).

---

## [0.41.3] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.69.1 -> 0.70.0, `FluxIndex.Storage.SQLite` 0.69.1 -> 0.70.0. No source changes.

---

## [0.41.2] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.11 -> 0.33.12, `FluxIndex.Core` 0.69.0 -> 0.69.1, `FluxIndex.Storage.SQLite` 0.69.0 -> 0.69.1. No source changes.

---

## [0.41.1] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.68.0 -> 0.69.0, `FluxIndex.Storage.SQLite` 0.68.0 -> 0.69.0. No source changes.

---

## [0.41.0] - 2026-10-01

### Changed
- **A keyword-index repair no longer stops at the first entry that fails.** `RepairKeywordIndexAsync` (every overload)
  rewrites every other entry, then throws `KeywordIndexRepairException` with the counts of the run (`Result`) and the
  entries that were not rewritten (`Failures`: source path, document id, error). Before, a timeout on one entry of a large
  vault ended the run there, leaving the rest written the old way and no record of where it stopped. A failed entry keeps
  its previous keyword rows. Cancellation still stops the run at once.

### Added
- **`IVault.RepairKeywordIndexAsync(filePaths, scope)`** rewrites the keyword rows of the named entries only — for
  retrying `KeywordIndexRepairException.Failures`. An untracked path is refused (`KeyNotFoundException`) before anything
  is written. Default interface implementation throws `NotSupportedException`; `VaultManager` implements it.

### Changed (search)
- **Vault search embeds its query in the query role** (`IEmbeddingService.GenerateQueryEmbeddingAsync`, FluxIndex 0.68.0),
  so an asymmetric embedding model registered for the vault applies its query convention. A symmetric model behaves as
  before. A test double that stubs only `GenerateEmbeddingAsync` must stub `GenerateQueryEmbeddingAsync` for searches.
- **Every package now carries the `LICENSE` text**, not only the MIT expression.

### Dependencies
- `FluxIndex.Core` / `FluxIndex.Storage.SQLite` 0.67.2 -> 0.68.0.

---

## [0.40.1] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.67.1 -> 0.67.2, `FluxIndex.Storage.SQLite` 0.67.1 -> 0.67.2. No source changes.

---

## [0.40.0] - 2026-10-01

### Added
- **A folder watched with `autoMemorize: true` now keeps the vault in step by itself.** Created and saved files are
  memorized (debounced — one save, one memorize), deleted files removed, renamed files moved with `MoveAsync` and
  renamed folders with `MoveFolderAsync` (nothing extracted or embedded again); a rename to a name the folder's patterns
  reject removes the entry. Changes apply in order on a background loop; a failure is logged and the next change still
  applies. Before, `autoMemorize` was recorded and never read, and nothing subscribed to the watcher's events.
  If you subscribed to `IFileWatcherService` yourself for a folder you add with `autoMemorize: true`, drop those handlers
  (the work would run twice); folders added without it behave as before.

### Fixed
- **Watched folders belong to the vault, not to the scope they were added from.** Every scope of the container's
  `IVault` now sees, pauses, resumes and removes the same folders (before, a later scope saw none, and a folder's watcher
  could no longer be stopped through the vault). Each `IVaultFactory` tenant keeps its own folders.

## [0.39.5] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.67.0 -> 0.67.1, `FluxIndex.Storage.SQLite` 0.67.0 -> 0.67.1. No source changes.

---

## [0.39.4] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.66.1 -> 0.67.0, `FluxIndex.Storage.SQLite` 0.66.1 -> 0.67.0. No source changes.

---

## [0.39.3] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.10 -> 0.33.11, `FluxIndex.Core` 0.66.0 -> 0.66.1, `FluxIndex.Storage.SQLite` 0.66.0 -> 0.66.1. No source changes.

---

## [0.39.2] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.9 -> 0.33.10.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.65.1 -> 0.66.0, `FluxIndex.Storage.SQLite` 0.65.1 -> 0.66.0.

### Fixed
- **A vault can be built in a container whose embedding service is FluxIndex's keyword-only placeholder**
  (`NoEmbeddingService`, what a FluxIndex context without an embedder registers since FluxIndex 0.65.0). Building one
  threw, because the pipeline bound the vector store to an identity the placeholder refuses to name. The vault now
  behaves as it does with no embedding service: files are processed, vectors are not indexed, a search returns nothing.
- `FluxIndexMemorizer` resolved in such a container says it needs an embedding service instead of failing on the
  identity.

---

## [0.39.1] - 2026-10-01

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.8 -> 0.33.9, `FluxIndex.Core` 0.64.0 -> 0.65.1, `FluxIndex.Storage.SQLite` 0.64.0 -> 0.65.1. No source changes.

---

## [0.39.0] - 2026-09-30

### Added
- **A tracked file can be moved or renamed without being embedded again.** `IVault.MoveAsync(sourcePath,
  destinationPath)` moves the entry directory (git history included), the entry record and every index leg — vector
  store, keyword index, GraphRAG graph — to the new path. Chunks get the ids a memorize at the new path would write and
  new `source_path`, `file_name`, `filepath_hash` and `document_id` metadata; their vectors are kept. Everything is
  checked before anything is written, and a later failure moves the earlier legs and the directory back. Returns a
  `VaultMoveResult`.
- **`IVault.MoveFolderAsync(sourceFolder, destinationFolder)`** moves every entry under a folder; an entry that cannot
  move is reported in `VaultFolderMoveResult.Errors` and stays tracked under its old path while the others move.
- `IVaultPipeline.ReassignAsync`, `IVaultStorageService.MoveEntryStorageAsync` and `VaultEntry.Relocate` are the steps
  the move is made of. The two `IVault` members have a default implementation that throws `NotSupportedException`, so
  other `IVault` implementations keep compiling.

### Changed
- **The documentation no longer says a watched folder is indexed automatically.** It never was: `IFileWatcherService`
  raises created, modified, deleted and renamed events, and nothing in FluxFeed subscribes to them — the `autoMemorize`
  argument of `AddWatchedFolderAsync` is only recorded on `WatchedFolder.AutoMemorize`. The README quick start now
  registers the folder and runs `SyncAsync`, and a new *Watching folders* section shows how to route the events to
  `MemorizeAsync`, `RemoveAsync` and `MoveAsync`. The XML docs of `AddWatchedFolderAsync`, `IFileWatcherService` and
  `WatchedFolder.AutoMemorize` say the same.
- **Documentation comments describe behaviour only.** Comments and test descriptions no longer cite tracker ids,
  decision records or the applications a defect was found in; past CHANGELOG entries keep the change and drop the tool
  that made it.
- Re-pinned sibling package(s) `FileFlux` 0.33.7 -> 0.33.8, `FluxGuard.Remote` 0.18.0 -> 0.18.1.

### Fixed
- **`RemoveWatchedFolderAsync(folderId, removeTrackedFiles: true)` no longer removes entries from sibling folders.** It
  matched a bare string prefix, so removing `docs` also removed the entries under `docs2/`. Entries now have to lie
  inside the folder (up to a directory separator; a trailing separator on the watched path makes no difference) — the
  check `MoveFolderAsync`, folder scans and directory search scopes already made, now shared by all four.

### Dependencies
- Requires FluxIndex.Core 0.64.0 (`ReassignDocumentAsync` on the vector store and keyword index, `ReassignChunksAsync`
  on the GraphRAG service).

---

## [0.38.0] - 2026-09-30

### Changed
- Re-pinned sibling package(s) `FluxGuard.Remote` 0.17.1 -> 0.18.0.
- Re-pinned sibling package(s) `FileFlux` 0.33.6 -> 0.33.7.

### Fixed
- **A search no longer writes to the vector store or reads the vault document by document.** Earlier releases copied
  each chunk's document id into its metadata on the first scoped search of each document, so old chunks could match
  the scope filter. FluxIndex 0.63.0 resolves the `document_id` scope to the chunk's own document id in every vector
  store and keyword index, so the migration is gone. The copy it made never reached the keyword index, which is why a
  hybrid or keyword search could still miss chunks indexed before the copy existed; those chunks are now in scope with
  no re-index.

### Removed
- **Breaking: `VaultScopeTagBackfillState` and the `scopeTagBackfillState` parameter of the `VaultPipeline`
  constructor.** Migration: drop the argument; `AddFileVault` no longer registers the type.

### Dependencies
- `FluxIndex.Core` and `FluxIndex.Storage.SQLite` 0.62.1 -> 0.63.0 (required: the scope resolution above).

---

## [0.37.14] - 2026-09-30

### Changed
- Re-pinned sibling package(s) `FluxGuard.Remote` 0.17.0 -> 0.17.1.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.62.0 -> 0.62.1, `FluxIndex.Storage.SQLite` 0.62.0 -> 0.62.1.

### Fixed
- **Every C# example in the README compiles against the current API, and a test keeps it that way.** Seven examples did
  not compile as written: six used types (`VaultEntry`, `VaultJobPriority`, `VaultJobStatus`, `FileVaultOptions`,
  `VaultPipeline`, the `LogWarning` extension) from namespaces they did not import, and the image-enricher example read an
  undeclared field and declared its class before a statement. A test now compiles each block.
- **The README no longer says a store bound to a different embedder fails at startup.** FluxFeed binds the store when the
  vault pipeline is first built, not at host start; the sqlite-vec store gives a different embedder its own vector table
  rather than throwing, and `EmbeddingModelMismatchException` is thrown only when one store instance is bound to two
  different embedders.
- **The hybrid-search documentation describes every keyword leg and both fusion modes.** A registered
  `IHybridSearchService` without `IKeywordSearchService` also reports `KeywordIndex`, over an index FluxFeed does not
  write; the stock hybrid service fuses queries containing `API`, `HTTP`, `JSON`, `SQL`, `AI` or `ML` by weighted sum, so
  their scores are not rank-sized. The `VaultSearchOptions.SearchStrategy` XML doc now names the `IKeywordSearchService`
  path it omitted.

---

## [0.37.13] - 2026-09-30

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.5 -> 0.33.6. No source changes.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.61.6 -> 0.62.0, `FluxIndex.Storage.SQLite` 0.61.6 -> 0.62.0.

---

## [0.37.12] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.61.5 -> 0.61.6, `FluxIndex.Storage.SQLite` 0.61.5 -> 0.61.6. No source changes.

---

## [0.37.11] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.4 -> 0.33.5, `FluxIndex.Core` 0.61.4 -> 0.61.5, `FluxIndex.Storage.SQLite` 0.61.4 -> 0.61.5. No source changes.

---

## [0.37.10] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.3 -> 0.33.4. No source changes.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.61.3 -> 0.61.4, `FluxIndex.Storage.SQLite` 0.61.3 -> 0.61.4.

---

## [0.37.9] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.2 -> 0.33.3, `FluxIndex.Core` 0.61.2 -> 0.61.3, `FluxIndex.Storage.SQLite` 0.61.2 -> 0.61.3. No source changes.

---

## [0.37.8] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.61.1 -> 0.61.2, `FluxIndex.Storage.SQLite` 0.61.1 -> 0.61.2. No source changes.

---

## [0.37.7] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.1 -> 0.33.2, `FluxIndex.Core` 0.61.0 -> 0.61.1, `FluxIndex.Storage.SQLite` 0.61.0 -> 0.61.1. No source changes.

---

## [0.37.6] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.33.0 -> 0.33.1, `FluxIndex.Core` 0.60.1 -> 0.61.0, `FluxIndex.Storage.SQLite` 0.60.1 -> 0.61.0. No source changes.

---

## [0.37.5] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.32.0 -> 0.33.0, `FluxIndex.Core` 0.60.0 -> 0.60.1, `FluxIndex.Storage.SQLite` 0.60.0 -> 0.60.1. No source changes.

---

## [0.37.4] - 2026-09-29

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.59.5 -> 0.60.0, `FluxIndex.Storage.SQLite` 0.59.5 -> 0.60.0.

### Fixed
- **The "GraphRAG is enabled but IGraphRAGService is not registered" error names only `AddFullGraphRAG()`.** It also suggested
  `AddGraphRAGService()`, which registered the service without what it needs and is removed in FluxIndex 0.60.0.

---

## [0.37.3] - 2026-09-29

### Fixed
- **The `VaultBasePath` documentation states the real default:** one vault at `<working directory>/.vault` (`VaultDirectoryName`),
  not a `.vault` next to each source file. The `AdditionalTextExtensions` doc now says where the extensions apply (the plain-text
  fallback and the indexable set).
- **README:** the vector-store package the examples use (`FluxIndex.Storage.SQLite`), options binding from configuration, the RAG
  security DI registration, the queue operations on `IVaultQueueService`, what `AddFileVaultWithFluxIndex` adds, and the other
  registration entry points and options.

## [0.37.2] - 2026-09-28

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.13 -> 0.32.0, `FluxIndex.Core` 0.59.4 -> 0.59.5, `FluxIndex.Storage.SQLite` 0.59.4 -> 0.59.5. No source changes.

---

## [0.37.1] - 2026-09-28

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.12 -> 0.31.13, `FluxIndex.Core` 0.59.3 -> 0.59.4, `FluxIndex.Storage.SQLite` 0.59.3 -> 0.59.4. No source changes.

---

## [0.37.0] - 2026-09-28

### Fixed
- **`FileVaultOptions.MaxRetryCount` sets how often a failed job is retried.** Every job was created with three retries whatever the option said; jobs now take the value of the options the queue service is built with.
- **`FileVaultOptions.EnableRealTimeWatch = false` stops file watching.** Every added or resumed folder was watched regardless. With it off, a watched folder is still registered for scans and sync, but no file-system watcher runs for it.

### Removed
- **Breaking: `FileVaultOptions.QueuePollingIntervalMs`.** The queue worker waits on a job signal rather than polling, so the setting had nothing to control. Delete the assignment.
- **Breaking: `FileVaultOptions.VersionRetentionCount`.** The vault keeps no per-file version history, so there was nothing to retain. Delete the assignment.

---

## [0.36.3] - 2026-09-28

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.11 -> 0.31.12, `FluxIndex.Core` 0.59.2 -> 0.59.3, `FluxIndex.Storage.SQLite` 0.59.2 -> 0.59.3. No source changes.

---

## [0.36.2] - 2026-09-28

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.10 -> 0.31.11, `FluxIndex.Core` 0.59.1 -> 0.59.2, `FluxIndex.Storage.SQLite` 0.59.1 -> 0.59.2. No source changes.

---

## [0.36.1] - 2026-09-28

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.8 -> 0.31.10, `FluxIndex.Core` 0.59.0 -> 0.59.1, `FluxIndex.Storage.SQLite` 0.59.0 -> 0.59.1. No source changes.

---

## [0.36.0] - 2026-09-27

### Fixed
- **Removing or re-indexing a document now also cleans the GraphRAG graph store.** The vault deleted a document's chunks
  from the vector and keyword legs only. The graph store kept the communities, entity-to-chunk links and relationships
  derived from them, so a removed document stayed in community listings and entity lookups, and an edit that replaced
  every chunk left the old communities behind. `RemoveAsync` and the re-index swap now call FluxIndex's
  `IGraphRAGService.ForgetChunksAsync` with the chunks they delete, in the vault's graph partition. It does nothing when
  no GraphRAG service or graph store is registered.

### Dependencies
- FluxIndex.Core / FluxIndex.Storage.SQLite 0.58.0 -> 0.59.0 (`ForgetChunksAsync`).

---

## [0.35.11] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.57.1 -> 0.58.0, `FluxIndex.Storage.SQLite` 0.57.1 -> 0.58.0. No source changes.

---

## [0.35.10] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.7 -> 0.31.8, `FluxIndex.Core` 0.57.0 -> 0.57.1, `FluxIndex.Storage.SQLite` 0.57.0 -> 0.57.1. No source changes.

---

## [0.35.9] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.56.0 -> 0.57.0, `FluxIndex.Storage.SQLite` 0.56.0 -> 0.57.0. No source changes.

---

## [0.35.8] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.55.4 -> 0.56.0, `FluxIndex.Storage.SQLite` 0.55.4 -> 0.56.0. No source changes.

---

## [0.35.7] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.6 -> 0.31.7, `FluxIndex.Core` 0.55.3 -> 0.55.4, `FluxIndex.Storage.SQLite` 0.55.3 -> 0.55.4. No source changes.

---

## [0.35.6] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.5 -> 0.31.6, `FluxIndex.Core` 0.55.2 -> 0.55.3, `FluxIndex.Storage.SQLite` 0.55.2 -> 0.55.3. No source changes.

---

## [0.35.5] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.4 -> 0.31.5, `FluxIndex.Core` 0.55.1 -> 0.55.2, `FluxIndex.Storage.SQLite` 0.55.1 -> 0.55.2. No source changes.

---

## [0.35.4] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.3 -> 0.31.4, `FluxIndex.Core` 0.55.0 -> 0.55.1, `FluxIndex.Storage.SQLite` 0.55.0 -> 0.55.1. No source changes.

---

## [0.35.3] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.54.1 -> 0.55.0, `FluxIndex.Storage.SQLite` 0.54.1 -> 0.55.0. No source changes.

---

## [0.35.2] - 2026-09-27

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.1 -> 0.31.3, `FluxIndex.Core` 0.53.1 -> 0.54.1, `FluxIndex.Storage.SQLite` 0.53.1 -> 0.54.1. No source changes.

---

## [0.35.1] - 2026-09-26

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.31.0 -> 0.31.1, `FluxIndex.Core` 0.53.0 -> 0.53.1, `FluxIndex.Storage.SQLite` 0.53.0 -> 0.53.1. No source changes.

---

## [0.35.0] - 2026-09-26

### Added
- **A vault search hit can say which page or stretch of a recording it came from.** Chunks now carry `pageNumber`, `ff_start_page`/`ff_end_page` (paginated sources) and `ff_start_seconds`/`ff_end_seconds` (recordings) metadata — the keys are `VaultPipeline.*MetadataKey` constants and match FluxIndex's own FileFlux integration. Extraction records where each stretch of the stored text came from (`ExtractionResult.Spans`, kept beside `extracted.md` as `extracted.spans.json`), and memorize hands those spans to the chunker. If `refined.md` was edited by hand, the stored offsets no longer fit it: its chunks are indexed without a location and a warning is logged, until the document is re-extracted.

### Changed
- **Breaking**: `IChunker.ChunkAsync(string content, IReadOnlyList<ContentSpan>? spans, ChunkingOptions options, CancellationToken ct)` returns `IReadOnlyList<ContentChunk>` (text plus `Location`). A custom chunker takes the extra `spans` argument (it may ignore it) and wraps each string in `new ContentChunk(text)`.
- `IVaultStorageService` gains `StoreContentSpansAsync` / `GetContentSpansAsync`; a custom storage implementation adds them.
- The FileFlux extractor stores the refined text (rule-based, then LLM when a refiner is registered) instead of the text of one whole-document chunk, and the FileFlux chunker hands FileFlux the stored text directly (`IDocumentProcessorFactory.Create(RawContent)`, FileFlux 0.31.0) instead of writing it to a temporary `.txt` file. The chunker no longer runs LLM refinement a second time on text that was already refined at extraction.
- Re-pinned sibling package(s) `FileFlux` 0.30.0 -> 0.31.0.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.52.3 -> 0.53.0, `FluxIndex.Storage.SQLite` 0.52.3 -> 0.53.0.

---

## [0.34.5]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.29.2 -> 0.30.0, `FluxIndex.Core` 0.52.1 -> 0.52.2, `FluxIndex.Storage.SQLite` 0.52.1 -> 0.52.2. No source changes.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.52.2 -> 0.52.3, `FluxIndex.Storage.SQLite` 0.52.2 -> 0.52.3.

---

## [0.34.4]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.29.1 -> 0.29.2. No source changes.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.52.0 -> 0.52.1, `FluxIndex.Storage.SQLite` 0.52.0 -> 0.52.1.

---

## [0.34.3]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.29.0 -> 0.29.1, `FluxIndex.Core` 0.51.4 -> 0.52.0, `FluxIndex.Storage.SQLite` 0.51.4 -> 0.52.0. No source changes.

---

## [0.34.2]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.28.2 -> 0.29.0, `FluxIndex.Core` 0.51.3 -> 0.51.4, `FluxIndex.Storage.SQLite` 0.51.3 -> 0.51.4. No source changes.

---

## [0.34.1]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.28.1 -> 0.28.2, `FluxIndex.Core` 0.51.2 -> 0.51.3, `FluxIndex.Storage.SQLite` 0.51.2 -> 0.51.3. No source changes.

---

## [0.34.0]

### Added
- **Vault search can rerank.** `VaultSearchOptions.UseReranker = true` hands a candidate pool to the registered
  FluxIndex `IReranker` and returns `TopK` of its order; `RerankCandidateCount` sets the pool (default `TopK * 3`,
  never below `TopK`). Same names and meaning as FluxIndex `SearchOptions`, so a vault search and an SDK search over
  one index rerank the same way. `VaultSearchResultItem.Score` is then the reranker's score and the new
  `RetrievalScore` keeps the retrieval score; `MinScore` still filters on the retrieval score, before reranking.
  Setting `UseReranker` with no reranker registered throws `InvalidOperationException`. Off by default.
- Tenant vaults from `IVaultFactory` resolve the reranker from their scope, like the pipeline's optional services.

---

## [0.33.19]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.28.0 -> 0.28.1, `FluxIndex.Core` 0.51.1 -> 0.51.2, `FluxIndex.Storage.SQLite` 0.51.1 -> 0.51.2. No source changes.

---

## [0.33.18]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.51.0 -> 0.51.1, `FluxIndex.Storage.SQLite` 0.51.0 -> 0.51.1. No source changes.

---

## [0.33.17]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.27.1 -> 0.28.0, `FluxIndex.Core` 0.50.6 -> 0.51.0, `FluxIndex.Storage.SQLite` 0.50.6 -> 0.51.0. No source changes.

---

## [0.33.16]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.27.0 -> 0.27.1, `FluxIndex.Core` 0.50.5 -> 0.50.6, `FluxIndex.Storage.SQLite` 0.50.5 -> 0.50.6. No source changes.

---

## [0.33.15]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.26.1 -> 0.27.0, `FluxIndex.Core` 0.50.4 -> 0.50.5, `FluxIndex.Storage.SQLite` 0.50.4 -> 0.50.5. No source changes.

---

## [0.33.14]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.26.0 -> 0.26.1, `FluxIndex.Core` 0.50.2 -> 0.50.4, `FluxIndex.Storage.SQLite` 0.50.2 -> 0.50.4. No source changes.

---

## [0.33.13]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.25.1 -> 0.26.0, `FluxIndex.Core` 0.50.1 -> 0.50.2, `FluxIndex.Storage.SQLite` 0.50.1 -> 0.50.2. No source changes.

---

## [0.33.12]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.25.0 -> 0.25.1, `FluxIndex.Core` 0.49.0 -> 0.50.1, `FluxIndex.Storage.SQLite` 0.49.0 -> 0.50.1. No source changes.

---

## [0.33.11]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.24.1 -> 0.25.0, `FluxIndex.Core` 0.48.1 -> 0.49.0, `FluxIndex.Storage.SQLite` 0.48.1 -> 0.49.0. No source changes.

---

## [0.33.10]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.20 -> 0.24.1, `FluxIndex.Core` 0.48.0 -> 0.48.1, `FluxIndex.Storage.SQLite` 0.48.0 -> 0.48.1. No source changes.

---

## [0.33.9]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.47.0 -> 0.48.0, `FluxIndex.Storage.SQLite` 0.47.0 -> 0.48.0. No source changes.

---

## [0.33.8]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.19 -> 0.23.20, `FluxIndex.Core` 0.46.4 -> 0.47.0, `FluxIndex.Storage.SQLite` 0.46.4 -> 0.47.0. No source changes.

---

## [0.33.7]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.18 -> 0.23.19, `FluxIndex.Core` 0.46.3 -> 0.46.4, `FluxIndex.Storage.SQLite` 0.46.3 -> 0.46.4. No source changes.

---

## [0.33.6]

### Changed
- Re-pinned sibling package(s) `FluxGuard.Remote` 0.16.0 -> 0.17.0, `FluxIndex.Core` 0.46.2 -> 0.46.3, `FluxIndex.Storage.SQLite` 0.46.2 -> 0.46.3. No source changes.

---

## [0.33.5]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.46.1 -> 0.46.2, `FluxIndex.Storage.SQLite` 0.46.1 -> 0.46.2. No source changes.

---

## [0.33.4]

### Changed
- Re-pinned sibling package(s) `FluxGuard.Remote` 0.15.1 -> 0.16.0, `FluxIndex.Core` 0.46.0 -> 0.46.1, `FluxIndex.Storage.SQLite` 0.46.0 -> 0.46.1. No source changes.

---

## [0.33.3]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.45.0 -> 0.46.0, `FluxIndex.Storage.SQLite` 0.45.0 -> 0.46.0. No source changes.

---

## [0.33.2]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.44.7 -> 0.45.0, `FluxIndex.Storage.SQLite` 0.44.7 -> 0.45.0. No source changes.
- README: how to register the keyword index on the SQLite stack (`AddSQLiteKeywordSearch`, FluxIndex 0.45.0).
  FluxIndex 0.45.0 also makes every hybrid leg fetch at least as many candidates as the fused list returns, for any
  caller of `HybridSearchService`.

---

## [0.33.1]

### Fixed
- **A `MinScore` on a `Hybrid` search no longer empties the result (regression in 0.32.0).** With a keyword service
  registered, 0.32.0 compared `VaultSearchOptions.MinScore` with the fused score, which is rank-sized (about 0.016
  at best), so any similarity-sized threshold — `0.3`, say — dropped every hit. It is again a similarity floor on
  the vector leg, applied before fusion, as it was through 0.31.4 and still is on the native leg. The same holds
  when you register your own `IHybridSearchService`: the threshold now arrives as `VectorOptions.MinScore`, not as
  `MinFusedScore`, so such a consumer may see *more* results than before for the same threshold.
- **A search no longer re-reads every document of the vault.** The one-time check for chunks that predate the
  `document_id` scope tag remembered what it had examined on the pipeline, which is registered scoped — so a host
  that opens a scope per request paid one `GetByDocumentIdAsync` per vault entry on every search (6 206 reads per
  search on a 6 206-entry vault). What it has examined is now a singleton (`VaultScopeTagBackfillState`, registered
  by `AddFileVault`): each document is read once per process. A pipeline you construct yourself keeps a private one
  unless you pass the new optional constructor argument.
- **A `Hybrid` search returns up to `TopK` results.** Since 0.32.0 each leg fetched its default of 10 candidates
  whatever `TopK` asked for, so a request for 25 returned about 15. Each leg now fetches `TopK * 2`.

### Changed
- Correction to the 0.32.0 note below: the keyword-index leg does not fuse by relative score at 0.7 / 0.3. It
  fuses by weighted reciprocal rank with weights chosen from the query's length — see "Hybrid" in the README.
  Hybrid scores were rank-sized before 0.32.0 as well; only the weights changed.
- Re-pinned sibling package(s) `FluxIndex.Core` 0.44.6 -> 0.44.7, `FluxIndex.Storage.SQLite` 0.44.6 -> 0.44.7. No source changes.

---

## [0.33.0]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.44.5 -> 0.44.6, `FluxIndex.Storage.SQLite` 0.44.5 -> 0.44.6. No source changes.

### Fixed
- **`FileVaultOptions.MaxFileSizeMB` is enforced.** It was declared (default 100 MB, documented as "larger files will be
  skipped") and read by nothing, so files of any size were extracted and indexed. A folder scan now skips a file over
  the limit (counted in `ScanResult.SkippedFilesCount`), and memorizing one fails permanently with the size in the
  message — the queue does not retry it. `0` or less means no limit.

### Removed
- **Breaking: `WatchOptions`.** No API accepted it; folder watching is configured through `FileVaultOptions` and the
  watch call's own parameters.

### Added
- An options-reachability roster test (`Iyu.Conventions.Testing`) keeps every public option read by the library.

---

## [0.32.0]

### Changed
- **Hybrid search fuses over your keyword index when you register one, so a text analyzer and keyword fields apply to
  hybrid too.** With `IKeywordSearchService` registered, a `Hybrid` request now fuses the vector leg with that same
  index — the one the `Keyword` strategy searches and ingestion writes — through the registered `IHybridSearchService`,
  or the stock `HybridSearchService` when none is registered. Before, a vector store with native hybrid (sqlite-vec +
  FTS5) always won, so hybrid ran over a second keyword table with its own tokenizer: a registered `ITextAnalyzer`
  (for example `CjkBigramTextAnalyzer`) and `KeywordFieldOptions` reached the keyword strategy only. Native hybrid is
  still used when no keyword service is registered. The document scope reaches both legs before fusion either way.
  **Scores change** for consumers with both registered: fusion is `HybridSearchService`'s rather than the store's.
  (This note first said "relative score, vector 0.7 / keyword 0.3"; that was wrong — see 0.33.1.)

### Added
- **`IVaultPipeline.HybridKeywordLeg` reports which keyword index hybrid uses** (`KeywordIndex`, `Native` or `None`),
  and the pipeline logs it once at Information. **Breaking** for code that implements `IVaultPipeline` itself.

---

## [0.31.4]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.17 -> 0.23.18, `FluxIndex.Core` 0.44.4 -> 0.44.5, `FluxIndex.Storage.SQLite` 0.44.4 -> 0.44.5. No source changes.

---

## [0.31.3]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.16 -> 0.23.17, `FluxIndex.Core` 0.44.2 -> 0.44.4, `FluxIndex.Storage.SQLite` 0.44.2 -> 0.44.4. No source changes.

---

## [0.31.2]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.15 -> 0.23.16, `FluxIndex.Core` 0.44.0 -> 0.44.2, `FluxIndex.Storage.SQLite` 0.44.0 -> 0.44.2. No source changes.

---

## [0.31.1]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.14 -> 0.23.15, `FluxGuard.Remote` 0.15.0 -> 0.15.1. No source changes.

---

## [0.31.0]

### Changed
- A tenant-scoped vault (`FileVaultOptions.VaultId` set) builds its GraphRAG index in the graph partition named by its `VaultId` (FluxIndex 0.44.0 partitions), so vaults sharing one graph store no longer merge each other's entities or see each other's communities. The caller's `MemorizeOptions.GraphRAGOptions` are copied, not changed; options that name a different partition are refused. A vault without a `VaultId` is unchanged.
- FluxIndex dependency raised to 0.44.0.

---

## [0.30.3]

### Changed
- Microsoft.Extensions.* / Microsoft.Data.Sqlite / EF Core pins raised to 10.0.12 (September 2026 .NET servicing).
- Re-pinned sibling package(s) `FileFlux` 0.23.12 -> 0.23.14, `FluxIndex.Core` 0.43.0 -> 0.43.1, `FluxIndex.Storage.SQLite` 0.43.0 -> 0.43.1. No source changes.

---

## [0.30.2]

### Fixed
- The package now ships its XML documentation file. Every public member of `IVault`, `IVaultPipeline` and the
  option types has carried `///` docs for a long time, but `GenerateDocumentationFile` was never set, so the
  nupkg held only the dll and a consumer's IDE showed nothing — the 0.30.0 `RepairKeywordIndexAsync(KeywordIndexRepairScope, …)`
  overload and `KeywordIndexRepairScope` itself read as undocumented from the outside. Generating the file also
  surfaced what a never-compiled doc set hides: three `cref`s that did not resolve or were ambiguous
  (`SyncAsync`, `RefreshAsync`, `RepairKeywordIndexAsync` — the ambiguity a consumer hit in its own docs) and
  missing `<param>` tags on the `IVaultQueueService` enqueue and `GetJobsAsync` members; all fixed. No code changes.

---

## [0.30.1]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.11 -> 0.23.12, `FluxGuard.Remote` 0.14.2 -> 0.15.0, `FluxIndex.Core` 0.42.0 -> 0.43.0, `FluxIndex.Storage.SQLite` 0.42.0 -> 0.43.0. No source changes.

---

## [0.30.0]

### Added
- **`KeywordIndexRepairScope`** — `IVault.RepairKeywordIndexAsync(scope)` and
  `IVaultPipeline.RepairKeywordIndexAsync(entries, scope)`. `Mismatched` is the existing behaviour;
  `All` rewrites every entry from the vector leg whether or not its legs agree. That is the shape a
  text-analyzer or keyword-field change leaves behind: every keyword row present under the right id,
  every one written the old way, so the id comparison found nothing to do and the only way to
  re-index the keyword leg was a full re-memorize (re-extract, re-chunk, re-embed). The rows are
  rebuilt from the chunks the vector store returns, metadata included, so the fields the current
  configuration reads (`file_name`, `title`, ...) are populated; nothing is re-embedded.

### Fixed
- **A missing file is now recorded as a `Permanent` failure.** The worker's own `FileNotFoundException`
  path reported through the unclassified `FailAsync` overload, so the job was persisted as "not
  classified" — retried by budget and never refused on an operator's rerun request — although
  `MemorizeFailureClassifier` already ranks a missing file `Permanent`.

### Changed
- `IVaultQueueService.FailAsync(jobId, errorMessage)` documents what "not classified" means: the queue
  treats it as retryable, and only a recorded `Permanent` stops retries and refuses a rerun. A consumer
  that replaces the default worker and reports deterministic failures through this overload gets them
  retried and rerun with nothing to say why; prefer the classifying overload.

---

## [0.29.5]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.10 -> 0.23.11, `FluxGuard.Remote` 0.14.1 -> 0.14.2, `FluxIndex.Core` 0.41.1 -> 0.42.0, `FluxIndex.Storage.SQLite` 0.41.1 -> 0.42.0. No source changes.

---

## [0.29.4]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.9 -> 0.23.10. No source changes.

---

## [0.29.3]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.8 -> 0.23.9, `FluxIndex.Core` 0.41.0 -> 0.41.1, `FluxIndex.Storage.SQLite` 0.41.0 -> 0.41.1. No source changes.
- Raised `Microsoft.Extensions.*` package references to 10.0.12 (latest servicing release). The re-pinned sibling releases declare `Microsoft.Extensions.*` floors above the previous references.

---

## [0.29.2]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.40.1 -> 0.41.0, `FluxIndex.Storage.SQLite` 0.40.1 -> 0.41.0. No source changes.

---

## [0.29.1]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.40.0 -> 0.40.1, `FluxIndex.Storage.SQLite` 0.40.0 -> 0.40.1. No source changes.

---

## [0.29.0]

### Breaking
- `IVault.StatusAsync()` no longer runs change detection, queries the index legs or walks the entry directories.
  It used to call `DetectChangesAsync` for every entry — a source hash, a `git status` process and a `meta.json`
  write each, roughly 30 ms per entry — so a status read was a change sweep that wrote to disk. It now costs what
  listing the entries costs and writes nothing.
- Removed from `VaultStatus`: `ChangedSourceCount`, `ChangedVaultCount`, `ChangedEntries` (use
  `DetectChangesAsync()`), `VectorRowCount`, `KeywordRowCount`, `IndexMismatchedEntryCount` (use
  `AuditIndexAsync()`), `TotalStorageSizeBytes` (use `GetStorageSizeAsync()`).
- `StatusAsync()` no longer refreshes the persisted `SyncStatus` that `GetEntriesNeedingSyncAsync()` and
  `ListByStatusAsync()` read. Call `DetectChangesAsync()` where fresh sync state is needed.
- `TotalEntries`, the stage counts and `OrphanedCount` exclude entries being removed (`RemovalPending`,
  `RemovalPartial`); `RemovalPendingCount` and `RemovalPartialCount` still report them.
- New `IVault` members (a hand-written implementation must add them): `DetectChangesAsync(CancellationToken)`,
  `AuditIndexAsync(CancellationToken)`, `GetStorageSizeAsync(CancellationToken)`.

### Added
- `IVault.DetectChangesAsync(CancellationToken)` runs change detection for every entry not being removed, persists
  each entry's `SyncStatus`, and returns a `VaultChangeReport` (`SourceChanged`, `VaultChanged`, `SourceDeleted`).
- `IVault.AuditIndexAsync(CancellationToken)` returns a `VaultIndexAudit` (`IndexedChunkCount`, `VectorRowCount`,
  `KeywordRowCount`, `MismatchedEntryCount`) for the searchable entries.
- `IVault.GetStorageSizeAsync(CancellationToken)` returns the bytes on disk under the entry directories.

---

## [0.28.1]

### Changed
- Contextual enrichment: when the `IContextualEnrichmentService` port succeeds but returns a blank context for a
  chunk, that chunk is still indexed as it was, but it is now tagged `enrichment=empty` and a warning names how many
  chunks of which document got no context. Before, such a chunk carried no `enrichment` tag at all and looked exactly
  like enrichment being off. A blank context is not a failure under either `ContinueOnError` setting.
  `VaultPipeline.EnrichmentMetadataKey` now has three values: `contextual`, `empty`, `failed`.

---

## [0.28.0]

### Breaking
- `IVault.RepairKeywordIndexAsync` and `IVaultPipeline.RepairKeywordIndexAsync` are new interface members. A
  hand-written `IVault` or `IVaultPipeline` implementation (for example a test fake) must add them; substitutes
  generated by a mocking library are unaffected.
- Requires FluxIndex 0.40.0 (`FluxIndex.Core`, `FluxIndex.Storage.SQLite`), which removes unreferenced SDK settings,
  service implementations and options — see its changelog.

### Added
- `IVault.RepairKeywordIndexAsync` rebuilds a drifted keyword leg from the vector leg: for every searchable entry
  whose two legs disagree, it writes the vector rows to the keyword index and removes the keyword rows the vector
  store does not hold. Nothing is re-embedded, and entries whose legs agree are left alone. It throws when no keyword
  index or vector store is registered.

### Changed
- A generation swap and a rollback remove superseded keyword rows with a single
  `IKeywordSearchService.DeleteChunksAsync` call instead of one call per chunk.
- Pinned FileFlux 0.23.8: the FileVault entry points no longer leave `SizeLimit` set on the host's shared
  `IMemoryCache`.

### Fixed
- `RecoverStuckJobsAsync` no longer resets a job this process is still running. Called while a pipeline held a job
  (a periodic recovery pass, or a second queue instance over the same `queue.db`), it used to hand that job out again,
  and two pipelines raced the same entry. Recovery now skips the jobs this process has dequeued and not yet reported,
  so it is safe to call at any time. One `queue.db` is consumed by one process.
- Entry artifacts (extracted and refined markdown, append text, QA, image bytes, image manifest) are written
  atomically. Overlapping writers could previously leave the tail of a longer version behind a shorter one, after
  which the image manifest no longer parsed and every later memorize of the entry failed.
- Image manifest updates are serialized per entry, so overlapping description updates no longer drop each other.
- A memorize that finds an unreadable image manifest logs a warning and rebuilds it instead of failing, so an
  already-damaged entry recovers.
