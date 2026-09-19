# Changelog

All notable changes to FluxFeed are documented here.
Follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) conventions. FluxFeed is pre-1.0, so a minor
version may contain breaking changes; they are listed under **Breaking**.

Releases before 0.28.0 predate this file — see the git history.

---

## [Unreleased]

---

## [0.33.1]

### Fixed
- **A `MinScore` on a `Hybrid` search no longer empties the result (regression in 0.32.0).** With a keyword service
  registered, 0.32.0 compared `VaultSearchOptions.MinScore` with the fused score, which is rank-sized (about 0.016
  at best), so any similarity-sized threshold — `0.3`, say — dropped every hit. It is again a similarity floor on
  the vector leg, applied before fusion, as it was through 0.31.4 and still is on the native leg. The same holds
  when you register your own `IHybridSearchService`: the threshold now arrives as `VectorOptions.MinScore`, not as
  `MinFusedScore`, so such a consumer may see *more* results than before for the same threshold.
- **A `Hybrid` search returns up to `TopK` results.** Since 0.32.0 each leg fetched its default of 10 candidates
  whatever `TopK` asked for, so a request for 25 returned about 15. Each leg now fetches `TopK * 2`.

### Changed
- Correction to the 0.32.0 note below: the keyword-index leg does not fuse by relative score at 0.7 / 0.3. It
  fuses by weighted reciprocal rank with weights chosen from the query's length — see "Hybrid" in the README.
  Hybrid scores were rank-sized before 0.32.0 as well; only the weights changed.

---

## [0.33.0]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.44.5 -> 0.44.6, `FluxIndex.Storage.SQLite` 0.44.5 -> 0.44.6 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

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
- Re-pinned sibling package(s) `FileFlux` 0.23.17 -> 0.23.18, `FluxIndex.Core` 0.44.4 -> 0.44.5, `FluxIndex.Storage.SQLite` 0.44.4 -> 0.44.5 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

---

## [0.31.3]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.16 -> 0.23.17, `FluxIndex.Core` 0.44.2 -> 0.44.4, `FluxIndex.Storage.SQLite` 0.44.2 -> 0.44.4 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

---

## [0.31.2]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.15 -> 0.23.16, `FluxIndex.Core` 0.44.0 -> 0.44.2, `FluxIndex.Storage.SQLite` 0.44.0 -> 0.44.2 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

---

## [0.31.1]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.14 -> 0.23.15, `FluxGuard.Remote` 0.15.0 -> 0.15.1 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

---

## [0.31.0]

### Changed
- A tenant-scoped vault (`FileVaultOptions.VaultId` set) builds its GraphRAG index in the graph partition named by its `VaultId` (FluxIndex 0.44.0 partitions), so vaults sharing one graph store no longer merge each other's entities or see each other's communities. The caller's `MemorizeOptions.GraphRAGOptions` are copied, not changed; options that name a different partition are refused. A vault without a `VaultId` is unchanged.
- FluxIndex dependency raised to 0.44.0.

---

## [0.30.3]

### Changed
- Microsoft.Extensions.* / Microsoft.Data.Sqlite / EF Core pins raised to 10.0.12 (September 2026 .NET servicing).
- Re-pinned sibling package(s) `FileFlux` 0.23.12 -> 0.23.14, `FluxIndex.Core` 0.43.0 -> 0.43.1, `FluxIndex.Storage.SQLite` 0.43.0 -> 0.43.1 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

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
- Re-pinned sibling package(s) `FileFlux` 0.23.11 -> 0.23.12, `FluxGuard.Remote` 0.14.2 -> 0.15.0, `FluxIndex.Core` 0.42.0 -> 0.43.0, `FluxIndex.Storage.SQLite` 0.42.0 -> 0.43.0 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

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
- Re-pinned sibling package(s) `FileFlux` 0.23.10 -> 0.23.11, `FluxGuard.Remote` 0.14.1 -> 0.14.2, `FluxIndex.Core` 0.41.1 -> 0.42.0, `FluxIndex.Storage.SQLite` 0.41.1 -> 0.42.0 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

---

## [0.29.4]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.9 -> 0.23.10 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

---

## [0.29.3]

### Changed
- Re-pinned sibling package(s) `FileFlux` 0.23.8 -> 0.23.9, `FluxIndex.Core` 0.41.0 -> 0.41.1, `FluxIndex.Storage.SQLite` 0.41.0 -> 0.41.1 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.
- Raised `Microsoft.Extensions.*` package references to 10.0.12 (latest servicing release). The re-pinned sibling releases declare `Microsoft.Extensions.*` floors above the previous references.

---

## [0.29.2]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.40.1 -> 0.41.0, `FluxIndex.Storage.SQLite` 0.40.1 -> 0.41.0 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

---

## [0.29.1]

### Changed
- Re-pinned sibling package(s) `FluxIndex.Core` 0.40.0 -> 0.40.1, `FluxIndex.Storage.SQLite` 0.40.0 -> 0.40.1 — re-consumption of already-consumed iyulab packages via `check-pin-drift.ps1 -Fix`. No source changes.

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
