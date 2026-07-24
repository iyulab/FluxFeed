# FluxFeed

문서 파이프라인 표면(④b) — 수집·파싱·정제를 묶어 FluxIndex(④a)에 적재(feed)한다. FluxIndex에 비대칭 의존(④b는 ④a 필요, ④a는 ④b 불필요).

## Quick Start

```csharp
// 파일-소스 vault: FileFlux 추출/청킹 + FluxIndex 적재
services.AddFileVaultFactoryWithFluxIndex(o => o.VaultBasePath = "./data");
```

## File selection patterns

`FileVaultOptions.DefaultIncludePatterns` / `DefaultExcludePatterns`는 **discovery 경로에만** 실효한다:

| 경로 | 패턴 적용 |
|------|-----------|
| `ScanFolderAsync` / `SyncAsync` | ✅ 패턴 밖 파일은 감지·큐잉 전에 skip |
| 폴더 워처 이벤트 | ✅ (폴더별 패턴이 기본값보다 우선) |
| 명시적 `MemorizeAsync` / `RefreshAsync` | ❌ 의도적 미적용 — 명시 호출은 명시 의도이며, 조용히 skip하면 호출자 오류가 숨는다 |

exclude가 include보다 우선하며, include가 빈 리스트면 "전부 포함"이다.

## Extraction diagnostics

추출기가 보고한 구조화 진단은 `VaultEntry`에 그대로 실려 `meta.json`에 영속된다. 스캔 PDF처럼 **정당한 0청크**가 "조용한 성공"으로 보이지 않게 하는 것이 목적이다.

```csharp
var entry = await vault.MemorizeAsync(path, waitForCompletion: true);  // Stage=Memorized, ChunkCount=0

if (entry.ExtractionHints?.TryGetValue("extraction_failure_reason", out var reason) == true)
{
    // reason: "no_text_layer" (이미지-only/스캔 문서) | "blank_page" (빈 문서)
    // entry.ExtractionWarnings: 사람이 읽는 설명 (예: "... requires OCR")
}
```

- **불투명 패스스루** — 키/값은 추출기(FileFlux `RawContent.Hints`/`Warnings`) 어휘 그대로다. FluxFeed는 해석하지 않는다.
- **스칼라만 영속** — 값이 문자열·불리언·수치·날짜류인 힌트만 저장한다. 리더 내부 구조값(예: `PageRanges`)은 타입명으로 문자열화되어 `meta.json`을 오염시키므로 제외한다. 키가 아니라 **값 타입** 기준이라 FileFlux가 힌트를 추가해도 드리프트가 없다.
- **항상 최신 추출분** — 재추출 시 교체되고, 진단 없이 추출되면 이전 값은 지워진다. 예외 경로 전용인 `LastError`와는 별개 채널이다.
- FileFlux **0.14.0+** 필요 (`no_text_layer`/`blank_page` 세분 라벨의 출처).

> 상세 표면·경계는 [CHARTER.md](CHARTER.md) 참조.