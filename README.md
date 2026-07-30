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

## Failure diagnostics

실패 사유는 두 필드로 나뉜다. **`LastError`는 마지막 실패, `FirstError`는 그 실패 에피소드를 시작한 실패**다.

```csharp
if (entry.FirstError is not null)
{
    // FirstError: 최초 원인 (추출기 분류가 실려 있는 쪽)
    // LastError:  가장 최근 실패 — 재시도가 다른 이유로 실패했다면 다르다
}
```

- 재시도가 자기 이유로 실패하면 `LastError`는 덮이지만 `FirstError`는 남는다. 최초 실패가 추출기의 진단을
  싣고 있으므로, 그것이 지워지면 원본 파일 없이 진단할 방법이 사라진다.
- `FirstError`는 `LastError`와 **같은 생애를 갖는다** — 처리 단계가 진행되거나(`Extracted`/`Refined`/`Memorized`)
  `ResetToSource()` 하면 둘 다 지워진다. 반대로 **sync 상태 전이는 어느 쪽도 지우지 않는다**(`MarkInSync` 포함).
  따라서 `FirstError != null`을 "지금 고장난 엔트리"로 읽지 말 것 — 그 판정은 `Stage`/`SyncStatus`로 한다.
- 이전 버전이 쓴 `meta.json`은 `LastError`를 `FirstError`로 승계한다(한 번만 실패한 엔트리에서는 같은 값).
- `ChangeDetectionResult`에도 같은 두 필드가 실린다.

## Refresh 전제

`refresh`는 **refined 콘텐츠를 재인덱싱**하며 재추출하지 않는다. 따라서 전제는 refined 콘텐츠의 존재이고,
**`ProcessingStage`는 그것을 함의하지 않는다** — 인덱싱할 내용이 없던 memorize는 refine 단계를 건너뛰므로
`Memorized`인데 refined가 없는 엔트리가 정상적으로 생기고, `Error`는 어떤 산출물이 남았는지 말해주지 않는다.

- `RefreshAsync`는 refined 콘텐츠가 없으면 거부한다(메시지가 `memorize`를 지시한다). 이 전제는
  가드와 파이프라인이 **같은 값**을 쓴다.
- `DetectChangesAsync`는 refresh가 성공할 수 없는 엔트리에 `Refresh`를 추천하지 않고 **`Memorize`로 낮춘다.**
  memorize는 재추출하므로 실패했거나 빈 엔트리가 **스스로 회복**할 수 있다.

## Image enrichment

문서에서 추출된 이미지는 파이프라인이 vault에 저장한다. 소비앱이 **설명 생성기만** 등록하면 그 설명이 인덱싱까지 이어진다 — 호출 시점·멱등·재시도·본문 반영·출처 노출은 파이프라인이 소유한다.

```csharp
public sealed class VisionEnricher : IVaultImageEnricher
{
    public async Task<string?> DescribeAsync(VaultImageDescriptionRequest request, CancellationToken ct)
        => await _vision.CaptionAsync(request.Image.FilePath, request.DocumentText, ct);
        // null 반환 = 이번엔 실패 → 파이프라인이 다음 실행에 재시도
}

services.AddSingleton<IVaultImageEnricher, VisionEnricher>();
```

- **미등록 시 종전과 동일** — 이미지는 저장되지만 설명되지 않는다 (하위 호환).
- **멱등** — 설명은 이미지 단위로 영속된다. 재-memorize 시 이미 설명된 이미지는 생성기를 재호출하지 않는다(이미지가 바이트 동일한 한). 실패한 이미지만 다음 실행에서 재시도되며, 한 이미지의 실패가 다른 이미지나 memorize 자체를 중단시키지 않는다.
- **출처는 메타데이터로** — 설명은 **전용 청크**로 인덱싱되고 `chunk_kind="image_description"` · `image_id` · `image_file` 메타데이터를 갖는다. 본문에 마커를 심지 않으므로 사용자에게 보여줄 때 걷어낼 것도 없다.
- **이미지-only 문서** — 텍스트 레이어가 없는 스캔·도표 문서는 종전에 0청크로 끝났으나, 설명이 있으면 그 설명이 곧 내용으로 인덱싱된다. (텍스트도 없고 설명도 없으면 종전대로 0청크.)
- `request.DocumentText`는 원문서 추출 텍스트이며, 텍스트 레이어가 없으면 null이다.

> 상세 표면·경계는 [CHARTER.md](CHARTER.md) 참조.