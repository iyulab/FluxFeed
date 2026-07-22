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

> 상세 표면·경계는 [CHARTER.md](CHARTER.md) 참조.