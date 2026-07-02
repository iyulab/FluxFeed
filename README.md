# FluxFeed

문서 파이프라인 표면(④b) — 수집·파싱·정제를 묶어 FluxIndex(④a)에 적재(feed)한다. FluxIndex에 비대칭 의존(④b는 ④a 필요, ④a는 ④b 불필요).

## Quick Start

```csharp
// 파일-소스 vault: FileFlux 추출/청킹 + FluxIndex 적재
services.AddFileVaultFactoryWithFluxIndex(o => o.VaultBasePath = "./data");
```

> 상세 표면·경계는 [CHARTER.md](CHARTER.md) 참조.