# FluxFeed Charter

## 1. 정체성 (Identity)

FluxFeed는 문서 파이프라인 표면(④b) — 수집·파싱·정제를 묶어 **FluxIndex(④a)에 적재(feed)한다.** FileFlux의 청킹 전략과 변환 인프라를 통해 RAG 최적화 문서 플로우를 제공한다.

## 2. 책임 (Responsibility)

- 파일 소스로부터 문서 수집 (파일시스템, 원격 저장소)
- 문서 파싱 및 메타데이터 추출
- 텍스트 청킹 및 정제
- FluxIndex에 청크 적재
- 파일 변경 추적 및 실시간 동기화

**FluxFeed는 가공과 적재만 담당한다.** 벡터 임베딩, 검색, 의미 이해는 ④a(FluxIndex)의 책임.

## 3. 경계 (Boundary)

### ④b (FluxFeed) 담당:
- Document ingestion pipeline (수집 → 파싱 → 청킹)
- File-vault with git-like tracking
- Real-time folder monitoring
- Transformation & normalization

### ④a (FluxIndex) 담당:
- Vector embedding & hybrid search
- Semantic indexing (keyword + vector)
- Query execution
- Ranking & retrieval

**명시적 의존:**
- FluxFeed → FluxIndex.Core (필요)
- FluxIndex → FluxFeed (불필요 — 일방향)

## 4. 의존 (Dependencies)

**Internal:**
- FluxIndex.Core 0.15.0+
- FileFlux 0.10.6+

**External:**
- Microsoft.Extensions.Hosting.Abstractions
- Microsoft.Extensions.Options
- Microsoft.Data.Sqlite (선택적, FileVault persistence)

## 5. 게이트 (Release Gate)

- 공개 API 변경 시 [docs/MIDDLEWARE-ALIGNMENT.md §M3](../docs/MIDDLEWARE-ALIGNMENT.md) 에서 ④b 로드맵 섹션 갱신
- 주요 기능 추가는 FluxIndex와의 호환성 사전 검증
- CI/CD: 빌드 + 테스트(Integration 제외) + 버전 변경 시 NuGet 자동 배포

---

**관련 문서:**
- [MIDDLEWARE-ALIGNMENT.md §M3](../docs/MIDDLEWARE-ALIGNMENT.md) — 파이프라인 아키텍처 로드맵
- [CONSTITUTION.md](../docs/CONSTITUTION.md) — 프로젝트 헌법 및 우선순위
- [LAYERING.md](../docs/LAYERING.md) — 종속성 그래프 및 릴리스 순서
