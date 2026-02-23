# Week 5: Data Platforms with Bruin Homework

## Q1: Bruin Pipeline Structure

**Answer: `.bruin.yml and pipeline.yml (assets can be anywhere)`**

`bruin init`이 생성하는 필수 구조:
- `.bruin.yml` — 프로젝트 레벨 환경/연결 설정 (dot prefix 필수)
- `pipeline.yml` — 파이프라인 정의
- `assets/` — 에셋 파일들 (어느 디렉토리에도 배치 가능)

---

## Q2: Materialization Strategies

**Answer: `time_interval - incremental based on a time column`**

특정 시간 구간의 데이터를 삭제 후 재삽입(delete + insert)하는 전략.
`pickup_datetime` 기준 월별 처리에 적합하며, 지정한 시간 범위만 재처리하여 효율적이다.

---

## Q3: Pipeline Variables

**Answer: `bruin run --var 'taxi_types=["yellow"]'`**

배열(array) 타입 변수는 JSON 배열 형식으로 전달해야 한다.

---

## Q4: Running with Dependencies

**Answer: `bruin run ingestion/trips.py --downstream`**

`--downstream` 플래그를 사용하면 지정한 에셋과 해당 에셋에 의존하는 모든 다운스트림 에셋을 함께 실행한다.

---

## Q5: Quality Checks

**Answer: `name: not_null`**

`pickup_datetime` 컬럼에 NULL 값이 없음을 보장하는 품질 체크.

```yaml
columns:
  - name: pickup_datetime
    checks:
      - name: not_null
```

---

## Q6: Lineage and Dependencies

**Answer: `bruin lineage`**

`bruin lineage <asset-path>` 명령으로 특정 에셋의 업스트림/다운스트림 의존성 그래프를 확인할 수 있다.

---

## Q7: First-Time Run

**Answer: `--full-refresh`**

신규 DuckDB 데이터베이스에서 처음 실행 시 `--full-refresh` 플래그를 사용하면 테이블을 처음부터 생성(truncate 후 재생성)한다.

```bash
bruin run --full-refresh
```
