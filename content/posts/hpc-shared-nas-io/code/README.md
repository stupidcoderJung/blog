# Shared NAS I/O lab

이 디렉터리의 코드는 블로그 글의 예제를 실제로 실행할 수 있도록 만든 독립 실습입니다.
회사 코드, 내부 경로, 실제 데이터 형식은 사용하지 않으며 Python 표준 라이브러리만 필요합니다.

## 빠른 실행

```bash
cd code

# 1. 합성 레코드 생성
python nas_io_lab.py prepare --root /tmp/nas-lab --records 3000

# 2. 알려진 경로 직접 열기와 디렉터리 탐색 비교
python nas_io_lab.py benchmark --root /tmp/nas-lab --records 3000 --rounds 3

# 3. async 함수 안의 동기 I/O와 스레드 오프로딩 비교
python async_pipeline.py \
  --root /tmp/nas-lab \
  --records 1000 \
  --concurrency 64 \
  --injected-io-latency-ms 3 \
  --compare

# 4. 워커별 체크포인트와 완료 sentinel 시연
python checkpoint_protocol.py demo --root /tmp/checkpoint-lab --workers 4 --records 1000

# 5. 로컬 SQLite + 불변 델타 청크 교환 시연
python delta_cache.py demo --root /tmp/delta-cache-lab

# 6. 전체 테스트
python -m unittest -v
```

`/tmp`는 재현 편의를 위한 예시입니다. 실제 NAS의 메타데이터 지연을 측정하려면
`--root`만 테스트용 NAS 디렉터리로 바꾸십시오. 운영 데이터 경로나 여러 사용자가
공유하는 디렉터리에서 실습하지 마십시오.

## 파일

| 파일 | 다루는 문제 |
|---|---|
| `nas_io_lab.py` | direct open, check-then-open, `scandir`, manifest의 비용 비교 |
| `async_pipeline.py` | 동기 파일 I/O가 이벤트 루프와 처리량에 미치는 영향 |
| `checkpoint_protocol.py` | single-writer journal, batch flush, sentinel, 검증 후 merge |
| `delta_cache.py` | worker-local SQLite와 immutable delta chunk 교환 |
| `test_nas_patterns.py` | 정상·실패·재시작 경로를 검증하는 단위 테스트 |

## 해석할 때의 주의점

- 로컬 SSD 결과를 NAS 결과처럼 해석하면 안 됩니다.
- 첫 실행과 재실행은 page cache, attribute cache, directory cache 조건이 다릅니다.
- 평균 하나만 보지 말고 p50, p95, 최대값, 실패 건수를 함께 보십시오.
- 동시성을 올리는 실험은 다른 사용자의 워크로드에 영향을 줄 수 있습니다.
- 이 코드는 패턴을 설명하는 실습이지 특정 NAS 제품의 튜닝 가이드가 아닙니다.
