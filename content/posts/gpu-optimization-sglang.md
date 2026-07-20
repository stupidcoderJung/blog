---
title: "GPU Utilization 22%에서 70%까지 — SGLang 추론 최적화 실전기"
date: 2026-07-20
tags: ["GPU", "SGLang", "optimization", "VLM", "performance"]
series: ["GPU 최적화"]
---

## 문제 인식

VLM 추론 파이프라인을 운영하던 중, NVIDIA H200 GPU의 utilization이 22%에 머물러 있는 걸 발견했다. 분명 GPU는 비싼데, 실제로는 1/5도 못 쓰고 있었던 것.

## 병목 진단

원인이 하나가 아니었다. 순서대로 파고들었다:

**1. NAS I/O 병목**
이미지 파일을 NAS에서 읽어오는 과정에서 I/O 대기 시간이 전체 처리 시간의 상당 부분을 차지하고 있었다.

**2. 이미지 존재율 (ghost image)**
상품 이미지 중 상당수가 실제로 존재하지 않는 URL이었다. 존재하지 않는 이미지를 fetch하려다 timeout까지 기다리느라 GPU가 놀고 있었다.

**3. Worker imbalance**
여러 worker가 이미지를 처리하는 구조였는데, 일부 worker에만 작업이 몰리고 나머지는 idle 상태였다.

## 해결 과정

### SGLang Topology 튜닝
```
기본 설정:
  TP=1, DP=1

변경 후:
  TP=2, DP=4
```

Tensor Parallelism으로 모델을 2개 GPU에 분산시키고, Data Parallelism으로 요청을 4개 복제본에 분배했다.

### KV Cache 최적화

긴 컨텍스트에서 KV cache가 불필요하게 많은 메모리를 점유하고 있었다. target token 수를 조정하고, MTP speculative decoding을 활성화했다.

### max-running-requests 조정

동시 처리 요청 수를 GPU 메모리에 맞춰 최적화했다. 너무 적으면 GPU가 놀고, 너무 많으면 OOM이 발생한다.

## 결과

| 지표 | 최적화 전 | 최적화 후 |
|------|----------|----------|
| GPU Utilization | 22% | **70%** |
| 처리량 | baseline | **3.2x** |

## 교훈

1. GPU utilization이 낮을 때 병목은 GPU 자체가 아닌 경우가 대부분이다. I/O, 데이터 품질, worker 분배를 먼저 보라.
2. SGLang의 TP/DP 설정은 trial-and-error가 필요하다. 워크로드 특성에 맞게 직접 튜닝해야 한다.
3. 측정 없이 최적화하지 마라. `nvidia-smi`, SGLang metrics를 습관적으로 확인하자.
