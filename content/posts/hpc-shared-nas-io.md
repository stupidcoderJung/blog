---
title: "공유 NAS 위의 대규모 AI 파이프라인: 메타데이터 I/O부터 재시작 가능한 분산 쓰기까지"
date: 2026-07-20
description: "빠른 GPU와 InfiniBand급 네트워크를 갖춘 HPC에서도 작은 파일과 공유 상태가 처리량을 무너뜨리는 이유, 측정법, 비동기 I/O, 체크포인트, 로컬 캐시와 델타 동기화를 코드로 깊게 살펴본다."
tags:
  - HPC
  - NAS
  - distributed-systems
  - Python
  - performance
  - asyncio
---

# 공유 NAS 위의 대규모 AI 파이프라인

## 메타데이터 I/O부터 재시작 가능한 분산 쓰기까지

대규모 AI 배치 작업을 처음 설계할 때 우리의 시선은 자연스럽게 GPU로 향한다. 모델을 몇 장의 GPU에 나눌지, 요청 동시성을 얼마로 둘지, 배치 크기를 어떻게 고를지, 커널을 어떤 방식으로 최적화할지부터 생각한다. GPU가 비싸고 눈에 잘 보이는 자원이기 때문이다. 그런데 실제 운영에서 GPU 사용률이 낮을 때 원인이 언제나 GPU 안에 있는 것은 아니다. 모델 호출 직전에 작은 JSON 파일을 찾는 코드, 완료 ID를 한 줄씩 기록하는 코드, 여러 워커가 같은 상태 파일을 갱신하는 코드가 전체 처리량과 복구 가능성을 결정하는 경우가 적지 않다.

이 글은 그런 상황을 다룬다. 구체적으로는 여러 계산 노드가 하나의 공유 NAS를 보면서 수백만 개 이상의 작은 레코드를 처리하는 오프라인 AI 파이프라인을 생각한다. 각 레코드는 메타데이터, 하나 이상의 바이너리 입력, 추론 결과, 체크포인트로 구성된다. 계산 노드는 빠른 패브릭으로 연결되어 있고 GPU도 충분하다. 그럼에도 파이프라인은 시간이 갈수록 느려지거나, 동시성을 높여도 처리량이 늘지 않거나, 완료 기록이 손상되어 이미 처리한 일을 다시 수행한다.

핵심 주장은 다음과 같다.

> 공유 NAS 파이프라인의 성능과 정확성은 바이트 처리량만으로 설명되지 않는다. 경로 탐색, `stat`, `open`, 작은 append, 디렉터리 열거, 잠금, 체크포인트 게시처럼 “메타데이터와 상태를 다루는 방식”이 시스템의 상한을 결정한다.

이 글에서는 먼저 NAS를 로컬 디스크처럼 다루면 왜 문제가 되는지 비용 모델을 세운다. 이어서 직접 경로 접근과 디렉터리 탐색을 비교하는 벤치마크, `asyncio` 이벤트 루프를 막는 동기 파일 I/O, 작은 쓰기를 배치하는 방법, 워커별 저널과 완료 sentinel, 로컬 SQLite와 불변 델타 청크를 이용한 캐시 교환을 코드로 구현한다. 마지막에는 이 패턴을 하나의 재시작 가능한 참조 아키텍처로 결합하고, 실제 시스템에 단계적으로 적용하는 방법을 정리한다.

본문의 경험은 대규모 오프라인 AI 파이프라인을 운영하며 반복해서 관찰한 문제를 일반화한 것이다. 회사명, 서비스명, 스토리지 제품명, 내부 경로, 노드 이름, 작업 ID, 정확한 데이터 규모와 원시 처리량은 의도적으로 제거하거나 범주화했다. 예제 데이터와 벤치마크 코드는 이 글을 위해 새로 작성했으며 실제 업무 코드를 포함하지 않는다. 수치는 특정 제품의 보편적 성능을 주장하기 위한 것이 아니라, 독자가 자신의 환경에서 같은 질문을 검증하도록 돕기 위한 것이다.

---

## 목차

1. [GPU가 놀고 있는데 GPU 문제가 아니었다](#1-gpu가-놀고-있는데-gpu-문제가-아니었다)
2. [공유 NAS를 보는 두 개의 평면](#2-공유-nas를-보는-두-개의-평면)
3. [메타데이터 I/O 비용 모델](#3-메타데이터-io-비용-모델)
4. [측정 단위부터 고정한다](#4-측정-단위부터-고정한다)
5. [실습 1: 파일을 찾지 말고 주소를 계산하라](#5-실습-1-파일을-찾지-말고-주소를-계산하라)
6. [매니페스트는 성능 최적화이자 계약이다](#6-매니페스트는-성능-최적화이자-계약이다)
7. [비동기 함수 안의 동기 NAS I/O](#7-비동기-함수-안의-동기-nas-io)
8. [동시성은 RTT를 겹치되 부하를 제한해야 한다](#8-동시성은-rtt를-겹치되-부하를-제한해야-한다)
9. [작은 쓰기와 체크포인트의 증폭](#9-작은-쓰기와-체크포인트의-증폭)
10. [공유 append가 깨뜨리는 정확성](#10-공유-append가-깨뜨리는-정확성)
11. [워커별 저널과 완료 sentinel](#11-워커별-저널과-완료-sentinel)
12. [로컬 스크래치와 SQLite의 올바른 경계](#12-로컬-스크래치와-sqlite의-올바른-경계)
13. [불변 델타 청크로 상태를 교환한다](#13-불변-델타-청크로-상태를-교환한다)
14. [재시작과 멱등성은 별도 기능이 아니다](#14-재시작과-멱등성은-별도-기능이-아니다)
15. [샤딩의 목표는 건수 균등이 아니라 비용 균등이다](#15-샤딩의-목표는-건수-균등이-아니라-비용-균등이다)
16. [관측성과 장애 분류](#16-관측성과-장애-분류)
17. [효과를 증명하는 실험 설계](#17-효과를-증명하는-실험-설계)
18. [끝에서 끝까지 이어지는 참조 아키텍처](#18-끝에서-끝까지-이어지는-참조-아키텍처)
19. [단계적 마이그레이션 플레이북](#19-단계적-마이그레이션-플레이북)
20. [어디까지 일반화할 수 있는가](#20-어디까지-일반화할-수-있는가)
21. [마무리](#21-마무리)
22. [부록 A: 세 개의 디버깅 타임라인](#부록-a-세-개의-디버깅-타임라인)
23. [부록 B: 설계 결정 기록 템플릿](#부록-b-설계-결정-기록-템플릿)
24. [부록 C: 자주 묻는 질문](#부록-c-자주-묻는-질문)

---

## 1. GPU가 놀고 있는데 GPU 문제가 아니었다

문제의 시작은 흔하다. 여러 노드에 모델 서버와 파이프라인 워커를 띄웠는데 GPU 사용률이 기대보다 낮다. 초반에는 처리 속도가 좋아 보이지만 어느 시점부터 급격히 느려진다. 클라이언트 동시성을 두 배로 올려도 GPU 큐가 차지 않고, 오히려 타임아웃과 재시도만 증가한다. 워커별 처리량은 몇 배씩 벌어지고, 빠른 워커는 먼저 종료한 뒤 느린 워커를 오래 기다린다.

이 장면에서 가장 먼저 떠오르는 가설은 모델 서버의 설정이다.

- 텐서 병렬화 구성이 잘못되었을까?
- 요청 배치가 너무 작을까?
- KV 캐시가 부족할까?
- GPU 커널이 비효율적일까?
- 클라이언트 동시성이 서버의 실행 슬롯보다 작을까?

모두 합리적인 질문이다. 실제로 모델 서버 내부에 병목이 있을 수도 있다. 하지만 시스템을 구간별로 계측하면 다른 모습이 보인다. 한 레코드가 완료되기까지의 시간을 다음처럼 나누어 보자.

```text
레코드 선택
  → 메타데이터 경로 확인
  → 메타데이터 읽기
  → 입력 파일 탐색
  → 입력 파일 읽기와 전처리
  → 모델 서버 대기
  → 모델 추론
  → 결과 파일 쓰기
  → 완료 상태 기록
```

GPU가 실제로 관여하는 구간은 `모델 서버 대기 → 모델 추론`뿐이다. 앞단에서 입력을 충분히 공급하지 못하면 GPU는 할 일이 없다. 뒷단의 결과 쓰기와 체크포인트가 이벤트 루프를 막으면 다음 요청 생성도 지연된다. 다시 말해 GPU 사용률은 GPU 자체의 효율뿐 아니라 파이프라인 전체가 GPU에 일을 전달하는 속도의 함수다.

이를 아주 단순한 식으로 표현할 수 있다. 레코드 하나의 평균 서비스 시간을 다음처럼 둔다.

\[
T_{\text{record}}
= T_{\text{discover}}
+ T_{\text{metadata}}
+ T_{\text{payload}}
+ T_{\text{preprocess}}
+ T_{\text{queue}}
+ T_{\text{inference}}
+ T_{\text{persist}}
+ T_{\text{checkpoint}}
\]

직렬 처리라면 처리량은 대략 \(1/T_{\text{record}}\)이다. 비동기 파이프라인에서는 여러 구간을 겹칠 수 있지만, 각 단계의 유효 용량 중 가장 작은 값이 전체 처리량의 상한이 된다.

\[
X_{\text{pipeline}}
\le
\min(
X_{\text{scanner}},
X_{\text{reader}},
X_{\text{preprocessor}},
X_{\text{inference}},
X_{\text{writer}}
)
\]

모델 서버가 초당 100개의 요청을 처리할 수 있어도 스캐너가 초당 20개의 유효 입력밖에 만들지 못하면 전체 처리량은 20을 넘지 못한다. 이때 GPU 사용률을 높이려고 모델 서버의 배치 크기만 바꾸면 원인을 건드리지 못한다.

### 1.1 초반 처리량이 거짓말하는 이유

운영 로그에서 자주 보는 패턴이 있다. 작업 시작 직후에는 초당 처리 건수가 매우 높다가, 몇 분 뒤 안정 구간에서 크게 떨어진다. 초반의 빠른 구간은 종종 실제 추론이 아니라 이미 완료된 레코드를 건너뛰는 구간이다. 완료 ID를 메모리에 읽어 놓았다면 set 조회만으로 레코드가 끝난다. 반면 새로운 레코드가 등장하는 순간 NAS 읽기와 모델 호출이 시작된다.

따라서 시작부터 누적 평균으로 계산한 처리량은 의미가 약하다. 다음 세 구간을 분리해야 한다.

1. 준비 구간: 모델 로딩, 워밍업, 매니페스트 로딩, 캐시 복원
2. skip 구간: 기존 완료 항목, 캐시 hit, 비대상 레코드
3. steady-state 구간: 실제 NAS 읽기, 전처리, 추론, 결과 쓰기가 지속되는 구간

`완료 레코드/전체 경과 시간`만 보면 skip 비율이 높은 실행이 더 빠른 것처럼 보인다. 모델 성능을 비교하려면 실제 모델 miss만 세어야 하고, 파이프라인 성능을 비교하려면 skip을 포함하되 hit 비율을 함께 표시해야 한다.

### 1.2 네트워크가 빠르면 NAS도 로컬처럼 빠르다는 착각

HPC 환경의 계산 노드는 대개 빠른 네트워크 패브릭을 사용한다. 큰 파일을 순차적으로 읽거나 대규모 collective I/O를 수행할 때 이 대역폭은 큰 효과를 낸다. 그래서 “네트워크가 빠르니 작은 파일도 충분히 빠를 것”이라는 직관이 생긴다.

하지만 대역폭과 왕복 지연은 다른 축이다. 1GB 파일 하나를 읽는 작업은 높은 대역폭의 도움을 크게 받는다. 반면 1KB JSON 파일 100만 개를 서로 다른 디렉터리에서 찾고, 존재를 확인하고, 열고, 닫는 작업은 전송되는 바이트보다 메타데이터 연산 횟수와 왕복 지연의 영향을 더 받는다.

간단한 비교를 해 보자. 1KB 파일을 100만 개 읽으면 데이터 자체는 약 1GB다. 순차 1GB 읽기라면 빠른 스토리지에서 짧은 시간에 끝날 수 있다. 그러나 각 파일마다 평균 2ms의 메타데이터 지연만 추가되어도 지연을 완전히 직렬로 지불할 경우 2,000초가 더해진다. 데이터 크기는 같지만 접근 모양이 전혀 다르다.

```text
패턴 A: 1GB 파일 × 1개
  메타데이터 연산 수가 작고 전송 대역폭이 중요

패턴 B: 1KB 파일 × 1,000,000개
  메타데이터 연산 수가 크고 RTT, 캐시, 동시성이 중요
```

이 차이는 “NAS가 느리다”라는 뭉뚱그린 결론보다 훨씬 중요하다. NAS는 어떤 작업에는 빠르고 어떤 작업에는 느리다. 문제는 제품명이 아니라 워크로드의 I/O 모양이다.

### 1.3 첫 번째 반전: 파일을 읽는 시간이 아니라 찾는 시간이 길었다

대규모 검증 작업에서 흔한 코드는 레코드 디렉터리를 열거해 특정 확장자의 입력이 있는지 확인한다. 로컬 개발 환경에서는 자연스럽고 충분히 빠르다.

```python
for entry in os.scandir(record_dir):
    if entry.is_file() and entry.name.endswith((".jpg", ".png")):
        has_input = True
        break
```

그런데 레코드 디렉터리가 수십만 개 이상이고 각 디렉터리를 한 번씩만 방문한다면 디렉터리 캐시의 재사용률은 낮다. 입력 파일 목록이 이미 메타데이터 JSON 안에 있거나, 결과 파일의 존재가 입력이 있었음을 간접적으로 증명하는데도 모든 디렉터리를 열거하면 같은 사실을 두 번 확인한다.

이때 최적화의 핵심은 `scandir`를 더 빠르게 병렬화하는 것이 아니다. 먼저 질문 자체를 바꾸는 것이다.

> “이 디렉터리에 어떤 파일이 있는가?”를 묻지 않고도 필요한 경로를 계산하거나 이미 알고 있는가?

파일 이름과 샤딩 규칙을 안다면 직접 경로를 계산한다. 입력 목록이 메타데이터에 있다면 그 목록을 사용한다. 정상 경로에서 결과 파일이 거의 항상 존재한다면 `exists()`로 미리 확인하지 않고 곧바로 열어 본 뒤 `FileNotFoundError`를 처리한다. 자세한 디렉터리 검사는 예외 레코드에만 수행한다.

현장에서는 이 순서 변경만으로 디렉터리 열거 호출 대부분을 없애고 검증 작업 시간을 30%대 줄인 사례가 있었다. 중요한 것은 그 비율이 모든 환경에서 재현된다는 주장이 아니다. “이미 알고 있는 사실을 파일시스템에 다시 묻는 호출”이 전체 비용의 큰 부분이 될 수 있다는 점이다.

### 1.4 두 번째 반전: `async`였지만 직렬이었다

파이프라인 코드가 `async def`로 작성되어 있고 동시성 설정이 64나 128이라면 충분히 병렬로 보인다. 그러나 코루틴 안에서 `Path.exists()`, `open()`, `json.load()`, `mkdir()`, `json.dump()`를 직접 호출하면 그 시간 동안 이벤트 루프 스레드가 멈춘다.

```python
async def process(record):
    if record.result_path.exists():        # blocking stat
        return
    with record.meta_path.open() as file:  # blocking open/read
        meta = json.load(file)
    result = await call_model(meta)
    with record.result_path.open("w") as file:
        json.dump(result, file)             # blocking write
```

코루틴 100개를 만들어도 이벤트 루프에서 실행되는 동기 파일 I/O는 한 번에 하나씩 진행된다. 한 `stat`이 몇 밀리초, 한 `open+read`가 몇 밀리초라면 각 코루틴이 그 지연을 이벤트 루프 전체에 전파한다. 모델 서버 응답을 받은 다른 코루틴도 결과를 처리하지 못한다. 타임아웃 타이머와 재시도 스케줄링도 늦어진다.

문제는 “NAS가 느리다”와 “이벤트 루프를 막았다”가 결합한다는 데 있다. 동일한 동기 호출이 로컬 SSD에서 0.1ms라면 증상이 잘 보이지 않을 수 있다. 공유 파일시스템에서 5ms, 20ms의 꼬리 지연이 생기면 전체 비동기성이 무너진다.

### 1.5 세 번째 반전: 완료 기록이 결과보다 더 위험했다

분산 배치에서는 각 레코드 결과가 고유한 경로에 저장되므로 자연스럽게 단일 writer가 된다. 예를 들어 레코드 ID를 워커 수로 나누거나 사전 분배하면 같은 결과 파일을 두 워커가 동시에 쓰지 않는다. 이 부분은 안전할 수 있다.

그러나 “완료된 레코드 ID”를 한 파일에 append하는 순간 공유 writer가 생긴다.

```python
with open("done.txt", "a", encoding="utf-8") as file:
    file.write(record_id + "\n")
    file.flush()
```

여러 노드의 여러 프로세스가 같은 NAS 파일에 append하면 로컬 파일시스템에서 기대한 의미가 유지되지 않을 수 있다. Linux `open(2)` 문서는 `O_APPEND`가 로컬에서는 offset 이동과 쓰기를 하나의 원자 단계로 수행한다고 설명하면서도, NFS에서는 여러 프로세스의 동시 append를 클라이언트가 모사해야 해 경쟁 조건과 손상 가능성이 있다고 별도로 경고한다. 자세한 내용은 [`open(2)`의 O_APPEND 설명](https://man7.org/linux/man-pages/man2/open.2.html)을 참고할 수 있다.

운영에서 더 무서운 점은 추론 결과 파일은 정상인데 완료 목록만 손상될 수 있다는 것이다. 계산 비용을 들여 결과를 만들었지만 다음 실행이 그 사실을 모른다. 작업은 완료되지 않은 것으로 보이고, 막대한 재처리가 발생한다. 캐시 DB를 각 워커가 로컬로 복사한 뒤 종료 시 같은 원본 경로에 덮어쓰는 구조도 유사하다. 마지막 writer의 상태만 남고 다른 워커의 갱신은 조용히 사라진다.

이 세 번의 반전은 하나의 원칙으로 모인다.

> 공유 스토리지에서 읽기 경로의 비용과 쓰기 경로의 소유권을 명시적으로 설계하지 않으면, 계산 자원을 늘릴수록 성능과 정확성이 함께 나빠질 수 있다.

다음 장부터는 이 원칙을 모델로 만들고 코드로 검증한다.

---

## 2. 공유 NAS를 보는 두 개의 평면

공유 스토리지 설계를 명확하게 하려면 파일을 “데이터”라는 한 단어로 묶지 않는 편이 좋다. 대규모 파이프라인에는 성격이 다른 두 평면이 있다.

### 2.1 데이터 평면

데이터 평면은 실제 업무 산출물을 운반한다.

- 원본 이미지, 오디오, 텍스트와 같은 큰 입력
- 모델 가중치와 토크나이저
- 전처리된 텐서나 샤드 파일
- 추론 결과와 최종 데이터셋
- 대용량 checkpoint

이 평면에서는 바이트 처리량, 순차 접근, 블록 크기, 압축, 병렬 읽기, 캐시 효율이 중요하다. 파일 하나가 크고 접근 횟수가 상대적으로 적다면 빠른 네트워크와 스토리지 대역폭을 잘 활용할 수 있다.

### 2.2 제어 평면

제어 평면은 어떤 데이터를 언제 누가 처리했는지를 표현한다.

- ID 목록과 매니페스트
- 작업 할당과 샤딩 정보
- 완료 ID와 실패 ID
- `.done` sentinel
- 캐시 인덱스와 델타 목록
- 버전, 설정 fingerprint, 실행 메타데이터
- 재시작 위치와 cursor

제어 평면 파일은 작지만 자주 접근된다. 파일 크기가 작기 때문에 중요하지 않아 보이지만, 실제로는 메타데이터 연산과 동시성의 중심이다. 제어 평면이 손상되면 데이터 평면의 정상 결과도 발견되지 않거나 신뢰되지 않는다.

```text
                       ┌──────────────────────────┐
                       │       제어 평면           │
                       │ manifest / lease / done  │
                       │ cursor / version / hash  │
                       └────────────┬─────────────┘
                                    │ 무엇을 읽고 쓸지 결정
                                    ▼
┌─────────────┐     ┌──────────────────────────┐     ┌─────────────┐
│  큰 입력     │ ──▶ │       계산 파이프라인      │ ──▶ │  큰 결과     │
│  data plane │     │ scan → load → infer → save│     │ data plane  │
└─────────────┘     └──────────────────────────┘     └─────────────┘
```

좋은 설계는 두 평면을 구분할 뿐 아니라 서로 다른 규칙을 적용한다.

| 질문 | 데이터 평면 | 제어 평면 |
|---|---|---|
| 주요 비용 | 바이트 전송, 디코딩 | 메타데이터 RTT, 작은 쓰기 |
| 파일 형태 | 큰 불변 파일, 샤드 | 작은 매니페스트, 저널 |
| writer 모델 | 레코드/샤드별 단일 writer | 소유권이 명확한 단일 writer |
| 게시 방식 | 임시 파일 후 rename | 검증 가능한 atomic publish |
| 재시작 기준 | 결과 파일 또는 데이터 버전 | 완료 sentinel과 digest |
| 캐시 | page cache, 로컬 데이터 캐시 | 메모리 set, 로컬 인덱스 |

### 2.3 NAS는 저장소가 아니라 원격 상태 머신이다

애플리케이션에서 `open("/shared/a.json")`은 로컬 파일을 여는 것처럼 보인다. 이 추상화는 매우 유용하지만 비용과 실패 모드를 숨긴다. 네트워크 파일시스템에서는 경로 구성 요소 조회, 권한 확인, 속성 캐시 검증, 파일 핸들 획득, 데이터 읽기, close와 writeback이 클라이언트와 서버 사이의 프로토콜로 변환된다.

NFSv4.1 프로토콜 명세인 [RFC 8881](https://www.rfc-editor.org/rfc/rfc8881)에는 상태, 잠금, 파일·속성·디렉터리 캐싱과 일관성에 관한 긴 설명이 있다. 우리가 모든 프로토콜 세부를 알아야 한다는 뜻은 아니다. 다만 애플리케이션 호출 하나가 항상 로컬 시스템 콜 하나의 비용과 의미로 끝나지는 않는다는 사실을 설계의 출발점으로 삼아야 한다.

특히 다음 세 가지를 분리해서 생각해야 한다.

1. 이름 조회: 경로의 각 구성 요소를 파일 객체로 해석한다.
2. 속성 조회: 타입, 크기, 수정 시각, 권한 같은 메타데이터를 얻는다.
3. 데이터 접근: 실제 파일 내용을 전송한다.

`Path.exists()`는 파일 내용을 읽지 않지만 공짜가 아니다. `os.scandir()`는 파일 내용을 읽지 않지만 디렉터리 엔트리를 가져온다. `DirEntry.is_file()`은 플랫폼과 파일 타입 정보의 가용성에 따라 추가 시스템 호출이 필요할 수 있다. Python 공식 문서도 [`os.scandir`](https://docs.python.org/3/library/os.html#os.scandir)가 Unix에서 `opendir()`와 `readdir()`를 사용하며, `DirEntry` 메서드가 시스템 호출을 수행할 수 있다고 설명한다.

### 2.4 “작은 파일 문제”를 파일 개수로만 설명하면 부족하다

작은 파일이 많다는 사실은 출발점일 뿐이다. 같은 100만 개 파일도 다음 조건에 따라 결과가 다르다.

- 하나의 디렉터리에 몰려 있는가, 여러 단계로 샤딩되어 있는가
- 파일 경로를 이미 아는가, 디렉터리를 탐색해야 하는가
- 파일을 한 번 순차적으로 읽는가, 같은 파일을 반복해서 읽는가
- 모든 워커가 같은 디렉터리를 스캔하는가, 범위가 분리되어 있는가
- 파일이 불변인가, 실행 중 생성·삭제되는가
- 클라이언트의 attribute cache와 dentry cache가 얼마나 재사용되는가
- 읽기만 하는가, 작은 쓰기와 `fsync`가 섞이는가
- 정상 파일 비율과 missing 파일 비율이 얼마인가

따라서 “작은 파일이 많아서 느리다”는 진단은 행동으로 이어지기 어렵다. 다음처럼 호출 그래프로 바꾸어야 한다.

```text
레코드 1개를 처리할 때
  stat       몇 번?
  open/read  몇 번?
  opendir    몇 번?
  readdir    몇 번?
  mkdir      몇 번?
  append     몇 번?
  fsync      몇 번?

전체 실행에서
  위 호출 수 × 레코드 수 × 재시도 수 × 워커 수
```

이 그래프를 그리면 코드 한 줄이 얼마나 증폭되는지 보인다. 레코드마다 `exists()` 한 번은 작아 보인다. 하지만 천만 레코드, 두 단계, 재시도 한 번이면 수천만 회의 메타데이터 호출이 된다. 반대로 시작 시 매니페스트 하나를 메모리에 읽는 비용은 전체 실행에서 한 번만 지불한다.

### 2.5 공유는 읽기의 편의이지 쓰기의 자유가 아니다

공유 NAS의 큰 장점은 모든 노드가 같은 경로를 볼 수 있다는 것이다. 이 장점을 “모든 노드가 같은 파일에 자유롭게 써도 된다”로 확대하면 문제가 생긴다.

읽기 공유와 쓰기 공유는 별개다.

```text
좋은 공유:
  N readers → immutable input shard
  N readers → published manifest
  1 writer  → one result object

위험한 공유:
  N writers → one append log
  N writers → one SQLite database
  N writers → one mutable JSON manifest
  N writers → one "latest" symlink without ownership
```

안전한 기본값은 “공유 경로에 쓰되 파일 소유권은 분리한다”다. 워커 7은 `worker-00007/` 아래만 쓴다. 각 결과 객체는 하나의 레코드 owner만 쓴다. coordinator만 최종 매니페스트를 게시한다. 다른 워커는 불변으로 게시된 파일만 읽는다.

이 모델은 잠금을 더 정교하게 만드는 대신 잠금이 필요 없는 구조를 만든다. 분산 시스템에서 가장 다루기 쉬운 경쟁 조건은 존재하지 않는 경쟁 조건이다.

---

## 3. 메타데이터 I/O 비용 모델

정확한 NAS 지연은 제품, 프로토콜 버전, 마운트 옵션, 서버 부하, 클라이언트 캐시, 디렉터리 크기에 따라 다르다. 그러므로 고정된 숫자를 외우기보다 비용의 형태를 이해하는 것이 낫다.

레코드 하나를 처리하기 위한 메타데이터 시간을 다음처럼 근사하자.

\[
T_{\text{meta}}
\approx
N_{\text{lookup}}L_{\text{lookup}}
+ N_{\text{stat}}L_{\text{stat}}
+ N_{\text{open}}L_{\text{open}}
+ N_{\text{dir}}L_{\text{dir}}
+ N_{\text{sync}}L_{\text{sync}}
\]

여기서 \(N\)은 호출 횟수, \(L\)은 각 호출의 유효 지연이다. 유효 지연에는 서버 왕복뿐 아니라 클라이언트 캐시 hit/miss, 큐 대기, 재전송, 서버 내부 처리 시간이 포함된다.

전체 레코드 수가 \(R\), 재시도 배수가 \(A\), 워커 수가 \(W\)일 때 총 호출 수는 단순히 \(R\)에 비례하지 않는다. 모든 워커가 전체 목록을 스캔한 뒤 자기 몫만 고르는 구조라면 탐색 호출은 \(R \times W\)까지 증가할 수 있다.

\[
N_{\text{global scan}}
\approx R \times W \times A
\]

반대로 coordinator가 한 번 매니페스트를 만들고 각 워커에 범위를 전달하면 탐색은 \(R\), 워커의 직접 읽기는 자기 몫인 \(R/W\)가 된다.

### 3.1 RTT 지배 구간과 대역폭 지배 구간

I/O 시간을 매우 거칠게 다음처럼 볼 수 있다.

\[
T_{\text{io}}
\approx N_{\text{round trips}} \cdot RTT
+ \frac{B}{BW}
\]

\(B\)는 전송 바이트, \(BW\)는 유효 대역폭이다. 작은 파일과 메타데이터 작업에서는 첫 항이 지배하고, 큰 연속 파일에서는 둘째 항이 지배한다.

예를 들어 2KB JSON을 읽는 데 데이터 전송 자체는 매우 짧다. 그러나 경로 조회와 open, read 응답을 위해 여러 단계가 필요하고 캐시가 miss라면 RTT가 시간을 지배한다. 이때 압축률을 조금 높이는 최적화는 큰 효과가 없다. 호출 수를 줄이거나 여러 독립 호출을 제한된 동시성으로 겹치는 편이 낫다.

반대로 수GB 샤드 파일을 읽는 작업에서 `open` 한 번을 줄이는 것은 영향이 작다. 읽기 크기, readahead, 병렬 스트림, 압축 해제, NUMA 위치가 더 중요할 수 있다.

이 구분이 중요한 이유는 같은 “I/O 최적화”라도 방향이 반대일 수 있기 때문이다.

| 워크로드 | 주된 병목 | 우선 질문 |
|---|---|---|
| 작은 JSON 수백만 개 | RTT, metadata ops | 호출 수를 줄일 수 있는가 |
| 큰 샤드 수십 개 | bandwidth, decode | 더 큰 순차 읽기가 가능한가 |
| 작은 결과를 매번 append | write RTT, sync, contention | 배치하고 owner를 나눌 수 있는가 |
| 불변 모델 가중치 | 최초 읽기, page cache | 노드 로컬 staging이 가능한가 |
| 디렉터리 전수 탐색 | readdir, cache miss | 매니페스트로 대체할 수 있는가 |

### 3.2 check-then-act는 두 번 묻는다

다음 코드는 읽기 쉽고 로컬에서는 큰 문제가 없어 보인다.

```python
if result_path.exists():
    with result_path.open() as file:
        result = json.load(file)
```

하지만 파일이 대부분 존재하는 정상 경로라면 `exists()`의 속성 조회 후 `open()`을 다시 수행한다. 애플리케이션은 “있습니까?”라고 물은 뒤 “그럼 열어 주세요”라고 다시 묻는다. 두 호출 사이에 파일이 사라질 수 있으므로 정확성 면에서도 TOCTOU(time-of-check to time-of-use) 경쟁을 완전히 제거하지 못한다.

존재 확률이 높고 missing을 정상적으로 처리할 수 있다면 EAFP 패턴이 더 적합하다.

```python
try:
    with result_path.open() as file:
        result = json.load(file)
except FileNotFoundError:
    result = None
```

이것이 언제나 빠르다는 뜻은 아니다. 파일이 거의 항상 없다면 예외 생성 비용과 실패 open의 비용을 측정해야 한다. 핵심은 hit/miss 분포를 알고 정상 경로의 호출 수를 최소화하는 것이다.

### 3.3 디렉터리 열거의 증폭

`os.scandir()`는 `os.listdir()`보다 파일 타입 정보가 필요할 때 효율적인 API다. Python 문서가 설명하듯 `DirEntry`는 가능한 정보를 캐시하여 추가 `stat`을 줄인다. 따라서 로컬 파일시스템에서 `listdir + stat`보다 `scandir`가 좋은 선택인 경우가 많다.

그러나 “`scandir`가 `listdir`보다 빠르다”와 “알려진 파일 하나를 열기 위해 디렉터리를 열거하는 것이 좋다”는 다른 주장이다.

```text
질문 A: 이 디렉터리의 모든 파일을 분류해야 한다.
  scandir가 합리적일 수 있다.

질문 B: meta.json이라는 파일을 읽어야 한다.
  directory / "meta.json"을 직접 여는 편이 자연스럽다.
```

질문 B에서 `scandir`를 사용하면 필요한 한 이름뿐 아니라 디렉터리 엔트리 집합을 가져오고 반복한다. 파일 타입 검사가 추가 속성 조회를 유발할 수도 있다. 이것을 수백만 레코드에 적용하면 디렉터리 연산 수가 커진다.

### 3.4 작은 쓰기의 고정비

완료 ID 한 줄이 20바이트라고 해 보자. 100만 줄은 데이터 크기로 약 20MB다. 그러나 한 줄마다 `open → write → flush → close`를 수행하면 100만 번의 open과 write가 발생한다. 1,000줄을 메모리에 모아 한 번에 쓰면 같은 논리 데이터에 대해 약 1,000번의 쓰기로 줄어든다.

\[
\text{write amplification reduction}
\approx \frac{R}{\lceil R/B \rceil}
\approx B
\]

여기서 \(B\)는 flush batch 크기다. 배치 크기 100이면 고정비 호출 수가 대략 100분의 1이 된다. 물론 프로세스가 죽을 때 메모리 버퍼의 최대 \(B-1\)개 항목을 잃을 수 있다. 그러므로 배치 크기는 성능과 복구 지점 간의 계약이다.

이를 시간 기반 flush와 결합하면 상한을 명확히 할 수 있다.

```text
flush 조건:
  buffer_count >= 100
  OR now - last_flush >= 5 seconds
```

이 정책의 의미는 “정상 부하에서는 100개씩 쓰고, 저부하에서도 최대 5초 안에는 체크포인트를 게시한다”다. 배치 크기만 설정하면 저부하에서 버퍼가 오래 남을 수 있다.

### 3.5 동시성이 RTT를 숨기는 방법

독립적인 파일 읽기 지연이 \(L\), 동시 실행 수가 \(C\)라고 하자. 서버와 클라이언트가 충분한 용량을 갖고 있고 호출이 완전히 독립적이라면 이상적인 처리량은 대략 \(C/L\)까지 증가할 수 있다.

\[
X_{\text{io}} \approx \frac{C}{L}
\]

하지만 이 식은 \(C\)가 커질수록 무한히 빨라진다는 뜻이 아니다. 서버의 메타데이터 처리 용량, 클라이언트 스레드 수, 파일 디스크립터, 네트워크 큐, 작업당 메모리, 다른 사용자의 부하가 상한을 만든다. 동시성을 지나치게 높이면 평균보다 p95와 p99가 급격히 커지고 타임아웃과 재시도가 새로운 부하를 만든다.

실전에서는 다음 세 구간을 찾는다.

1. 동시성 증가에 비례해 처리량이 늘고 tail latency가 안정적인 구간
2. 처리량 증가가 둔화되지만 tail latency는 아직 관리 가능한 구간
3. 처리량은 그대로거나 감소하고 timeout, retry, queue가 급증하는 구간

목표는 3번 직전의 숫자를 찾는 것이 아니라, 장시간 실행과 다른 사용자 부하 변화까지 견디는 1번 후반 또는 2번 초반의 안정 지점을 고르는 것이다.

### 3.6 비용 모델의 목적

이 모델은 실제 시간을 정확히 예측하려는 것이 아니다. 코드 변경의 방향을 고르는 데 목적이 있다.

- `scandir`를 직접 경로 open으로 바꾸면 \(N_{\text{dir}}\)가 줄어든다.
- `exists + open`을 EAFP로 바꾸면 정상 hit에서 \(N_{\text{stat}}\)가 줄어든다.
- 매 레코드 append를 batch flush로 바꾸면 \(N_{\text{open}}\)과 \(N_{\text{sync}}\)가 줄어든다.
- 동기 I/O를 thread로 offload하면 호출 수는 같지만 이벤트 루프의 직렬 임계 구간에서 제거된다.
- 워커별 파일로 나누면 충돌과 잠금 대기는 줄지만 파일 수는 늘어난다.
- 매니페스트를 사용하면 반복 탐색을 한 번의 선행 스캔과 직접 접근으로 바꾼다.

좋은 최적화는 어떤 항을 줄였는지 설명할 수 있다. “스레드를 늘렸더니 빨라졌다”에서 멈추지 않고, “독립적인 메타데이터 RTT를 겹쳐 유휴 시간을 줄였으며, 동시성 32 이후에는 p95가 급증해 그 지점을 상한으로 정했다”라고 말할 수 있어야 한다.

---

## 4. 측정 단위부터 고정한다

성능 문제를 해결할 때 가장 먼저 필요한 것은 더 많은 로그가 아니라 같은 말을 같은 뜻으로 쓰는 일이다. `처리량`, `완료`, `요청`, `캐시 hit`, `I/O 시간`이 실행마다 다른 단위를 가리키면 숫자는 많아져도 결론을 검증할 수 없다.

대규모 이미지 파이프라인을 예로 들면 하나의 레코드가 여러 입력 파일을 포함하고, 입력 파일 하나가 여러 세그먼트로 나뉘며, 각 세그먼트가 모델 요청 하나가 될 수 있다.

```text
record 1개
  ├─ image A
  │    ├─ segment A-0 → request 1
  │    └─ segment A-1 → request 2
  └─ image B
       └─ segment B-0 → request 3
```

이때 `3 calls/s`와 `1 record/s`는 같은 실행을 표현할 수 있다. 평균 세그먼트 수가 바뀌면 calls/s가 올라가도 records/s는 내려갈 수 있다. 캐시 hit가 늘면 records/s는 올라가지만 실제 GPU request 수는 줄어 GPU 사용률이 내려갈 수 있다.

### 4.1 최소 지표 사전

본문과 실습에서는 다음 단위를 사용한다.

| 지표 | 정의 |
|---|---|
| candidate record | 스캐너가 검토한 레코드 |
| eligible record | 입력과 필수 메타데이터가 있어 처리 대상이 된 레코드 |
| completed record | 결과가 안전하게 게시되고 체크포인트에 반영된 레코드 |
| payload | 실제로 읽은 바이너리 입력 하나 |
| model request | 모델 서버에 보낸 API 호출 하나 |
| cache hit | 같은 cache key의 유효 결과를 재사용해 모델 요청을 생략한 경우 |
| records/s | 완료 레코드 수를 steady-state 경과 시간으로 나눈 값 |
| requests/s | 완료 모델 요청 수를 같은 구간으로 나눈 값 |
| metadata op | `stat`, open, directory listing, rename 등 이름·속성 중심 연산 |
| retry rate | 최초 시도를 제외한 재시도 수 / 최초 요청 수 |
| queue depth | 특정 단계 앞에서 대기 중인 항목 수 |
| event-loop gap | 주기적 heartbeat가 예정 시각보다 늦게 실행된 간격 |

`completed`의 정의가 특히 중요하다. 모델 응답을 받았다고 완료가 아니다. 결과가 영속 저장소에 게시되고 재시작 시 그 결과를 발견할 수 있어야 완료다. 반대로 체크포인트에 ID가 기록되었지만 결과 파일이 없으면 완료로 취급하면 안 된다.

### 4.2 누적 평균 대신 측정 창을 고정한다

누적 평균은 준비 시간, skip 구간, steady-state, 종료 merge를 모두 섞는다. 다음처럼 구간별 창을 둔다.

```text
T0 ── process start
T1 ── manifest loaded
T2 ── model warmup complete
T3 ── first real miss request
T4 ── steady-state sample start
T5 ── steady-state sample end
T6 ── last record persisted
T7 ── final checkpoint published
```

목적에 따라 분모가 달라진다.

- 사용자 관점 전체 소요 시간: \(T7 - T0\)
- 모델 포함 파이프라인 처리량: \(T5 - T4\)
- 워밍업 비용: \(T2 - T0\)
- 종료 정리 비용: \(T7 - T6\)
- 실제 모델 miss 처리량: T4~T5 사이 miss request 수

작업 A는 warmup이 길지만 steady-state가 빠를 수 있다. 작업 B는 warmup이 짧지만 종료 merge가 길 수 있다. 하나의 평균으로 합치면 무엇을 고쳐야 하는지 알 수 없다.

### 4.3 단계별 타이머는 합이 맞아야 한다

레코드 처리 함수 안에 타이머를 넣을 때 모든 구간을 빠짐없이 나누고 총 시간과 비교한다.

```python
from contextlib import contextmanager
from time import perf_counter

@contextmanager
def timed(metrics: dict[str, float], name: str):
    started = perf_counter()
    try:
        yield
    finally:
        metrics[name] = perf_counter() - started

def process(record):
    metrics: dict[str, float] = {}
    total_started = perf_counter()

    with timed(metrics, "metadata_read"):
        meta = read_meta(record)
    with timed(metrics, "payload_read"):
        raw = read_payload(record)
    with timed(metrics, "preprocess"):
        request = preprocess(raw, meta)
    with timed(metrics, "model"):
        response = call_model(request)
    with timed(metrics, "persist"):
        persist(record, response)

    metrics["total"] = perf_counter() - total_started
    metrics["unaccounted"] = metrics["total"] - sum(
        value for key, value in metrics.items()
        if key not in {"total", "unaccounted"}
    )
    return metrics
```

`unaccounted`가 크다면 queue 대기, semaphore 획득, task scheduling, 로그 출력처럼 타이머 밖의 비용이 있다는 뜻이다. 단계별 합이 총 시간보다 큰 경우에는 병렬 구간을 단순히 더했을 가능성이 있다. 여러 이미지 읽기를 동시에 수행했다면 각 이미지 latency의 합과 wall time은 다르다.

다음 두 값을 분리한다.

```text
work time: 각 하위 작업이 소비한 시간의 합
wall time: 사용자가 실제로 기다린 경과 시간
```

동시화의 목적은 work time을 없애는 것이 아니라 독립적인 대기를 겹쳐 wall time을 줄이는 데 있다.

### 4.4 평균만 보면 tail이 사라진다

NAS 지연은 일정하지 않다. 대부분의 `open`이 빠르더라도 일부 요청이 서버 부하, 캐시 miss, 네트워크 재전송, 디렉터리 상태 때문에 길어질 수 있다. 128개의 코루틴이 하나의 이벤트 루프를 공유하면 긴 동기 호출 하나가 다른 모든 코루틴에 영향을 준다.

최소한 다음 값을 함께 본다.

- count
- success와 failure
- mean
- p50
- p95
- p99 또는 max
- bytes
- queue wait

샘플 수가 작을 때 p99는 불안정하므로 max와 원시 분포를 함께 보아야 한다. percentile 계산 방식도 기록한다. 여러 워커의 percentile을 다시 평균내는 것은 전체 percentile과 같지 않다. 가능하면 histogram bucket을 합치거나 원시 샘플의 대표 구간을 중앙 수집한다.

### 4.5 호출 수를 로그로 세지 말고 가능한 곳에서 계수한다

파일 I/O 병목에서는 시간뿐 아니라 호출 수가 원인이다. 다음 카운터를 코드에 직접 둔다.

```python
from dataclasses import dataclass

@dataclass
class IoCounters:
    direct_open: int = 0
    missing_open: int = 0
    stat: int = 0
    scandir: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    checkpoint_flush: int = 0

    def emit(self) -> dict[str, int]:
        return self.__dict__.copy()
```

“메타데이터 최적화 후 빨라졌다”보다 “레코드당 directory listing이 1.0회에서 예외 레코드에만 발생하도록 줄었고, 전체 호출의 대부분이 제거되었다”가 더 강한 설명이다.

코드를 수정하기 어려운 탐색 단계에서는 시스템 도구를 사용할 수 있다.

```bash
# 테스트 프로세스의 파일 관련 시스템 호출 분포
strace -f -c -e trace=%file python nas_io_lab.py benchmark ...

# NFS 클라이언트 통계가 제공되는 환경
nfsstat -c

# 프로세스별 CPU, context switch, I/O 대기 관찰
pidstat -druw -p "$PID" 1
```

도구를 운영 전체에 무작정 적용하면 오버헤드가 생길 수 있다. 작은 canary나 단일 워커에서 먼저 사용한다. 또한 NFS 클라이언트 통계는 해당 호스트의 다른 작업을 포함할 수 있으므로 전후 차이를 보거나 격리된 측정 창을 사용한다.

### 4.6 캐시 조건을 기록한다

로컬 SSD와 NAS 모두 캐시의 영향을 받는다. 첫 실행은 경로와 데이터가 차갑고, 재실행은 page cache와 attribute cache가 따뜻할 수 있다. 다음 정보를 결과와 같이 남긴다.

- 같은 레코드를 반복했는지, 매번 다른 레코드를 읽었는지
- 실행 순서를 무작위화했는지
- 첫 round를 warmup으로 버렸는지
- 클라이언트가 같은 노드인지
- 입력 파일이 실행 사이에 변경되었는지
- 워커 수와 스레드 수
- 다른 대규모 작업의 동시 실행 여부

캐시를 완전히 비우는 실험은 공유 환경에서 위험하거나 권한이 없을 수 있다. 반드시 cold cache를 만들 필요도 없다. 운영이 주로 한 번만 방문하는 레코드라면 서로 다른 경로를 순회하는 방식으로 그 특성을 근사한다. 운영이 같은 데이터셋을 반복한다면 warm cache 성능도 중요한 결과다.

### 4.7 비교 표의 조건을 잠근다

좋은 ablation 표는 한 번에 하나만 바꾼다.

| 실험 | 경로 접근 | I/O 동시성 | 레코드 | cache 조건 | 측정 창 |
|---|---|---:|---:|---|---|
| A | `scandir` | 1 | 동일 목록 | round 1 | steady-state |
| B | direct open | 1 | 동일 목록 | round 1 | steady-state |
| C | direct open | 16 | 동일 목록 | round 2 | steady-state |
| D | manifest open | 16 | 동일 목록 | round 2 | steady-state |

`A → B`는 호출 수 변경의 효과를 본다. `B → C`는 RTT 겹치기의 효과를 본다. `C → D`는 경로 계산과 매니페스트 차이를 본다. direct open, 동시성, 모델 설정을 한꺼번에 바꾸면 최종 속도는 알 수 있어도 원인은 분리할 수 없다.

---

## 5. 실습 1: 파일을 찾지 말고 주소를 계산하라

첫 실습의 질문은 단순하다.

> 레코드 ID와 샤딩 규칙을 알고 있을 때 `meta.json`을 읽기 위해 디렉터리를 열거할 이유가 있는가?

실습 코드는 [`code/nas_io_lab.py`](code/nas_io_lab.py)에 있다. 운영 데이터와 무관한 합성 레코드를 만들며 Python 표준 라이브러리만 사용한다.

### 5.1 합성 디렉터리 구조

레코드 ID의 마지막 여섯 자리를 두 자리씩 사용해 세 단계로 분산한다.

```text
/tmp/nas-lab/
  manifest.tsv
  records/
    00/
      00/
        00/
          000000000000/
            meta.json
            image-00.bin
            image-01.bin
    01/
      00/
        00/
          000000000001/
            ...
```

경로 함수는 순수 함수다.

```python
def shard_path(root: Path, rid: str) -> Path:
    if len(rid) < 6:
        raise ValueError("record id must have at least six characters")
    return (
        root
        / "records"
        / rid[-2:]
        / rid[-4:-2]
        / rid[-6:-4]
        / rid
    )
```

같은 ID는 언제나 같은 경로가 된다. 디렉터리를 순회해 레코드를 찾을 필요가 없다.

### 5.2 네 가지 접근 방법

첫 번째는 직접 경로를 계산해 연다.

```python
def direct_open(root: Path, rid: str) -> dict:
    path = shard_path(root, rid) / "meta.json"
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)
```

두 번째는 존재 여부를 확인한 뒤 연다.

```python
def exists_then_open(root: Path, rid: str) -> dict:
    path = shard_path(root, rid) / "meta.json"
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)
```

세 번째는 디렉터리를 열거해 이름을 찾는다.

```python
def scandir_then_open(root: Path, rid: str) -> dict:
    directory = shard_path(root, rid)
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.name == "meta.json" and entry.is_file():
                with open(entry.path, encoding="utf-8") as file:
                    return json.load(file)
    raise FileNotFoundError(directory / "meta.json")
```

네 번째는 시작 시 읽은 매니페스트의 정확한 경로를 사용한다.

```python
manifest = {
    rid: Path(path)
    for rid, path in read_manifest("manifest.tsv")
}

def manifest_direct_open(rid: str) -> dict:
    with manifest[rid].open("r", encoding="utf-8") as file:
        return json.load(file)
```

네 방법은 같은 JSON을 반환한다. 차이는 파일시스템에 어떤 질문을 몇 번 하는가다.

### 5.3 실행

먼저 로컬 임시 디렉터리에 3,000개 레코드를 만든다.

```bash
cd code
python nas_io_lab.py prepare \
  --root /tmp/nas-lab \
  --records 3000

python nas_io_lab.py benchmark \
  --root /tmp/nas-lab \
  --records 3000 \
  --rounds 3
```

실제 테스트용 NAS 경로가 있다면 `--root`만 바꾼다. 운영 데이터 디렉터리에서 실행하면 안 된다. 합성 fixture도 파일과 디렉터리를 많이 만들므로 작은 수부터 시작하고 관리자 정책을 확인한다.

출력은 round마다 JSON 한 줄이다.

```json
{
  "event": "benchmark_result",
  "round": 1,
  "method": "direct_open",
  "samples": 3000,
  "failures": 0,
  "elapsed_seconds": 0.0,
  "operations_per_second": 0.0,
  "mean_ms": 0.0,
  "p50_ms": 0.0,
  "p95_ms": 0.0,
  "max_ms": 0.0
}
```

위 숫자는 형식만 보여 주기 위해 0으로 두었다. 독자는 자신의 결과를 사용해야 한다. 스크립트는 매 round마다 ID 순서를 섞고 메서드 실행 순서를 회전해 특정 메서드가 항상 차가운 캐시나 따뜻한 캐시를 독점하지 않도록 한다.

### 5.4 결과를 해석하는 순서

첫째, 실패가 0인지 본다. 빠르지만 다른 파일을 읽거나 일부 레코드를 누락한 구현은 비교 대상이 아니다.

둘째, `direct_open`과 `exists_then_open`을 비교한다. 모든 파일이 존재하는 fixture에서는 후자가 정상 경로에 속성 확인을 추가한다. 차이가 거의 없다면 클라이언트 캐시가 잘 작동하거나 로컬 파일시스템에서 측정했을 수 있다. 차이가 크다면 check-then-act 호출이 실제 비용으로 나타난 것이다.

셋째, `scandir_then_open`과 `direct_open`을 비교한다. 디렉터리 엔트리가 적어도 공유 파일시스템에서는 `opendir/readdir` 경로가 추가된다. 디렉터리에 파일을 더 많이 넣어 디렉터리 크기 민감도를 볼 수도 있다.

넷째, `manifest_direct_open`과 계산 경로를 비교한다. 이 fixture에서는 경로 계산이 매우 싸므로 둘이 비슷할 수 있다. 실제 시스템에서는 매니페스트가 단순 경로뿐 아니라 입력 유효성, 크기, 버전, 예상 비용을 포함해 더 큰 효과를 낸다.

다섯째, round 사이 변화 폭을 본다. 첫 round만 느리고 뒤가 빨라지면 cache warmup의 영향이 크다. 모든 round의 p95가 불안정하면 공유 스토리지의 다른 부하 또는 클라이언트 측 병목을 의심한다.

### 5.5 `scandir`를 금지하는 것이 목적은 아니다

디렉터리의 실제 구성 자체가 입력이라면 열거가 필요하다. 사용자가 업로드한 임의 파일을 수집하거나, 알 수 없는 확장자를 복구하거나, 외부 시스템이 생성한 파일을 발견하는 작업이 그렇다.

이 경우에도 hot path와 repair path를 분리할 수 있다.

```python
def load_record(record_dir: Path, meta: dict) -> list[Path]:
    known = [record_dir / item["filename"] for item in meta["inputs"]]
    missing = [path for path in known if not path.exists()]
    if not missing:
        return known
    return repair_by_scanning(record_dir, meta)
```

이 코드는 설명을 위한 중간 형태다. 정상 파일 비율이 높다면 `exists()`를 각 파일에 수행하는 것보다 직접 open 단계에서 missing을 수집하는 편이 나을 수 있다. 중요한 패턴은 비싼 탐색을 전체 레코드가 아니라 복구가 필요한 작은 집합으로 밀어내는 것이다.

```text
Before:
  모든 레코드 → directory scan → 정상/예외 분류

After:
  알려진 경로 직접 접근
       ├─ 성공 → hot path 완료
       └─ 실패 → 작은 repair queue → directory scan
```

repair queue의 크기를 계수하면 데이터 품질 문제도 보인다. 예외 비율이 갑자기 증가하면 upstream 파일명 규칙이나 다운로드 정책이 바뀌었을 가능성이 있다.

### 5.6 경로 계산의 함정

직접 경로 접근도 계약이 틀리면 실패한다.

- ID 정규화 방식이 생산자와 소비자에서 다르다.
- 확장자가 원본 메타데이터와 실제 저장 파일에서 다르다.
- 샤딩 규칙의 버전이 바뀌었다.
- 숫자 ID의 선행 0을 한쪽에서 제거했다.
- 대소문자 정규화가 파일시스템마다 다르다.
- URL 인코딩과 Unicode normalization이 다르다.

그래서 경로 함수를 여러 곳에 복사하지 않는다. 하나의 버전이 있는 모듈이나 매니페스트 생산 단계에서 정의하고, golden test를 둔다.

```python
def test_shard_path_contract():
    root = Path("/fixture")
    assert shard_path(root, "000000123456") == (
        root / "records" / "56" / "34" / "12" / "000000123456"
    )
```

정상 예제만으로 부족하다. 짧은 ID, 비숫자 ID, Unicode, 최대 길이, 잘못된 separator를 테스트한다. 경로 계산은 최적화인 동시에 데이터 주소 체계이므로 변경을 schema migration처럼 다루어야 한다.

---

## 6. 매니페스트는 성능 최적화이자 계약이다

매니페스트는 “이번 실행이 읽어야 할 객체 목록”을 한 번에 기술한 파일이다. 단순한 ID 목록일 수도 있고, 각 레코드의 경로와 버전, 크기, checksum, 예상 비용을 포함할 수도 있다.

```json
{
  "schema_version": 1,
  "dataset_version": "2026-07-public-example",
  "records": [
    {
      "record_id": "000000123456",
      "meta_path": "records/56/34/12/000000123456/meta.json",
      "payloads": [
        {
          "path": "records/56/34/12/000000123456/image-00.bin",
          "bytes": 1024,
          "sha256": "..."
        }
      ],
      "estimated_cost": 2
    }
  ]
}
```

실제 대규모 목록은 하나의 거대한 JSON 배열보다 JSONL, Parquet, CSV, 고정 크기 shard 같은 스트리밍 가능한 형식이 적합하다. 여기서는 개념을 보여 주기 위해 JSON을 사용한다.

### 6.1 반복 탐색을 한 번의 인덱싱으로 바꾼다

매 실행마다 모든 디렉터리를 순회하면 탐색 비용을 반복해서 지불한다. 매니페스트 생산자는 한 번 스캔하고 결과를 게시한다. 소비자는 이미 알려진 경로를 직접 연다.

\[
\text{without manifest}
\approx E \times R \times C_{\text{discover}}
\]

\[
\text{with manifest}
\approx R \times C_{\text{discover}}
+ E \times R \times C_{\text{direct}}
\]

\(E\)는 실행 횟수다. 같은 데이터셋을 여러 모델, 여러 설정, 여러 평가 작업이 재사용할수록 매니페스트의 이득이 커진다.

### 6.2 매니페스트는 재현성을 만든다

디렉터리를 실행 중에 직접 스캔하면 작업 시작과 끝 사이에 파일이 추가되거나 삭제될 수 있다. 어떤 워커는 새 파일을 보고 다른 워커는 보지 못할 수 있다. 실행 결과가 “그때 보였던 디렉터리 상태”에 의존한다.

불변 매니페스트를 입력으로 삼으면 대상 집합이 고정된다.

```text
run_id
  ├─ code revision
  ├─ configuration fingerprint
  ├─ input manifest digest
  ├─ model/prompt/preprocess version
  └─ output manifest digest
```

오류를 재현할 때 “같은 경로를 다시 스캔했다”가 아니라 “같은 매니페스트 digest를 사용했다”라고 말할 수 있다.

### 6.3 매니페스트의 게시도 원자적이어야 한다

producer가 매니페스트를 직접 최종 파일에 쓰는 동안 consumer가 읽으면 부분 파일을 볼 수 있다. 동일 파일시스템의 임시 파일에 완성본을 쓰고 `fsync`한 뒤 `os.replace()`로 게시한다.

```python
def atomic_write(path: Path, payload: bytes) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("wb") as file:
        file.write(payload)
        file.flush()
        os.fsync(file.fileno())
    os.replace(tmp, path)
```

`rename/replace` 의미와 내구성은 파일시스템과 장애 모델에 따라 확인해야 한다. 강한 내구성이 필요하면 부모 디렉터리도 `fsync`하고, 테스트 환경에서 전원 장애나 클라이언트 실패에 가까운 주입 테스트를 수행한다. “원자적으로 보인다”와 “서버의 영속 매체에 확정되었다”는 서로 다른 요구다.

### 6.4 content와 readiness를 분리한다

큰 매니페스트 파일과 작은 readiness pointer를 분리하면 게시가 단순해진다.

```text
manifests/
  manifest-<sha256>.jsonl       # immutable content
  READY.json                    # small pointer, atomically replaced
```

`READY.json`은 다음처럼 digest와 schema를 가리킨다.

```json
{
  "schema_version": 1,
  "manifest": "manifest-a91f....jsonl",
  "sha256": "a91f...",
  "records": 1000000
}
```

consumer는 `READY.json`을 직접 열고, 가리키는 불변 파일의 digest를 확인한 뒤 실행한다. producer는 기존 매니페스트를 수정하지 않는다. 새 버전은 새 content 파일과 새 pointer로 게시한다.

### 6.5 매니페스트에 무엇을 넣지 말아야 하는가

매니페스트가 모든 상태를 담는 mutable database가 되면 다시 경쟁 조건이 생긴다. 각 워커가 같은 JSON의 `status` 필드를 갱신하게 만들면 작은 변경마다 큰 파일을 다시 쓰고 충돌을 해결해야 한다.

입력 매니페스트는 불변 대상 집합에 집중한다. 실행 중 진행 상태는 워커별 저널에 둔다. 최종 coordinator가 저널을 검증한 뒤 새로운 결과 매니페스트를 만든다.

```text
immutable input manifest
        │
        ├─ worker 0 → private journal
        ├─ worker 1 → private journal
        ├─ worker 2 → private journal
        └─ worker N → private journal
                         │
                         ▼
                 validated final manifest
```

이 분리는 성능과 정확성을 동시에 개선한다. 읽기 많은 불변 객체는 캐시하기 쉽고, 쓰기 객체는 owner가 하나라 잠금이 필요 없다.

---

## 7. 비동기 함수 안의 동기 NAS I/O

`asyncio`는 파일 I/O를 자동으로 비동기로 바꾸지 않는다. `async def` 안에서 일반 `open()`을 호출하면 해당 호출이 끝날 때까지 이벤트 루프 스레드가 멈춘다. Python 공식 문서는 [`asyncio.to_thread()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread)를 이벤트 루프를 막을 수 있는 I/O-bound 함수를 별도 스레드에서 실행하는 방법으로 제공한다.

### 7.1 이벤트 루프가 막히는 모습을 그려 보기

세 개의 코루틴 A, B, C가 있다고 하자.

```text
시간 ─────────────────────────────────────────────▶

A: [NAS open 20ms][await model........][write 10ms]
B:                 실행 못 함
C:                 실행 못 함
loop heartbeat:     지연됨
```

A가 동기 `open`을 수행하는 20ms 동안 B와 C는 Python 코드를 실행하지 못한다. B의 모델 응답이 이미 도착했어도 callback 처리가 늦어진다. `asyncio.sleep` 기반 timeout도 정확한 시각에 깨어나지 못한다.

I/O를 스레드에 넘기면 이벤트 루프는 다른 작업을 진행할 수 있다.

```text
시간 ─────────────────────────────────────────────▶

I/O thread A: [NAS open 20ms]
event loop:   schedule B → schedule C → heartbeat → A callback
```

NAS 호출 자체의 latency는 사라지지 않는다. 다만 그 대기가 이벤트 루프 전체의 직렬 임계 구간에서 빠진다.

### 7.2 나쁜 코드와 최소 수정

```python
async def process_bad(record):
    with record.meta_path.open() as file:
        meta = json.load(file)

    response = await model_client.generate(meta)

    record.output_dir.mkdir(parents=True, exist_ok=True)
    with record.result_path.open("w") as file:
        json.dump(response, file)
```

동기 함수를 작게 분리한다.

```python
def read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)

def write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False)
        file.flush()
    os.replace(tmp, path)

async def process_good(record):
    meta = await asyncio.to_thread(read_json, record.meta_path)
    response = await model_client.generate(meta)
    await asyncio.to_thread(write_json_atomic, record.result_path, response)
```

이 패턴의 장점은 동기 함수가 독립적으로 테스트 가능하다는 점이다. `to_thread` 안에 큰 lambda를 넣기보다 I/O 경계를 이름 있는 함수로 만든다.

### 7.3 실습 2: heartbeat로 블로킹을 본다

[`code/async_pipeline.py`](code/async_pipeline.py)는 같은 레코드 집합을 두 모드로 처리한다.

- `blocking`: 코루틴에서 파일 읽기와 쓰기를 직접 수행
- `offloaded`: `asyncio.to_thread()`로 동일 함수를 실행

로컬 SSD에서도 차이를 재현할 수 있도록 선택적 지연을 주입한다.

```bash
python async_pipeline.py \
  --root /tmp/nas-lab \
  --records 1000 \
  --concurrency 64 \
  --io-threads 32 \
  --injected-io-latency-ms 3 \
  --service-latency-ms 2 \
  --compare
```

실제 NAS에서는 먼저 `--injected-io-latency-ms 0`으로 실행한다. 인위적 지연 결과를 실제 NAS 성능으로 보고하면 안 된다. 지연 주입은 이벤트 루프의 구조적 차이를 재현하는 교육 도구다.

heartbeat는 10ms마다 깨어나 실제 간격을 기록한다.

```python
async def heartbeat(stop: asyncio.Event, interval_seconds: float):
    gaps_ms = []
    previous = time.perf_counter()
    while not stop.is_set():
        await asyncio.sleep(interval_seconds)
        now = time.perf_counter()
        gaps_ms.append((now - previous) * 1000)
        previous = now
    return gaps_ms
```

blocking 모드에서는 여러 파일 호출이 이벤트 루프에서 연속 실행되므로 heartbeat 최대 간격이 커진다. offloaded 모드에서는 I/O 스레드가 대기하는 동안 heartbeat와 모델 callback이 실행된다.

비교할 값은 두 가지다.

1. `records_per_second`: 전체 처리량
2. `heartbeat_max_gap_ms`: 이벤트 루프 응답성

처리량만 좋아지고 heartbeat가 계속 크게 지연된다면 다른 동기 구간이 남아 있을 수 있다. 반대로 heartbeat는 좋아졌지만 처리량이 늘지 않으면 NAS 서버 또는 모델 서버가 이미 포화되었을 수 있다.

### 7.4 `to_thread`는 마법이 아니다

오프로딩에도 비용과 한계가 있다.

- 스레드 풀에 작업을 제출하고 결과를 전달하는 오버헤드가 있다.
- 스레드 수보다 많은 호출은 executor queue에서 기다린다.
- 한 번에 읽은 raw bytes가 많으면 메모리가 증가한다.
- Python 코드가 CPU-bound이고 GIL을 오래 잡으면 스레드가 병렬 계산을 보장하지 않는다.
- 같은 SQLite connection이나 mutable 객체를 여러 스레드가 건드리면 경쟁 조건이 생긴다.
- 취소된 코루틴이 이미 시작한 blocking 함수는 즉시 중단되지 않을 수 있다.

따라서 모든 작은 함수를 무조건 `to_thread`로 감싸지 않는다. I/O 경계를 적당한 크기로 묶는다.

```python
# 너무 잘게 나눈 예
exists = await asyncio.to_thread(path.exists)
text = await asyncio.to_thread(path.read_text)
value = json.loads(text)

# 정상 경로 호출 수와 경계를 함께 줄인 예
value = await asyncio.to_thread(read_json_eafp, path)
```

두 번째 함수는 open, read, parse, 예외 변환을 하나의 작업으로 처리한다. 스레드 제출 횟수도 줄고 check-then-act도 제거된다.

### 7.5 스레드 풀 크기는 명시적으로 측정한다

`asyncio.to_thread`는 기본 executor를 사용한다. Python 버전에 따라 `ThreadPoolExecutor` 기본 worker 수가 달라질 수 있다. 공식 [`concurrent.futures` 문서](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)는 현재 기본값과 버전별 변경 이력을 설명한다. 많은 CPU 코어를 가진 HPC 노드라도 기본 스레드 수가 NAS I/O 동시성 목표와 같다고 가정하면 안 된다.

명시적으로 설정할 수 있다.

```python
async def main():
    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor(
        max_workers=32,
        thread_name_prefix="nas-io",
    )
    loop.set_default_executor(executor)
    try:
        await run_pipeline()
    finally:
        executor.shutdown(wait=True)
```

스레드 수를 정할 때 CPU 코어 수만 보지 않는다. NAS I/O는 대기 시간이 많아 코어 수보다 많은 스레드가 유효할 수 있다. 반면 이미지 디코딩 같은 CPU 작업이 섞이면 너무 많은 스레드가 context switch와 메모리를 증가시킨다.

가능하면 풀을 역할별로 분리한다.

```text
metadata I/O pool  → open, stat, small JSON
payload I/O pool   → larger byte reads
CPU pool/process   → decode, parse, transform
event loop         → network client, queue, orchestration
```

풀 분리는 작은 메타데이터 호출이 큰 파일 읽기 뒤에서 오래 기다리거나, 캐시 hit 경로가 CPU decode 작업에 막히는 head-of-line blocking을 줄인다.

### 7.6 캐시 hit 이전에 비싼 일을 하지 않는다

비동기 문제와 캐시 문제는 자주 만난다. 콘텐츠 해시로 캐시를 조회하려면 원본 bytes를 읽고 hash를 계산해야 할 수 있다. 기존 함수가 읽기, hash, 이미지 decode를 한 번에 수행하면 cache hit에서도 decode 비용을 지불한다.

```python
def load_hash_and_decode(path):
    raw = path.read_bytes()
    digest = sha256(raw).hexdigest()
    image = decode(raw)                 # hit에서도 수행
    return digest, image
```

단계를 분리한다.

```python
def read_and_hash(path):
    raw = path.read_bytes()
    return sha256(raw).hexdigest(), raw

async def load_for_model(path, cache):
    digest, raw = await asyncio.to_thread(read_and_hash, path)
    cached = cache.get(digest)
    if cached is not None:
        return cached

    decoded = await asyncio.to_thread(decode, raw)
    return await call_model(decoded)
```

일반 원칙은 “가장 싼 판별을 먼저, 비싼 변환은 miss에서만”이다. 단, hash만으로 cache key를 만들면 안 된다. 모델 버전, 프롬프트 버전, 전처리 버전, 출력 schema 버전도 포함해야 결과 오염을 막을 수 있다.

```python
cache_key = sha256(
    b"\0".join(
        [
            content_digest.encode(),
            model_version.encode(),
            prompt_version.encode(),
            preprocess_version.encode(),
            schema_version.encode(),
        ]
    )
).hexdigest()
```

### 7.7 오프로딩 후에도 종료 의미를 설계한다

파이프라인이 취소되거나 제한 시간에 도달했을 때 다음 순서가 필요하다.

1. 새 레코드 수락을 중단한다.
2. in-flight 모델 요청을 정책에 따라 기다리거나 취소한다.
3. 완료된 결과의 I/O future를 기다린다.
4. 워커 저널 버퍼를 flush한다.
5. 완료 sentinel을 게시한다.
6. executor를 종료한다.

프로세스 종료 직전에 `executor.shutdown(wait=False)`로 빠져나오면 결과 파일 또는 체크포인트 쓰기가 끝나지 않을 수 있다. 반대로 무한정 기다리면 배치 스케줄러의 hard kill에 걸린다. 종료 유예 시간을 따로 예약하고 단계별 timeout을 둔다.

```python
async def graceful_shutdown(pipeline, journal):
    pipeline.stop_accepting()
    async with asyncio.timeout(30):
        await pipeline.drain_inflight()
    await asyncio.to_thread(journal.flush)
    await asyncio.to_thread(journal.mark_done)
```

`mark_done`은 모든 결과가 게시된 뒤에만 호출한다. sentinel은 성능 로그가 아니라 정확성 계약이다.

---

## 8. 동시성은 RTT를 겹치되 부하를 제한해야 한다

동기 I/O를 이벤트 루프에서 제거하면 다음 유혹은 동시성을 크게 올리는 것이다. 대기 시간이 많은 I/O에서는 어느 정도 맞는 방향이지만, 무제한 task 생성은 새로운 장애를 만든다.

```python
# 위험: 입력 크기만큼 task를 즉시 생성
await asyncio.gather(*(process(record) for record in all_records))
```

레코드가 수백만 개라면 task 객체 자체가 메모리를 차지한다. 각 task가 raw bytes를 읽고 모델 요청을 만들면 in-flight 메모리가 급증한다. NAS와 모델 서버의 queue가 동시에 커지고 timeout이 발생한다.

### 8.1 동시성, queue, 처리량의 관계

안정 상태에서 Little의 법칙을 직관적으로 적용하면 다음 관계를 생각할 수 있다.

\[
L = \lambda W
\]

\(L\)은 시스템 안에 머무는 평균 작업 수, \(\lambda\)는 처리량, \(W\)는 평균 체류 시간이다. 처리량이 고정된 상태에서 latency가 두 배가 되면 같은 흐름을 유지하기 위해 in-flight 작업도 두 배 필요하다. 반대로 in-flight를 무작정 늘렸는데 처리량이 늘지 않으면 체류 시간과 메모리만 증가한다.

파이프라인의 각 단계에 bounded queue를 두면 이 관계를 관찰하고 제어할 수 있다.

```text
manifest reader
  │ queue_scan(max=2,000)
  ▼
metadata loaders
  │ queue_ready(max=512)
  ▼
preprocessors
  │ queue_model(max=256)
  ▼
model clients
  │ queue_result(max=512)
  ▼
result writers + checkpoint
```

downstream이 느려지면 queue가 차고 upstream의 `put()`이 기다린다. 이것이 backpressure다. 메모리를 무한 queue로 사용하지 않고 병목을 생산자에게 전달한다.

### 8.2 단계별 제한을 분리한다

한 개의 전역 `concurrency=128`은 이해하기 쉽지만 어떤 자원을 제한하는지 모호하다.

```python
limits = {
    "metadata_io": asyncio.Semaphore(32),
    "payload_io": asyncio.Semaphore(16),
    "model_requests": asyncio.Semaphore(64),
    "result_writes": asyncio.Semaphore(16),
}
```

메타데이터 파일은 작고 RTT 지배이므로 상대적으로 높은 동시성이 유리할 수 있다. 큰 payload 읽기는 메모리와 대역폭을 사용하므로 더 낮은 제한이 필요할 수 있다. 모델 서버는 실제 실행 슬롯과 queue 정책에 맞추어야 한다. 결과 쓰기는 NAS writeback과 checkpoint 정책을 고려한다.

```python
async def load_metadata(record):
    async with limits["metadata_io"]:
        return await asyncio.to_thread(read_json, record.meta_path)

async def call_inference(request):
    async with limits["model_requests"]:
        return await model_client.generate(request)
```

semaphore 대기 시간을 별도로 측정한다.

```python
async def acquire_timed(semaphore, metrics, name):
    started = perf_counter()
    await semaphore.acquire()
    metrics[f"{name}_queue_wait"] += perf_counter() - started
```

`metadata_io` 대기가 길면 스레드 풀 또는 NAS가 포화되었을 수 있다. `model_requests` 대기가 길고 모델 서버 queue는 안정적이면 클라이언트 제한이 작을 수 있다. 모델 서버 queue도 길다면 제한을 늘리는 것이 답이 아니다.

### 8.3 worker 수와 task 동시성은 다른 축이다

분산 환경에서는 세 층의 동시성이 겹친다.

\[
C_{\text{total}}
= N_{\text{nodes}}
\times N_{\text{processes per node}}
\times C_{\text{tasks per process}}
\]

노드당 코루틴 64개가 적당해 보여도 노드 수가 32라면 클러스터 전체에서 2,048개의 I/O가 동시에 발생할 수 있다. NAS 서버 관점의 부하는 로컬 설정 하나가 아니라 전체 곱이다.

그러므로 canary 1노드 결과를 전체 규모로 단순 복제하지 않는다. 다음 순서로 확장한다.

1. 한 프로세스에서 동시성 sweep
2. 한 노드에서 프로세스 수 sweep
3. 소수 노드에서 전체 메타데이터 op/s 확인
4. 절반 규모에서 tail latency와 다른 사용자 영향 확인
5. 전체 규모에서 안정 구간 관찰

각 단계에서 처리량이 선형으로 늘지 않는 첫 지점을 찾는다. 노드 수를 두 배로 했는데 총 처리량이 1.2배만 늘고 p95가 세 배가 되면 공유 저장소 또는 모델 서비스의 포화 신호다.

### 8.4 queue가 비는 이유를 분류한다

모델 queue가 비어 있다는 사실만으로 “입력 I/O가 느리다”라고 결론 내릴 수 없다.

- 대상 레코드 자체가 적다.
- cache hit가 높아 모델 요청이 생기지 않는다.
- scanner가 느리다.
- payload read가 느리다.
- decode가 CPU를 점유한다.
- prompt 생성이 느리다.
- upstream 오류로 레코드가 탈락한다.
- model client가 semaphore에서 막힌다.

단계별 counter와 queue depth를 함께 본다.

```text
candidate/s = 1,000
eligible/s  =   650
cache_hit/s =   500
model_miss/s=   150
model_done/s=   148
persisted/s =   148
```

이 예에서 모델 queue가 작아도 비정상이 아닐 수 있다. eligible 650개 중 500개가 cache hit라 실제 모델 수요는 초당 150개다. GPU 사용률만 높이려고 cache를 끄는 것은 완료 처리량이라는 목적과 반대다.

### 8.5 retry는 숨은 동시성이다

timeout이 발생하면 같은 요청이 재시도된다. 원래 동시성 64라도 재시도 queue가 독립적으로 커지면 실제 부하는 더 높다.

\[
\text{effective offered load}
= \text{new requests} + \text{retries}
\]

포화 때문에 timeout이 나는데 즉시 재시도하면 포화를 더 심하게 만든다. exponential backoff와 jitter를 사용하고, retry budget을 전체 요청 대비 비율로 제한한다.

```python
async def with_retry(operation, attempts=4):
    for attempt in range(attempts):
        try:
            return await operation()
        except RetryableError:
            if attempt + 1 == attempts:
                raise
            base = min(30.0, 0.5 * (2 ** attempt))
            await asyncio.sleep(base * random.uniform(0.8, 1.2))
```

모든 오류를 재시도하지 않는다. 파일이 영구적으로 없거나 JSON schema가 잘못된 경우는 quarantine으로 보낸다. 모델 서버의 일시적 503과 잘못된 입력의 400을 같은 정책으로 다루면 쓸모없는 NAS 재읽기까지 반복한다.

### 8.6 적응형 동시성은 마지막 단계다

latency나 timeout을 보고 자동으로 동시성을 조절할 수 있다. 하지만 관측 지표가 불완전한 상태에서 적응형 제어를 넣으면 원인 분석이 더 어려워진다. 먼저 고정 동시성에서 안정 구간과 실패 모드를 이해한다.

적응형 정책을 사용한다면 목적 함수를 명시한다.

```text
증가 조건:
  p95 < target
  retry_rate < limit
  downstream_queue not saturated

감소 조건:
  p95 > redline
  OR retry_rate > limit
  OR memory > budget
```

GPU 사용률 하나만으로 조절하지 않는다. cache hit가 높아 GPU 사용률이 자연스럽게 낮은 상황에서 동시성을 계속 올릴 수 있기 때문이다.

---

## 9. 작은 쓰기와 체크포인트의 증폭

읽기 경로를 최적화한 뒤에도 처리량이 기대만큼 늘지 않는다면 결과와 체크포인트 쓰기를 살펴본다. 레코드 하나당 결과 JSON 하나를 쓰는 것은 산출물의 자연스러운 단위일 수 있다. 그러나 완료 ID, 통계, 캐시 상태를 레코드마다 별도 파일 또는 append로 쓰면 제어 평면이 쓰기 병목이 된다.

### 9.1 한 줄 append의 실제 비용

다음 코드는 논리적으로 매우 작다.

```python
with done_path.open("a", encoding="utf-8") as file:
    file.write(record_id + "\n")
```

하지만 반복문 안에 있다면 매 레코드마다 open과 close가 발생한다. `flush`나 `fsync`를 추가하면 더 강한 내구성을 얻는 대신 원격 write latency를 자주 지불한다.

```python
for record_id in completed:
    with done_path.open("a", encoding="utf-8") as file:
        file.write(record_id + "\n")
        file.flush()
        os.fsync(file.fileno())
```

이 코드는 처리량을 느리게 할 뿐 아니라 결과 저장과 체크포인트 순서를 잘못 구성하기 쉽다. 완료 ID를 먼저 기록한 뒤 결과 쓰기가 실패하면 재시작이 레코드를 건너뛴다.

### 9.2 메모리 버퍼와 batch flush

한 worker가 소유한 저널에 여러 이벤트를 묶어 쓴다.

```python
class BufferedJournal:
    def __init__(self, path: Path, flush_every: int = 100):
        self.path = path
        self.flush_every = flush_every
        self.buffer: list[str] = []

    def append(self, record_id: str) -> None:
        self.buffer.append(record_id)
        if len(self.buffer) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        payload = "".join(f"{rid}\n" for rid in self.buffer)
        with self.path.open("a", encoding="utf-8") as file:
            file.write(payload)
        self.buffer.clear()
```

이 코드는 시작점일 뿐이다. 실전에서는 partial write, checksum, sequence, 중복, process crash를 고려해야 한다. 하지만 호출 수를 줄이는 핵심은 보인다.

### 9.3 batch 크기는 복구 손실 상한이다

buffer 100개를 사용하고 durable flush 전에 프로세스가 죽으면 최대 99개의 완료 이벤트가 저널에 남지 않을 수 있다. 결과 객체는 이미 게시되었을 수도 있다.

이것을 무조건 데이터 유실이라고 부를 필요는 없다. 결과 파일을 source of truth로 복구할 수 있고 재실행이 멱등적이라면 저널 누락은 일부 재탐색 또는 재처리 비용이 된다. 반대로 결과 생성 비용이 매우 크고 재탐색도 어려우면 더 자주 flush해야 한다.

선택지를 표로 만들 수 있다.

| 정책 | 쓰기 비용 | crash 시 최근 상태 | 적합한 경우 |
|---|---:|---|---|
| 매 이벤트 `fsync` | 매우 높음 | 최소 손실 | 거래성 상태 |
| 100개마다 append | 낮음 | 최대 99개 미기록 | 재처리 가능한 배치 |
| 5초마다 append | 부하 의존 | 최대 5초 미기록 | 시간 기반 RPO |
| 종료 시 한 번 | 매우 낮음 | worker 전체 미기록 가능 | 결과 스캔 복구가 쉬움 |

RPO(recovery point objective)를 먼저 정하고 flush 정책을 고른다.

### 9.4 결과와 체크포인트의 순서

안전한 기본 순서는 다음과 같다.

```text
1. 결과를 임시 파일에 쓴다.
2. 결과 내용을 검증한다.
3. 결과를 최종 이름으로 atomic publish한다.
4. worker journal에 완료 이벤트를 추가한다.
5. 정책에 따라 journal을 flush한다.
```

체크포인트가 결과보다 앞서면 false positive completion이 생긴다. 결과가 먼저이고 체크포인트가 누락되면 false negative completion이 생기지만, 재시작 시 결과를 발견하거나 멱등 재계산으로 복구할 수 있다. 일반적인 배치에서는 false negative가 더 다루기 쉽다.

```python
async def persist_then_checkpoint(record, result, journal):
    await asyncio.to_thread(
        write_json_atomic,
        record.result_path,
        result,
    )
    journal.append(record.record_id)
```

결과 쓰기와 저널 append 사이에 프로세스가 죽는 창은 남는다. 정확히 한 번(exactly-once)을 파일 두 개만으로 완벽하게 구현하려 하면 분산 트랜잭션 문제가 된다. 대신 결과 게시를 멱등적으로 만들고, 재시작 시 결과 존재와 버전을 확인해 저널을 보정하는 방식이 실용적이다.

### 9.5 로그와 체크포인트를 분리한다

사람이 읽는 로그를 복구 상태로 사용하지 않는다.

```text
application.log
  목적: 진단, 시간순 사건, 오류 stack trace

worker journal
  목적: 기계적 복구, sequence, record ID, status

metrics
  목적: 집계, rate, latency distribution
```

로그 rotation이나 샘플링이 체크포인트 정확성에 영향을 주어서는 안 된다. 반대로 체크포인트에 긴 오류 stack trace를 넣어 작은 쓰기를 크게 만들 필요도 없다. 저널에는 오류 코드와 외부 blob 참조만 넣을 수 있다.

### 9.6 결과 파일 수 자체가 문제가 될 때

레코드별 결과 파일은 owner를 나누기 쉽고 재처리 범위가 작다는 장점이 있다. 그러나 수천만 개 작은 결과는 후속 탐색과 inode/metadata 용량에 부담을 줄 수 있다. 이 경우 shard 단위 writer가 여러 결과를 묶는다.

```text
worker-00007/
  part-000000.jsonl
  part-000001.jsonl
  part-000002.jsonl
  index.json
```

단, 묶음 파일은 실패 격리와 재시작 복잡도를 높인다. 한 shard를 쓰다 죽으면 마지막 부분을 어떻게 검증할지, 중복 레코드를 어떻게 제거할지 설계해야 한다. immutable part + final index 패턴이 유용하다.

```text
write part-N.tmp
→ validate count/checksum
→ rename part-N.jsonl
→ append part metadata to private index buffer
→ publish final index at worker completion
```

무조건 파일 수를 줄이는 것이 답은 아니다. 후속 소비 형태, 재처리 비용, 파일시스템의 metadata 용량을 함께 본다.

---

## 10. 공유 append가 깨뜨리는 정확성

성능 문제는 느리면 눈에 띈다. 동시 쓰기 문제는 조용히 성공한 것처럼 보일 수 있어 더 위험하다. 여러 worker가 같은 파일에 쓸 때 확인해야 할 것은 “테스트에서 몇 번 잘 됐다”가 아니라 파일시스템과 프로토콜이 요구하는 의미를 보장하는가다.

### 10.1 로컬 POSIX 직관을 원격 파일에 그대로 적용하지 않는다

POSIX의 `O_APPEND`는 각 `write()` 전에 파일 offset을 끝으로 이동시키는 의미를 정의한다. 로컬 Linux 파일시스템에서는 offset 이동과 쓰기가 원자 단계로 구현된다. 그러나 앞서 본 Linux `open(2)` 문서는 NFS가 native append를 지원하지 않아 클라이언트가 이를 모사하고, 여러 프로세스의 동시 append가 경쟁 조건을 만들 수 있다고 명시한다.

여기에 Python buffered I/O가 더해진다.

```python
file.write(record_id + "\n")
file.flush()
```

애플리케이션의 한 `write` 호출이 커널의 정확히 한 `write(2)`와 항상 같은 경계라는 가정도 조심해야 한다. encoding, buffering, 큰 문자열, 예외가 경계를 바꿀 수 있다.

따라서 “한 줄이 작으니 atomic일 것”이라는 추측에 진행 상태의 정확성을 맡기지 않는다.

### 10.2 손상은 interleaving만이 아니다

공유 append의 실패를 두 줄이 섞이는 경우로만 생각하기 쉽다.

```text
원래 기대:
A123\n
B456\n

가능한 손상:
A1B423
56\n\n
```

운영에서는 다음 형태도 고려해야 한다.

- 일부 write가 누락된다.
- 빈 구간 또는 예기치 않은 NUL bytes가 생긴다.
- 한 writer의 buffered 내용이 늦게 반영된다.
- client crash 후 close되지 않은 데이터가 사라진다.
- reader가 writer의 중간 상태를 본다.
- 마지막 writer가 전체 snapshot을 덮어써 다른 writer 갱신이 사라진다.

특히 “각 worker가 시작 시 master DB를 로컬로 복사하고 종료 시 원본에 덮어쓴다”는 패턴은 파일 corruption 없이도 논리적 유실을 만든다.

```text
T0: master = {a}
T1: worker 0 local = {a}
T1: worker 1 local = {a}
T2: worker 0 adds b → {a,b}
T2: worker 1 adds c → {a,c}
T3: worker 0 copies back → master {a,b}
T4: worker 1 copies back → master {a,c}

결과: b가 조용히 사라짐
```

이것은 last-writer-wins다. 파일은 정상 SQLite일 수 있지만 전체 계산의 상태는 틀렸다.

### 10.3 잠금으로 해결할 수 있는가

이론적으로는 분산 잠금, 파일 lock, coordinator service를 둘 수 있다. 실제로도 적절한 프로토콜과 검증된 구현을 사용하면 shared writer를 직렬화할 수 있다. 그러나 다음 비용이 생긴다.

- lock 획득마다 네트워크 왕복
- lock holder 장애와 lease 만료 처리
- stale lock 복구
- lock convoy와 head-of-line blocking
- 파일시스템별 잠금 의미 차이
- 운영 중 프로토콜 버전 변경

완료 ID처럼 자연스럽게 분할 가능한 데이터라면 잠금보다 ownership 분리가 단순하다.

```text
worker 0 → done/worker-00000/events.jsonl
worker 1 → done/worker-00001/events.jsonl
worker 2 → done/worker-00002/events.jsonl
```

각 파일은 한 writer만 갖는다. 읽는 쪽은 완료 sentinel이 있는 worker만 검증해서 merge한다.

### 10.4 “한 writer”를 코드가 아니라 데이터 모델에 넣는다

팀 문서에 “동시에 실행하지 마세요”라고 적는 것만으로 부족하다. 경로에 owner를 포함하고, 다른 owner가 쓰려 하면 실패하게 만든다.

```python
def worker_output(root: Path, worker_id: int) -> Path:
    return root / f"worker-{worker_id:05d}"
```

실행 metadata에 worker ID와 총 worker 수를 넣는다. resume 시 같은 worker namespace를 재사용할지 새로운 attempt namespace를 만들지 정한다.

```text
run-<run-id>/
  attempt-0001/
    worker-00000/
    worker-00001/
  attempt-0002/
    worker-00000/
    worker-00001/
```

attempt를 분리하면 이전 실패 실행의 부분 파일과 새 실행이 섞이지 않는다. 최종 coordinator가 어떤 attempt의 어떤 worker 결과를 채택했는지 명시한다.

### 10.5 공유 쓰기 테스트는 crash를 포함해야 한다

happy path에서 4개 프로세스가 1,000줄씩 쓴 뒤 줄 수를 확인하는 테스트만으로 부족하다.

- write 중 프로세스 kill
- flush 전 kill
- rename 직전과 직후 kill
- 네트워크 일시 중단
- reader가 publish 중 읽기
- worker 0이 가장 먼저 끝나는 경우
- worker 하나가 영원히 sentinel을 만들지 않는 경우
- 같은 worker ID가 중복 실행되는 경우

정확성 판정은 “프로세스가 0으로 종료했다”가 아니다.

```text
expected record set
  == validated result set
  == merged checkpoint set

and

no duplicate ownership
no unreferenced final objects
no sentinel for incomplete output
```

---

## 11. 워커별 저널과 완료 sentinel

이제 공유 append를 대체하는 프로토콜을 구현한다. 전체 코드는 [`code/checkpoint_protocol.py`](code/checkpoint_protocol.py)에 있다.

### 11.1 디렉터리 레이아웃

```text
checkpoint-root/
  worker-00000/
    events.jsonl
    DONE.json
  worker-00001/
    events.jsonl
    DONE.json
  completed-manifest.json
```

규칙은 간단하다.

1. worker는 자기 디렉터리만 쓴다.
2. `events.jsonl`은 sequence가 연속인 private append log다.
3. worker는 모든 결과 쓰기와 journal flush가 끝난 뒤 `DONE.json`을 게시한다.
4. coordinator는 `DONE.json`이 있는 worker만 검증한다.
5. 검증이 끝난 뒤 `completed-manifest.json`을 atomic publish한다.

### 11.2 이벤트 schema

```json
{
  "sequence": 41,
  "record_id": "record-00000040",
  "status": "ok",
  "worker_id": 3
}
```

sequence는 worker 내부에서 1부터 단조 증가한다. coordinator는 누락과 중복을 탐지할 수 있다. record ID와 status 외의 큰 payload는 결과 객체에 두고 저널에는 참조만 둔다.

### 11.3 단일 append 호출로 batch를 쓴다

실습의 `append_bytes`는 `O_APPEND`로 파일을 열지만 이 파일에는 owner worker 하나만 쓴다.

```python
def append_bytes(path: Path, payload: bytes, durable=False):
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_APPEND,
        0o644,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        if durable:
            os.fsync(descriptor)
    finally:
        os.close(descriptor)
```

`os.write`가 short write를 반환할 수 있으므로 남은 view를 반복한다. 여러 writer의 atomicity에 기대지 않고 single writer invariant에 의존한다.

저널은 raw line과 digest를 함께 관리한다.

```python
def flush(self):
    if not self._buffer:
        return
    payload = b"".join(self._buffer)
    append_bytes(self.journal_path, payload)
    for raw in self._buffer:
        self._digest.update(raw)
        self._events += 1
    self._buffer.clear()
```

### 11.4 sentinel은 증거를 담는다

`DONE.json`은 빈 파일이어도 readiness를 표현할 수 있지만, 검증 정보를 담으면 부분·손상 저널을 탐지할 수 있다.

```json
{
  "worker_id": 3,
  "events": 250000,
  "sha256": "8a0d...",
  "last_sequence": 250000,
  "journal_path": "events.jsonl"
}
```

worker는 buffer flush 후 이 파일을 임시 이름으로 완성하고 `os.replace`로 게시한다.

```python
def mark_done(self):
    self.flush()
    summary = {
        "worker_id": self.worker_id,
        "events": self._events,
        "sha256": self._digest.hexdigest(),
        "last_sequence": self._sequence,
        "journal_path": self.journal_path.name,
    }
    atomic_write_bytes(
        self.done_path,
        (json.dumps(summary) + "\n").encode(),
    )
```

sentinel이 있다는 사실만 믿지 않는다. coordinator는 실제 journal을 다시 읽어 count, digest, sequence를 비교한다.

### 11.5 파일 존재와 완료는 다른 상태다

worker가 `events.jsonl`을 시작 시 생성하면 빈 파일도 존재한다. coordinator가 `exists(events.jsonl)`를 완료 판정으로 사용하면 먼저 끝난 worker가 merge를 시작할 때 다른 worker의 부분 파일을 채택할 수 있다.

상태를 명시적으로 구분한다.

```text
ABSENT:
  worker가 시작하지 않음

IN_PROGRESS:
  journal 존재, DONE 없음

COMPLETE:
  DONE 존재, journal 검증 성공

CORRUPT:
  DONE 존재, digest/count/sequence 불일치
```

이 네 상태를 로그와 대시보드에 그대로 사용하면 “파일은 있는데 왜 merge하지 않나” 같은 혼동도 줄어든다.

### 11.6 coordinator는 알려진 경로를 직접 확인한다

worker 수를 알고 있다면 hot path에서 `glob("worker-*/DONE.json")`로 발견할 필요가 없다.

```python
for worker_id in range(num_workers):
    done_path = (
        root
        / f"worker-{worker_id:05d}"
        / "DONE.json"
    )
    try:
        validate_completed_journal(root, worker_id)
    except FileNotFoundError:
        missing.append(worker_id)
```

제어 단계의 작은 glob 한 번이 항상 문제인 것은 아니다. 그러나 worker 목록이 계약에 이미 있는데 다시 디렉터리에서 추론할 이유가 없다. 명시적 목록은 예상하지 못한 stale worker 디렉터리를 실수로 포함하는 것도 막는다.

### 11.7 strict merge와 partial merge

모든 worker가 필요하면 하나라도 missing일 때 실패한다.

```python
if require_all and missing:
    raise RuntimeError(
        f"workers without valid completion sentinel: {missing}"
    )
```

일부 결과도 가치가 있는 분석 작업이라면 partial manifest를 게시할 수 있다. 이 경우 `missing_workers`를 숨기지 않는다.

```json
{
  "workers": [...],
  "missing_workers": [7],
  "completed_record_count": 875000,
  "status": "partial"
}
```

downstream은 partial을 허용하는지 명시적으로 선택한다. 파일이 존재한다는 이유로 완전한 데이터셋처럼 읽으면 안 된다.

### 11.8 실행과 테스트

```bash
python checkpoint_protocol.py demo \
  --root /tmp/checkpoint-lab \
  --workers 4 \
  --records 1000
```

불완전 worker를 주입한다.

```bash
python checkpoint_protocol.py demo \
  --root /tmp/checkpoint-lab-partial \
  --workers 4 \
  --records 1000 \
  --incomplete-worker 2
```

테스트는 완료된 worker만 merge되는지, sentinel 이후 journal을 변조하면 검증이 실패하는지 확인한다.

```python
def test_tampering_is_detected():
    journal = WorkerJournal(root, 0)
    journal.append("record-a")
    summary = journal.mark_done()

    path = root / "worker-00000" / summary.journal_path
    with path.open("a") as file:
        file.write('{"sequence":2,...}\n')

    with pytest.raises(ValueError):
        validate_completed_journal(root, 0)
```

실제 코드는 `unittest`를 사용하며 생략 없이 실행할 수 있다.

### 11.9 중복 worker ID를 막는다

두 프로세스가 같은 worker ID로 시작하면 single writer invariant가 깨진다. 배치 스케줄러 설정 오류, 재시작 wrapper, 수동 실행이 원인이 될 수 있다.

선택지는 다음과 같다.

- attempt마다 scheduler가 유일한 worker ID를 보장한다.
- 시작 시 owner lease를 별도 서비스에서 획득한다.
- `O_CREAT|O_EXCL` 기반 claim 파일을 쓰되 대상 파일시스템 의미를 검증한다.
- worker 디렉터리에 host, PID, start token을 기록하고 충돌 시 fail closed한다.

NAS의 lock 의미가 확실하지 않다면 claim 파일만으로 분산 lease를 직접 발명하지 않는 편이 좋다. 스케줄러가 이미 task ID의 유일성을 보장한다면 그 제어 평면을 활용한다.

### 11.10 저널이 결과의 유일한 진실인가

저널은 빠른 resume index로 유용하지만 결과 객체와 어긋날 수 있다. 최종 검증에서 다음 불변식을 확인한다.

```text
모든 journal ok event에 대응하는 결과 객체가 있다.
결과의 record_id와 journal record_id가 같다.
결과의 configuration fingerprint가 현재 run과 같다.
결과 checksum 또는 parse 검증이 성공한다.
```

결과 객체를 모두 `stat`하면 다시 메타데이터 비용이 커진다. 이 검증은 worker가 결과를 쓸 때 digest와 bytes를 journal 이벤트에 기록하거나, worker별 result index를 함께 만드는 방식으로 최적화할 수 있다.

```json
{
  "sequence": 41,
  "record_id": "record-00000040",
  "status": "ok",
  "result_path": "parts/part-0003.jsonl",
  "result_offset": 182991,
  "result_sha256": "..."
}
```

성능을 위해 검증을 없애는 대신, 쓰는 순간 만들어진 증거를 작은 제어 정보로 남긴다.

---

## 12. 로컬 스크래치와 SQLite의 올바른 경계

공유 NAS는 노드 간 교환과 장기 보존에 적합하다. 매 요청의 임시 상태, 잦은 transaction, decode 중간 결과까지 모두 공유 경로에 둘 필요는 없다. HPC 노드의 로컬 SSD나 임시 디렉터리를 작업 공간으로 사용하면 작은 I/O latency와 잠금 경합을 줄일 수 있다.

기본 생명주기는 세 단계다.

```text
stage in:
  shared immutable snapshot → worker-local scratch

work:
  local reads/writes/transactions

stage out:
  validated delta/result → shared immutable object
```

### 12.1 로컬이 빠르다는 이유만으로 복구를 잊지 않는다

로컬 scratch는 빠르지만 ephemeral할 수 있다.

- 노드가 교체되면 사라진다.
- batch 종료 후 정리될 수 있다.
- 같은 노드에서 다음 attempt가 이전 파일을 볼 수 있다.
- 디스크 용량이 worker마다 다를 수 있다.
- 작업이 비정상 종료되면 stale WAL과 temp가 남는다.

따라서 로컬 상태를 진실의 유일한 원본으로 두지 않는다. 로컬은 재구성 가능한 cache 또는 아직 게시되지 않은 작업 공간이어야 한다.

```text
shared input + published deltas
  ──rebuild──▶ local state
```

재시작할 때 “로컬 파일이 있으니 그대로 사용”하면 이전 attempt의 손상 파일을 재사용할 수 있다. run fingerprint가 다르거나 integrity check가 실패하면 삭제하고 다시 만든다.

```python
def prepare_local_db(local_path, source_snapshot, fingerprint):
    remove_stale_sidecars(local_path)   # db, -wal, -shm, temp
    if source_snapshot is not None:
        shutil.copy2(source_snapshot, local_path)
    connection = sqlite3.connect(local_path)
    status = connection.execute("PRAGMA integrity_check").fetchone()[0]
    if status != "ok":
        connection.close()
        local_path.unlink(missing_ok=True)
        connection = create_empty_db(local_path, fingerprint)
    return connection
```

실제 구현에서는 무조건 source를 덮어쓰기 전에 남은 로컬 파일을 quarantine할지 삭제할지 정책을 정한다. 진단이 필요한 장애라면 경로와 크기, checksum을 기록하고 별도 제한된 보존 영역으로 이동한다.

### 12.2 SQLite를 공유 NAS에서 직접 열지 않는다

SQLite는 한 파일 안에서 transaction과 index를 제공하는 훌륭한 embedded database다. 하지만 database engine은 애플리케이션 프로세스 안에서 실행되므로 DB 파일이 네트워크 파일시스템에 있으면 locking, journal, page read/write가 네트워크를 건넌다.

SQLite 공식 문서인 [SQLite Over a Network, Caveats and Considerations](https://www.sqlite.org/useovernet.html)는 여러 시스템이 network filesystem 위의 같은 SQLite 파일을 직접 여는 구성을 일반적으로 권장하지 않는다. 네트워크 지연뿐 아니라 filesystem별 sync와 locking 신뢰성 차이를 강조한다.

피해야 할 구조는 다음과 같다.

```python
# 여러 노드가 같은 경로를 직접 여는 구조
connection = sqlite3.connect(
    "/shared/cache/master.sqlite"
)
```

`PRAGMA journal_mode=WAL`을 켠다고 원격 multi-writer가 자동으로 안전하고 빠르게 되는 것은 아니다. WAL은 DB 파일 옆의 `-wal`, `-shm`과 잠금 의미에 의존한다. 해당 파일시스템과 배포 토폴로지에서 공식 지원과 검증이 없다면 local-only로 제한한다.

### 12.3 세 계층으로 역할을 나눈다

실용적인 cache 구조는 다음과 같다.

```text
L1: process memory dict
  가장 빠른 hit, process crash 시 소멸

L2: worker-local SQLite
  로컬 재시작, index, transaction

L3: shared immutable delta chunks
  worker 간 교환, 장기 복구 재료
```

L1은 hot lookup을 담당한다. 모든 cache key가 메모리에 들어갈 수 없으면 작은 index 또는 최근 사용 집합만 둔다. L2는 로컬 디스크에서 WAL을 사용하고 한 process가 connection 소유권을 관리한다. L3는 SQLite master를 여러 worker가 덮어쓰는 대신 각 worker가 자기 신규 항목을 불변 파일로 게시한다.

### 12.4 `check_same_thread=False`는 잠금이 아니다

Python SQLite connection을 다른 스레드에서 사용하려고 `check_same_thread=False`를 설정하는 경우가 있다.

```python
connection = sqlite3.connect(
    local_path,
    check_same_thread=False,
)
```

이 옵션은 기본 thread affinity 검사를 끌 뿐 여러 스레드의 transaction을 자동 직렬화하는 application lock이 아니다. 한 스레드가 `INSERT`로 implicit transaction을 연 상태에서 다른 스레드가 같은 connection에 `commit`이나 query를 수행하면 논리적 경쟁이 생길 수 있다.

선택지는 명확하다.

1. connection owner thread 하나와 command queue를 둔다.
2. thread별 connection을 사용하고 SQLite의 transaction 경계를 지킨다.
3. 공유 connection 접근 전체를 `threading.Lock`으로 보호한다.
4. DB 작업을 이벤트 루프 thread 하나에 제한하고 느린 export만 snapshot 후 offload한다.

간단한 cache라면 owner thread가 가장 이해하기 쉽다.

```python
class CacheWriter:
    def __init__(self, connection):
        self.connection = connection
        self.lock = threading.Lock()

    def put(self, key, value):
        with self.lock:
            self.connection.execute(
                "INSERT OR IGNORE INTO cache VALUES (?, ?)",
                (key, value),
            )
            self.connection.commit()
```

lock 범위 안에서 NAS I/O나 긴 CPU 작업을 수행하지 않는다. payload 변환을 먼저 끝내고 짧은 DB transaction만 보호한다.

### 12.5 local cache schema에 버전을 넣는다

콘텐츠 hash가 같아도 모델이나 프롬프트가 바뀌면 결과는 달라진다. key를 여러 column으로 명시하면 audit하기 쉽다.

```sql
CREATE TABLE cache_entries (
    content_sha256   TEXT NOT NULL,
    model_version    TEXT NOT NULL,
    prompt_version   TEXT NOT NULL,
    preprocess_ver   TEXT NOT NULL,
    schema_version   TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    created_run      TEXT NOT NULL,
    PRIMARY KEY (
        content_sha256,
        model_version,
        prompt_version,
        preprocess_ver,
        schema_version
    )
);
```

한 문자열 hash로 합칠 수도 있지만 원래 component를 함께 저장해 충돌과 invalidation을 설명할 수 있게 한다.

### 12.6 stage-out은 snapshot 덮어쓰기가 아니라 delta 게시다

worker 로컬 DB 전체를 종료 시 NAS master에 복사하면 last-writer-wins가 된다. 모든 worker DB를 coordinator가 나중에 merge할 수도 있지만 DB가 커질수록 전체 복사 비용이 증가한다.

신규 항목만 추적한다.

```sql
CREATE TABLE own_delta (
    sequence      INTEGER PRIMARY KEY AUTOINCREMENT,
    cache_key     TEXT NOT NULL UNIQUE,
    payload_json  TEXT NOT NULL
);
```

cache에 처음 삽입한 항목을 `own_delta`에도 기록한다. 주기적으로 아직 export하지 않은 sequence 범위를 불변 청크로 게시한다. 다음 장에서 전체 프로토콜을 구현한다.

---

## 13. 불변 델타 청크로 상태를 교환한다

분산 cache의 목적을 좁혀 보자.

1. worker가 새 cache entry를 만들 수 있다.
2. 다른 worker가 잠시 뒤 그 entry를 재사용할 수 있다.
3. worker crash가 다른 worker의 상태를 손상시키면 안 된다.
4. 같은 delta를 여러 번 읽어도 결과가 같아야 한다.
5. 공유 SQLite multi-writer는 사용하지 않는다.

이 요구에 필요한 것은 복잡한 분산 database가 아니라 owner별 immutable log일 수 있다.

### 13.1 공유 레이아웃

[`code/delta_cache.py`](code/delta_cache.py)는 다음 구조를 사용한다.

```text
delta-root/
  local/
    worker-00000/cache.sqlite
    worker-00001/cache.sqlite
  shared/
    worker-00000/
      latest.json
      chunk-000000000001-000000000100-<hash>.jsonl
      chunk-000000000101-000000000200-<hash>.jsonl
    worker-00001/
      latest.json
      chunk-...
```

worker 0만 `shared/worker-00000/`을 쓴다. chunk는 한번 게시되면 수정하지 않는다. `latest.json`만 작은 mutable pointer이며 owner 하나가 atomic replace한다.

### 13.2 왜 누적 snapshot 하나가 아니라 chunk인가

매 동기화마다 지금까지의 모든 delta를 하나의 파일에 다시 쓰면 다음 문제가 생긴다.

- cache가 커질수록 export bytes가 계속 증가한다.
- peer는 파일이 바뀔 때 전체를 다시 읽을 수 있다.
- 큰 snapshot replace 시간이 길어진다.
- 실패 시 임시 공간이 커진다.

불변 chunk는 새 항목만 추가한다.

\[
\text{export bytes per interval}
\approx \text{new entries}
\]

peer는 이미 처리한 chunk 이름을 로컬 DB에 기록하고 새 chunk만 읽는다.

### 13.3 export 흐름

로컬 DB에서 아직 export하지 않은 sequence를 읽는다.

```python
exported = self._state_int("exported_sequence")
rows = self.connection.execute(
    """
    SELECT sequence, cache_key, payload_json
    FROM own_delta
    WHERE sequence > ?
    ORDER BY sequence
    LIMIT ?
    """,
    (exported, limit),
).fetchall()
```

각 row를 canonical JSONL로 직렬화하고 SHA-256을 계산한다.

```python
payload = "".join(lines).encode("utf-8")
digest = hashlib.sha256(payload).hexdigest()
filename = (
    f"chunk-{first_sequence:012d}-"
    f"{last_sequence:012d}-{digest[:16]}.jsonl"
)
```

파일명 자체에 sequence 범위와 content digest 일부가 들어간다. 같은 내용은 같은 이름이 되고, 다른 내용이 같은 sequence 범위를 주장하면 이름이 달라진다.

로컬 임시 파일에서 완성한 뒤 shared owner 디렉터리의 임시 파일로 복사하고 `os.replace`한다.

```python
with tempfile.TemporaryDirectory() as temporary:
    local_chunk = Path(temporary) / filename
    local_chunk.write_bytes(payload)

    shared_tmp = shared_dir / f".{filename}.{os.getpid()}.tmp"
    shutil.copyfile(local_chunk, shared_tmp)
    fsync_file(shared_tmp)
    os.replace(shared_tmp, shared_dir / filename)
```

그 다음에만 `latest.json`에 chunk metadata를 추가한다. content가 먼저, pointer가 나중이다. consumer가 새 pointer를 보았는데 content가 없는 상태를 피한다.

실습 구현은 SQLite connection을 `to_thread`에서 사용할 수 있도록 thread affinity 검사만 끄는 데서 멈추지 않는다. local DB의 짧은 transaction은 `RLock`으로 보호하고, 한 worker의 export publication은 별도의 lock으로 직렬화한다. chunk bytes를 만들고 shared path에 게시하는 느린 구간은 DB lock 밖에서 수행하므로 정상 `put`이 원격 쓰기 전체를 기다리지 않는다. connection을 닫기 전에는 모든 export/import future를 drain해야 한다.

manifest 게시 뒤 local `exported_sequence`를 갱신하기 전에 crash하면 같은 범위를 다시 export할 수 있다. 파일명이 sequence와 digest로 결정적이므로 재시도는 같은 chunk 이름을 만든다. 실습 코드는 manifest에 같은 이름과 같은 metadata가 이미 있으면 중복 항목을 추가하지 않고 local cursor만 전진시키며, 같은 이름에 다른 metadata가 나타나면 충돌로 실패한다.

### 13.4 manifest 예시

```json
{
  "schema_version": 1,
  "worker_id": 0,
  "revision": 2,
  "chunks": [
    {
      "filename": "chunk-000000000001-000000000100-a19f.jsonl",
      "first_sequence": 1,
      "last_sequence": 100,
      "records": 100,
      "sha256": "a19f..."
    },
    {
      "filename": "chunk-000000000101-000000000200-9b20.jsonl",
      "first_sequence": 101,
      "last_sequence": 200,
      "records": 100,
      "sha256": "9b20..."
    }
  ]
}
```

production에서는 chunk 수가 매우 많아지면 manifest도 커진다. epoch별 manifest를 만들거나 일정 구간을 compaction한 snapshot으로 바꾸고 이전 chunk를 garbage collection할 수 있다. 그러나 compaction은 별도 owner/coordinator가 수행하고, 새 snapshot이 검증되기 전 기존 chunk를 삭제하지 않는다.

### 13.5 peer import

worker 수를 알고 있으므로 각 peer의 `latest.json` 경로도 안다.

```python
for peer in range(num_workers):
    if peer == worker_id:
        continue
    import_peer(peer)
```

manifest에 나열된 chunk 중 `imported_chunks` table에 없는 것만 직접 연다.

```sql
CREATE TABLE imported_chunks (
    peer_worker INTEGER NOT NULL,
    filename    TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    PRIMARY KEY (peer_worker, filename)
);
```

읽은 bytes의 digest를 검증한 뒤 `INSERT OR IGNORE`로 local cache에 넣는다. chunk 반영과 `imported_chunks` 기록은 같은 local transaction에 있어야 한다. 중간에 crash하면 다음 실행에서 chunk를 다시 읽어도 idempotent하다.

### 13.6 충돌 정책을 숨기지 않는다

두 worker가 같은 key에 다른 payload를 만들 수 있다.

```text
worker 0: key K → payload A
worker 1: key K → payload B
```

“같은 content와 같은 버전이면 같은 결과”가 시스템 불변식이라면 A와 B가 다르다는 사실 자체가 설정 불일치, 비결정성, cache key 누락을 의미한다.

실습은 first-writer-wins로 기존 값을 유지하지만 충돌 수를 반환한다.

```python
existing = connection.execute(
    "SELECT payload_json FROM entries WHERE cache_key = ?",
    (cache_key,),
).fetchone()

if existing:
    if existing[0] != encoded:
        conflicts += 1
    continue
```

충돌을 조용히 무시하면 cache가 오염되어도 알 수 없다. production에서는 payload digest와 origin run을 기록하고 conflict rate가 0이 아니면 경고 또는 실행 실패로 승격하는 편이 좋다.

### 13.7 eventual consistency의 비용

worker 0이 entry를 만든 직후 worker 1이 같은 입력을 처리하면 아직 delta가 게시되지 않아 중복 계산할 수 있다. 동기화 주기를 짧게 하면 중복은 줄지만 NAS metadata와 작은 write 부하는 늘어난다.

\[
\text{sync interval} \downarrow
\Rightarrow
\text{duplicate work} \downarrow,
\quad
\text{control I/O} \uparrow
\]

결과의 정확성이 중복 계산에 영향을 받지 않고 계산 비용만 늘어난다면 몇십 초 또는 몇 분의 지연을 허용할 수 있다. 반대로 한 요청이 매우 비싸고 중복률이 높다면 중앙 cache service나 더 빠른 exchange channel이 적합할 수 있다.

중요한 점은 이 프로토콜이 strong consistency를 제공한다고 착각하지 않는 것이다. 이것은 불변 delta의 eventual exchange다.

### 13.8 worker 0 rebuild 패턴의 함정

모든 delta를 하나의 master snapshot으로 합치려면 coordinator가 필요하다. array task에서 worker 0을 coordinator로 쓰는 패턴이 간단해 보이지만 worker 0이 가장 먼저 계산을 끝낼 수 있다.

```text
worker 0 done → 즉시 rebuild
worker 7 still running → 아직 마지막 delta 미게시
master snapshot → worker 7의 후반 delta 누락
```

rebuild 전에 모든 worker의 검증된 completion sentinel을 기다린다.

```python
for worker_id in range(num_workers):
    wait_and_validate_done(worker_id)
rebuild_master_from_all_manifests()
```

한 worker가 실패하면 무한 대기하지 않는다. timeout 후 partial snapshot을 명시적으로 만들거나 전체 rebuild를 실패시킨다. 정책은 작업 목적에 따라 다르지만 silent partial은 허용하지 않는다.

### 13.9 실행

```bash
python delta_cache.py demo \
  --root /tmp/delta-cache-lab
```

demo는 두 worker가 고유 key와 하나의 충돌 key를 만든 뒤 chunk를 게시하고 상호 import한다. 출력의 `conflict_is_observable`이 `true`여야 한다.

단위 테스트는 다음을 확인한다.

- peer entry가 전달된다.
- 같은 chunk 재import는 아무 변화가 없다.
- 충돌은 count된다.
- chunk bytes를 변조하면 checksum 검증이 실패한다.

이 네 가지가 delta exchange의 최소 정확성 기반이다.

### 13.10 이 설계가 적합하지 않은 경우

다음 요구가 있다면 client/server cache나 database를 검토한다.

- write 직후 모든 worker에서 보여야 한다.
- 같은 key에 대한 원자적 compare-and-set이 필요하다.
- 실시간 eviction과 TTL이 필요하다.
- 수천 worker가 매우 짧은 주기로 상태를 교환한다.
- conflict resolution이 비즈니스 transaction이다.
- query가 key lookup을 넘어 복잡하다.

불변 파일 exchange는 폐쇄된 배치 환경, 느슨한 동기화, 재계산 가능한 cache에 잘 맞는다. 모든 분산 상태 문제의 보편적 해법은 아니다.

---

## 14. 재시작과 멱등성은 별도 기능이 아니다

분산 배치는 실패한다. 노드 장애뿐 아니라 스케줄러 시간 제한, 모델 서버 warmup 실패, 입력 손상, 네트워크 일시 중단, 코드 배포 오류가 있다. “정상 실행이 빨라진 뒤 resume을 붙인다”는 순서로 생각하면 데이터 모델을 다시 뜯게 된다.

재시작 가능성은 처음부터 결과 주소, 상태 전이, writer ownership에 들어가야 한다.

### 14.1 레코드 상태 머신

```text
DISCOVERED
  → ELIGIBLE
  → LOADED
  → INFERRED
  → RESULT_PUBLISHED
  → CHECKPOINTED

실패 분기:
  → RETRYABLE_FAILED
  → QUARANTINED
  → SKIPPED
```

외부에서 관측 가능한 완료는 `RESULT_PUBLISHED` 이후다. `CHECKPOINTED`는 resume을 빠르게 하는 index다. 모델 응답을 메모리에 가진 `INFERRED` 상태는 process crash 후 사라진다.

각 전이가 멱등적인지 묻는다.

- 같은 입력을 다시 load해도 안전한가?
- 같은 cache key로 모델을 다시 호출해도 허용되는가?
- 같은 결과 경로에 같은 content를 다시 게시해도 안전한가?
- 같은 journal event가 두 번 나타나면 dedupe할 수 있는가?

### 14.2 결과 경로에 실행 fingerprint를 넣는다

기존 결과가 있다는 이유만으로 skip하면 설정 변경이 반영되지 않는다.

fingerprint에 최소 다음 항목을 포함한다.

```python
def configuration_fingerprint(config) -> str:
    relevant = {
        "model": config.model_version,
        "prompt": config.prompt_version,
        "preprocess": config.preprocess_version,
        "schema": config.schema_version,
        "decode": config.decode_parameters,
    }
    canonical = json.dumps(
        relevant,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()
```

결과 JSON 안에 fingerprint를 기록하거나 경로 namespace에 넣는다.

```text
outputs/<fingerprint>/<shard>/<record-id>/result.json
```

경로에 넣으면 서로 다른 실행 결과가 덮어써지지 않는다. 결과 안에도 넣으면 파일이 이동되었을 때 검증할 수 있다.

### 14.3 at-least-once + idempotency가 실용적이다

파일 결과와 별도 저널 사이에 원자 transaction이 없으면 crash window를 완전히 제거하기 어렵다. 정확히 한 번을 주장하기보다 다음 계약이 현실적이다.

```text
processing delivery: at least once
result publication: idempotent by key/fingerprint
journal merge: deduplicated by record_id + fingerprint
final manifest: exactly one published version per coordinator epoch
```

같은 레코드가 두 번 처리될 수 있지만 최종 결과는 같은 key에 같은 content로 수렴한다. 서로 다른 content가 나오면 비결정성 conflict로 탐지한다.

### 14.4 resume set을 항상 적용한다

코드 경로가 여러 개일 때 특정 입력 옵션이 resume logic을 우회하는 버그가 생기기 쉽다.

```python
# 위험한 구조
if config.explicit_ids:
    return list(config.explicit_ids)  # done set 적용 전 반환

done = load_done_set()
return [rid for rid in discover_all() if rid not in done]
```

공통 filter를 마지막에 적용한다.

```python
def build_workset(config):
    candidates = (
        list(config.explicit_ids)
        if config.explicit_ids
        else discover_from_manifest(config.manifest)
    )

    if config.force_reprocess:
        return candidates

    done = load_validated_done_set(config)
    return [rid for rid in candidates if rid not in done]
```

`force_reprocess`는 명시적 옵션이어야 한다. 입력 ID 파일을 제공했다는 사실이 기존 결과를 무시한다는 뜻이 되어서는 안 된다.

### 14.5 저널과 결과가 어긋났을 때

네 경우가 있다.

| 결과 | 저널 | 해석 | 조치 |
|---|---|---|---|
| 있음 | 있음 | 정상 후보 | fingerprint/digest 검증 |
| 있음 | 없음 | result publish 후 crash | journal 보정 또는 skip |
| 없음 | 있음 | 잘못된 완료/결과 유실 | 완료 취소, 재처리 |
| 없음 | 없음 | 미처리 | 처리 |

전수 `stat`이 비싸다면 worker별 result index와 journal을 함께 비교한다. 정기적인 audit job은 전체 결과를 샤드 단위로 검증하고 새로운 compact manifest를 만든다. hot resume은 저널을 사용하고, 느린 audit은 진실을 교정한다.

### 14.6 sentinel을 재시작 경계로 사용한다

worker의 `DONE.json`이 없다면 그 worker의 journal은 부분 상태다. 재시작 방식은 두 가지다.

1. 같은 attempt를 이어서 sequence 이후에 append한다.
2. 새 attempt 디렉터리를 만들고 이전 부분 journal의 유효 결과만 seed로 사용한다.

두 번째가 더 명확한 경우가 많다.

```text
attempt-0001/worker-00007/  # incomplete, immutable after failure
attempt-0002/worker-00007/  # new owner output
```

coordinator는 attempt 1의 검증 가능한 결과와 attempt 2의 완료 결과를 record key로 merge한다. lineage가 남고 stale local state 혼입을 줄인다.

### 14.7 제한 시간보다 먼저 종료 준비를 시작한다

배치 스케줄러가 hard kill하는 시각까지 계산을 계속하면 final flush와 sentinel을 게시하지 못한다. 예상 종료 \(D\) 전에 grace \(G\)를 예약한다.

```python
if deadline - monotonic() < shutdown_grace:
    stop_accepting_new_records()
    await drain_and_checkpoint()
```

grace에는 다음 시간을 포함한다.

- 최대 모델 요청 취소/완료 대기
- 결과 write p95
- journal flush와 fsync
- delta export
- sentinel publish
- 로그/metrics flush

실제 p99에 여유를 더해 정한다. final merge를 각 worker 종료 안에 넣지 않고 별도 coordinator job으로 분리하면 worker grace를 줄일 수 있다.

---

## 15. 샤딩의 목표는 건수 균등이 아니라 비용 균등이다

레코드 ID를 worker 수로 나누는 modulo 샤딩은 결정적이고 간단하다.

```python
owner = int(record_id) % num_workers
```

각 ID가 균등하게 분포하면 건수도 비슷하다. 하지만 레코드 비용이 다르면 완료 시간은 크게 벌어질 수 있다.

레코드 비용에는 다음이 들어간다.

- payload 개수와 총 bytes
- 이미지 segment 수
- cache hit 여부
- 모델 input/output token
- repair path 진입 여부
- retry 가능성
- NAS 디렉터리 분포와 cache locality

worker \(w\)의 총 비용은 다음처럼 본다.

\[
C_w = \sum_{r \in S_w} c(r)
\]

전체 makespan은 평균이 아니라 가장 느린 worker에 의해 결정된다.

\[
T_{\text{job}} \approx \max_w \frac{C_w}{X_w}
\]

모든 worker가 barrier에서 기다리면 한 worker의 긴 tail이 전체 GPU 시간과 scheduler allocation을 늘린다.

### 15.1 index round-robin이 해결하는 것

전체 ID 목록이 고정되어 있다면 다음 방식은 건수 편차를 거의 없앤다.

```python
my_ids = all_ids[worker_id::num_workers]
```

원래 목록이 무거운 레코드 기준으로 무작위화되어 있거나 비용이 고르게 섞여 있다면 modulo보다 나을 수 있다. 그러나 이것도 비용을 직접 보지 않는다. 목록이 카테고리나 파일 크기로 정렬되어 있으면 stride가 특정 패턴과 공진할 수 있다.

### 15.2 weighted greedy 배분

매니페스트에 예상 비용이 있다면 무거운 레코드부터 현재 합계가 가장 작은 worker에 배치한다.

```python
def weighted_partition(records, workers):
    bins = [(0, worker_id, []) for worker_id in range(workers)]
    heapq.heapify(bins)

    for record in sorted(
        records,
        key=lambda item: item.estimated_cost,
        reverse=True,
    ):
        cost, worker_id, assigned = heapq.heappop(bins)
        assigned.append(record.record_id)
        heapq.heappush(
            bins,
            (cost + record.estimated_cost, worker_id, assigned),
        )
    return bins
```

예상 비용은 완벽할 필요가 없다. payload 수, bytes, 과거 latency bucket만으로도 건수보다 나을 수 있다. 실제 비용과 예측의 오차를 실행 후 기록해 다음 매니페스트를 개선한다.

### 15.3 동적 queue와 work stealing

비용 예측이 어렵고 중앙 queue를 운영할 수 있다면 worker가 다음 작업을 동적으로 가져가게 할 수 있다. 느린 worker가 무거운 레코드를 잡아도 다른 worker가 남은 일을 계속 가져가 tail이 줄어든다.

그러나 trade-off가 있다.

- 중앙 queue 가용성
- task lease와 중복 delivery
- 네트워크 단절 시 lease 복구
- exactly-once 착각
- 순서와 locality 손실
- 운영 복잡도

정적 shard + 충분히 작은 chunk가 중간 해법이다.

```text
manifest를 10,000개 큰 worker shard가 아니라
수백~수천 개 작은 immutable chunk로 나눈다.

worker는 scheduler/queue에서 chunk를 하나씩 가져간다.
chunk 안에서는 direct path와 private journal을 사용한다.
```

### 15.4 coordinator 역할과 계산 역할을 분리한다

worker 0이 계산도 하고 모든 worker의 완료를 기다려 merge도 하면 worker 0이 먼저 계산을 끝냈을 때 자원을 붙잡고 기다린다. 반대로 worker 0이 실패하면 merge가 없다.

별도 작은 coordinator job을 dependency로 제출할 수 있다.

```text
array workers
   ├─ private results
   ├─ private journals
   └─ DONE sentinels
          │
          ▼
coordinator job
   validate → merge → publish final manifest
```

coordinator는 GPU가 필요 없고 CPU와 소량의 메모리만 요청할 수 있다. 계산 작업의 제한 시간과 merge 시간을 분리하고 재시도도 독립적으로 할 수 있다.

### 15.5 샤딩 평가 지표

평균 records/s만 보지 않는다.

- worker별 assigned records
- estimated cost 합
- actual payload bytes
- model miss requests
- cache hit ratio
- completion time
- idle wait at barrier
- p50/p95/max worker duration

불균형 비율을 단순하게 다음처럼 본다.

\[
\text{imbalance ratio}
= \frac{\max(T_w)}{\operatorname{median}(T_w)}
\]

최대/최소는 아주 빠른 빈 worker 때문에 과장될 수 있어 max/median이 해석하기 편하다. workload가 충분히 큰데 ratio가 높으면 비용 feature를 추가하거나 chunk granularity를 줄인다.

---

## 16. 관측성과 장애 분류

대규모 파이프라인의 로그가 풍부해도 “느리다”와 “실패했다”를 구분하지 못하면 대응이 늦어진다. 관측성의 목적은 모든 내부 상태를 출력하는 것이 아니라 다음 행동을 결정할 수 있게 하는 것이다.

### 16.1 네 층을 함께 본다

```text
application
  record rate, cache hit, retry, queue, step latency

runtime
  event-loop gap, thread-pool queue, process RSS, open FDs

client OS
  CPU, iowait, context switch, D state, network, NFS client stats

shared service
  metadata op latency, server queue, throughput, errors
```

application 지표만 보면 NAS 응답 지연과 CPU decode 병목을 혼동할 수 있다. 시스템 지표만 보면 높은 load average가 실제 파이프라인에 어떤 영향을 주는지 알기 어렵다.

### 16.2 load average와 D state를 조심해서 해석한다

Linux load average에는 CPU에서 실행 가능한 task뿐 아니라 uninterruptible sleep 상태의 task도 포함될 수 있다. 공유 I/O를 기다리는 프로세스나 스레드가 많으면 CPU가 완전히 바쁘지 않아도 load가 높아질 수 있다.

따라서 다음 추론은 안전하지 않다.

```text
load average가 코어 수보다 높다
→ CPU가 포화되었다
```

대신 CPU user/system/iowait, run queue, D state task, context switch, 단계별 latency를 함께 본다.

```bash
ps -eo pid,ppid,state,wchan:32,comm | awk '$3 == "D"'
pidstat -druw -p "$PID" 1
```

`wchan`은 힌트이지 완전한 진단이 아니다. 컨테이너와 권한 설정에 따라 정보가 제한될 수 있다.

### 16.3 구조화 로그의 최소 필드

레코드마다 긴 문장을 출력하면 로그 I/O 자체가 병목이 된다. 이벤트를 구조화하고 정상 성공은 샘플링하거나 주기 집계로 낸다.

```json
{
  "ts": "2026-07-20T12:00:00Z",
  "event": "progress",
  "run_id": "public-example",
  "worker_id": 7,
  "window_seconds": 60,
  "candidate": 41000,
  "eligible": 27000,
  "cache_hit": 19000,
  "model_done": 7900,
  "persisted": 7880,
  "retry": 37,
  "queue_metadata": 512,
  "queue_model": 64,
  "queue_result": 21,
  "p95_metadata_ms": 8.4,
  "p95_model_ms": 940.0,
  "p95_persist_ms": 14.3,
  "event_loop_max_gap_ms": 32.0
}
```

원시 내부 경로와 입력 내용을 로그에 넣지 않는다. record ID도 개인 정보나 사내 식별자가 될 수 있다면 salted digest나 run-local sequence를 사용한다. 오류 샘플은 접근 통제된 별도 저장소에 둔다.

### 16.4 실패를 재시도 가능성으로 분류한다

`except Exception: retry`는 비용을 폭발시킨다. 최소한 다음 분류가 필요하다.

| 분류 | 예 | 기본 행동 |
|---|---|---|
| transient I/O | 일시적 timeout, 연결 reset | backoff 후 제한 재시도 |
| overload | queue saturation, 503, tail 급증 | 동시성 감소, retry 지연 |
| permanent missing | 입력 파일 없음 | quarantine 또는 skip |
| corrupt input | JSON parse, checksum 불일치 | quarantine, 재시도 금지 |
| invariant violation | 같은 cache key의 다른 결과 | 실행 중단 또는 강한 경고 |
| local resource | disk full, FD 부족, OOM | worker fail, resource 수정 |
| programming error | KeyError, assertion | fail fast, 코드 수정 |
| cancellation | scheduler signal, deadline | graceful drain |

오류 code는 저널과 metrics에서 안정적인 vocabulary로 사용한다.

```python
class FailureCode(Enum):
    IO_TIMEOUT = "io_timeout"
    INPUT_MISSING = "input_missing"
    INPUT_CORRUPT = "input_corrupt"
    MODEL_OVERLOADED = "model_overloaded"
    CACHE_CONFLICT = "cache_conflict"
    LOCAL_DISK_FULL = "local_disk_full"
```

문자열 stack trace를 파싱해 집계하지 않는다.

### 16.5 poison record를 전체 queue에서 분리한다

항상 실패하는 레코드가 무한 재시도되면 정상 처리량을 깎는다. 최대 attempt와 마지막 오류를 기록해 quarantine manifest로 보낸다.

```json
{
  "record_id": "hashed-id",
  "failure_code": "input_corrupt",
  "attempts": 2,
  "input_manifest_digest": "...",
  "config_fingerprint": "...",
  "diagnostic_ref": "restricted://..."
}
```

quarantine은 쓰레기통이 아니다. 오류 비율, upstream source, schema version별로 집계해 데이터 계약 문제를 고친다. repair가 끝나면 새로운 input manifest로 재실행한다.

### 16.6 queue depth를 스파크라인처럼 본다

한 시점의 queue 값보다 시간 변화가 중요하다.

```text
metadata queue:  ████████████████  항상 가득 참
model queue:     ▁▁▁▁▁▁▁▁▁▁▁▁▁▁  거의 비어 있음
result queue:    ▁▁▁▁▁▁▁▁▁▁▁▁▁▁
```

metadata queue가 가득하고 model queue가 비면 metadata loader 이후 단계가 느리거나 loader가 queue에 넣지 못하는 이유가 있다. 반대로 model queue가 가득하고 result queue가 비면 모델 단계가 병목이다. result queue가 계속 가득 차면 persistence가 upstream에 backpressure를 주고 있다.

queue의 “가득 찬 시간 비율”도 유용하다.

\[
\text{saturation ratio}
= \frac{\text{queue at max인 관측 수}}{\text{전체 관측 수}}
\]

### 16.7 진행률은 분모를 설명해야 한다

`80% 완료`가 무엇의 80%인지 명시한다.

- input manifest 전체
- eligible record
- cache miss record
- worker assigned record
- 성공 + 영구 skip

최종 상태를 합이 맞는 표로 낸다.

```text
manifest total
  = success
  + valid skip
  + quarantined
  + retry exhausted
  + not yet processed
```

분류가 겹치지 않아야 한다. `retry exhausted`를 success에도 세거나 cache hit를 candidate에서 빼면 합이 맞지 않는다.

### 16.8 runbook은 증상에서 시작한다

좋은 운영 문서는 구성 요소 설명보다 관측 증상에서 다음 확인으로 이어진다.

```text
증상: GPU queue가 비고 records/s가 하락
  1. eligible/s와 cache hit/s 확인
  2. metadata/payload queue depth 확인
  3. event-loop gap 확인
  4. I/O thread queue와 p95 open 확인
  5. retry rate와 NAS client error 확인
  6. canary에서 direct-open counter 회귀 확인
```

```text
증상: worker 완료 수가 예상보다 작음
  1. DONE sentinel 수 확인
  2. 각 sentinel digest 검증
  3. missing worker와 scheduler 상태 대조
  4. result index와 journal count 비교
  5. partial manifest 게시 여부 확인
```

명령에 내부 고정 경로를 박기보다 `RUN_ROOT`, `WORKER_ID` 같은 명시적 인자를 받는 검증 스크립트를 제공한다. wildcard로 광범위한 NAS tree를 스캔하는 응급 명령은 마지막 수단으로 둔다.

---

## 17. 효과를 증명하는 실험 설계

성능 글의 신뢰도는 가장 큰 개선 숫자가 아니라 독자가 비교 조건을 재구성할 수 있는가에 달려 있다. 여러 변경이 섞인 운영 이력을 공개 글로 옮길 때 특히 조심해야 한다.

### 17.1 관찰, 가설, 예측, 반증 조건

한 실험을 네 문장으로 쓰면 사고가 선명해진다.

```text
관찰:
  모델 queue가 자주 비고 metadata 단계 p95가 높다.

가설:
  알려진 meta.json을 찾기 위해 수행하는 directory listing이 병목이다.

예측:
  direct open으로 바꾸면 directory op count가 감소하고,
  같은 입력·동시성에서 metadata p95와 records/s가 개선된다.

반증 조건:
  directory op count가 줄어도 wall time이 같거나,
  모델/결과 단계가 이미 상한이면 전체 throughput은 개선되지 않는다.
```

반증 조건을 미리 쓰면 결과가 기대와 달라도 지식을 얻는다.

### 17.2 microbenchmark에서 production까지 네 단계

1. microbenchmark: 호출 하나의 비용과 분포를 본다.
2. component benchmark: scanner 또는 writer만 실제 모양으로 실행한다.
3. single-node canary: 모델과 I/O가 만나는 상호작용을 본다.
4. multi-node canary: 공유 서비스 포화와 worker tail을 본다.

`nas_io_lab.py`는 1단계다. 이것만으로 전체 job이 몇 배 빨라진다고 주장하지 않는다. component와 canary에서 queue가 실제로 이동했는지 확인한다.

### 17.3 baseline을 고정한다

baseline artifact를 남긴다.

```yaml
experiment:
  id: nas-direct-open-001
  code_revision: "<public-example>"
  input_manifest_sha256: "..."
  records: 100000
  workers: 1
  metadata_concurrency: 16
  payload_concurrency: 8
  cache_condition: "round-1"
  duration_window: "steady-state-minutes-5-to-15"
  variant: "scandir"
```

variant만 `direct-open`으로 바꾼다. 모델 server revision, 입력 manifest, concurrency가 달라지면 별도 실험이다.

### 17.4 순서 효과를 줄인다

A를 먼저 실행하고 B를 나중에 실행하면 B가 warm cache 이득을 볼 수 있다.

- ABBA 순서: A, B, B, A
- round마다 ID order shuffle
- 서로 다른 동등 shard를 교차 배정
- 첫 round를 warmup으로 보고 별도 표시
- 같은 시간대에 A/B worker를 병렬 실행하되 서로 부하 간섭 기록

공유 NAS에서 완전한 격리는 어렵다. 결과에 같은 시간대의 다른 workload가 있었는지 남기고 반복 측정으로 분산을 본다.

### 17.5 정규화된 공개 수치

회사 기밀을 제거할 때 모든 수치를 없애면 글이 검증 불가능해진다. 다음 형태로 일반화할 수 있다.

- baseline을 1.00으로 둔 normalized throughput
- 절대 노드 수 대신 single-node, small-canary, full-scale
- 정확한 데이터 수 대신 \(10^5\), \(10^6\) 규모 범주
- 원시 경로 대신 `/shared/dataset/...`
- 정확한 장비명 대신 data-center GPU와 shared NAS
- 특정 시각 대신 steady-state 10분 창

```text
Variant               throughput   metadata p95   retry
directory discovery      1.00×         1.00×       1.00×
direct known path        1.31×         0.58×       0.92×
direct + bounded I/O     1.74×         0.44×       0.31×
```

위 표는 형식 예시이며 실제 결과가 아니다. 공개 글에 넣는 수치는 실제 측정의 범주화인지 합성 예시인지 라벨을 붙인다.

### 17.6 효과를 묶어 과장하지 않는다

NAS offload, batch flush, 모델 concurrency, GPU graph, speculative decoding을 한꺼번에 바꾸고 총 5배가 빨라졌다면 그 5배를 NAS 최적화의 효과라고 말할 수 없다.

| 변경 | 직접 영향 | 확인 지표 |
|---|---|---|
| direct path | directory ops 감소 | scandir count, metadata p95 |
| `to_thread` | event loop unblock | heartbeat gap, callback delay |
| batch flush | write ops 감소 | flush count, persist p95 |
| model concurrency | GPU queue | request latency, retry, throughput |
| GPU optimization | inference capacity | token/request throughput |

전체 성과와 개별 기여를 구분한다.

```text
전체 시스템: baseline 대비 N×
NAS/code-path change alone: canary에서 M×
나머지: 모델 서버와 scheduling change 포함
```

### 17.7 성공 지표와 guardrail

records/s만 높으면 실패를 버리거나 검증을 생략해도 빨라 보일 수 있다.

- 결과 성공률
- checksum/parse 검증
- cache conflict
- duplicate result
- retry exhausted
- worker completion sentinel
- 메모리와 local disk 사용량
- 다른 사용자의 NAS latency redline

```text
승격 조건:
  throughput >= baseline × 1.15
  AND error_rate <= baseline
  AND cache_conflict == 0
  AND all worker sentinels valid
  AND p99 metadata latency < redline
```

### 17.8 실패 주입도 성능 실험의 일부다

- worker를 journal flush 직전에 종료
- chunk 게시 후 manifest 게시 전에 종료
- manifest 게시 후 peer import 전에 종료
- local DB를 손상시키고 시작
- 하나의 input file을 제거
- model service에 일시적 503 주입
- coordinator 실행 전에 worker 하나를 incomplete로 둠

검증할 것은 처리량이 아니라 불변식이다.

```text
손상된 chunk는 import되지 않는다.
DONE 없는 worker는 final manifest에 포함되지 않는다.
같은 chunk 재적용은 중복을 만들지 않는다.
결과 없는 journal event는 audit에서 탐지된다.
재시작은 유효 결과를 보존하고 나머지만 처리한다.
```

### 17.9 벤치마크 자체의 부하

metadata benchmark는 공유 NAS에 실제 부하를 준다. 동시성 sweep을 크게 돌리면 다른 작업의 p95를 악화시킬 수 있다.

- 별도 테스트 tree 사용
- 작은 record 수로 시작
- 동시성 상한 사전 합의
- off-peak 시간
- 서버/클라이언트 모니터링
- 중단 redline
- 생성 fixture 정리 계획

성능을 측정한다는 이유로 운영 안정성의 경계를 넘지 않는다.

---

## 18. 끝에서 끝까지 이어지는 참조 아키텍처

지금까지의 패턴을 하나의 파이프라인으로 연결해 보자.

```text
                         immutable
                 ┌─────────────────────┐
                 │ input READY pointer │
                 └──────────┬──────────┘
                            ▼
                 ┌─────────────────────┐
                 │ input manifest shard│
                 └──────────┬──────────┘
                            │ direct known paths
                            ▼
┌──────────────┐   bounded queue   ┌───────────────┐
│ metadata I/O │ ────────────────▶ │ payload I/O   │
│ thread pool  │                   │ thread pool   │
└──────────────┘                   └───────┬───────┘
                                          ▼
                                  ┌───────────────┐
                                  │ cache lookup  │
                                  │ memory+localDB│
                                  └───┬───────┬───┘
                                      │hit    │miss
                                      │       ▼
                                      │  ┌───────────┐
                                      │  │ preprocess│
                                      │  └─────┬─────┘
                                      │        ▼
                                      │  ┌───────────┐
                                      │  │model client│
                                      │  └─────┬─────┘
                                      └────┬───┘
                                           ▼
                                  ┌────────────────┐
                                  │ atomic result  │
                                  │ publish        │
                                  └───────┬────────┘
                                          ▼
                         ┌──────────────────────────┐
                         │ private worker journal   │
                         │ + local cache delta      │
                         └────────────┬─────────────┘
                                      ▼
                         ┌──────────────────────────┐
                         │ DONE sentinel            │
                         └────────────┬─────────────┘
                                      ▼
                         ┌──────────────────────────┐
                         │ coordinator validate     │
                         │ + final manifest publish │
                         └──────────────────────────┘
```

### 18.1 디렉터리 구조

```text
run-root/
  input/
    READY.json
    manifest-<digest>-part-00000.jsonl
  attempts/
    attempt-0001/
      workers/
        worker-00000/
          result-parts/
          events.jsonl
          cache-deltas/
          DONE.json
        worker-00001/
          ...
  published/
    result-manifest-<digest>.jsonl
    READY.json
```

실제 결과가 별도 dataset root에 있다면 journal은 result object의 URI와 digest를 참조한다. 핵심은 attempt와 owner namespace가 경로에 보인다는 점이다.

### 18.2 시작 전 검증

```python
def preflight(config):
    ready = read_and_validate_ready(config.input_ready)
    manifest = open_manifest(ready)

    assert manifest.schema_version in SUPPORTED_SCHEMA
    assert ready.sha256 == sha256_file(manifest.path)
    assert config.worker_id < config.num_workers
    assert local_scratch_has_space(config.minimum_local_bytes)

    fingerprint = configuration_fingerprint(config)
    return manifest, fingerprint
```

모델 서버 `/health`만 성공했다고 실제 요청이 준비되었다고 가정하지 않는다. 작은 real-shaped request로 warmup을 확인한다. 모델 warmup 실패 시 데이터 pipeline을 시작하지 않아 대량 retry를 막는다.

### 18.3 worker main loop

```python
async def run_worker(config):
    manifest, fingerprint = preflight(config)
    journal = WorkerJournal(
        config.checkpoint_root,
        config.worker_id,
        flush_every=config.journal_flush_records,
    )
    cache = DeltaCache(
        config.cache_root,
        config.worker_id,
        config.num_workers,
    )

    try:
        async with Pipeline(config) as pipeline:
            async for record in assigned_records(
                manifest,
                config.worker_id,
                config.num_workers,
            ):
                if valid_result_exists(record, fingerprint):
                    journal.append(record.id, status="ok")
                    continue
                await pipeline.submit(record)

            async for completed in pipeline.drain():
                await asyncio.to_thread(
                    write_result_atomic,
                    completed.path,
                    completed.result,
                    fingerprint,
                )
                journal.append(completed.record_id, status="ok")

                if completed.new_cache_entry:
                    cache.put(
                        completed.cache_key,
                        completed.cache_payload,
                    )
                if cache.should_export():
                    await asyncio.to_thread(cache.export_pending)

        await asyncio.to_thread(cache.export_pending)
        await asyncio.to_thread(journal.mark_done)
    finally:
        cache.close()
```

이것은 개념 코드다. 실제 `valid_result_exists`가 매 레코드 NAS `stat`을 추가하지 않도록 resume manifest나 worker-local done set을 사용해야 한다. result 검사도 fingerprint와 parse 검증을 포함한다.

### 18.4 Pipeline 내부의 bounded stage

```python
class Pipeline:
    def __init__(self, config):
        self.scan_queue = asyncio.Queue(
            maxsize=config.scan_queue_size
        )
        self.model_queue = asyncio.Queue(
            maxsize=config.model_queue_size
        )
        self.result_queue = asyncio.Queue(
            maxsize=config.result_queue_size
        )
        self.metadata_limit = asyncio.Semaphore(
            config.metadata_concurrency
        )
        self.model_limit = asyncio.Semaphore(
            config.model_concurrency
        )
```

TaskGroup로 stage worker를 묶으면 한 stage의 치명적 예외가 나머지를 취소하고 정리할 수 있다. 종료 sentinel을 queue에 전달할 때 producer 수와 consumer 수를 정확히 관리한다. `None` 하나를 넣으면 consumer 하나만 종료하고 나머지가 기다릴 수 있다.

### 18.5 설정 예시

```yaml
run:
  attempt: 1
  shutdown_grace_seconds: 120

io:
  metadata_threads: 32
  payload_threads: 16
  metadata_concurrency: 32
  payload_concurrency: 16

queues:
  scan: 2048
  model: 256
  result: 512

model:
  max_inflight: 64
  request_timeout_seconds: 120
  retry_attempts: 3

checkpoint:
  flush_records: 100
  flush_seconds: 5
  durable_each_flush: false

cache:
  export_records: 1000
  export_seconds: 60
  conflict_policy: fail
```

이 숫자는 권장 기본값이 아니라 조절할 knob의 예다. 각 환경의 NAS, payload, 모델 server, 메모리 예산으로 sweep해야 한다.

### 18.6 핵심 불변식

참조 아키텍처를 코드 리뷰할 때 다음 문장을 테스트로 바꾼다.

1. 한 shared mutable path에는 owner writer가 하나다.
2. 모든 input manifest와 result part는 게시 후 불변이다.
3. result publish가 journal completion보다 먼저다.
4. DONE은 journal flush와 cache delta export 뒤에 게시된다.
5. coordinator는 DONE 존재뿐 아니라 digest와 sequence를 검증한다.
6. 같은 delta를 두 번 import해도 entry가 중복되지 않는다.
7. 같은 cache key의 다른 payload는 관측 가능한 conflict다.
8. force reprocess가 아니면 모든 입력 경로에 resume filter가 적용된다.
9. event loop thread에서 blocking file I/O를 수행하지 않는다.
10. 모든 queue와 executor는 상한이 있다.

### 18.7 전체 실습 테스트

```bash
cd code
python -m unittest -v
```

현재 실습에는 다음 테스트가 있다.

```text
test_shard_path_is_deterministic
test_all_access_methods_return_same_record
test_offloaded_mode_preserves_event_loop_and_is_faster
test_only_completed_worker_is_merged
test_tampering_is_detected
test_round_trip_is_idempotent_and_conflict_is_visible
test_corrupt_chunk_is_rejected
```

테스트가 빠른 이유는 실제 NAS를 사용하지 않고 임시 디렉터리와 지연 주입을 사용하기 때문이다. 이것은 correctness와 구조를 검증한다. 실제 파일시스템의 성능·rename·locking 의미는 별도 integration test가 필요하다.

### 18.8 참조 아키텍처의 목적

이 설계가 유일한 답은 아니다. 핵심은 다음 경계를 눈에 보이게 만드는 것이다.

```text
discovery vs direct access
event loop vs blocking I/O
local mutable state vs shared immutable exchange
result content vs completion evidence
worker ownership vs coordinator publication
normal path vs repair path
```

경계가 명확하면 성능 문제가 어느 쪽에 있는지, 실패했을 때 무엇을 다시 만들 수 있는지 설명할 수 있다.

---

## 19. 단계적 마이그레이션 플레이북

기존 파이프라인을 한 번에 참조 아키텍처로 바꾸는 것은 위험하다. 데이터 형식, 스케줄러, 모델 서버, NAS 경로가 얽혀 있기 때문이다. 정확성 경계를 먼저 세우고 측정 가능한 작은 변경을 순서대로 적용한다.

### 19.1 0단계: 작업의 진실을 한 장에 적는다

코드보다 먼저 다음 표를 채운다.

| 질문 | 답해야 할 내용 |
|---|---|
| 기본 처리 단위 | record, image, segment, request 중 무엇인가 |
| 완료의 정의 | result publish, journal, downstream ack 중 어디인가 |
| input source of truth | manifest, DB query, directory tree 중 무엇인가 |
| result source of truth | object, part file, database 중 무엇인가 |
| writer owner | 어떤 key를 어떤 worker가 쓰는가 |
| resume key | record ID만인지, configuration fingerprint 포함인지 |
| 제한 시간 | hard kill 전에 필요한 shutdown grace |
| 허용 복구 손실 | checkpoint RPO |
| partial 허용 | missing worker가 있을 때 publish 가능한가 |

이 질문에 답이 다르면 같은 팀에서도 서로 다른 시스템을 상상하고 있는 것이다.

### 19.2 1단계: 읽기 전용 inventory

파일 I/O 호출을 찾는다.

```bash
rg -n \
  'os\.scandir|os\.listdir|os\.walk|glob\(|rglob\(|\.exists\(|\.stat\(|open\(|read_text|write_text|sqlite3\.connect' \
  src tests scripts
```

검색 결과를 모두 문제로 간주하지 않는다. 각 호출에 다음 annotation을 붙인다.

```text
path:
hot path인가:
records당 호출 수:
shared/local:
known path인가:
read/write:
writer 수:
async event loop 안인가:
재시도 시 반복되는가:
```

작업 시작 script와 종료 merge script도 포함한다. 성능 병목은 main model loop보다 preflight scan이나 final aggregation에 있을 수 있다.

### 19.3 2단계: 정확성 위험부터 제거한다

성능 변경 전에 shared multi-writer를 찾는다.

- 하나의 `done.txt`에 여러 worker append
- 하나의 mutable JSON을 read-modify-write
- 동일 SQLite DB를 여러 노드가 open
- 각 worker의 local snapshot을 같은 master에 copy
- worker 0이 다른 worker 완료 전에 merge

worker별 namespace를 추가하고 기존 shared 파일은 read-only compatibility input으로만 사용한다.

```text
Before:
  done.txt

Transition:
  done.txt                 # 이전 실행 read-only
  worker-00000/events.jsonl
  worker-00001/events.jsonl

After:
  workers/*/events.jsonl
  final manifest
```

호환 기간에 두 형식을 모두 쓰는 dual-write는 실패 모드를 늘릴 수 있다. 가능하면 새 시도부터 새 namespace만 사용하고, 이전 형식은 시작 시 한 번 import한다.

### 19.4 3단계: 지표와 counter를 먼저 배포한다

코드 경로를 바꾸기 전에 다음을 관측한다.

- record당 `exists`, `scandir`, `open`, checkpoint flush 수
- metadata/payload/persist latency
- queue wait
- event-loop heartbeat
- worker별 cache hit와 모델 miss
- retry 원인
- 완료 sentinel 검증 결과

기존 코드에 counter를 넣으면 변경 후 실제로 호출 수가 줄었는지 확인할 수 있다. 속도만 비교하면 다른 날의 NAS 부하에 속을 수 있다.

### 19.5 4단계: discovery를 매니페스트로 분리한다

기존 directory walk를 즉시 삭제하지 않는다. 별도 manifest builder로 옮긴 뒤 old scan과 결과 집합을 비교한다.

```python
old_ids = set(discover_by_walk(root))
new_ids = set(read_manifest(build_manifest(root)))

assert old_ids == new_ids
```

대규모 set이 메모리에 들어가지 않으면 정렬된 shard별 count와 digest를 비교한다.

```text
shard 00: count, xor/hash aggregate
shard 01: count, xor/hash aggregate
...
```

단순 XOR은 충돌 가능성이 있어 강한 검증에는 정렬 후 cryptographic digest 또는 Merkle tree를 사용한다. 목적은 목록 차이를 작은 증거로 비교하는 것이다.

manifest builder 자체의 directory scan은 필요하다. 다만 매 모델 실행마다 반복하지 않고 dataset version마다 한 번 수행한다.

### 19.6 5단계: direct path와 EAFP

가장 많이 호출되는 known path부터 바꾼다.

```python
# before
for entry in os.scandir(record_dir):
    if entry.name == "meta.json":
        ...

# after
try:
    meta = read_json(record_dir / "meta.json")
except FileNotFoundError:
    quarantine(record_id, "metadata_missing")
```

확장자 fallback이 필요하면 정상 확장자를 먼저 열고 실패 시 제한된 후보만 시도한다. 모든 레코드에서 네 확장자를 `exists`로 확인하지 않는다.

```python
def open_with_fallback(base: Path, preferred: str):
    candidates = [preferred, ".jpg", ".jpeg", ".png"]
    seen = set()
    for suffix in candidates:
        if suffix in seen:
            continue
        seen.add(suffix)
        try:
            return base.with_suffix(suffix).open("rb")
        except FileNotFoundError:
            pass
    raise FileNotFoundError(base)
```

fallback count를 metric으로 기록한다. 비율이 높아지면 upstream naming 계약을 고쳐야 한다.

### 19.7 6단계: event loop audit

모든 `async def`에서 다음 호출을 검토한다.

```text
Path.exists/stat/read_bytes/write_bytes/mkdir
open/json.load/json.dump
shutil.copy/copy2
sqlite transaction
PIL decode
compression
subprocess.run
requests 같은 blocking HTTP client
```

blocking I/O는 이름 있는 동기 함수로 묶어 `to_thread`에 전달한다. CPU-bound decode는 GIL 해제 여부와 library 특성을 측정해 thread 또는 process pool을 선택한다.

한 번에 모든 함수를 옮기기보다 heartbeat gap이 큰 경로부터 바꾼다. canary에서 output parity와 exception type이 유지되는지 확인한다. thread 안의 exception은 await 시점에 다시 발생하므로 retry 분류가 달라지지 않아야 한다.

### 19.8 7단계: bounded queue와 overload guard

기존 `gather(all_records)`를 streaming producer와 bounded queue로 바꾼다.

```python
async for record in manifest:
    await input_queue.put(record)  # full이면 backpressure
```

처음에는 보수적 동시성을 사용한다. single-node sweep 후 multi-node 합산 offered load를 계산한다.

rollout redline을 정한다.

```text
중단:
  metadata p99 > baseline × 3 for 5 min
  OR retry rate > 5%
  OR local disk > 85%
  OR result queue full > 80% of samples
```

### 19.9 8단계: checkpoint protocol 전환

worker별 저널을 추가하고 기존 결과와 대조한다.

```text
validation:
  journal ok count
  result index count
  unique record count
  digest mismatch
  missing result
```

한두 worker를 의도적으로 중간 종료해 DONE 없는 journal이 final merge에서 제외되는지 확인한다. worker가 재시작할 때 이전 attempt를 덮어쓰지 않는지 확인한다.

### 19.10 9단계: local cache와 delta

먼저 local cache만 도입해 shared master write를 제거한다. worker 간 cache 공유가 없어 중복 계산이 늘 수 있지만 정확성 경계를 단순하게 만든다. 다음 단계에서 immutable delta exchange를 추가한다.

순서는 중요하다.

```text
1. shared multi-writer 제거
2. local cache correctness 검증
3. worker-owned delta export
4. checksum + idempotent import
5. conflict metric
6. periodic exchange
7. optional compaction
```

처음부터 master rebuild와 garbage collection까지 넣으면 장애 위치를 분리하기 어렵다.

### 19.11 10단계: 비용 기반 샤딩

manifest에서 비용 feature를 계산한다.

```python
estimated_cost = (
    1
    + payload_count * payload_weight
    + total_bytes // byte_bucket
    + expected_segments * segment_weight
)
```

baseline partition과 weighted partition의 worker별 비용 분포를 실행 전에 비교한다. 실제 실행 후 residual을 분석한다.

```text
actual duration
  ~ a × bytes
  + b × model requests
  + c × cache misses
  + d × repair events
```

정교한 ML predictor가 필요하지 않은 경우가 많다. 큰 비용 요인 두세 개만으로 tail이 크게 줄 수 있다.

### 19.12 rollout과 rollback

각 변경은 feature flag와 output namespace를 분리한다.

```yaml
io_mode: "direct-offloaded"       # legacy-scan | direct | direct-offloaded
checkpoint_mode: "worker-journal" # shared-append | worker-journal
cache_mode: "local-delta"         # disabled | local | local-delta
```

rollback은 새 결과를 지우고 이전 경로를 덮는 것이 아니다. 새 attempt를 중단하고 이전 검증된 manifest pointer를 다시 가리킨다.

```text
published/
  result-manifest-old.jsonl
  result-manifest-new.jsonl
  READY.json → old
```

새 결과는 사후 분석을 위해 유지하되 downstream에서 참조하지 않는다. 보존 기간 후 별도 garbage collector가 삭제한다.

### 19.13 코드 리뷰 체크리스트

#### 읽기 경로

- 파일 이름을 이미 아는데 디렉터리를 열거하지 않는가?
- `exists()` 후 `open()`을 반복하지 않는가?
- 정상 경로와 repair scan이 분리되어 있는가?
- 같은 JSON을 레코드당 여러 번 읽지 않는가?
- cache key가 모델·프롬프트·전처리·schema 버전을 포함하는가?
- cache hit 전에 decode 같은 비싼 변환을 하지 않는가?
- manifest digest와 schema를 검증하는가?

#### 비동기 경로

- 이벤트 루프에서 동기 파일 I/O를 호출하지 않는가?
- executor queue와 worker 수가 명시적인가?
- CPU 작업과 I/O 작업의 풀 경계가 적절한가?
- task와 queue 수에 상한이 있는가?
- cancellation 시 결과 write future를 어떻게 다루는가?
- retry가 overload를 증폭하지 않는가?

#### 쓰기 경로

- shared mutable 파일마다 writer owner가 하나인가?
- 결과는 임시 파일에서 완성한 뒤 게시되는가?
- result publish가 checkpoint보다 먼저인가?
- batch flush의 RPO가 문서화되어 있는가?
- DONE이 모든 flush 뒤에 만들어지는가?
- partial worker file을 완료로 오인하지 않는가?

#### 분산 상태

- 같은 worker ID 중복 실행을 막는가?
- delta는 불변이고 checksum이 있는가?
- import가 idempotent한가?
- conflict를 조용히 버리지 않는가?
- coordinator가 모든 sentinel을 검증하는가?
- worker 0 조기 종료가 final snapshot을 누락시키지 않는가?

#### 재시작

- 모든 input mode에 resume filter가 적용되는가?
- force reprocess가 명시적인가?
- stale local scratch를 검증하거나 지우는가?
- 결과와 journal 불일치를 audit하는가?
- hard deadline 전 shutdown grace가 있는가?
- configuration 변경 시 기존 결과를 잘못 재사용하지 않는가?

---

## 20. 어디까지 일반화할 수 있는가

이 글의 패턴은 공유 POSIX-like 파일시스템 위의 작은 파일 중심 배치에 잘 맞는다. 그러나 스토리지 종류와 workload에 따라 결론이 달라질 수 있다.

### 20.1 모든 NAS와 NFS가 같지 않다

NFS 버전, mount option, client kernel, server implementation, metadata cache, delegation, directory layout에 따라 의미와 성능이 다르다. SMB, clustered NAS, parallel filesystem도 서로 다른 특성을 가진다.

따라서 다음 문장을 피한다.

```text
NAS에서 stat은 항상 X ms다.
스레드는 항상 128개가 최적이다.
rename은 모든 장애에서 완전한 transaction이다.
```

대신 요구 의미를 적고 실제 환경에서 검증한다.

```text
요구:
  같은 directory 안에서 temp→final replace를 reader가
  partial content 없이 보아야 한다.

검증:
  target mount, client versions, crash injection에서 테스트.
```

### 20.2 parallel filesystem에서는 다를 수 있다

Lustre, Spectrum Scale, BeeGFS 같은 HPC parallel filesystem은 data와 metadata를 분산하고 대규모 병렬 I/O 기능을 제공할 수 있다. 그래도 작은 파일과 metadata server 부하는 별도 고려가 필요하다. 디렉터리 striping, file layout, collective I/O 같은 filesystem-specific 기능이 도움이 될 수 있다.

이 글의 direct path, immutable object, single writer, bounded concurrency 원칙은 여전히 유용하지만 최적 숫자와 배치 형태는 달라진다. 시스템 관리자의 권장 layout과 공식 benchmark를 따른다. [IOR와 mdtest](https://github.com/hpc/ior)는 HPC I/O와 metadata 패턴을 측정하는 공개 도구다. 애플리케이션 microbenchmark와 함께 사용하면 저장소 계층과 코드 계층을 분리해서 볼 수 있다.

### 20.3 object storage에서는 API가 다르다

object storage에는 전통적인 디렉터리가 없고 prefix listing이 discovery 역할을 한다. `rename`이 copy+delete로 구현될 수 있다. 대신 conditional put, versioning, multipart upload, object checksum이 있다.

패턴을 다음처럼 번역한다.

| POSIX-like | Object storage |
|---|---|
| direct path open | exact object key GET |
| directory scan | prefix LIST |
| temp + rename | upload immutable content + pointer PUT |
| file sentinel | small READY object |
| worker directory | worker key prefix |
| fsync | provider durability contract |

LIST를 매 레코드마다 하지 않고 exact key 또는 manifest를 사용하는 원칙은 그대로다.

### 20.4 shared SQLite가 항상 금지인 것은 아니다

한 호스트의 한 process만 DB를 열고 NAS는 backup 보관에만 사용한다면 문제 성격이 다르다. network filesystem 위에서도 exclusive single client, 적절한 rollback journal, 구현 검증으로 사용할 수 있는 경우가 있다. SQLite 공식 문서도 상황별 선택지를 설명한다.

이 글이 피하는 것은 “여러 노드가 같은 SQLite 파일을 동시에 직접 읽고 쓰면서 local DB와 같은 성능·잠금 의미를 기대하는 구조”다. 요구가 단순하면 local SQLite + immutable export가 더 설명하기 쉽다는 판단이다.

### 20.5 디렉터리 열거가 올바른 경우

파일 집합 자체가 외부 입력이고 별도 manifest 생산자를 둘 수 없다면 listing은 필수다. 이때 다음을 최적화한다.

- scan 전용 단계로 격리
- directory fan-out
- 중복 scan 방지
- incremental change feed 또는 timestamp cursor
- bounded parallel scan
- scan 결과 manifest 게시
- 예외와 permission error 계수

`scandir`는 나쁜 API가 아니다. 이미 아는 파일 하나를 찾기 위해 반복적으로 사용하는 질문이 잘못된 것이다.

### 20.6 local staging이 항상 이득은 아니다

큰 input을 NAS에서 local로 전부 복사한 뒤 한 번만 읽으면 복사 비용만 추가된다. staging은 다음 조건에서 유리하다.

- 같은 데이터를 여러 epoch 또는 여러 단계가 반복 읽는다.
- 작은 random read를 local sequential form으로 바꾼다.
- local decode/cache가 재사용된다.
- NAS의 peak 부하를 시작 단계로 모아 제어할 수 있다.

다음 조건에서는 불리할 수 있다.

- 데이터가 local disk보다 크다.
- 한 번만 읽는다.
- 모든 worker가 같은 큰 파일을 중복 복사한다.
- stage-in이 동시에 시작되어 thundering herd를 만든다.
- local disk cleanup 실패가 잦다.

stage-in 시간까지 전체 job 지표에 포함해 비교한다.

### 20.7 checksum 비용도 측정한다

SHA-256은 integrity를 높이지만 큰 payload를 다시 읽어 계산하면 I/O와 CPU가 증가한다. 데이터를 처음 읽을 때 streaming digest를 함께 계산하거나 upstream manifest의 checksum을 신뢰할 수 있는 경계에서 재사용한다.

```python
digest = hashlib.sha256()
with path.open("rb") as file:
    while chunk := file.read(1024 * 1024):
        digest.update(chunk)
        consume(chunk)
```

작은 control file은 매번 검증해도 비용이 작다. 수TB result 전체를 매 resume마다 다시 hash하는 것은 다른 설계가 필요하다. part 단위 checksum과 Merkle root를 사용할 수 있다.

### 20.8 보안과 경로 안전성

매니페스트의 경로를 그대로 신뢰하면 path traversal이나 잘못된 mount 접근이 생길 수 있다. 공개 실습은 단순하지만 production에서는 root 아래 경로인지 검증한다.

```python
def resolve_under(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    root_resolved = root.resolve()
    if not candidate.is_relative_to(root_resolved):
        raise ValueError("path escapes dataset root")
    return candidate
```

symlink 정책도 명시한다. 다른 namespace를 가리키는 symlink를 허용할지, `O_NOFOLLOW`가 필요한지, 파일 owner와 permission이 맞는지 확인한다.

저널과 metrics에 secret, 원문 payload, 개인 식별자를 넣지 않는다. digest도 작은 추측 가능한 ID 공간에서는 역추적될 수 있으므로 threat model에 맞는 keyed hash를 사용한다.

### 20.9 garbage collection은 별도 프로토콜이다

immutable chunk와 attempt를 계속 만들면 저장 공간이 증가한다. “latest가 아니니 삭제”는 위험하다. reader가 이전 manifest를 사용 중이거나 rollback이 필요할 수 있다.

안전한 GC 흐름은 다음과 같다.

```text
1. live READY pointers와 보존할 run 목록 수집
2. 참조되는 manifest와 content graph 계산
3. minimum retention age 적용
4. 삭제 후보 manifest 게시
5. dry-run과 audit
6. bounded delete
7. 삭제 결과 기록
```

GC도 directory 전체를 무제한 scan하지 않도록 object catalog 또는 manifest graph를 사용한다.

### 20.10 더 단순한 시스템을 선택할 수 있다

데이터 규모가 작고 worker가 몇 개뿐이라면 복잡한 delta exchange가 필요 없다.

```text
작은 작업:
  worker별 JSONL + 종료 후 단일 merge

중간 작업:
  manifest + private journal + local cache

큰 반복 작업:
  manifest shards + bounded pipeline + immutable deltas

강한 실시간 일관성:
  검증된 client/server database or cache
```

규모와 실패 비용에 맞는 가장 단순한 구조를 선택한다.

---

## 21. 마무리

빠른 GPU와 빠른 네트워크를 갖춘 HPC에서 파이프라인이 느릴 때 GPU부터 의심하는 것은 자연스럽다. 그러나 모델 호출 전후의 작은 파일과 공유 상태가 시스템의 용량을 결정할 수 있다.

이 글에서 따라온 흐름을 다시 압축하면 다음과 같다.

```text
관찰:
  GPU queue가 비고 워커 처리량이 흔들린다.

측정:
  단계별 latency, 호출 수, queue, event-loop gap을 분리한다.

읽기:
  파일을 탐색하지 않고 알려진 경로를 직접 연다.
  반복 discovery를 불변 manifest로 치환한다.

비동기:
  blocking NAS I/O를 event loop에서 thread로 옮긴다.
  bounded queue와 단계별 concurrency로 backpressure를 만든다.

쓰기:
  작은 checkpoint를 batch한다.
  shared multi-writer를 worker별 single-writer journal로 분리한다.

복구:
  result를 먼저 게시하고 journal을 나중에 기록한다.
  DONE sentinel과 digest로 완료를 검증한다.

캐시:
  mutable SQLite는 worker local에 둔다.
  공유는 immutable delta chunk와 small pointer로 수행한다.

분산:
  건수가 아니라 예상 비용을 균등하게 나눈다.
  coordinator는 모든 worker 증거를 검증한 뒤 final manifest를 게시한다.
```

### 21.1 가장 중요한 다섯 가지 원칙

#### 첫째, 파일시스템에 이미 아는 사실을 다시 묻지 않는다

경로를 계산할 수 있으면 계산한다. 매니페스트에 파일 목록이 있으면 사용한다. 결과 파일이 정상 경로의 증거라면 모든 디렉터리를 먼저 스캔하지 않는다. repair가 필요한 예외만 비싼 탐색으로 보낸다.

#### 둘째, `async`라는 문법이 blocking I/O를 없애 주지 않는다

`async def` 안의 `open`, `stat`, `json.load`, SQLite transaction은 여전히 동기다. 이벤트 루프 heartbeat와 queue를 계측하고, 적절한 경계에서 `asyncio.to_thread` 또는 executor로 offload한다.

#### 셋째, 공유 경로와 공유 writer를 구분한다

모든 worker가 같은 mount를 보아도 같은 mutable file에 쓸 필요는 없다. 경로에 owner를 넣고 worker별 journal, worker별 delta, record별 result를 사용한다. 잠금을 정교하게 만드는 것보다 경쟁을 없애는 편이 단순하다.

#### 넷째, 완료는 파일 존재가 아니라 검증된 상태 전이다

부분 journal도 파일로 존재한다. 시작 시 만들어진 빈 worker file도 존재한다. 결과 publish, journal flush, DONE sentinel, coordinator validation의 순서를 명시한다. count, sequence, checksum이 맞아야 완료다.

#### 다섯째, 처리량은 성공률과 복구 비용을 포함해야 한다

검증을 생략하거나 실패를 버리면 records/s가 올라갈 수 있다. cache hit 비율이 바뀌면 GPU util과 requests/s의 의미도 달라진다. steady-state 처리량, retry, tail latency, conflict, sentinel, 전체 wall time을 함께 본다.

### 21.2 증상에서 패턴으로

| 증상 | 먼저 확인 | 자주 맞는 패턴 |
|---|---|---|
| GPU queue가 비어 있음 | eligible, cache hit, 앞단 queue | direct path, lazy decode, I/O offload |
| async 동시성을 올려도 느림 | event-loop gap, executor queue | `to_thread`, pool 분리 |
| NAS op가 폭증 | record당 stat/scandir/open count | manifest, EAFP, batch |
| 완료 ID가 결과보다 적음 | shared append, journal digest | worker별 journal |
| cache가 worker마다 다름 | master overwrite, sync order | immutable delta |
| worker 시간이 몇 배 차이 | bytes, miss, segments | weighted shard/chunks |
| 종료 후 merge가 누락됨 | sentinel, coordinator timing | all-worker validation |
| 재시작이 전체를 다시 처리 | resume path, fingerprint | 공통 done filter, idempotency |

### 21.3 성능 최적화의 순서

마지막으로 순서를 기억해 두면 좋다.

1. 정확성 불변식을 세운다.
2. 같은 단위와 측정 창을 고정한다.
3. 불필요한 일을 제거한다.
4. 남은 독립 대기를 제한된 동시성으로 겹친다.
5. 작은 쓰기를 batch하고 owner를 분리한다.
6. 재시작과 failure injection으로 검증한다.
7. 그 다음에 모델 서버와 GPU 내부 최적화를 한다.

입력 파이프라인이 GPU에 충분한 일을 공급하고 결과가 안전하게 게시된다는 증거가 있어야 GPU tuning 결과도 해석할 수 있다.

### 21.4 한 문장 결론

> 공유 NAS 위의 대규모 AI 파이프라인은 “파일을 많이 읽는 프로그램”이 아니라 “원격 메타데이터와 분산 상태를 조율하는 시스템”으로 설계해야 한다.

그 관점으로 바꾸면 `scandir` 한 줄, `exists()` 한 번, `done.txt` append 하나가 왜 중요한지 보인다. 더 나아가 성능과 복구를 별개의 사후 작업이 아니라 같은 데이터 모델의 두 결과로 다룰 수 있다.

---

## 부록 A: 세 개의 디버깅 타임라인

앞 장들은 원칙별로 정리했다. 실제 장애 분석은 그렇게 정돈된 순서로 진행되지 않는다. 관측이 모순되어 보이고, 먼저 세운 가설이 틀리며, 한 병목을 없애면 다음 병목이 드러난다. 일반화한 세 개의 타임라인으로 그 과정을 재구성해 보자.

### A.1 타임라인 1: 검증 스크립트가 결과 파일보다 디렉터리를 더 오래 보았다

#### 초기 상황

대규모 추론 결과의 완전성을 확인하는 검증 스크립트가 있었다. 레코드마다 다음 세 가지를 확인했다.

1. `meta.json`을 읽는다.
2. 입력 이미지가 하나 이상 있는지 디렉터리를 스캔한다.
3. `result.json`을 읽고 성공 여부를 판정한다.

소규모 개발 데이터에서는 문제가 없었다. 전체 데이터로 확장하자 검증만 수십 분 이상 걸렸다. 검증은 모델을 호출하지 않고 작은 JSON을 읽을 뿐이므로 CPU가 느리거나 JSON parser가 문제라고 생각하기 쉬웠다.

#### 첫 가설

“JSON parsing이 단일 스레드라 느리다. 프로세스를 늘리면 된다.”

프로세스를 늘리는 실험은 일부 개선될 수 있지만 NAS에 더 많은 directory operation을 동시에 보낸다. 공유 서버가 포화되면 p95가 커지고 전체 효율이 낮아진다. 더구나 불필요한 일을 병렬화했을 뿐이다.

#### 계측

레코드당 단계를 분리했다.

```text
meta open/read
directory scan + file-type check
result open/read
JSON parse
```

데이터 bytes는 작았고 JSON parse는 매우 짧았다. directory scan이 정상 경로의 큰 비중을 차지했다. 레코드 디렉터리를 한 번씩만 방문해 directory cache 재사용도 제한적이었다.

#### 의미를 다시 본다

검증이 묻는 질문은 “이미지가 실제로 있는가?”였다. 그러나 성공한 `result.json`은 파이프라인이 적어도 하나의 유효 입력을 읽고 처리했다는 강한 증거였다. 입력 목록도 `meta.json`에 있었다.

정상 결과가 있는 레코드에서 다시 directory scan을 수행할 필요가 없었다.

#### 변경

```text
Before:
  meta → scan inputs → result

After:
  meta → result
             └─ result missing/error인 예외만 input repair scan
```

정상 경로에서는 알려진 두 파일을 직접 열었다. 디렉터리 탐색은 missing을 `no-input`, `lost-result`, `naming-mismatch`로 세분해야 하는 작은 repair queue로 이동했다.

#### 검증

- old/new의 최종 분류 count가 일치하는지 확인했다.
- 일부 missing 샘플에서 repair scan 결과가 같음을 확인했다.
- `scandir` counter 감소를 측정했다.
- 전체 wall time과 metadata p95를 비교했다.
- repair queue 비율이 예상보다 높으면 최적화를 승격하지 않도록 했다.

#### 배운 점

성능 개선은 API 교체보다 정보의 중복을 찾는 문제였다. `scandir`가 느리다는 지식만으로는 충분하지 않다. 결과 파일과 메타데이터가 이미 어떤 사실을 증명하는지 이해해야 호출 자체를 제거할 수 있다.

### A.2 타임라인 2: 동시성은 큰데 요청이 직렬로 도착했다

#### 초기 상황

비동기 모델 client가 많은 코루틴을 실행했다. 설정상 동시성은 충분했고 서버도 더 많은 요청을 받을 수 있었다. 그러나 초반 skip 구간이 끝나자 처리량이 급격히 떨어졌다. timeout과 retry가 늘었고 모델 queue는 예상만큼 차지 않았다.

#### 첫 가설

“모델 서버의 실행 슬롯이 작다. client concurrency를 더 높이자.”

동시성을 높이면 잠깐 queue가 커졌지만 timeout도 늘었다. 최초 요청과 retry가 섞여 offered load가 증가했고 안정 처리량은 좋아지지 않았다.

#### 단계 계측

한 코루틴 안에는 다음 동기 호출이 있었다.

```text
result exists
meta open/read
output mkdir
result write
done append
```

각 호출은 작았지만 이벤트 루프 스레드에서 실행됐다. heartbeat를 추가하자 NAS tail latency가 발생할 때 loop gap도 같이 커졌다. 모델 응답 callback과 timeout 처리까지 지연되었다.

#### 변경 1: I/O 경계 묶기

`exists`와 `open`을 따로 offload하지 않았다. 정상 경로에서 EAFP로 읽는 동기 함수 하나를 만들었다.

```python
def load_meta(path):
    try:
        with path.open() as file:
            return json.load(file)
    except FileNotFoundError as error:
        raise PermanentInputError(path) from error
```

코루틴에서는 `await asyncio.to_thread(load_meta, path)`를 사용했다. 결과 쓰기도 atomic write 함수로 묶어 offload했다.

#### 변경 2: 동시성 정렬

client concurrency와 모델 server capacity를 같은 단위로 맞췄다. 코루틴 수, executor thread 수, 서버 실행 슬롯, retry queue를 별도로 기록했다. 모델 요청 semaphore와 metadata I/O semaphore를 나눴다.

#### 변경 3: 체크포인트 batch

매 레코드의 done open/write/close를 worker-private buffer로 바꿨다. result publish 후 journal event를 추가하고 count 또는 시간 기준으로 flush했다.

#### 결과 해석

I/O offload만으로 모든 성능이 해결된 것은 아니었다. 이후에는 모델 서버 내부 설정이 다음 병목으로 드러났다. 중요한 점은 최적화 전후의 전장이 달라졌다는 것이다.

```text
Before:
  event loop + NAS path가 모델을 굶김

After:
  input queue가 안정적으로 차고 모델 capacity가 상한
```

이 상태에서야 모델 서버의 batch, scheduling, GPU kernel을 조정한 결과를 올바르게 해석할 수 있었다.

#### 배운 점

`async` 코드의 동시성 설정은 실제 동시성을 증명하지 않는다. event-loop gap과 단계별 queue가 증거다. 또 하나의 변경으로 얻은 전체 개선을 그 변경 하나의 기여로 돌리면 안 된다. NAS 경로 개선과 모델 서버 개선의 ablation을 분리해야 한다.

### A.3 타임라인 3: 계산 결과는 있었지만 완료 목록은 비어 있었다

#### 초기 상황

여러 worker가 각자 고유한 result path에 결과를 썼다. 작업 종료 후 결과 파일 샘플은 정상인데, 공유 `done.txt`의 unique ID 수가 예상보다 훨씬 작았다. 파일 안에는 비정상 bytes와 누락된 구간이 있었다.

별도의 cache snapshot도 worker마다 시작 시 같은 master를 local로 복사하고 종료 시 master에 덮어썼다. DB 파일 자체는 정상적으로 열렸지만 일부 worker가 만든 cache entry가 보이지 않았다.

#### 첫 가설

“프로세스가 flush하지 않고 종료했거나 로그 집계가 잘못됐다.”

모든 worker가 정상 종료했고 각 worker의 application count 합은 결과 파일 수와 대체로 맞았다. 문제는 계산이 아니라 공유 상태 게시였다.

#### 두 가지 실패 모델

```text
done.txt:
  여러 client가 한 append stream을 동시에 변경
  → 손상 또는 누락

cache master:
  각 client가 서로 다른 full snapshot을 같은 이름에 게시
  → last-writer-wins
```

둘은 표면이 다르지만 ownership이 없다는 공통 원인이 있었다.

#### 즉시 복구

손상된 done 파일을 유일한 진실로 신뢰하지 않았다. 고유 result path와 result 내용을 샤드 단위로 검증해 완료 manifest를 재구축했다. 이 작업은 metadata 비용이 크므로 일회성 audit job으로 실행하고 새 compact manifest를 게시했다.

#### 구조 변경

```text
done:
  shared append → worker-private journal + DONE

cache:
  shared master overwrite
  → local SQLite + worker-owned immutable delta

final:
  worker 0 즉시 merge
  → all-worker sentinel 검증 후 coordinator merge
```

#### 실패 주입

- worker 하나를 flush 전에 종료했다.
- journal 뒤에 bytes를 추가해 digest mismatch를 확인했다.
- delta chunk를 변조해 import 거부를 확인했다.
- worker 0이 먼저 끝나는 순서로 실행해 coordinator가 다른 worker를 기다리는지 확인했다.
- 같은 chunk를 두 번 import해 중복이 생기지 않는지 확인했다.

#### 배운 점

진행률 파일은 부가 로그가 아니다. 다음 실행의 작업 집합과 비용을 결정하는 데이터다. 그러나 결과보다 먼저 기록되어서는 안 되며 결과와 독립적으로 검증 가능해야 한다. 단일 writer와 불변 게시가 성능 최적화인 동시에 정확성 설계였다.

### A.4 세 타임라인의 공통 구조

세 문제는 다음 순서로 해결되었다.

```text
겉으로 보이는 증상
  → 호출과 상태 전이를 계측
  → 먼저 믿은 가설을 반증
  → 이미 가진 정보를 재사용
  → owner와 불변 경계를 명시
  → 작은 canary와 실패 주입
  → 다음 병목으로 이동
```

이 구조는 특정 스토리지나 모델에 종속되지 않는다. 대규모 크롤링, 데이터 전처리, feature extraction, checkpoint 변환, 평가 파이프라인에서도 같은 방식으로 적용할 수 있다.

---

## 부록 B: 설계 결정 기록 템플릿

성능과 복구 관련 결정은 시간이 지나면 이유가 사라진다. “스레드 32”, “flush 100”, “worker별 파일” 같은 숫자와 구조만 남으면 다음 개발자가 임의로 바꾸거나 cargo cult로 복사한다. 작은 ADR(Architecture Decision Record)을 남긴다.

### B.1 템플릿

```markdown
# ADR: worker-private checkpoint journal

## 상태
accepted

## 맥락
- 여러 계산 노드가 shared filesystem을 사용한다.
- 결과 객체는 record별 single writer다.
- 기존 shared append log는 concurrent writer를 가진다.
- resume은 완료 ID를 빠르게 읽어야 한다.

## 결정
- run/attempt/worker namespace를 경로에 포함한다.
- worker는 private JSONL journal만 append한다.
- result publish 뒤 journal event를 기록한다.
- count 또는 5초 기준으로 batch flush한다.
- worker 종료 시 digest/count/sequence가 있는 DONE을 게시한다.
- coordinator는 모든 DONE을 검증한 뒤 final manifest를 게시한다.

## 대안
1. shared append + file lock
2. 결과 tree 전수 scan
3. 중앙 database

## 결과
- shared write contention과 append 의미 의존을 제거한다.
- 파일 수와 coordinator merge 단계가 증가한다.
- crash 직전 buffer는 journal에 없을 수 있으나 result audit로 복구한다.

## 검증
- incomplete worker exclusion test
- tamper detection test
- duplicate worker ID test
- full-scale metadata impact

## 재검토 조건
- worker 수가 현재 범주를 크게 초과한다.
- strong real-time progress consistency가 필요하다.
- client/server state service가 표준으로 제공된다.
```

### B.2 기록해야 할 숫자의 이유

`metadata_threads=32`라고만 쓰지 않는다.

```text
후보: 8, 16, 32, 64
입력: 동일 manifest shard
관측: 32까지 throughput 증가, 64에서 p95와 retry 증가
선택: 32
guardrail: cluster 전체 offered load가 당시 canary 범주 이내
재측정: node 수 또는 mount 변경 시
```

`flush_records=100`도 RPO와 연결한다.

```text
선택 이유:
  open/write 고정비를 약 두 자릿수 배 이상 줄이면서
  crash 시 journal 미반영 완료를 최대 99개로 제한.

복구:
  result index에서 journal을 보정할 수 있음.
```

### B.3 대안을 공정하게 남긴다

기각한 대안이 언제나 나쁜 것은 아니다.

#### 중앙 database

장점:

- 강한 transaction
- query와 progress dashboard
- worker lease와 compare-and-set

기각한 현재 이유:

- 폐쇄 배치 환경에서 별도 서비스 운영 비용
- cache는 eventual consistency로 충분
- 결과 자체는 immutable files

재검토 조건:

- 실시간 global dedupe가 중요해짐
- worker 수와 동기화 빈도가 크게 증가
- 조직 표준 managed database가 제공됨

#### 결과 tree 전수 scan

장점:

- result object가 진실의 원본
- 별도 checkpoint protocol이 단순

기각한 현재 이유:

- 수백만 object의 매 resume metadata 비용
- 부분 파일과 version 검증 필요

사용 위치:

- 주기 audit와 재구축
- journal 손상 복구

이렇게 남기면 “왜 database를 안 썼지?”라는 질문에 당시 제약과 미래 변경 조건으로 답할 수 있다.

### B.4 성능 결정의 만료 조건

성능 최적값은 영구 사실이 아니다. 다음 변화가 있으면 ADR을 재검토한다.

- 파일시스템 제품 또는 protocol version 변경
- mount option 변경
- node 수와 worker topology 변경
- 평균 payload 크기와 파일 수 변화
- cache hit 분포 변화
- Python version과 executor 기본값 변화
- model server capacity 변화
- local scratch 종류 변화
- security/durability 요구 변화

ADR에 `last_validated`와 benchmark artifact digest를 둔다.

```yaml
decision: metadata-concurrency
value: 32
last_validated: 2026-07-20
benchmark_manifest_sha256: "..."
environment_class: "shared-posix-small-file"
```

### B.5 블로그를 운영 문서로 오해하지 않는다

이 글은 원리와 재현 코드다. 실제 production 값은 내부 runbook과 ADR에 있어야 한다. 공개 글의 예시 숫자를 운영 기본값으로 복사하지 않는다. 반대로 내부 문서에 특정 경로와 명령만 남기고 원리를 생략하면 환경 변경 때 판단하기 어렵다.

두 문서의 역할을 나눈다.

```text
공개 기술 글:
  원리, 비용 모델, 일반 패턴, 합성 코드, 참고문헌

내부 runbook:
  실제 mount, scheduler, owner, redline, 연락 체계

ADR:
  선택 이유, 대안, 측정 artifact, 재검토 조건
```

이 구분은 기밀을 보호하면서도 기술 경험을 잃지 않는 방법이기도 하다.

---

## 부록 C: 자주 묻는 질문

### C.1 NAS 대역폭이 충분한데 왜 작은 파일이 느린가

대역폭은 많은 bytes를 지속적으로 전송할 때의 용량이다. 작은 파일 workload는 이름 조회, 속성 확인, open, close 같은 고정 비용을 파일마다 반복한다. 전송 bytes가 작아 \(B/BW\) 항이 작아도 왕복 횟수 × RTT가 크다.

같은 총 1GB라도 1GB 파일 하나와 1KB 파일 백만 개는 다른 workload다. 전자는 data throughput, 후자는 metadata operation rate와 tail latency를 먼저 본다.

### C.2 스레드를 늘리면 언제나 빨라지는가

아니다. 스레드는 독립적인 I/O 대기를 겹쳐 latency를 숨긴다. 서버가 포화되거나 client의 file descriptor, 메모리, context switch가 병목이면 더 늘려도 처리량이 증가하지 않는다. 오히려 p95, timeout, retry가 커진다.

8, 16, 32, 64처럼 sweep하고 cluster 전체 동시성을 계산한다. 최고 순간 처리량보다 장시간 안정 처리량과 다른 workload 영향으로 선택한다.

### C.3 missing 파일이 많아도 EAFP가 좋은가

hit/miss 분포에 따라 다르다. 파일이 거의 항상 존재하면 `exists + open`의 정상 경로 두 호출보다 직접 open이 유리할 가능성이 높다. 파일이 대부분 없고 실패 open이 비싸다면 manifest나 in-memory index로 먼저 거르는 편이 나을 수 있다.

중요한 것은 “Python에서는 EAFP가 관용구다”라는 스타일 규칙이 아니라 target filesystem에서 정상 경로 호출 수와 분포를 측정하는 것이다.

### C.4 결과 존재를 확인하기 위해 `stat` 한 번 정도는 괜찮지 않은가

한 번의 비용이 아니라 전체 증폭을 계산한다.

```text
records × stages × attempts × workers-that-scan
```

재시작 때 한 번 수행하는 수천 건 `stat`은 괜찮을 수 있다. hot path에서 수천만 레코드마다 같은 result를 두 번 확인하면 큰 비용이 된다. 이미 validated done set이 있다면 메모리 lookup을 사용한다. 정기 audit에서는 result object를 실제로 검증한다.

### C.5 Python에 진짜 비동기 파일 I/O가 없어서 thread를 쓰는 것인가

일반 파일은 socket처럼 모든 플랫폼에서 동일한 `asyncio` 비동기 API를 제공하지 않는다. `asyncio.to_thread`는 blocking I/O를 이벤트 루프에서 분리하는 이식성 높은 방법이다. Linux io_uring이나 filesystem-specific async API를 쓰는 library도 있지만 지원 범위, cancellation, buffered I/O 의미를 검증해야 한다.

표준 thread offload로 병목이 충분히 해결되지 않고 profile이 근거를 줄 때 더 낮은 수준의 API를 검토한다.

### C.6 매 레코드마다 `fsync`하면 가장 안전하지 않은가

내구성 창은 줄지만 비용이 매우 높을 수 있다. 또한 result와 journal 두 파일 사이의 transaction이 생기는 것은 아니다. result가 영속화되기 전에 journal만 `fsync`하면 잘못된 완료가 남을 수 있다.

업무 요구의 RPO를 정하고 result publish → journal 순서를 지킨다. 재처리 가능한 배치라면 batch/time flush와 result audit가 합리적일 수 있다. 거래성 상태라면 파일 두 개 대신 transaction을 제공하는 시스템이 맞을 수 있다.

### C.7 `flock`이나 file lock으로 shared append를 고치면 안 되는가

target filesystem과 모든 client에서 lock 의미가 검증되고 성능이 충분하다면 가능하다. 하지만 lock 획득 RTT, holder crash, stale lock, 구현별 의미를 운영해야 한다. 완료 이벤트처럼 자연스럽게 분할 가능한 상태는 worker-private file이 더 단순하다.

잠금은 공유 mutation이 본질적일 때 사용한다. 단순히 기존 파일 이름을 유지하기 위해 분산 잠금을 추가하지 않는다.

### C.8 Redis나 중앙 database가 더 낫지 않은가

강한 실시간 일관성, compare-and-set, TTL, query, lease가 필요하면 더 낫다. 반면 폐쇄형 batch cluster에서 별도 서비스 운영이 어렵고 상태가 재구성 가능한 cache라면 immutable file exchange가 충분할 수 있다.

결정 기준은 익숙한 기술이 아니라 필요한 일관성, 장애 허용, 운영 인력, 네트워크 경계다. ADR에 재검토 조건을 남긴다.

### C.9 exactly-once를 구현해야 중복 계산이 사라지지 않는가

외부 모델 호출, 파일 result, journal이 하나의 transaction에 있지 않으면 엄밀한 exactly-once는 어렵다. worker가 모델 응답을 받은 뒤 result publish 전에 죽거나, result publish 뒤 journal 전에 죽을 수 있다.

실용적 패턴은 at-least-once delivery와 idempotent result key다. 중복 계산은 허용하되 같은 fingerprint와 content로 수렴하게 만들고, 다른 content는 conflict로 탐지한다. 중복 비용이 매우 크면 중앙 lease/dedupe 서비스를 추가할 수 있다.

### C.10 cache hit가 높아 GPU 사용률이 낮으면 문제인가

반드시 그렇지 않다. cache가 유효한 결과를 재사용해 모델 호출을 줄였다면 낮은 GPU 사용률과 높은 record 완료 처리량이 동시에 나타날 수 있다. 목적 함수가 결과 완료라면 성공이다.

문제는 cache miss 작업이 있는데도 payload read나 decode가 느려 GPU에 도달하지 못하는 경우다. hit/miss를 분리하고 miss-only queue와 throughput을 본다.

### C.11 파일 수를 줄이기 위해 결과를 하나의 큰 파일에 쓰면 되는가

파일 수와 metadata 부담은 줄지만 shared writer, failure isolation, resume offset 문제가 생긴다. 여러 worker가 한 파일에 직접 쓰지 말고 worker별 immutable part를 만든 뒤 final index로 묶는다.

part 크기는 downstream read와 재처리 단위의 절충이다. 마지막 미완성 part를 검증할 수 있도록 count, checksum, footer 또는 side manifest를 둔다.

### C.12 atomic rename만 쓰면 crash safety가 완성되는가

rename은 reader가 partial 최종 파일을 보는 문제를 줄인다. 그러나 temp content가 durable한지, 부모 directory entry가 crash 후 남는지, source와 destination이 같은 filesystem인지, object storage에서 rename이 실제 원자인지는 별도 문제다.

요구 durability에 따라 file과 parent directory `fsync`, checksum, recovery scan을 사용하고 target 환경에서 실패 주입으로 검증한다.

### C.13 모델 최적화는 언제 시작해야 하는가

모델 queue가 안정적으로 공급되고, 앞뒤 단계 시간이 설명되며, retry가 통제된 뒤다. GPU queue가 비어 있는데 tensor parallel이나 kernel만 바꾸면 workload가 GPU에 도달하지 않는 원인을 가릴 수 있다.

그렇다고 NAS 문제를 모두 완벽하게 해결할 때까지 GPU를 보지 말라는 뜻은 아니다. 단계별 capacity를 측정하고 현재 가장 작은 상한을 순서대로 올린다. 병목은 이동한다.

### C.14 공개 글에서 경험의 구체성을 잃지 않고 기밀을 빼는 방법은 무엇인가

서비스명과 원시 경로를 삭제하는 것만으로 부족하다. 정확한 데이터 수, 노드 수, 장비 조합, 시간, 처리량을 함께 공개하면 시스템이 식별될 수 있다.

다음 세 층으로 분리한다.

```text
공개:
  문제 형태, 비용 모델, 일반 패턴, 합성 코드,
  normalized 비교와 공개 참고문헌

내부:
  실제 topology, mount, job, raw benchmark,
  장애 ID와 담당 체계

연결:
  내부 ADR이 공개 원칙을 참조하되
  공개 문서가 내부 식별자를 참조하지 않음
```

코드는 회사 함수명을 일부 바꾸는 방식이 아니라 같은 불변식을 구현하는 독립 예제로 새로 작성한다. 이렇게 해야 독자도 문맥 없이 실행할 수 있고 기밀 경계도 선명하다.

---

## 실습 코드

전체 실행 방법은 [`code/README.md`](code/README.md)에 있다.

```bash
cd code

python nas_io_lab.py prepare \
  --root /tmp/nas-lab \
  --records 3000

python nas_io_lab.py benchmark \
  --root /tmp/nas-lab \
  --records 3000 \
  --rounds 3

python async_pipeline.py \
  --root /tmp/nas-lab \
  --records 1000 \
  --concurrency 64 \
  --injected-io-latency-ms 3 \
  --compare

python checkpoint_protocol.py demo \
  --root /tmp/checkpoint-lab \
  --workers 4 \
  --records 1000

python delta_cache.py demo \
  --root /tmp/delta-cache-lab

python -m unittest -v
```

실제 NAS 벤치마크는 별도 테스트 디렉터리와 보수적인 동시성으로 시작해야 한다. 합성 fixture 생성도 metadata 부하를 만든다.

---

## 참고 자료

1. Python Software Foundation, [`os.scandir()` and `os.DirEntry`](https://docs.python.org/3/library/os.html#os.scandir). `scandir`가 Unix에서 `opendir/readdir`를 사용하며 `DirEntry` 메서드가 추가 시스템 호출을 수행할 수 있는 조건을 설명한다.
2. Python Software Foundation, [`asyncio.to_thread()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread). 이벤트 루프를 막는 I/O-bound 함수를 별도 스레드에서 실행하는 표준 API다.
3. Python Software Foundation, [`ThreadPoolExecutor`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor). worker 상한, 종료 의미, 버전별 기본 worker 수를 설명한다.
4. Michael Kerrisk and Linux man-pages project, [`open(2)`](https://man7.org/linux/man-pages/man2/open.2.html). `O_APPEND`의 로컬 의미와 NFS에서 concurrent append를 모사할 때의 경쟁 조건을 설명한다.
5. The Open Group, [`open()`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/open.html) and [`write()`](https://pubs.opengroup.org/onlinepubs/9699919799/functions/write.html). POSIX file offset, append, write 의미의 기준이다.
6. IETF, [RFC 8881: Network File System Version 4 Minor Version 1 Protocol](https://www.rfc-editor.org/rfc/rfc8881). NFSv4.1의 state, locking, caching, directory와 attribute 의미를 정의한다.
7. SQLite, [SQLite Over a Network, Caveats and Considerations](https://www.sqlite.org/useovernet.html). network filesystem 위 remote SQLite의 latency, sync, locking, reliability trade-off를 설명한다.
8. SQLite, [How To Corrupt An SQLite Database File](https://www.sqlite.org/howtocorrupt.html). filesystem lock이 기대와 다를 때 multi-process 접근에서 발생할 수 있는 문제를 다룬다.
9. HPC I/O Benchmark Repository, [IOR and mdtest](https://github.com/hpc/ior). data I/O와 metadata 성능을 측정하는 공개 HPC benchmark suite다.
10. Linux kernel documentation, [Network Filesystem Caching API](https://docs.kernel.org/filesystems/caching/netfs-api.html). network filesystem의 local caching과 coherency를 이해하는 참고 자료다.

---

## 공개 범위에 대한 메모

이 글은 실제 업무에서 얻은 원리와 실패 패턴을 공개 가능한 형태로 재구성했다. 특정 회사, 서비스, 저장소 제품, mount path, scheduler job, node, 모델, 데이터셋을 식별할 수 있는 값은 포함하지 않았다. 본문의 숫자 예시는 형식을 설명하기 위한 합성 값이며 특정 조직의 운영 수치를 나타내지 않는다. 코드 역시 같은 문제를 독립적으로 재현하기 위해 새로 작성한 교육용 구현이다.
