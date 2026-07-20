---
title: "공유 NAS 위의 대규모 AI 파이프라인: 메타데이터 I/O부터 재시작 가능한 분산 쓰기까지"
date: 2026-07-20
description: "빠른 GPU가 작은 파일을 기다리는 이유부터 안전한 완료 기록과 재시작 설계까지, 공유 NAS 기반 AI 파이프라인을 그림과 실행 코드로 차근차근 설명한다."
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

비싼 GPU가 아무 일도 하지 않고 기다리는 모습을 본 적이 있다. 처음에는 GPU 설정이 잘못되었다고 생각하기 쉽다. 하지만 원인은 뜻밖에도 GPU가 읽어야 할 작은 파일을 찾는 코드였다. 파일 하나를 찾는 데 걸리는 시간은 짧았다. 그 짧은 일을 아주 많이 반복하자 GPU에 전달할 일감이 끊겼다.

이 글은 그 문제를 처음부터 천천히 풀어 간다. 어려운 이름이 나오면 바로 뜻을 설명하고, 먼저 그림과 비유로 이해한 뒤 코드로 확인한다. 수식은 결론을 외우기 위한 것이 아니라 “무엇을 줄이면 빨라지는가”를 짧게 적는 도구로만 사용한다.

먼저 전체 상황을 한 장으로 보자.

![여러 계산 노드와 공유 NAS로 구성된 HPC의 단순화된 구조](assets/hpc-shared-storage.svg)

*그림 1. 운영 문서에 있던 클러스터 구성도를 공개용으로 다시 그렸다. 실제 제품명, 장비 수, 속도는 제거했다.*

여기에는 여러 대의 계산 노드가 있다. **계산 노드**는 CPU, 메모리, GPU가 들어 있는 컴퓨터 한 대라고 생각하면 된다. **스케줄러**는 큰 작업을 어느 노드에 맡길지 정하는 배정 담당자다. 각 노드는 자기 안의 빠른 임시 저장소를 사용할 수 있고, 모두가 함께 보는 **공유 NAS**에도 접근할 수 있다.

NAS는 `Network Attached Storage`의 줄임말이다. 어렵게 들리지만 뜻은 단순하다. “네트워크 너머에 있고, 여러 컴퓨터가 함께 쓰는 저장소”다. 내 컴퓨터 안의 SSD를 열 때와 달리, NAS 파일을 열 때는 네트워크로 요청을 보내고 답을 받아야 한다.

이 구조에서 기억할 이야기는 다섯 문장뿐이다.

1. GPU는 앞 단계가 만들어 준 일만 처리할 수 있다.
2. NAS의 작은 파일은 내용보다 “어디 있는지 묻는 횟수”가 더 비쌀 수 있다.
3. `async`라는 글자가 붙어도 동기 파일 읽기를 직접 하면 다른 작업이 함께 멈춘다.
4. 여러 작업자가 한 파일에 동시에 쓰면 기록이 섞이거나 사라질 수 있다.
5. 그래서 알려진 경로로 바로 읽고, 기다림은 제한해서 겹치고, 쓰는 파일의 주인은 한 명으로 정한다.

나머지 내용은 이 다섯 문장을 코드와 실험으로 확인하는 과정이다.

### 이 글에서 자주 만날 이름

전문용어를 먼저 모두 외울 필요는 없다. 아래 네 단어만 눈에 익혀 두면 본문을 훨씬 편하게 읽을 수 있다.

| 이름 | 이 글에서 쓰는 쉬운 뜻 |
|---|---|
| 지연 시간, latency | 한 번의 일이 끝날 때까지 기다린 시간 |
| 처리량, throughput | 일정 시간 동안 끝낸 일의 개수 |
| RTT, round-trip time | 요청을 보내고 답을 받기까지의 왕복 시간 |
| 메타데이터, metadata | 파일 내용이 아니라 이름, 경로, 크기, 수정 시각 같은 정보 |

예를 들어 파일 한 개를 여는 데 5밀리초가 걸렸다면 5밀리초는 **지연 시간**이다. 1초 동안 파일 200개를 열었다면 초당 200개는 **처리량**이다. 둘은 관련 있지만 같은 숫자는 아니다. 여러 요청을 겹치면 각 요청의 지연 시간이 그대로여도 전체 처리량은 올라갈 수 있다.

### 글을 읽는 방법

각 장은 같은 순서를 따른다.

```text
무슨 일이 생겼는가
  → 익숙한 비유로 보면 무엇인가
  → 컴퓨터에서는 어떤 호출이 일어나는가
  → 코드로 어떻게 바꾸는가
  → 언제 이 방법을 쓰지 말아야 하는가
```

코드를 모두 실행하지 않아도 흐름은 이해할 수 있다. 직접 확인하고 싶은 독자를 위해 모든 실습은 [`code/README.md`](code/README.md)에 실행 순서대로 모아 두었다. Python 표준 라이브러리만 사용하고, 실제 업무 코드나 운영 데이터는 들어 있지 않다.

본문의 경험은 대규모 오프라인 AI 파이프라인에서 반복해서 만난 문제를 일반화한 것이다. 회사명, 서비스명, 스토리지 제품명, 내부 경로, 노드 이름, 작업 ID, 정확한 데이터 규모와 원시 처리량은 제거하거나 범주화했다. 그림도 운영 이슈에서 사용한 핵심 관계만 남겨 공개용으로 다시 그렸다. 특정 장비의 성능을 주장하려는 글이 아니라, 독자가 자기 환경에서 같은 질문을 검증할 수 있게 만드는 글이다.

---

## 목차

1. [GPU가 놀고 있는데 GPU 문제가 아니었다](#1-gpu가-놀고-있는데-gpu-문제가-아니었다)
2. [큰 짐과 안내표를 나눠 본다: 데이터 평면과 제어 평면](#2-큰-짐과-안내표를-나눠-본다-데이터-평면과-제어-평면)
3. [작은 질문의 비용을 센다: RTT와 메타데이터 I/O](#3-작은-질문의-비용을-센다-rtt와-메타데이터-io)
4. [무엇을 세는지 먼저 정한다](#4-무엇을-세는지-먼저-정한다)
5. [실습 1: 파일을 찾지 말고 주소를 계산하라](#5-실습-1-파일을-찾지-말고-주소를-계산하라)
6. [매니페스트: 미리 만든 파일 주소록](#6-매니페스트-미리-만든-파일-주소록)
7. [`async` 안에서 파일을 읽으면 왜 줄이 멈추는가](#7-async-안에서-파일을-읽으면-왜-줄이-멈추는가)
8. [한꺼번에 너무 많이 시키지 않는다: 제한된 동시성](#8-한꺼번에-너무-많이-시키지-않는다-제한된-동시성)
9. [한 줄씩 쓰지 말고 모아서 쓴다](#9-한-줄씩-쓰지-말고-모아서-쓴다)
10. [한 파일에 여러 사람이 동시에 쓰면](#10-한-파일에-여러-사람이-동시에-쓰면)
11. [워커마다 자기 저널과 완료 도장을 둔다](#11-워커마다-자기-저널과-완료-도장을-둔다)
12. [SQLite는 각 노드 안에 둔다](#12-sqlite는-각-노드-안에-둔다)
13. [전체 DB 대신 새 내용만 교환한다: 불변 델타](#13-전체-db-대신-새-내용만-교환한다-불변-델타)
14. [다시 실행해도 결과가 같게 만든다: 멱등성](#14-다시-실행해도-결과가-같게-만든다-멱등성)
15. [건수보다 실제 일의 무게를 나눈다](#15-건수보다-실제-일의-무게를-나눈다)
16. [어디서 막혔는지 보이게 만든다](#16-어디서-막혔는지-보이게-만든다)
17. [효과를 믿을 수 있게 실험한다](#17-효과를-믿을-수-있게-실험한다)
18. [모든 조각을 이어 붙인 참조 구조](#18-모든-조각을-이어-붙인-참조-구조)
19. [서비스를 멈추지 않고 옮기는 순서](#19-서비스를-멈추지-않고-옮기는-순서)
20. [이 방법이 맞지 않는 경우](#20-이-방법이-맞지-않는-경우)
21. [마무리](#21-마무리)
22. [부록 A: 세 개의 디버깅 타임라인](#부록-a-세-개의-디버깅-타임라인)
23. [부록 B: 설계 결정 기록 템플릿](#부록-b-설계-결정-기록-템플릿)
24. [부록 C: 자주 묻는 질문](#부록-c-자주-묻는-질문)

---

## 1. GPU가 놀고 있는데 GPU 문제가 아니었다

여러 계산 노드에서 AI 작업을 돌렸는데 GPU 사용률이 자꾸 바닥으로 내려갔다. GPU가 일하는 구간과 쉬는 구간이 반복되었다. 설정값을 높이면 잠깐 나아지는 듯하다가, 곧 기다리는 요청과 재시도만 늘었다.

![GPU가 계산하는 구간과 입력을 기다리는 구간이 반복되는 모습](assets/gpu-waiting-pattern.svg)

*그림 2. 실제 운영 이슈에 첨부했던 GPU 사용률 화면에서 핵심 모양만 남겼다. 높은 구간은 계산 중이고, 0% 가까이 내려간 구간은 다음 입력을 기다린 시간일 수 있다.*

식당 주방을 떠올려 보자. 요리사가 아무리 빨라도 주문서와 재료가 오지 않으면 요리할 수 없다. 요리사의 손이 쉬고 있다는 이유만으로 칼이나 화구를 바꾸면 문제가 해결되지 않는다. 주문서를 전달하는 직원, 재료 창고, 완성된 음식의 포장 단계도 함께 봐야 한다.

GPU는 이 이야기의 요리사다. **파이프라인**은 입력을 찾고, 읽고, 손질하고, GPU에 건네고, 결과를 저장하는 전체 작업 줄이다. 이 줄의 어느 한 곳이 느리면 GPU 앞의 일감 상자가 빈다.

그래서 가장 먼저 “GPU가 왜 느린가?”가 아니라 “일 하나가 어디를 지나가는가?”를 적었다. 한 레코드가 완료되기까지의 길은 다음과 같다.

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

이를 짧게 적기 위해 아래 식을 쓸 수 있다. 겁먹을 필요는 없다. 뜻은 “한 레코드의 전체 시간은 각 단계에서 기다린 시간을 모두 더한 값”이라는 한 문장이다.

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

두 번째 식은 “전체 속도는 가장 느린 단계보다 빨라질 수 없다”는 뜻이다. 공항 보안 검색대가 시간당 100명을 통과시켜도, 앞의 탑승권 확인 창구가 시간당 20명만 보내면 전체는 시간당 20명을 넘지 못한다.

모델 서버가 초당 100개의 요청을 처리할 수 있어도 파일을 찾는 단계가 초당 20개의 입력밖에 만들지 못하면 전체 처리량은 20을 넘지 못한다. 이때 GPU의 배치 크기만 바꾸면 가장 느린 창구는 그대로다.

### 1.1 시작할 때만 빨라 보일 수 있다

작업 시작 직후에는 초당 처리 건수가 매우 높다가, 몇 분 뒤 갑자기 떨어질 수 있다. 초반에 새 일을 한 것이 아니라 이미 끝난 일을 빠르게 건너뛰었기 때문이다. 이런 건너뛰기를 `skip`이라고 부른다. 이전 결과를 다시 사용해 모델 계산을 생략했다면 `cache hit`, 즉 캐시에서 답을 찾았다고 말한다.

이미 완료된 ID를 메모리에 올려 두었다면 확인은 매우 빠르다. 하지만 처음 보는 레코드가 나오면 그때부터 NAS에서 파일을 읽고 모델을 호출해야 한다. 서로 다른 두 구간의 평균을 한데 섞으면 실제 속도를 알 수 없다.

따라서 시작부터 누적 평균으로 계산한 처리량은 의미가 약하다. 다음 세 구간을 분리해야 한다.

1. 준비 구간: 모델 로딩, 워밍업, 매니페스트 로딩, 캐시 복원
2. skip 구간: 기존 완료 항목, 캐시 hit, 비대상 레코드
3. 안정 구간(`steady state`): 실제 NAS 읽기, 전처리, 추론, 결과 쓰기가 계속되는 구간

`완료 레코드 ÷ 전체 시간`만 보면 건너뛴 일이 많은 실행이 더 빠른 것처럼 보인다. 모델 성능을 비교할 때는 캐시에서 답을 찾지 못해 실제 모델을 부른 경우, 즉 `cache miss`를 따로 센다. 전체 파이프라인을 비교할 때는 건너뛴 비율과 캐시 적중 비율도 함께 적는다.

### 1.2 길이 넓어도 왕복이 공짜가 되는 것은 아니다

HPC의 계산 노드는 보통 빠른 네트워크로 연결된다. 여기서 **대역폭**은 일정 시간에 얼마나 많은 데이터를 옮길 수 있는지를 뜻한다. 차선이 많은 넓은 도로와 비슷하다. 큰 파일을 길게 보낼 때는 넓은 길의 효과가 크다. 그래서 “네트워크가 빠르니 작은 파일도 로컬 SSD처럼 빠르겠지”라고 생각하기 쉽다.

하지만 길이 넓은 것과 목적지가 가까운 것은 다른 문제다. 1GB 파일 하나를 읽는 작업은 높은 대역폭의 도움을 크게 받는다. 반면 1KB JSON 파일 백만 개를 서로 다른 폴더에서 찾고, 있는지 확인하고, 열고, 닫는 작업은 작은 질문을 아주 많이 보낸다. 이때는 데이터 크기보다 질문 횟수와 네트워크 왕복 시간이 더 중요할 수 있다.

간단한 비교를 해 보자. 1KB 파일을 100만 개 읽으면 데이터 자체는 약 1GB다. 순차 1GB 읽기라면 빠른 스토리지에서 짧은 시간에 끝날 수 있다. 그러나 각 파일마다 평균 2ms의 메타데이터 지연만 추가되어도 지연을 완전히 직렬로 지불할 경우 2,000초가 더해진다. 데이터 크기는 같지만 접근 모양이 전혀 다르다.

```text
패턴 A: 1GB 파일 × 1개
  메타데이터 연산 수가 작고 전송 대역폭이 중요

패턴 B: 1KB 파일 × 1,000,000개
  작은 질문이 많아서 왕복 횟수, 캐시, 동시성이 중요
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

### 1.4 두 번째 반전: `async`라고 썼지만 한 줄로 기다렸다

`async def`는 여러 일을 번갈아 진행하기 위한 Python 문법이다. 이 일을 배분하는 한 명의 진행 요원을 **이벤트 루프**라고 부른다. 진행 요원은 어떤 일이 네트워크 답을 기다리는 동안 다른 일을 살펴볼 수 있다.

그런데 코루틴 안에서 `Path.exists()`, `open()`, `json.load()`, `mkdir()`, `json.dump()` 같은 동기 파일 함수를 직접 호출하면 진행 요원 자신이 그 일이 끝날 때까지 서 있게 된다. 작업을 100개 만들어도 진행 요원이 멈춰 있으므로 다른 작업으로 넘어가지 못한다.

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

### 1.5 세 번째 반전: 결과보다 “완료 메모”가 더 쉽게 망가졌다

분산 배치에서는 작업을 맡은 프로그램을 **워커(worker)**라고 부른다. 각 워커가 서로 다른 레코드를 맡으면 결과 파일의 주인도 자연스럽게 한 명이 된다. 이 부분은 안전할 수 있다.

문제는 “어떤 레코드를 끝냈는지” 한 파일에 적을 때 생긴다. 파일 끝에 내용을 덧붙이는 일을 `append`라고 한다. 여러 워커가 같은 `done.txt`에 동시에 덧붙이면 한 공책에 여러 사람이 같은 순간 펜을 대는 셈이다.

```python
with open("done.txt", "a", encoding="utf-8") as file:
    file.write(record_id + "\n")
    file.flush()
```

여러 노드의 여러 프로세스가 같은 NAS 파일에 append하면 로컬 파일시스템에서 기대한 의미가 유지되지 않을 수 있다. Linux `open(2)` 문서는 `O_APPEND`가 로컬에서는 offset 이동과 쓰기를 하나의 원자 단계로 수행한다고 설명하면서도, NFS에서는 여러 프로세스의 동시 append를 클라이언트가 모사해야 해 경쟁 조건과 손상 가능성이 있다고 별도로 경고한다. 자세한 내용은 [`open(2)`의 O_APPEND 설명](https://man7.org/linux/man-pages/man2/open.2.html)을 참고할 수 있다.

더 무서운 점은 계산 결과는 멀쩡한데 완료 목록만 망가질 수 있다는 것이다. 비싼 계산을 마쳤어도 다음 실행은 그 사실을 모른다. 끝난 일을 다시 하게 된다. 캐시 데이터베이스를 각 워커가 복사해서 수정한 뒤 같은 원본에 덮어쓰는 구조도 비슷하다. 마지막으로 저장한 워커의 내용만 남고 다른 워커의 새 내용은 조용히 사라질 수 있다.

이 세 번의 반전은 하나의 원칙으로 모인다.

> 공유 스토리지에서 읽기 경로의 비용과 쓰기 경로의 소유권을 명시적으로 설계하지 않으면, 계산 자원을 늘릴수록 성능과 정확성이 함께 나빠질 수 있다.

다음 장부터는 이 원칙을 모델로 만들고 코드로 검증한다.

---

## 2. 큰 짐과 안내표를 나눠 본다: 데이터 평면과 제어 평면

도서관에는 책이 있고, 책을 찾기 위한 목록표와 대출 기록이 있다. 책이 모두 멀쩡해도 목록표가 틀리면 원하는 책을 찾지 못한다. 대출 기록이 사라지면 누가 어떤 책을 빌렸는지 알 수 없다.

AI 파이프라인의 파일도 두 무리로 나누어 보면 이해하기 쉽다.

- **데이터 평면(data plane)**: 실제로 처리할 큰 짐과 완성된 결과
- **제어 평면(control plane)**: 무엇을 누가 처리해야 하는지 알려 주는 안내표

여기서 `plane`은 어려운 장치 이름이 아니다. 역할이 다른 정보의 무리를 나누어 부르는 말이다.

### 2.1 데이터 평면

데이터 평면은 도서관의 책과 같다. 실제 내용이 들어 있다.

- 원본 이미지, 오디오, 텍스트와 같은 큰 입력
- 모델 가중치와 토크나이저
- 전처리된 텐서나 샤드 파일
- 추론 결과와 최종 데이터셋
- 대용량 checkpoint

이 평면에서는 바이트 처리량, 순차 접근, 블록 크기, 압축, 병렬 읽기, 캐시 효율이 중요하다. 파일 하나가 크고 접근 횟수가 상대적으로 적다면 빠른 네트워크와 스토리지 대역폭을 잘 활용할 수 있다.

### 2.2 제어 평면

제어 평면은 도서관의 목록표와 대출 기록에 가깝다. 어떤 데이터를 언제 누가 처리했는지 알려 준다.

- ID 목록과 매니페스트(처리할 파일 주소록)
- 작업 할당과 샤딩 정보
- 완료 ID와 실패 ID
- `.done` 센티널(sentinel, 완료를 알리는 작은 표식 파일)
- 캐시 인덱스와 델타 목록(새로 바뀐 내용의 목록)
- 버전, 설정 지문(fingerprint), 실행 정보
- 재시작 위치와 커서(cursor, 마지막으로 읽은 위치)

제어 평면 파일은 작지만 자주 읽고 쓴다. 파일이 작아서 중요하지 않아 보일 수 있다. 그러나 완료 목록이 망가지면 멀쩡한 결과도 찾지 못한다. 주소록이 절반만 저장되면 뒤의 파일은 존재하지 않는 것처럼 보인다. 작은 안내표가 전체 작업의 기억을 맡고 있는 셈이다.

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

### 2.3 `open()` 한 줄 뒤에서 벌어지는 일

코드에서 `open("/shared/a.json")`은 로컬 파일을 여는 한 줄과 똑같이 보인다. 하지만 NAS에서는 그 뒤에 작은 대화가 이어진다.

```text
계산 노드: shared 폴더가 맞나요?
NAS:       네.
계산 노드: a.json이 있나요? 읽을 권한이 있나요?
NAS:       네. 파일을 찾았습니다.
계산 노드: 내용을 보내 주세요.
NAS:       여기 있습니다.
```

실제 프로토콜은 이보다 훨씬 복잡하지만, 지금 필요한 직관은 이것으로 충분하다. 함수 한 번이 네트워크 요청 한 번으로 끝난다고 가정하면 안 된다.

NFSv4.1 프로토콜 명세인 [RFC 8881](https://www.rfc-editor.org/rfc/rfc8881)에는 상태, 잠금, 파일·속성·디렉터리 캐싱이 자세히 적혀 있다. 이를 전부 알아야 한다는 뜻은 아니다. 애플리케이션의 한 줄이 네트워크에서는 여러 질문과 답으로 바뀔 수 있다는 점만 기억하면 된다.

특히 다음 세 가지를 분리해서 생각해야 한다.

1. **이름 찾기**: 경로를 따라가며 실제 파일을 찾는다.
2. **정보 확인**: 파일 종류, 크기, 수정 시각, 권한을 확인한다.
3. **내용 읽기**: 실제 바이트를 전송한다.

`Path.exists()`는 파일 내용을 읽지 않지만 공짜가 아니다. `os.scandir()`는 파일 내용을 읽지 않지만 디렉터리 엔트리를 가져온다. `DirEntry.is_file()`은 플랫폼과 파일 타입 정보의 가용성에 따라 추가 시스템 호출이 필요할 수 있다. Python 공식 문서도 [`os.scandir`](https://docs.python.org/3/library/os.html#os.scandir)가 Unix에서 `opendir()`와 `readdir()`를 사용하며, `DirEntry` 메서드가 시스템 호출을 수행할 수 있다고 설명한다.

### 2.4 작은 파일은 “몇 개인가”보다 “어떻게 여는가”가 중요하다

작은 파일이 많다는 사실은 출발점일 뿐이다. 같은 100만 개 파일도 다음 조건에 따라 결과가 다르다.

- 하나의 디렉터리에 몰려 있는가, 여러 단계로 샤딩되어 있는가
- 파일 경로를 이미 아는가, 디렉터리를 탐색해야 하는가
- 파일을 한 번 순차적으로 읽는가, 같은 파일을 반복해서 읽는가
- 모든 워커가 같은 디렉터리를 스캔하는가, 범위가 분리되어 있는가
- 파일이 불변인가, 실행 중 생성·삭제되는가
- 클라이언트의 attribute cache와 dentry cache가 얼마나 재사용되는가
- 읽기만 하는가, 작은 쓰기와 `fsync`가 섞이는가
- 정상 파일 비율과 missing 파일 비율이 얼마인가

따라서 “작은 파일이 많아서 느리다”만으로는 무엇을 고쳐야 할지 알 수 없다. 레코드 하나를 처리할 때 파일시스템에 어떤 부탁을 몇 번 하는지 세어 보아야 한다.

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

### 2.5 함께 볼 수 있다는 말은 함께 고쳐도 된다는 뜻이 아니다

공유 NAS의 큰 장점은 모든 노드가 같은 경로를 볼 수 있다는 것이다. 같은 도서관 책장을 함께 볼 수 있는 셈이다. 그렇다고 여러 사람이 한 장의 종이에 동시에 글씨를 써도 안전하다는 뜻은 아니다.

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

안전한 기본값은 “모두가 보는 장소에 두되, 각 파일을 쓰는 주인은 한 명으로 정한다”다. 워커 7은 `worker-00007/` 아래만 쓴다. 각 결과 파일도 담당 워커 한 명만 쓴다. **코디네이터(coordinator)**는 여러 워커의 결과를 확인하고 최종 주소록을 만드는 조정자다. 다른 워커는 완성되어 더는 바뀌지 않는 파일만 읽는다.

이렇게 하면 “누가 먼저 쓰나”를 정하는 복잡한 잠금 자체가 필요 없어진다. 가장 고치기 쉬운 충돌은 애초에 일어나지 않도록 만든 충돌이다.

---

## 3. 작은 질문의 비용을 센다: RTT와 메타데이터 I/O

NAS가 얼마나 빠른지는 제품, 설정, 서버 부하, 캐시, 폴더 크기에 따라 달라진다. “NAS의 `open()`은 언제나 몇 밀리초” 같은 숫자를 외우는 것은 도움이 되지 않는다. 대신 시간이 어디서 더해지는지 알아야 한다.

![큰 파일 한 개와 작은 파일 여러 개의 네트워크 왕복 비교](assets/rtt-small-files.svg)

*그림 3. RTT는 요청을 보낸 뒤 답을 받을 때까지의 왕복 시간이다. 큰 파일은 왕복 횟수가 적고, 작은 파일 여러 개는 짧은 왕복을 계속 반복한다.*

택배를 예로 들어 보자. 큰 상자 하나를 한 번 배송하는 일과 작은 물건 백 개를 백 번 배송하는 일은 전체 무게가 같아도 시간이 다르다. 매번 접수하고, 주소를 확인하고, 출발하고, 도착 확인을 해야 하기 때문이다. NAS의 작은 파일도 비슷하다.

아래 식은 그 생각을 짧게 적은 것이다. \(N\)은 “몇 번 했는가”, \(L\)은 “한 번에 얼마나 기다렸는가”다.

\[
T_{\text{meta}}
\approx
N_{\text{lookup}}L_{\text{lookup}}
+ N_{\text{stat}}L_{\text{stat}}
+ N_{\text{open}}L_{\text{open}}
+ N_{\text{dir}}L_{\text{dir}}
+ N_{\text{sync}}L_{\text{sync}}
\]

즉, 파일 이름 찾기에 쓴 시간은 `찾은 횟수 × 한 번의 대기 시간`에 가깝다. `stat`, `open`, 폴더 목록 읽기, 저장 확정도 같은 방식으로 더한다. 여기에는 네트워크 왕복뿐 아니라 앞선 요청을 기다린 시간과 재전송 시간도 들어간다.

전체 레코드 수가 \(R\), 재시도 배수가 \(A\), 워커 수가 \(W\)일 때 총 호출 수는 단순히 \(R\)에 비례하지 않는다. 모든 워커가 전체 목록을 스캔한 뒤 자기 몫만 고르는 구조라면 탐색 호출은 \(R \times W\)까지 증가할 수 있다.

\[
N_{\text{global scan}}
\approx R \times W \times A
\]

반대로 coordinator가 한 번 매니페스트를 만들고 각 워커에 범위를 전달하면 탐색은 \(R\), 워커의 직접 읽기는 자기 몫인 \(R/W\)가 된다.

### 3.1 왕복 횟수가 중요한 일과 길의 넓이가 중요한 일

I/O 시간을 아주 거칠게 다음처럼 나눌 수 있다.

\[
T_{\text{io}}
\approx N_{\text{round trips}} \cdot RTT
+ \frac{B}{BW}
\]

\(B\)는 옮길 데이터의 크기이고, \(BW\)는 한 번에 얼마나 많이 옮길 수 있는지를 나타내는 대역폭이다. 식의 왼쪽 항은 “왕복을 몇 번 했는가”, 오른쪽 항은 “데이터를 옮기는 데 얼마나 걸렸는가”다. 작은 파일에서는 왼쪽이, 큰 연속 파일에서는 오른쪽이 더 커지기 쉽다.

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

### 3.2 “있나요?”라고 묻고 다시 여는 대신, 먼저 열어 본다

다음 코드는 읽기 쉽고 로컬에서는 큰 문제가 없어 보인다.

```python
if result_path.exists():
    with result_path.open() as file:
        result = json.load(file)
```

하지만 파일이 대부분 존재한다면 `exists()`로 “있나요?”라고 묻고, `open()`으로 “그럼 열어 주세요”라고 다시 묻게 된다. 질문이 두 번이다. 게다가 확인한 직후 다른 작업이 파일을 지울 수도 있어, 먼저 확인했다고 안전이 완전히 보장되는 것도 아니다.

파일이 거의 항상 있고, 없을 때의 행동이 정해져 있다면 먼저 열어 보고 “없음” 오류만 처리할 수 있다.

```python
try:
    with result_path.open() as file:
        result = json.load(file)
except FileNotFoundError:
    result = None
```

Python에서는 이 방식을 `EAFP`라고도 부른다. 이름을 외울 필요는 없다. “허락을 먼저 묻기보다 해 보고, 안 되는 경우를 처리한다”는 뜻이다. 언제나 빠른 것은 아니다. 파일이 거의 항상 없다면 실패하는 `open()` 비용을 측정해야 한다. 핵심은 가장 자주 지나가는 길에서 질문 수를 줄이는 것이다.

### 3.3 파일 하나를 원하면서 폴더 전체를 받지 않는다

`os.scandir()`는 `os.listdir()`보다 파일 타입 정보가 필요할 때 효율적인 API다. Python 문서가 설명하듯 `DirEntry`는 가능한 정보를 캐시하여 추가 `stat`을 줄인다. 따라서 로컬 파일시스템에서 `listdir + stat`보다 `scandir`가 좋은 선택인 경우가 많다.

그러나 “`scandir`가 `listdir`보다 빠르다”와 “알려진 파일 하나를 열기 위해 디렉터리를 열거하는 것이 좋다”는 다른 주장이다.

```text
질문 A: 이 디렉터리의 모든 파일을 분류해야 한다.
  scandir가 합리적일 수 있다.

질문 B: meta.json이라는 파일을 읽어야 한다.
  directory / "meta.json"을 직접 여는 편이 자연스럽다.
```

질문 B에서 `scandir`를 사용하면 필요한 한 이름뿐 아니라 디렉터리 엔트리 집합을 가져오고 반복한다. 파일 타입 검사가 추가 속성 조회를 유발할 수도 있다. 이것을 수백만 레코드에 적용하면 디렉터리 연산 수가 커진다.

### 3.4 한 줄짜리 메모도 배송 한 번은 필요하다

완료 ID 한 줄이 20바이트라고 해 보자. 백만 줄도 내용만 보면 약 20MB다. 그러나 한 줄마다 `열기 → 쓰기 → 밀어내기 → 닫기`를 하면 백만 번 배송하는 것과 같다. 1,000줄을 메모리에 모아 한 번에 쓰면 배송 횟수는 약 1,000번으로 줄어든다. 이렇게 여러 작은 일을 모아 한 번에 처리하는 것을 **배치(batch)**라고 한다.

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

### 3.5 한 사람의 답을 기다리는 동안 다른 질문을 보낸다

파일 읽기 하나가 끝나기를 기다리는 동안 다른 파일 요청을 보낼 수 있다. 이렇게 여러 일을 겹쳐 진행하는 정도를 **동시성(concurrency)**이라고 한다. 독립적인 파일 읽기의 지연 시간이 \(L\), 한꺼번에 진행하는 수가 \(C\)라면 이상적인 처리량은 대략 \(C/L\)까지 늘 수 있다.

\[
X_{\text{io}} \approx \frac{C}{L}
\]

하지만 줄을 여러 개 만든다고 계산대가 무한히 빨라지는 것은 아니다. NAS 서버의 처리 능력, 스레드 수, 열 수 있는 파일 수, 네트워크 줄, 메모리에는 한계가 있다. 너무 많이 보내면 뒤의 요청이 오래 기다리고, 시간 초과와 재시도가 새 부담을 만든다.

실전에서는 다음 세 구간을 찾는다.

1. 동시성을 올릴수록 처리량이 늘고 느린 요청의 지연도 안정적인 구간
2. 처리량 증가는 둔해졌지만 느린 요청의 지연은 아직 감당할 수 있는 구간
3. 처리량은 그대로거나 감소하고 timeout, retry, queue가 급증하는 구간

아주 느린 일부 요청을 가리켜 **꼬리 지연(tail latency)**이라고 한다. 평균이 좋아도 꼬리가 길면 작업 전체의 마지막 종료가 늦어진다. 목표는 무너지기 직전의 가장 큰 숫자가 아니다. 장시간 실행과 다른 사용자의 부하 변화에도 견디는 안정된 숫자를 고르는 것이다.

### 3.6 비용 모델의 목적

이 모델은 실제 시간을 정확히 예측하려는 것이 아니다. 코드 변경의 방향을 고르는 데 목적이 있다.

- `scandir`를 직접 경로 open으로 바꾸면 \(N_{\text{dir}}\)가 줄어든다.
- `exists + open`을 EAFP로 바꾸면 정상 hit에서 \(N_{\text{stat}}\)가 줄어든다.
- 매 레코드 append를 batch flush로 바꾸면 \(N_{\text{open}}\)과 \(N_{\text{sync}}\)가 줄어든다.
- 동기 I/O를 thread로 offload하면 호출 수는 같지만 이벤트 루프의 직렬 임계 구간에서 제거된다.
- 워커별 파일로 나누면 충돌과 잠금 대기는 줄지만 파일 수는 늘어난다.
- 매니페스트를 사용하면 반복 탐색을 한 번의 선행 스캔과 직접 접근으로 바꾼다.

좋은 최적화는 무엇이 줄었는지 설명할 수 있다. “스레드를 늘렸더니 빨라졌다”에서 멈추지 않는다. “독립적인 파일 요청의 기다림을 겹쳤고, 동시성을 더 올리자 가장 느린 5%의 대기 시간이 급격히 늘어 그 전 지점을 선택했다”라고 설명할 수 있어야 한다.

---

## 4. 무엇을 세는지 먼저 정한다

성능 문제를 풀 때는 초시계보다 먼저 자를 맞춰야 한다. 한 사람은 이미지 수를 세고 다른 사람은 모델 요청 수를 세면서 둘 다 “초당 처리량”이라고 부르면 비교할 수 없다. `완료`, `요청`, `캐시 적중`, `I/O 시간`이 정확히 무엇을 뜻하는지 먼저 정한다.

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

### 4.1 작은 용어 사전부터 만든다

본문과 실습에서는 다음 단위를 사용한다.

| 지표 | 정의 |
|---|---|
| 후보 레코드(candidate) | 스캐너가 검토한 레코드 |
| 처리 대상 레코드(eligible) | 입력과 필수 파일이 있어 실제 처리 대상으로 남은 레코드 |
| 완료 레코드(completed) | 결과가 안전하게 저장되고 완료 기록에도 반영된 레코드 |
| 입력(payload) | 실제로 읽은 이미지나 바이너리 파일 하나 |
| 모델 요청(request) | 모델 서버에 보낸 호출 하나 |
| 캐시 적중(cache hit) | 이전의 유효한 답을 재사용해 모델 요청을 생략한 경우 |
| records/s | 안정 구간에서 1초 동안 완료한 레코드 수 |
| requests/s | 완료 모델 요청 수를 같은 구간으로 나눈 값 |
| 메타데이터 연산 | `stat`, 열기, 폴더 목록 읽기, 이름 바꾸기처럼 파일 정보와 관련된 호출 |
| 재시도율(retry rate) | 다시 시도한 수를 최초 요청 수로 나눈 값 |
| 대기열 길이(queue depth) | 특정 단계 앞에서 차례를 기다리는 항목 수 |
| 이벤트 루프 지연 | 주기적인 상태 확인이 예정 시각보다 늦게 실행된 시간 |

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

### 4.4 평균만 보면 유난히 느린 요청이 숨는다

NAS 지연은 일정하지 않다. 대부분의 `open`이 빠르더라도 일부 요청이 서버 부하, 캐시 miss, 네트워크 재전송, 디렉터리 상태 때문에 길어질 수 있다. 128개의 코루틴이 하나의 이벤트 루프를 공유하면 긴 동기 호출 하나가 다른 모든 코루틴에 영향을 준다.

최소한 다음 값을 함께 본다.

- count
- success와 failure
- mean
- p50: 빠른 순서로 줄 세웠을 때 가운데 값
- p95: 100개 중 95개가 이 시간 안에 끝난다는 경계
- p99 또는 max: 거의 가장 느린 요청 또는 실제 최댓값
- bytes
- queue wait

`p`는 percentile, 우리말로 백분위수다. 요청 시간을 빠른 순서로 100개 늘어놓았을 때 p95는 95번째 값이다. p95가 20ms라면 100개 중 약 95개는 20ms 안에 끝났고 나머지 약 5개는 더 오래 걸렸다는 뜻이다.

표본이 작을 때 p99는 크게 흔들리므로 최댓값과 분포를 함께 본다. 워커별 p95를 단순히 평균낸 값은 전체 요청의 p95와 같지 않다. 가능하면 같은 구간의 측정값을 합쳐 계산한다.

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

### 4.7 한 번에 한 가지만 바꾼다

좋은 비교 실험은 한 번에 한 가지만 바꾼다. 연구 글에서는 이런 비교를 `ablation`이라고도 부른다. 이름보다 원칙이 중요하다. 타이어와 엔진을 동시에 바꾸면 어느 변화가 연비에 영향을 주었는지 알 수 없는 것과 같다.

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

![폴더 전체에서 파일을 찾는 방법과 알려진 주소로 바로 가는 방법](assets/direct-path-vs-scan.svg)

*그림 4. 운영 이슈에서 모든 폴더를 훑던 흐름과 파일 주소를 바로 계산하는 흐름을 비교한 뒤, 공개용으로 다시 그렸다.*

집 주소를 정확히 아는데 동네의 모든 집 문패를 읽으며 찾을 필요는 없다. 파일도 같다. 레코드 ID로 폴더 주소를 계산할 수 있다면 그 주소로 바로 간다. 폴더 목록을 읽는 것은 주소 규칙이 깨졌거나 파일 이름을 정말 모르는 예외에만 사용한다.

실습 코드는 [`code/nas_io_lab.py`](code/nas_io_lab.py)에 있다. 운영 데이터와 무관한 합성 레코드를 만들며 Python 표준 라이브러리만 사용한다.

### 5.1 합성 디렉터리 구조

한 폴더에 파일이 너무 많이 몰리지 않도록 레코드 ID의 마지막 여섯 자리를 두 자리씩 나누어 세 단계 폴더에 배치한다. 이렇게 데이터를 여러 묶음으로 나누는 일을 **샤딩(sharding)**이라고 한다.

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

경로 함수는 같은 입력에 언제나 같은 답을 내는 **순수 함수**다. 디스크를 읽지 않고 문자열 규칙만 계산한다.

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

### 5.2 같은 파일을 여는 네 가지 길

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

네 방법은 같은 JSON을 반환한다. 결과가 같아도 파일시스템에 던지는 질문의 수가 다르다. 이 실습은 바로 그 차이를 잰다.

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

출력은 실험 한 차례(`round`)마다 JSON 한 줄이다.

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

위 숫자는 출력 모양만 보여 주기 위해 0으로 두었다. 독자는 자기 환경에서 나온 숫자를 사용해야 한다. 스크립트는 매 차례 ID 순서를 섞고 메서드 실행 순서를 바꾼다. 특정 방법이 언제나 첫 번째여서 불리하거나, 언제나 마지막이어서 캐시 도움을 받는 일을 줄이기 위해서다.

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

## 6. 매니페스트: 미리 만든 파일 주소록

**매니페스트(manifest)**는 이번 실행이 읽어야 할 파일의 주소록이다. 이 이름은 화물선의 적재 목록에서 왔다. 배에 어떤 화물이 실렸는지 목록 하나로 확인하듯, 파이프라인도 어떤 레코드와 파일을 처리할지 미리 적어 둔다.

가장 단순한 매니페스트는 ID 목록이다. 더 자세한 매니페스트에는 각 파일의 경로, 버전, 크기, 검사값(checksum), 예상 작업량이 들어간다. 검사값은 파일 내용으로 계산한 짧은 지문이다. 내용이 한 글자라도 달라지면 지문도 달라져 손상을 발견할 수 있다.

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

실제 대규모 목록은 거대한 JSON 한 개보다 한 줄씩 읽을 수 있는 JSONL이나 여러 조각으로 나눈 파일이 편하다. 여기서는 주소록의 모양을 쉽게 보기 위해 JSON을 사용한다.

### 6.1 매번 책장을 뒤지는 대신 주소록을 한 번 만든다

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

### 6.2 같은 주소록을 쓰면 같은 대상을 다시 찾을 수 있다

디렉터리를 실행 중에 직접 스캔하면 작업 시작과 끝 사이에 파일이 추가되거나 삭제될 수 있다. 어떤 워커는 새 파일을 보고 다른 워커는 보지 못할 수 있다. 실행 결과가 “그때 보였던 디렉터리 상태”에 의존한다.

한 번 완성한 뒤에는 고치지 않는 매니페스트를 입력으로 삼으면 대상 집합이 고정된다. 이렇게 게시 뒤 바뀌지 않는 상태를 **불변(immutable)**이라고 부른다.

```text
run_id
  ├─ code revision
  ├─ configuration fingerprint
  ├─ input manifest digest
  ├─ model/prompt/preprocess version
  └─ output manifest digest
```

오류를 다시 확인할 때 “같은 폴더를 다시 훑었다”가 아니라 “같은 매니페스트 지문(digest)을 사용했다”라고 말할 수 있다. `digest`는 여러 검사값을 통틀어 부르는 말이다.

### 6.3 독자가 절반만 쓴 주소록을 보게 하지 않는다

주소록을 쓰는 프로그램을 **생산자(producer)**, 읽는 프로그램을 **소비자(consumer)**라고 해 보자. 생산자가 최종 파일에 한 줄씩 쓰는 중에 소비자가 열면 주소록의 앞부분만 볼 수 있다.

그래서 먼저 임시 파일에 완성본을 쓴다. `fsync`로 운영체제에 “저장 장치까지 내용을 밀어 달라”고 요청한 뒤 `os.replace()`로 최종 이름에 바꿔 놓는다. 소비자가 옛 완성본이나 새 완성본 중 하나만 보게 만드는 것이 목표다. 중간 상태를 보이지 않는 이런 게시를 흔히 **원자적(atomic) 게시**라고 부른다.

```python
def atomic_write(path: Path, payload: bytes) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("wb") as file:
        file.write(payload)
        file.flush()
        os.fsync(file.fileno())
    os.replace(tmp, path)
```

다만 이름 바꾸기와 `fsync`의 정확한 보장은 파일시스템마다 다를 수 있다. “독자에게 절반만 보이지 않는다”와 “갑작스러운 전원 장애 뒤에도 저장 장치에 남는다”는 서로 다른 요구다. 중요한 데이터라면 대상 NAS에서 실패 테스트로 확인해야 한다.

### 6.4 큰 내용과 작은 “준비 완료” 표지를 나눈다

큰 매니페스트와 “어느 파일이 완성본인지” 가리키는 작은 표지를 분리하면 게시가 단순해진다. 이 작은 표지를 포인터(pointer)라고 부른다.

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

소비자는 `READY.json`을 열고, 그 파일이 가리키는 불변 매니페스트의 지문을 확인한 뒤 실행한다. 생산자는 기존 매니페스트를 고치지 않는다. 새 버전은 새 내용 파일을 만든 뒤 작은 표지만 새 파일을 가리키도록 바꾼다.

### 6.5 주소록을 공동 편집 문서로 만들지 않는다

매니페스트에 모든 진행 상태를 넣고 여러 워커가 함께 고치게 만들면 다시 충돌이 생긴다. 레코드 하나의 상태만 바꾸려고 큰 JSON 전체를 다시 쓰게 되고, 누가 마지막으로 썼는지에 따라 다른 워커의 변경이 사라질 수 있다.

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

## 7. `async` 안에서 파일을 읽으면 왜 줄이 멈추는가

`asyncio`는 한 명의 진행 요원이 여러 일을 번갈아 살피게 해 주는 Python 도구다. 네트워크 답을 기다리는 작업을 잠시 내려놓고 다른 작업으로 이동할 수 있다.

하지만 `async def` 안에 일반 `open()`을 썼다고 파일 읽기까지 자동으로 비동기가 되지는 않는다. 진행 요원인 이벤트 루프가 직접 파일을 읽으면, 그 호출이 끝날 때까지 다른 작업을 살피지 못한다.

![파일 읽기를 이벤트 루프가 직접 할 때와 I/O 스레드에 맡길 때의 차이](assets/event-loop-offload.svg)

*그림 5. 운영 이슈에서 “비동기 코드인데도 요청이 한 줄로 진행된” 원인을 공개용으로 다시 그렸다. 파일 읽기 시간은 그대로지만, 스레드에 맡기면 다른 작업까지 세우지는 않는다.*

Python의 [`asyncio.to_thread()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread)는 오래 기다릴 수 있는 동기 함수를 별도 스레드에 맡기는 도구다. 스레드는 같은 프로그램 안에서 일을 나누어 맡는 실행 흐름이라고 생각하면 된다.

### 7.1 한 명뿐인 진행 요원이 창고에 직접 다녀오면

코루틴은 이벤트 루프가 번갈아 진행하는 작업 하나다. A, B, C 세 작업이 있다고 하자.

```text
시간 ─────────────────────────────────────────────▶

A: [NAS open 20ms][await model........][write 10ms]
B:                 실행 못 함
C:                 실행 못 함
loop heartbeat:     지연됨
```

A가 동기 `open`을 수행하는 20ms 동안 B와 C는 Python 코드를 실행하지 못한다. B의 모델 응답이 이미 도착했어도 그 답을 처리하는 일이 늦어진다. 시간 초과를 확인하는 타이머도 제시간에 깨어나지 못할 수 있다.

I/O를 스레드에 넘기면 이벤트 루프는 다른 작업을 진행할 수 있다.

```text
시간 ─────────────────────────────────────────────▶

I/O thread A: [NAS open 20ms]
event loop:   schedule B → schedule C → heartbeat → A callback
```

NAS 호출 자체의 기다림은 사라지지 않는다. 다만 그 기다림이 전체 작업을 세우는 구간에서 빠진다. 이것은 “파일이 빨라졌다”가 아니라 “파일을 기다리는 동안 다른 일을 할 수 있게 되었다”는 변화다.

### 7.2 파일 일을 이름 있는 동기 함수로 묶어 맡긴다

```python
async def process_bad(record):
    with record.meta_path.open() as file:
        meta = json.load(file)

    response = await model_client.generate(meta)

    record.output_dir.mkdir(parents=True, exist_ok=True)
    with record.result_path.open("w") as file:
        json.dump(response, file)
```

파일 읽기와 쓰기를 이름 있는 동기 함수로 분리한 뒤 스레드에 맡긴다.

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

### 7.3 실습 2: 주기적인 똑딱이가 늦어지는지 본다

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

`heartbeat`는 프로그램이 살아 있고 제시간에 반응하는지 확인하는 주기적인 똑딱이다. 10ms마다 깨어나기로 했는데 실제로 100ms 뒤에 깨어났다면 그 사이 이벤트 루프가 다른 일에 막혀 있었다는 신호다.

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

1. `records_per_second`: 1초 동안 완료한 레코드 수
2. `heartbeat_max_gap_ms`: 똑딱이가 가장 많이 늦어진 시간

처리량만 좋아지고 heartbeat가 계속 크게 지연된다면 다른 동기 구간이 남아 있을 수 있다. 반대로 heartbeat는 좋아졌지만 처리량이 늘지 않으면 NAS 서버 또는 모델 서버가 이미 포화되었을 수 있다.

### 7.4 스레드에 맡기는 것도 공짜는 아니다

오프로딩에도 비용과 한계가 있다.

- 스레드에 일을 전달하고 결과를 돌려받는 작은 비용이 있다.
- 준비된 스레드 수보다 일이 많으면 스레드 대기열에서 기다린다.
- 한 번에 읽은 raw bytes가 많으면 메모리가 증가한다.
- CPU 계산을 오래 하는 Python 코드는 스레드만 늘려도 동시에 빨라지지 않을 수 있다.
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

### 7.5 창구 수는 직접 정하고 측정한다

`asyncio.to_thread`는 기본 스레드 모음을 사용한다. 이 모음을 스레드 풀, 일을 배정하는 도구를 executor라고 부른다. Python 버전에 따라 기본 스레드 수가 달라질 수 있다. 공식 [`concurrent.futures` 문서](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor)는 현재 기본값을 설명한다. CPU 코어가 많은 노드라도 기본 스레드 수가 NAS에 알맞다고 가정하지 않는다.

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

풀을 역할별로 나누면 작은 파일 정보 요청이 큰 파일 읽기 뒤에서 오래 기다리는 일을 줄일 수 있다. 한 줄의 첫 번째 큰 짐 때문에 뒤의 작은 짐이 모두 멈추는 현상을 `head-of-line blocking`이라고도 부른다.

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

### 7.7 종료 버튼을 눌렀을 때 어디까지 마칠지도 정한다

파이프라인이 취소되거나 제한 시간에 도달했을 때 다음 순서가 필요하다.

1. 새 레코드 수락을 중단한다.
2. in-flight 모델 요청을 정책에 따라 기다리거나 취소한다.
3. 완료된 결과의 I/O future를 기다린다.
4. 워커 저널 버퍼를 flush한다.
5. 완료 sentinel을 게시한다.
6. executor를 종료한다.

종료 직전에 기다리지 않고 스레드를 닫으면 결과 파일이나 완료 기록이 끝까지 써지지 않을 수 있다. 반대로 무한정 기다리면 스케줄러가 정한 종료 시각에 강제로 꺼질 수 있다. 그래서 종료 정리 시간을 미리 남겨 두고 각 단계가 기다릴 최대 시간도 정한다.

```python
async def graceful_shutdown(pipeline, journal):
    pipeline.stop_accepting()
    async with asyncio.timeout(30):
        await pipeline.drain_inflight()
    await asyncio.to_thread(journal.flush)
    await asyncio.to_thread(journal.mark_done)
```

`mark_done`은 모든 결과가 저장된 뒤에만 호출한다. 완료 센티널은 단순한 로그가 아니라 “여기까지 안전하게 끝났다”는 도장이다.

---

## 8. 한꺼번에 너무 많이 시키지 않는다: 제한된 동시성

파일 기다림을 스레드로 옮기면 다음에는 작업 수를 크게 올리고 싶어진다. 어느 정도까지는 효과가 있다. 한 요청의 답을 기다리는 동안 다른 요청을 보낼 수 있기 때문이다. 그러나 일을 무제한으로 만들면 대기하는 객체가 메모리를 차지하고, NAS와 모델 서버에 한꺼번에 몰려가 새로운 장애를 만든다.

놀이공원은 입장객이 많다고 놀이기구 좌석보다 수천 배 많은 사람을 승강장 안에 넣지 않는다. 줄의 길이와 한 번에 들어가는 사람 수를 제한한다. 프로그램에서도 같은 규칙이 필요하다.

```python
# 위험: 입력 크기만큼 task를 즉시 생성
await asyncio.gather(*(process(record) for record in all_records))
```

레코드가 수백만 개라면 task 객체 자체가 메모리를 차지한다. 각 task가 raw bytes를 읽고 모델 요청을 만들면 in-flight 메모리가 급증한다. NAS와 모델 서버의 queue가 동시에 커지고 timeout이 발생한다.

### 8.1 대기열의 길이와 일하는 수를 따로 본다

안정된 줄에서는 “줄 안에 있는 사람 수 = 1초에 들어오는 사람 수 × 한 사람이 머무는 시간”이라는 관계가 성립한다. 이를 Little의 법칙이라고 부른다.

\[
L = \lambda W
\]

\(L\)은 줄 안의 평균 작업 수, \(\lambda\)는 초당 들어오고 나가는 작업 수, \(W\)는 한 작업이 머무는 시간이다. 파일 응답 시간이 두 배가 되면 같은 처리량을 유지하려고 더 많은 작업이 동시에 기다리게 된다. 반대로 기다리는 작업만 늘렸는데 처리량이 늘지 않으면 메모리와 대기 시간만 커진다.

그래서 파이프라인의 각 단계에 최대 길이가 정해진 대기열을 둔다. 이것이 **제한된 대기열(bounded queue)**이다.

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

뒤 단계가 느려지면 대기열이 차고 앞 단계의 `put()`이 기다린다. 뒤에서 “잠깐, 더 받기 어렵다”라는 신호를 앞으로 전달하는 셈이다. 이를 **역압(backpressure)**이라고 부른다. 프로그램의 메모리를 끝없는 대기실로 쓰지 않게 해 준다.

### 8.2 창구마다 알맞은 제한을 둔다

한 개의 전역 `concurrency=128`은 이해하기 쉽지만 어떤 자원을 제한하는지 모호하다.

```python
limits = {
    "metadata_io": asyncio.Semaphore(32),
    "payload_io": asyncio.Semaphore(16),
    "model_requests": asyncio.Semaphore(64),
    "result_writes": asyncio.Semaphore(16),
}
```

`asyncio.Semaphore(32)`는 동시에 안으로 들어갈 수 있는 작업을 32개로 제한하는 문지기다. 메타데이터 파일은 작고 왕복 기다림이 커서 비교적 많은 작업을 겹치는 편이 유리할 수 있다. 큰 입력 파일은 메모리와 대역폭을 많이 쓰므로 더 낮게 제한할 수 있다. 모델 요청 수는 서버가 실제로 처리할 수 있는 자리 수에 맞춘다.

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

### 8.3 컴퓨터 수와 컴퓨터 안의 작업 수는 다르다

분산 환경에서는 세 숫자를 곱해야 한다.

\[
C_{\text{total}}
= N_{\text{nodes}}
\times N_{\text{processes per node}}
\times C_{\text{tasks per process}}
\]

노드당 코루틴 64개가 적당해 보여도 노드 수가 32라면 클러스터 전체에서 2,048개의 I/O가 동시에 발생할 수 있다. NAS 서버 관점의 부하는 로컬 설정 하나가 아니라 전체 곱이다.

따라서 한 노드에서 잘 되었다고 전체 노드에 곧바로 적용하지 않는다. 작은 범위에서 먼저 시험하는 실행을 **카나리(canary)**라고 한다. 다음처럼 조금씩 키운다.

1. 한 프로세스에서 동시성 값을 하나씩 바꿔 보기
2. 한 노드에서 프로세스 수를 하나씩 바꿔 보기
3. 소수 노드에서 전체 메타데이터 op/s 확인
4. 절반 규모에서 tail latency와 다른 사용자 영향 확인
5. 전체 규모에서 안정 구간 관찰

각 단계에서 처리량이 선형으로 늘지 않는 첫 지점을 찾는다. 노드 수를 두 배로 했는데 총 처리량이 1.2배만 늘고 p95가 세 배가 되면 공유 저장소 또는 모델 서비스의 포화 신호다.

### 8.4 대기열이 비었다면 바로 좋은 일이라고 판단하지 않는다

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

### 8.5 재시도도 새 요청 한 개다

timeout이 발생하면 같은 요청이 재시도된다. 원래 동시성 64라도 재시도 queue가 독립적으로 커지면 실제 부하는 더 높다.

\[
\text{effective offered load}
= \text{new requests} + \text{retries}
\]

과부하 때문에 시간 초과가 났는데 즉시 다시 보내면 과부하를 더 키운다. 재시도 사이의 간격을 0.5초, 1초, 2초처럼 늘리는 **지수 백오프(exponential backoff)**를 사용한다. 여러 워커가 같은 순간 다시 몰리지 않도록 간격에 작은 무작위 흔들림인 **지터(jitter)**도 넣는다. 전체 요청 중 재시도가 차지할 수 있는 비율도 제한한다.

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

모든 오류를 재시도하지 않는다. 파일이 영구적으로 없거나 JSON 형식이 잘못된 경우는 따로 격리한다. 이 격리 목록을 `quarantine`이라고도 부른다. 서버가 잠깐 바쁜 오류와 입력 자체가 틀린 오류를 같은 정책으로 다루면, 성공할 수 없는 일을 계속 반복하게 된다.

### 8.6 자동으로 창구 수를 바꾸는 것은 마지막에 한다

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

## 9. 한 줄씩 쓰지 말고 모아서 쓴다

읽기를 빠르게 했는데도 속도가 기대만큼 오르지 않는다면 쓰는 쪽을 본다. **체크포인트(checkpoint)**는 “여기까지 끝냈다”라고 남기는 중간 기록이다. 게임의 저장 지점처럼, 프로그램이 다시 시작할 때 처음부터 하지 않게 해 준다.

결과 JSON 하나를 레코드마다 쓰는 것은 자연스러울 수 있다. 그러나 완료 ID, 통계, 캐시 상태까지 레코드 하나가 끝날 때마다 따로 쓰면 작은 기록이 수없이 쌓인다. 결과보다 완료 메모를 쓰는 데 더 많은 요청을 보낼 수도 있다.

### 9.1 한 줄도 매번 봉투에 넣어 보내면 비싸다

다음 코드는 논리적으로 매우 작다.

```python
with done_path.open("a", encoding="utf-8") as file:
    file.write(record_id + "\n")
```

하지만 반복문 안에 있다면 레코드마다 파일을 열고 닫는다. `flush`는 프로그램 안의 버퍼를 운영체제에 넘기라는 뜻이고, `fsync`는 저장 장치 쪽으로 내용을 밀어 달라는 더 강한 요청이다. 더 안전하게 남길 수 있지만 NAS와 자주 대화해야 한다.

```python
for record_id in completed:
    with done_path.open("a", encoding="utf-8") as file:
        file.write(record_id + "\n")
        file.flush()
        os.fsync(file.fileno())
```

이 코드는 처리량을 느리게 할 뿐 아니라 결과 저장과 체크포인트 순서를 잘못 구성하기 쉽다. 완료 ID를 먼저 기록한 뒤 결과 쓰기가 실패하면 재시작이 레코드를 건너뛴다.

### 9.2 메모지 여러 장을 한 봉투에 담는다

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

이 코드는 출발점일 뿐이다. 실제로는 쓰다가 중간에 멈춘 경우, 검사값, 순서 번호, 중복, 프로그램 종료를 다뤄야 한다. 하지만 핵심은 분명하다. 메모리에 잠시 모았다가 한 번에 쓰면 파일을 여는 횟수가 줄어든다.

### 9.3 몇 장씩 모을지는 “최근 얼마까지 다시 할 수 있는가”의 문제다

버퍼에 100개씩 모으는데 저장 직전에 프로그램이 꺼지면 최대 99개의 완료 기록이 저널에 남지 않을 수 있다. 결과 파일은 이미 만들어졌을 수도 있다.

이것을 무조건 데이터 유실이라고 부를 필요는 없다. 결과 파일을 source of truth로 복구할 수 있고 재실행이 멱등적이라면 저널 누락은 일부 재탐색 또는 재처리 비용이 된다. 반대로 결과 생성 비용이 매우 크고 재탐색도 어려우면 더 자주 flush해야 한다.

선택지를 표로 만들 수 있다.

| 정책 | 쓰기 비용 | crash 시 최근 상태 | 적합한 경우 |
|---|---:|---|---|
| 매 이벤트 `fsync` | 매우 높음 | 최소 손실 | 거래성 상태 |
| 100개마다 append | 낮음 | 최대 99개 미기록 | 재처리 가능한 배치 |
| 5초마다 append | 부하 의존 | 최대 5초 미기록 | 시간 기반 RPO |
| 종료 시 한 번 | 매우 낮음 | worker 전체 미기록 가능 | 결과 스캔 복구가 쉬움 |

`RPO`는 장애 뒤에 어느 시점까지의 기록을 잃어도 복구 가능한지를 정한 목표다. 이 글의 배치 작업에서는 “최근 5초나 100건은 다시 확인할 수 있다”처럼 정할 수 있다. 이 목표를 먼저 정하고 저장 주기를 고른다.

### 9.4 결과와 체크포인트의 순서

안전한 기본 순서는 다음과 같다.

```text
1. 결과를 임시 파일에 쓴다.
2. 결과 내용을 검증한다.
3. 결과를 최종 이름으로 atomic publish한다.
4. worker journal에 완료 이벤트를 추가한다.
5. 정책에 따라 journal을 flush한다.
```

체크포인트를 먼저 쓰고 결과 저장에 실패하면 “끝났다고 적었지만 결과가 없는” 거짓 완료가 생긴다. 반대로 결과를 먼저 쓰고 체크포인트를 남기기 전에 꺼지면 “결과는 있지만 완료 목록에는 없는” 상태가 된다. 두 번째는 결과를 다시 발견해 완료 목록을 고칠 수 있으므로 보통 더 다루기 쉽다.

```python
async def persist_then_checkpoint(record, result, journal):
    await asyncio.to_thread(
        write_json_atomic,
        record.result_path,
        result,
    )
    journal.append(record.record_id)
```

결과를 쓴 뒤 완료 기록을 쓰기 전까지는 아주 짧은 틈이 남는다. 파일 두 개를 언제나 정확히 한 번만 함께 바꾸는 일은 복잡하다. 실무에서는 같은 결과 저장을 다시 해도 문제가 없게 만들고, 재시작할 때 결과와 버전을 확인해 완료 목록을 고치는 방식이 더 단순하다.

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

### 9.6 작은 결과 파일 자체가 너무 많아질 때

레코드별 결과 파일은 주인을 나누기 쉽고 실패한 레코드만 다시 처리하기 좋다. 그러나 작은 결과가 너무 많으면 파일 이름과 위치를 기억하는 공간도 커진다. `inode`는 파일의 크기, 권한, 위치 같은 정보를 담는 파일시스템의 기록이다. 이 부담이 커지면 여러 결과를 묶음 파일 하나에 담을 수 있다.

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

## 10. 한 파일에 여러 사람이 동시에 쓰면

느린 프로그램은 기다리면 문제가 보인다. 동시 쓰기 문제는 프로그램이 성공했다고 끝났는데도 기록 일부가 사라질 수 있어 더 위험하다.

![여러 워커가 하나의 완료 파일에 쓰는 구조와 워커별 저널 구조](assets/single-writer-journals.svg)

*그림 6. 완료 기록 손상 이슈에서 확인한 원인과 해결 구조를 공개용으로 다시 그렸다. 왼쪽은 공용 공책 하나, 오른쪽은 각자의 공책과 검증 담당자다.*

핵심 규칙은 간단하다.

> 여러 사람이 읽을 수는 있어도, 한 파일을 쓰는 주인은 한 명으로 정한다.

여러 워커가 같은 파일에 쓸 때는 “몇 번 시험했더니 잘 됐다”로 안전을 판단하지 않는다. 파일시스템이 정말로 우리가 필요한 쓰기 순서와 보존을 보장하는지 확인해야 한다.

### 10.1 내 컴퓨터에서 되었다고 NAS에서도 같은 것은 아니다

POSIX는 운영체제들이 파일을 다루는 공통 규칙을 정한 표준이다. `O_APPEND`는 쓰기 전에 위치를 파일 끝으로 옮긴다는 뜻이다. 로컬 Linux 파일시스템에서는 위치 이동과 쓰기가 한 덩어리처럼 처리된다. 그러나 Linux `open(2)` 문서는 NFS에서 여러 프로세스가 동시에 덧붙일 때 경쟁과 손상이 생길 수 있다고 따로 경고한다.

여기에 Python buffered I/O가 더해진다.

```python
file.write(record_id + "\n")
file.flush()
```

애플리케이션의 한 `write` 호출이 커널의 정확히 한 `write(2)`와 항상 같은 경계라는 가정도 조심해야 한다. encoding, buffering, 큰 문자열, 예외가 경계를 바꿀 수 있다.

따라서 “한 줄이 작으니 한 번에 안전하게 써질 것”이라는 추측에 완료 기록을 맡기지 않는다.

### 10.2 글자가 섞이는 것만이 손상은 아니다

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

마지막으로 저장한 내용만 남는 이 현상을 `last-writer-wins`라고 부른다. 파일 자체는 정상 SQLite여도 전체 작업의 기억은 틀릴 수 있다.

### 10.3 열쇠 하나를 돌려 쓰는 방법도 있지만

한 번에 한 사람만 쓰도록 공용 열쇠를 둘 수도 있다. 프로그램에서는 이를 잠금(lock)이라고 한다. 검증된 잠금을 쓰면 공유 파일의 쓰기를 한 줄로 세울 수 있다. 하지만 다음 문제도 함께 생긴다.

- lock 획득마다 네트워크 왕복
- 열쇠를 가진 워커가 죽었을 때의 처리
- 오래되어 주인이 없는 잠금의 복구
- 한 워커가 오래 잡아 뒤의 워커가 모두 기다리는 현상
- 파일시스템별 잠금 의미 차이
- 운영 중 프로토콜 버전 변경

완료 ID처럼 자연스럽게 분할 가능한 데이터라면 잠금보다 ownership 분리가 단순하다.

```text
worker 0 → done/worker-00000/events.jsonl
worker 1 → done/worker-00001/events.jsonl
worker 2 → done/worker-00002/events.jsonl
```

각 파일은 한 writer만 갖는다. 읽는 쪽은 완료 sentinel이 있는 worker만 검증해서 merge한다.

### 10.4 파일 주소에 주인의 이름을 넣는다

팀 문서에 “동시에 실행하지 마세요”라고 적는 것만으로는 부족하다. 실수해도 충돌하지 않도록 파일 주소에 주인 ID를 넣는다. 다른 주인이 같은 주소에 쓰려 하면 프로그램이 실패하게 만든다.

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

### 10.5 정상 종료뿐 아니라 쓰는 도중 꺼지는 경우도 시험한다

모든 일이 순서대로 잘 끝나는 정상 경로만 시험해서는 부족하다. 여러 프로세스가 줄을 쓴 뒤 개수만 맞는지 보는 테스트는 시작일 뿐이다.

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

## 11. 워커마다 자기 저널과 완료 도장을 둔다

이제 공용 `done.txt`를 없애고 각 워커가 자기 기록장을 갖게 만든다. 이런 시간순 기록장을 **저널(journal)**이라고 부른다. 학교에서 날짜별로 관찰 일지를 쓰듯, 워커는 끝낸 레코드를 순서대로 적는다. 전체 코드는 [`code/checkpoint_protocol.py`](code/checkpoint_protocol.py)에 있다.

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
2. `events.jsonl`은 한 줄에 이벤트 하나를 적는 워커 전용 저널이다.
3. 워커는 모든 결과와 저널 저장이 끝난 뒤 `DONE.json`을 게시한다.
4. 조정자는 `DONE.json`이 있는 워커만 검증한다.
5. 검증이 끝난 뒤 `completed-manifest.json`을 한 번에 보이도록 게시한다.

### 11.2 이벤트 schema

```json
{
  "sequence": 41,
  "record_id": "record-00000040",
  "status": "ok",
  "worker_id": 3
}
```

`sequence`는 워커 안에서 1, 2, 3처럼 하나씩 커지는 순서 번호다. 중간에 7이 빠지거나 8이 두 번 나오면 조정자가 누락과 중복을 찾을 수 있다. 큰 결과는 결과 파일에 두고 저널에는 ID와 상태처럼 작은 정보만 적는다.

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

`os.write`가 요청한 바이트를 한 번에 모두 쓰지 못할 수 있으므로 남은 부분을 반복해서 쓴다. 여러 워커의 동시 쓰기가 안전할 것이라고 기대하지 않고, “이 파일을 쓰는 워커는 하나”라는 규칙에 의존한다.

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

### 11.4 완료 도장에는 확인할 숫자를 함께 적는다

`DONE.json`은 “끝났다”는 완료 도장이다. 빈 파일로도 신호를 보낼 수 있지만, 이벤트 수와 마지막 순서 번호, 저널 지문을 함께 적으면 덜 쓴 파일과 손상된 파일을 찾을 수 있다.

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

도장이 있다는 사실만 믿지는 않는다. 조정자는 실제 저널을 읽어 이벤트 수, 지문, 순서 번호가 도장에 적힌 값과 같은지 비교한다.

### 11.5 파일이 있다는 것과 작업이 끝났다는 것은 다르다

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

### 11.7 모두 끝나야 하는 경우와 일부라도 쓸 수 있는 경우

모든 워커의 결과가 꼭 필요하다면 하나라도 완료하지 못했을 때 전체 합치기를 실패시킨다. 이를 엄격한 합치기(`strict merge`)라고 부를 수 있다.

```python
if require_all and missing:
    raise RuntimeError(
        f"workers without valid completion sentinel: {missing}"
    )
```

일부 결과만으로도 가치가 있는 분석이라면 부분 매니페스트를 게시할 수 있다. 이때 빠진 워커 목록을 숨기지 않는다.

```json
{
  "workers": [...],
  "missing_workers": [7],
  "completed_record_count": 875000,
  "status": "partial"
}
```

다음 단계 프로그램은 부분 결과를 받아도 되는지 명시적으로 선택한다. 파일이 존재한다는 이유만으로 완전한 데이터셋이라고 생각하면 안 된다.

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

### 11.9 같은 이름의 워커 두 개가 생기지 않게 한다

두 프로그램이 같은 워커 ID로 시작하면 한 파일의 주인이 한 명이라는 규칙이 깨진다. 스케줄러 설정 오류나 잘못된 재시작이 원인이 될 수 있다.

선택지는 다음과 같다.

- attempt마다 scheduler가 유일한 worker ID를 보장한다.
- 시작 시 owner lease를 별도 서비스에서 획득한다.
- `O_CREAT|O_EXCL` 기반 claim 파일을 쓰되 대상 파일시스템 의미를 검증한다.
- worker 디렉터리에 host, PID, start token을 기록하고 충돌 시 fail closed한다.

NAS의 잠금 보장이 확실하지 않다면 작은 파일 하나만으로 복잡한 분산 임대 규칙을 새로 만들지 않는 편이 좋다. 스케줄러가 이미 작업 ID의 유일성을 보장한다면 그 기능을 사용한다.

### 11.10 저널만 믿지 않고 실제 결과와 맞춰 본다

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

## 12. SQLite는 각 노드 안에 둔다

공유 NAS는 여러 노드가 결과를 나누고 오래 보관하기 좋다. 그렇다고 계산 중에 생기는 모든 메모까지 NAS에 적을 필요는 없다.

각 계산 노드 안에는 보통 그 노드만 쓰는 빠른 임시 저장소가 있다. 공동 창고로 갈 필요가 없는 작은 메모는 자기 책상 서랍에 두는 편이 빠르다. 자주 바뀌는 캐시와 데이터베이스는 로컬에 두고, 다른 워커와 나눌 완성된 결과만 NAS에 보낸다.

기본 생명주기는 세 단계다.

```text
가져오기(stage in):
  shared immutable snapshot → worker-local scratch

작업(work):
  local reads/writes/transactions

내보내기(stage out):
  validated delta/result → shared immutable object
```

### 12.1 책상 서랍은 빠르지만 영원한 보관함은 아니다

로컬 작업 공간을 `scratch`라고 부른다. 빠르지만 작업이 끝나거나 노드가 교체되면 사라질 수 있다.

- 노드가 교체되면 사라진다.
- batch 종료 후 정리될 수 있다.
- 같은 노드에서 다음 attempt가 이전 파일을 볼 수 있다.
- 디스크 용량이 worker마다 다를 수 있다.
- 작업이 비정상 종료되면 stale WAL과 temp가 남는다.

따라서 로컬 상태를 유일한 원본으로 두지 않는다. 로컬 캐시는 공유 입력과 게시된 변경 파일로 다시 만들 수 있어야 한다.

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

### 12.2 SQLite 파일 하나를 여러 노드가 직접 함께 열지 않는다

SQLite는 별도 서버 없이 파일 하나로 쓸 수 있는 작은 데이터베이스다. 검색을 빠르게 하는 색인(index)과 여러 변경을 한 묶음으로 처리하는 트랜잭션(transaction)을 제공한다. 하지만 SQLite의 계산 엔진은 우리 프로그램 안에서 실행된다. DB 파일을 NAS에 두면 잠금, 저널, 작은 페이지 읽기와 쓰기가 모두 네트워크를 건넌다.

SQLite 공식 문서인 [SQLite Over a Network, Caveats and Considerations](https://www.sqlite.org/useovernet.html)는 여러 시스템이 network filesystem 위의 같은 SQLite 파일을 직접 여는 구성을 일반적으로 권장하지 않는다. 네트워크 지연뿐 아니라 filesystem별 sync와 locking 신뢰성 차이를 강조한다.

피해야 할 구조는 다음과 같다.

```python
# 여러 노드가 같은 경로를 직접 여는 구조
connection = sqlite3.connect(
    "/shared/cache/master.sqlite"
)
```

`WAL`은 변경 내용을 본 파일에 합치기 전에 별도 로그에 적는 SQLite 방식이다. 이 기능을 켠다고 NAS 위의 여러 노드 쓰기가 자동으로 안전해지지는 않는다. DB 옆의 보조 파일과 잠금 규칙에도 의존하기 때문이다. 대상 파일시스템에서 공식적으로 지원하고 검증한 구성이 아니라면 SQLite는 노드 로컬로 제한한다.

### 12.3 기억 장소를 세 층으로 나눈다

실용적인 cache 구조는 다음과 같다.

```text
L1: process memory dict
  가장 빠른 hit, process crash 시 소멸

L2: worker-local SQLite
  로컬 재시작, index, transaction

L3: shared immutable delta chunks
  worker 간 교환, 장기 복구 재료
```

L1은 프로그램 메모리라 가장 빠르지만 프로그램이 꺼지면 사라진다. L2는 로컬 SQLite라 조금 느리지만 더 많은 항목과 검색 색인을 담을 수 있다. L3는 공유 NAS다. 여러 워커가 하나의 SQLite를 고치는 대신, 각 워커가 새로 생긴 항목만 완성된 파일로 게시한다.

### 12.4 안전벨트 경고를 끈다고 길이 안전해지는 것은 아니다

Python SQLite connection을 다른 스레드에서 사용하려고 `check_same_thread=False`를 설정하는 경우가 있다.

```python
connection = sqlite3.connect(
    local_path,
    check_same_thread=False,
)
```

`check_same_thread=False`는 “이 연결을 만든 스레드와 다른 스레드가 사용했다”는 기본 검사를 끈다. 여러 스레드의 작업을 자동으로 한 줄로 세워 주지는 않는다. 한 스레드가 데이터를 넣는 중에 다른 스레드가 같은 연결을 저장하거나 조회하면 순서가 꼬일 수 있다.

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

잠금을 잡은 채 NAS 파일을 읽거나 긴 계산을 하지 않는다. 데이터 변환을 먼저 끝내고 짧은 DB 변경만 잠금으로 보호한다. 열쇠를 가진 사람이 창고까지 다녀오면 다른 모두가 오래 기다리는 것과 같기 때문이다.

### 12.5 같은 입력이라도 모델과 규칙이 다르면 다른 답이다

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

여러 버전을 한 문자열 지문으로 합칠 수도 있다. 그래도 원래 구성 요소를 함께 저장하면 왜 캐시를 더는 사용할 수 없는지 설명하기 쉽다.

### 12.6 전체 사본을 덮어쓰지 말고 새 내용만 내보낸다

각 워커가 자기 로컬 DB 전체를 종료할 때 같은 NAS 원본에 복사하면 마지막 워커의 사본만 남는다. 조정자가 모든 DB를 나중에 합칠 수도 있지만 DB가 커질수록 매번 전체를 복사하는 비용이 커진다. 그래서 새로 생긴 항목만 따로 기록한다.

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

## 13. 전체 DB 대신 새 내용만 교환한다: 불변 델타

**델타(delta)**는 전체가 아니라 “이전 상태에서 새로 바뀐 부분”을 뜻한다. 공책 전체를 복사하지 않고 오늘 새로 쓴 페이지만 보내는 셈이다.

![워커 로컬 SQLite와 공유 NAS 사이에서 불변 델타를 교환하는 구조](assets/immutable-delta-cache.svg)

*그림 7. 여러 워커가 캐시 원본을 덮어쓰며 내용이 사라졌던 이슈에서 발전한 구조다. 실제 경로와 규모를 제거하고 공개용으로 다시 그렸다.*

분산 캐시에서 필요한 일을 좁혀 보자.

1. worker가 새 cache entry를 만들 수 있다.
2. 다른 worker가 잠시 뒤 그 entry를 재사용할 수 있다.
3. worker crash가 다른 worker의 상태를 손상시키면 안 된다.
4. 같은 델타를 여러 번 읽어도 한 번 읽은 것과 결과가 같아야 한다.
5. 공유 SQLite multi-writer는 사용하지 않는다.

이 요구에는 복잡한 분산 데이터베이스 대신 “각 워커가 자기 변경 묶음을 완성해서 올리는 구조”로 충분할 수 있다.

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

워커 0만 `shared/worker-00000/`에 쓴다. **청크(chunk)**는 여러 변경을 모은 한 묶음 파일이다. 한 번 게시한 청크는 수정하지 않는다. `latest.json`만 “현재 어떤 청크가 있는지” 가리키는 작은 목록이고, 그 워커 한 명만 바꾼다.

### 13.2 왜 공책 전체가 아니라 새 페이지만 보내는가

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

다른 워커를 **피어(peer)**라고 부른다. 피어는 이미 읽은 청크 이름을 로컬 DB에 기록하고 새 청크만 읽는다.

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

각 행을 언제나 같은 모양의 JSONL로 바꾸고 SHA-256 지문을 계산한다. 같은 내용이면 같은 지문이 나오도록 키 순서와 줄 형식을 고정한다.

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

청크 파일이 완전히 게시된 다음에만 `latest.json` 목록에 추가한다. 내용이 먼저, 안내표가 나중이다. 다른 워커가 목록에서 새 청크를 보았는데 실제 파일은 아직 없는 상태를 피한다.

실습 코드는 SQLite의 스레드 검사만 끄고 끝내지 않는다. 로컬 DB의 짧은 변경은 잠금으로 보호하고, 한 워커의 델타 게시는 한 번에 하나만 진행한다. 느린 NAS 쓰기는 DB 잠금 밖에서 수행한다. 그래야 캐시에 새 값을 넣는 짧은 일이 원격 파일 저장 전체를 기다리지 않는다. DB 연결을 닫기 전에는 진행 중인 내보내기와 가져오기가 모두 끝났는지도 확인한다.

목록을 게시한 뒤 로컬의 “여기까지 내보냈다”는 위치를 고치기 전에 프로그램이 꺼질 수도 있다. 그러면 같은 범위를 다시 내보낸다. 파일 이름이 순서 번호와 내용 지문으로 결정되므로 같은 내용은 같은 이름이 된다. 이미 같은 이름과 같은 정보가 있으면 목록에 두 번 넣지 않고 위치만 앞으로 옮긴다. 같은 이름인데 내용 정보가 다르면 충돌로 판단해 실패한다.

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

### 13.5 다른 워커의 새 봉투를 가져온다

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

읽은 바이트의 지문을 확인한 뒤 로컬 캐시에 넣는다. 캐시에 값을 넣는 일과 “이 청크를 읽었다”라고 표시하는 일은 같은 트랜잭션에서 처리한다. 중간에 꺼져도 다음 실행에서 청크를 다시 읽으면 된다. 여러 번 해도 한 번 한 것과 결과가 같은 성질을 **멱등성(idempotence)**이라고 한다.

### 13.6 같은 열쇠에 다른 답이 나오면 숨기지 않는다

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

### 13.7 새 내용이 모두에게 보이기까지는 시간이 걸린다

worker 0이 entry를 만든 직후 worker 1이 같은 입력을 처리하면 아직 delta가 게시되지 않아 중복 계산할 수 있다. 동기화 주기를 짧게 하면 중복은 줄지만 NAS metadata와 작은 write 부하는 늘어난다.

\[
\text{sync interval} \downarrow
\Rightarrow
\text{duplicate work} \downarrow,
\quad
\text{control I/O} \uparrow
\]

결과의 정확성이 중복 계산에 영향을 받지 않고 계산 비용만 늘어난다면 몇십 초 또는 몇 분의 지연을 허용할 수 있다. 반대로 한 요청이 매우 비싸고 중복률이 높다면 중앙 cache service나 더 빠른 exchange channel이 적합할 수 있다.

이 방식에서는 새 항목이 게시되고 다른 워커가 다음 동기화를 할 때까지 시간이 걸린다. 언젠가는 모두 같은 변경을 보게 되는 성질을 **최종 일관성(eventual consistency)**이라고 한다. 저장 직후 모든 워커가 즉시 같은 값을 보게 하는 강한 일관성과는 다르다.

### 13.8 가장 먼저 끝난 워커가 너무 일찍 최종본을 만들 수 있다

모든 delta를 하나의 master snapshot으로 합치려면 coordinator가 필요하다. array task에서 worker 0을 coordinator로 쓰는 패턴이 간단해 보이지만 worker 0이 가장 먼저 계산을 끝낼 수 있다.

```text
worker 0 done → 즉시 rebuild
worker 7 still running → 아직 마지막 delta 미게시
master snapshot → worker 7의 후반 delta 누락
```

최종 캐시를 다시 만들기 전에 모든 워커의 검증된 완료 도장을 기다린다.

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

## 14. 다시 실행해도 결과가 같게 만든다: 멱등성

오래 실행하는 분산 작업은 언젠가 중간에 멈춘다. 컴퓨터가 고장 나지 않아도 스케줄러의 시간 제한, 모델 준비 실패, 잘못된 입력, 잠깐의 네트워크 장애, 코드 오류가 생길 수 있다.

따라서 “먼저 빠르게 만들고 나중에 재시작 기능을 붙이자”라고 생각하지 않는다. 같은 일을 다시 시도해도 최종 결과가 한 번 실행한 것과 같게 만드는 성질을 **멱등성**이라고 한다. 이 성질은 처음부터 결과 주소와 파일 주인 규칙에 넣어야 한다.

### 14.1 레코드가 지나가는 상태를 이름 붙인다

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

상태 머신은 레코드가 어떤 상태에서 어떤 다음 상태로 갈 수 있는지 적은 지도다. 외부에서 완료로 볼 수 있는 시점은 결과 파일이 게시된 `RESULT_PUBLISHED` 이후다. `CHECKPOINTED`는 재시작 때 빨리 찾기 위한 색인이다. 모델 답이 메모리에만 있는 `INFERRED` 상태는 프로그램이 꺼지면 사라진다.

각 전이가 멱등적인지 묻는다.

- 같은 입력을 다시 load해도 안전한가?
- 같은 cache key로 모델을 다시 호출해도 허용되는가?
- 같은 결과 경로에 같은 content를 다시 게시해도 안전한가?
- 같은 journal event가 두 번 나타나면 dedupe할 수 있는가?

### 14.2 어떤 설정으로 만든 결과인지 지문을 남긴다

결과 파일이 있다는 이유만으로 건너뛰면 모델이나 프롬프트를 바꾼 뒤에도 옛 답을 재사용할 수 있다. 결과를 만드는 데 영향을 주는 설정을 모아 지문을 만든다.

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

### 14.3 두 번 처리될 수 있음을 인정하고, 최종 결과를 같게 만든다

결과 파일과 저널을 하나의 데이터베이스 트랜잭션처럼 동시에 바꿀 수 없다면, 중간 종료의 틈을 완전히 없애기 어렵다. 그래서 “처리는 한 번 이상 일어날 수 있지만, 같은 키의 최종 결과는 같아진다”는 계약이 현실적이다.

```text
processing delivery: at least once
result publication: idempotent by key/fingerprint
journal merge: deduplicated by record_id + fingerprint
final manifest: exactly one published version per coordinator epoch
```

같은 레코드가 두 번 처리될 수 있다. 그래도 같은 설정 지문과 키에는 같은 내용만 게시한다. 서로 다른 내용이 나오면 프로그램이 매번 다른 답을 냈거나 캐시 키에 필요한 버전이 빠졌다는 충돌로 드러낸다.

### 14.4 어떤 입력 경로로 시작해도 완료 목록을 확인한다

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

### 14.6 완료 도장을 재시작 경계로 사용한다

워커의 `DONE.json`이 없다면 그 저널은 쓰는 중에 멈춘 부분 상태다. 재시작 방식은 두 가지다.

1. 같은 attempt를 이어서 sequence 이후에 append한다.
2. 새 attempt 디렉터리를 만들고 이전 부분 journal의 유효 결과만 seed로 사용한다.

두 번째가 더 명확한 경우가 많다.

```text
attempt-0001/worker-00007/  # incomplete, immutable after failure
attempt-0002/worker-00007/  # new owner output
```

조정자는 첫 시도의 검증 가능한 결과와 두 번째 시도의 완료 결과를 레코드 키로 합친다. 어떤 시도에서 나온 결과인지 계보(lineage)가 남고 오래된 로컬 파일이 섞일 가능성이 줄어든다.

### 14.7 종료 시각 전에 정리할 시간을 남겨 둔다

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

가장 느린 저장 시간에도 여유를 더해 정리 시간을 정한다. 최종 합치기를 별도 조정자 작업으로 분리하면 계산 워커가 종료 전에 할 일이 줄어든다.

---

## 15. 건수보다 실제 일의 무게를 나눈다

작업을 여러 워커에 나누는 가장 쉬운 방법은 레코드 ID를 워커 수로 나눈 나머지를 사용하는 것이다.

```python
owner = int(record_id) % num_workers
```

ID가 고르게 섞여 있으면 각 워커의 레코드 수도 비슷하다. 하지만 레코드마다 실제 일의 무게가 다르면 완료 시간은 크게 벌어진다. 사진 한 장짜리 레코드와 사진 열 장짜리 레코드를 모두 “한 건”으로만 세면 안 된다.

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

전체 작업이 끝나는 시각을 `makespan`이라고 한다. 모두가 끝나야 다음 단계로 갈 수 있다면 평균 워커가 아니라 가장 늦은 워커가 종료 시각을 결정한다.

\[
T_{\text{job}} \approx \max_w \frac{C_w}{X_w}
\]

모든 worker가 barrier에서 기다리면 한 worker의 긴 tail이 전체 GPU 시간과 scheduler allocation을 늘린다.

### 15.1 카드를 한 장씩 돌아가며 나누기

전체 ID 목록이 고정되어 있다면 다음 방식은 건수 편차를 거의 없앤다.

```python
my_ids = all_ids[worker_id::num_workers]
```

원래 목록이 무거운 레코드 기준으로 무작위화되어 있거나 비용이 고르게 섞여 있다면 modulo보다 나을 수 있다. 그러나 이것도 비용을 직접 보지 않는다. 목록이 카테고리나 파일 크기로 정렬되어 있으면 stride가 특정 패턴과 공진할 수 있다.

### 15.2 무거운 일부터 가장 가벼운 바구니에 넣기

매니페스트에 예상 비용이 있다면 무거운 레코드부터 꺼내 현재 합계가 가장 작은 워커에 배치한다. 이 방법을 `weighted greedy`, 즉 가중치가 있는 탐욕적 배분이라고 부른다.

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

예상 비용은 완벽할 필요가 없다. 입력 파일 수, 전체 바이트, 과거에 걸린 시간만으로도 단순 건수보다 나을 수 있다. 실행 뒤 실제 비용과 예측의 차이를 기록해 다음 주소록을 개선한다.

### 15.3 일이 끝난 워커가 다음 묶음을 가져가게 하기

비용을 미리 알기 어렵다면 워커가 한 묶음을 끝낼 때마다 다음 묶음을 가져가게 할 수 있다. 일이 끝난 워커가 남은 일을 가져가는 방식을 `work stealing`이라고도 부른다. 한 워커가 무거운 레코드를 맡아도 다른 워커가 남은 일을 계속 처리할 수 있다.

그러나 trade-off가 있다.

- 중앙 queue 가용성
- 작업 임대 시간과 중복 전달
- 네트워크 단절 시 lease 복구
- 모든 작업이 정확히 한 번만 전달된다는 잘못된 가정
- 순서와 locality 손실
- 운영 복잡도

정적 shard + 충분히 작은 chunk가 중간 해법이다.

```text
manifest를 10,000개 큰 worker shard가 아니라
수백~수천 개 작은 immutable chunk로 나눈다.

worker는 scheduler/queue에서 chunk를 하나씩 가져간다.
chunk 안에서는 direct path와 private journal을 사용한다.
```

### 15.4 계산하는 사람과 최종 확인하는 사람을 나눈다

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

### 15.5 잘 나누었는지는 가장 늦은 워커까지 본다

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

## 16. 어디서 막혔는지 보이게 만든다

자동차 계기판에는 속도, 연료, 온도, 경고등이 따로 있다. “자동차 상태 63점” 같은 숫자 하나로 합치지 않는다. 원인에 따라 다음 행동이 다르기 때문이다.

파이프라인도 마찬가지다. 로그가 많아도 “느리다”와 “실패했다”를 구분하지 못하면 무엇을 고칠지 알 수 없다. 시스템 안에서 무슨 일이 일어나는지 바깥에서 판단할 수 있게 만드는 능력을 **관측성(observability)**이라고 한다. 목적은 모든 것을 출력하는 것이 아니라 다음 행동을 고를 수 있게 하는 것이다.

### 16.1 프로그램부터 공유 저장소까지 네 층을 본다

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

프로그램 숫자만 보면 NAS 응답이 느린지 CPU 이미지 변환이 느린지 헷갈릴 수 있다. 운영체제 숫자만 보면 높은 부하가 실제 레코드 처리량에 어떤 영향을 주었는지 알기 어렵다. 네 층의 같은 시간 구간을 함께 본다.

### 16.2 “부하가 높다”가 곧 “CPU가 꽉 찼다”는 뜻은 아니다

Linux의 load average는 CPU를 쓰려고 기다리는 작업뿐 아니라, 파일 I/O처럼 중간에 깨우기 어려운 대기를 하는 작업도 포함할 수 있다. 이 상태가 `D state`다. NAS 답을 기다리는 작업이 많으면 CPU가 쉬는 중인데도 부하 숫자가 높아질 수 있다.

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

### 16.3 문장 대신 같은 칸을 가진 기록을 남긴다

레코드마다 긴 문장을 출력하면 로그 쓰기 자체가 느려질 수 있다. 키와 값이 정해진 JSON처럼 같은 칸을 가진 로그를 사용하고, 정상 성공은 일정 시간마다 묶어서 낸다.

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

### 16.4 다시 하면 나을 오류와 다시 해도 같은 오류를 나눈다

모든 오류를 잡아 다시 시도하면 성공할 수 없는 입력도 계속 파일을 읽고 모델을 호출한다. 최소한 다음처럼 나눈다.

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

### 16.5 언제나 실패하는 레코드를 정상 줄에서 뺀다

항상 실패하는 입력을 `poison record`라고도 부른다. 이 레코드가 무한히 재시도되면 정상 작업도 늦어진다. 최대 시도 횟수와 마지막 오류를 기록해 격리 매니페스트로 보낸다.

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

### 16.6 대기열은 한순간의 숫자보다 시간에 따른 모양을 본다

한 시점의 대기열 길이보다 시간에 따라 계속 찼는지, 가끔 찼는지가 중요하다. 작은 시간 그래프를 스파크라인이라고 한다.

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

### 16.8 운영 안내서는 증상에서 시작한다

운영 중 따라 할 점검 순서를 `runbook`이라고 한다. 좋은 런북은 구성 요소 설명보다 눈앞의 증상에서 시작해 다음 확인으로 이어진다.

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

## 17. 효과를 믿을 수 있게 실험한다

성능 글은 “몇 배 빨라졌다”는 큰 숫자만으로 믿을 수 있게 되지 않는다. 무엇을 같은 조건으로 두고 무엇 하나만 바꿨는지 독자가 알 수 있어야 한다. 운영 과정에서 여러 변경이 섞였다면 각 변화의 몫을 나누어 설명한다.

### 17.1 본 것, 생각한 원인, 예상 결과, 틀렸다고 볼 조건

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

마지막 문장은 특히 중요하다. 어떤 결과가 나오면 내 생각이 틀렸다고 인정할지 미리 적는다. 그러면 기대와 다른 결과도 실패한 실험이 아니라 새로운 정보가 된다.

### 17.2 작은 부품 시험에서 여러 노드 시험까지 키운다

1. 마이크로 벤치마크: 파일 호출 하나의 비용을 본다.
2. 구성 요소 벤치마크: 파일 찾기나 결과 쓰기 한 부분만 실제 모양으로 실행한다.
3. 단일 노드 카나리: 모델과 I/O가 만날 때를 본다.
4. 여러 노드 카나리: 공유 NAS가 몰리는 정도와 가장 늦은 워커를 본다.

`nas_io_lab.py`는 1단계다. 이것만으로 전체 job이 몇 배 빨라진다고 주장하지 않는다. component와 canary에서 queue가 실제로 이동했는지 확인한다.

### 17.3 비교 기준을 적어서 고정한다

변경 전 비교 기준을 **베이스라인(baseline)**이라고 한다. 코드 버전, 입력 목록, 워커 수, 동시성, 측정 구간을 파일로 남긴다.

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

### 17.4 먼저 실행한 쪽만 손해 보지 않게 한다

A를 먼저 실행하고 B를 나중에 실행하면 B가 warm cache 이득을 볼 수 있다.

- ABBA 순서: A, B, B, A
- round마다 ID order shuffle
- 서로 다른 동등 shard를 교차 배정
- 첫 round를 warmup으로 보고 별도 표시
- 같은 시간대에 A/B worker를 병렬 실행하되 서로 부하 간섭 기록

공유 NAS에서 완전한 격리는 어렵다. 결과에 같은 시간대의 다른 workload가 있었는지 남기고 반복 측정으로 분산을 본다.

### 17.5 내부 수치를 숨겨도 비교 방법은 남긴다

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

### 17.6 여러 변경을 한 가지의 효과라고 부르지 않는다

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

### 17.7 빨라지는 동안 넘지 말아야 할 선도 정한다

초당 완료 수만 보면 실패한 항목을 버리거나 검증을 생략해도 빨라 보일 수 있다. 성능을 올릴 때 함께 지켜야 하는 안전선을 **가드레일(guardrail)**이라고 한다.

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

### 17.8 일부러 중간에 꺼 보아야 복구를 믿을 수 있다

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

### 17.9 성능 시험도 NAS에는 실제 일이다

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

## 18. 모든 조각을 이어 붙인 참조 구조

지금까지 만든 조각을 하나의 흐름으로 연결해 보자. 처음에는 아래 그림이 복잡해 보일 수 있다. 위에서 아래로 화살표만 따라가면 된다.

```text
주소록을 읽는다.
  → 알려진 주소로 파일을 읽는다.
  → 캐시에 답이 있으면 재사용한다.
  → 없으면 모델에 보낸다.
  → 결과를 완성해서 게시한다.
  → 워커의 저널에 적는다.
  → 모든 워커의 완료 도장을 검증한다.
  → 최종 주소록을 게시한다.
```

이 순서를 더 정확한 구성 요소 이름으로 펼치면 다음 구조가 된다.

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

### 18.2 출발하기 전에 주소록과 작업 공간을 확인한다

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

모델 서버의 `/health`가 성공했다고 실제 요청까지 받을 준비가 끝났다고 가정하지 않는다. 실제와 같은 작은 요청 하나로 준비 운동(`warmup`)을 확인한다. 준비에 실패하면 데이터 파이프라인을 시작하지 않아 대량 재시도를 막는다.

### 18.3 워커가 실제로 반복하는 일

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

이것은 흐름을 보여 주는 개념 코드다. 실제 구현에서는 완료 주소록이나 워커 로컬의 완료 집합을 사용해, 매 레코드마다 NAS에 “결과가 있나요?”라고 다시 묻지 않게 해야 한다. 결과가 있다는 것뿐 아니라 같은 설정 지문으로 만들었고 JSON도 정상인지 확인한다.

### 18.4 각 단계 사이의 대기실 크기를 정한다

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

Python의 `TaskGroup`으로 같은 단계의 작업을 묶으면 하나가 치명적으로 실패했을 때 나머지도 함께 정리할 수 있다. 대기열에 종료 신호를 넣을 때는 기다리는 소비자 수를 정확히 알아야 한다. 종료 신호 하나만 넣으면 소비자 한 명만 끝나고 나머지는 영원히 기다릴 수 있다.

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

이 숫자는 권장값이 아니라 무엇을 조절할 수 있는지 보여 주는 예다. 자동차의 의자 위치처럼 환경에 맞게 조절해야 한다. 작은 값부터 하나씩 바꾸며 자기 NAS, 입력 크기, 모델 서버, 메모리 예산에 맞는 지점을 찾는다.

### 18.6 어떤 상황에서도 지켜야 할 열 가지 약속

어떤 상황에서도 참이어야 하는 약속을 **불변식(invariant)**이라고 한다. 코드 리뷰에서는 다음 문장을 실제 테스트로 바꾼다.

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

### 18.8 이 구조를 그대로 복사하는 것이 목적은 아니다

이 설계가 유일한 답은 아니다. 핵심은 역할의 경계를 눈에 보이게 만드는 것이다.

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

## 19. 서비스를 멈추지 않고 옮기는 순서

달리는 자전거의 부품을 한꺼번에 바꾸면 어느 부품에서 문제가 생겼는지 알기 어렵다. 기존 파이프라인도 한 번에 전부 바꾸지 않는다. 데이터 형식, 스케줄러, 모델 서버, NAS 경로가 서로 연결되어 있기 때문이다. 결과를 잃지 않는 경계를 먼저 세우고, 측정 가능한 작은 변경을 하나씩 적용한다.

### 19.1 0단계: “완료”의 뜻부터 한 장에 적는다

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

### 19.2 1단계: 파일을 읽고 쓰는 코드를 목록으로 만든다

파일 I/O 호출을 찾는다.

```bash
rg -n \
  'os\.scandir|os\.listdir|os\.walk|glob\(|rglob\(|\.exists\(|\.stat\(|open\(|read_text|write_text|sqlite3\.connect' \
  src tests scripts
```

이런 목록 조사를 인벤토리(inventory)라고 한다. 검색된 코드를 모두 문제라고 보지 않는다. 각 호출 옆에 다음 정보를 붙인다.

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

### 19.3 2단계: 속도보다 기록 손상 위험을 먼저 없앤다

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

### 19.4 3단계: 바꾸기 전에 호출 횟수와 시간을 센다

코드 경로를 바꾸기 전에 다음을 관측한다.

- record당 `exists`, `scandir`, `open`, checkpoint flush 수
- metadata/payload/persist latency
- queue wait
- event-loop heartbeat
- worker별 cache hit와 모델 miss
- retry 원인
- 완료 sentinel 검증 결과

기존 코드에 counter를 넣으면 변경 후 실제로 호출 수가 줄었는지 확인할 수 있다. 속도만 비교하면 다른 날의 NAS 부하에 속을 수 있다.

### 19.5 4단계: 폴더 탐색을 주소록 만드는 단계로 분리한다

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

### 19.6 5단계: 알려진 주소를 먼저 열고, 없을 때만 다음 길을 찾는다

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

### 19.7 6단계: 이벤트 루프를 멈추는 호출을 찾는다

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

### 19.8 7단계: 대기열에 상한과 과부하 중단선을 둔다

기존 `gather(all_records)`를 streaming producer와 bounded queue로 바꾼다.

```python
async for record in manifest:
    await input_queue.put(record)  # full이면 backpressure
```

처음에는 보수적 동시성을 사용한다. single-node sweep 후 multi-node 합산 offered load를 계산한다.

작은 범위부터 새 방식을 넓혀 가는 일을 **롤아웃(rollout)**이라고 한다. 롤아웃을 멈출 빨간 선도 미리 정한다.

```text
중단:
  metadata p99 > baseline × 3 for 5 min
  OR retry rate > 5%
  OR local disk > 85%
  OR result queue full > 80% of samples
```

### 19.9 8단계: 워커별 완료 기록으로 바꾼다

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

### 19.10 9단계: 로컬 캐시를 먼저 안전하게 만든 뒤 델타를 붙인다

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

### 19.11 10단계: 레코드의 무게를 보고 일을 나눈다

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

### 19.12 문제가 생기면 옛 완성본을 다시 가리킨다

각 변경은 feature flag와 output namespace를 분리한다.

```yaml
io_mode: "direct-offloaded"       # legacy-scan | direct | direct-offloaded
checkpoint_mode: "worker-journal" # shared-append | worker-journal
cache_mode: "local-delta"         # disabled | local | local-delta
```

새 방식을 되돌리는 일을 **롤백(rollback)**이라고 한다. 롤백은 서둘러 새 결과를 지우고 옛 파일로 덮는 일이 아니다. 새 시도를 중단하고, 작은 준비 완료 표지가 이전의 검증된 매니페스트를 다시 가리키게 한다.

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

## 20. 이 방법이 맞지 않는 경우

지금까지의 방법은 여러 노드가 함께 보는 파일시스템에서 작은 파일을 많이 처리하는 배치 작업에 잘 맞는다. 그러나 저장소와 작업 모양이 다르면 답도 달라진다. 아래는 이 글의 규칙을 그대로 복사하지 말아야 할 경우다.

### 20.1 NAS라는 이름이 같아도 동작은 다를 수 있다

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

### 20.2 HPC 전용 병렬 파일시스템은 다른 기능을 제공할 수 있다

Lustre, Spectrum Scale, BeeGFS 같은 HPC parallel filesystem은 data와 metadata를 분산하고 대규모 병렬 I/O 기능을 제공할 수 있다. 그래도 작은 파일과 metadata server 부하는 별도 고려가 필요하다. 디렉터리 striping, file layout, collective I/O 같은 filesystem-specific 기능이 도움이 될 수 있다.

이 글의 direct path, immutable object, single writer, bounded concurrency 원칙은 여전히 유용하지만 최적 숫자와 배치 형태는 달라진다. 시스템 관리자의 권장 layout과 공식 benchmark를 따른다. [IOR와 mdtest](https://github.com/hpc/ior)는 HPC I/O와 metadata 패턴을 측정하는 공개 도구다. 애플리케이션 microbenchmark와 함께 사용하면 저장소 계층과 코드 계층을 분리해서 볼 수 있다.

### 20.3 객체 저장소에서는 “폴더”와 “이름 바꾸기”의 뜻이 다르다

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

### 20.4 SQLite를 NAS에 두는 모든 경우가 금지인 것은 아니다

한 호스트의 한 process만 DB를 열고 NAS는 backup 보관에만 사용한다면 문제 성격이 다르다. network filesystem 위에서도 exclusive single client, 적절한 rollback journal, 구현 검증으로 사용할 수 있는 경우가 있다. SQLite 공식 문서도 상황별 선택지를 설명한다.

이 글이 피하는 것은 “여러 노드가 같은 SQLite 파일을 동시에 직접 읽고 쓰면서 local DB와 같은 성능·잠금 의미를 기대하는 구조”다. 요구가 단순하면 local SQLite + immutable export가 더 설명하기 쉽다는 판단이다.

### 20.5 정말로 파일 이름을 모르면 폴더 목록을 읽어야 한다

파일 집합 자체가 외부 입력이고 별도 manifest 생산자를 둘 수 없다면 listing은 필수다. 이때 다음을 최적화한다.

- scan 전용 단계로 격리
- directory fan-out
- 중복 scan 방지
- incremental change feed 또는 timestamp cursor
- bounded parallel scan
- scan 결과 manifest 게시
- 예외와 permission error 계수

`scandir`는 나쁜 API가 아니다. 이미 아는 파일 하나를 찾기 위해 반복적으로 사용하는 질문이 잘못된 것이다.

### 20.6 NAS 파일을 로컬로 먼저 복사하는 것이 항상 빠르지는 않다

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

파일을 로컬로 가져오는 시작 시간까지 전체 작업 시간에 포함해 비교한다.

### 20.7 검사값을 계산하는 데도 읽기와 CPU 시간이 든다

SHA-256은 integrity를 높이지만 큰 payload를 다시 읽어 계산하면 I/O와 CPU가 증가한다. 데이터를 처음 읽을 때 streaming digest를 함께 계산하거나 upstream manifest의 checksum을 신뢰할 수 있는 경계에서 재사용한다.

```python
digest = hashlib.sha256()
with path.open("rb") as file:
    while chunk := file.read(1024 * 1024):
        digest.update(chunk)
        consume(chunk)
```

작은 control file은 매번 검증해도 비용이 작다. 수TB result 전체를 매 resume마다 다시 hash하는 것은 다른 설계가 필요하다. part 단위 checksum과 Merkle root를 사용할 수 있다.

### 20.8 주소록에 적힌 경로도 그대로 믿지 않는다

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

### 20.9 오래된 파일을 지우는 일도 별도 절차가 필요하다

불변 청크와 실행 시도를 계속 만들면 저장 공간이 늘어난다. 하지만 “최신이 아니니 지운다”는 위험하다. 다른 프로그램이 이전 매니페스트를 읽는 중이거나 롤백에 필요할 수 있다. 이런 정리 작업을 `garbage collection`, 줄여서 GC라고 부른다.

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

### 20.10 작은 작업에는 더 작은 해법을 쓴다

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
