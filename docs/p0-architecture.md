# GAPGUARD 2.0 P0 아키텍처 기준안

상태: 구현 전 기준안
기준 브랜치: `spike/kafka-messaging-plan`
목적: VM 기반 가상 현장 Runtime의 생성·장애·복구와 벽 상태의 지속 보존을 검증한다.

## 1. P0 확정 범위

- Raspberry Pi는 사용하지 않는다.
- 모든 현장 데이터는 GCP VM 안의 Simulator Runtime이 생성한다.
- 현장 2개를 둔다.
- 현장마다 SITE 4개를 둔다.
- 전체 SITE는 8개이며, Generator Runtime도 8개다.
- `SITE 1개 = Generator Runtime 1개`로 매핑한다.
- Generator Runtime은 내부적으로 100 samples/s를 생성한다.
- 1초마다 샘플을 요약하고 위험도를 판정한다.
- 요약·판정 결과를 로컬에 보존한 뒤 MQTT로 전송한다.
- MQTT Broker는 사용한다.
- Kafka는 Managed Kafka를 사용하지 않고 오픈소스 Kafka 서버를 운영한다.
- Kafka 앞에는 Load Balancer를 두지 않는다.
- P0 Kafka는 단일 노드 KRaft로 시작한다.
- P0 MQTT Broker도 단일 노드로 시작한다.
- 벽 상태와 상태 이력은 Cloud SQL PostgreSQL에 저장한다.
- UI는 API를 통해 Runtime 상태, 벽 상태, 장애·복구 지표를 조회한다.

## 2. 논리적 리소스 구조

```text
Field-001
  ├─ SITE-001 / Generator-001
  ├─ SITE-002 / Generator-002
  ├─ SITE-003 / Generator-003
  └─ SITE-004 / Generator-004

Field-002
  ├─ SITE-005 / Generator-005
  ├─ SITE-006 / Generator-006
  ├─ SITE-007 / Generator-007
  └─ SITE-008 / Generator-008
```

각 SITE 아래에는 Zone, Wall, 가상 디바이스 또는 센서 스트림을 둘 수 있다.

```text
Field → Site → Zone → Wall → Stream
```

`Generator Runtime`은 SITE 단위의 실행·장애·복구 경계다. 한 Simulator VM 안에서 8개의 독립 프로세스 또는 컨테이너로 실행한다.

## 3. 전체 데이터 흐름

```text
                    ┌─────────────────────┐
                    │ Control UI           │
                    │ Runtime / Wall 상태  │
                    └──────────┬──────────┘
                               │ REST
                    ┌──────────▼──────────┐
                    │ Control API          │
                    │ 명령·조회·상태 집계  │
                    └──────┬─────────┬─────┘
                           │         │
                 MQTT command      Cloud SQL
                           │         │
┌──────────────────────────▼─────────┴────┐
│ Simulator VM                              │
│                                           │
│ VM Agent                                  │
│  ├─ Generator Runtime: SITE-001           │
│  ├─ Generator Runtime: SITE-002           │
│  └─ Generator Runtime: SITE-008           │
│                                           │
│ 100 samples/s                             │
│   → 1초 요약·판정                         │
│   → SQLite Outbox 저장                     │
│   → MQTT publish                          │
└──────────────────────────┬────────────────┘
                           │
                           ▼
                    MQTT Broker VM
                           │
                           ▼
                   MQTT–Kafka Bridge
                           │
                           ▼
                       Kafka VM
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        Storage Consumer Alert Consumer Reconciliation
              │
              ▼
       Cloud SQL PostgreSQL
              │
              ▼
           Query API
```

UI는 Kafka나 MQTT를 직접 읽지 않고 Query API만 호출한다.

## 4. Simulator VM과 Runtime 관리

### VM 구성

- Simulator VM 1대
- VM Agent 1개
- Generator Runtime 8개
- Runtime별 SQLite Outbox
- Outbox용 영속 디스크

VM Agent와 각 Runtime은 heartbeat를 발행한다. 따라서 VM 장애와 개별 SITE Runtime 장애를 구분할 수 있다.

```text
Simulator VM: ONLINE
  ├─ SITE-001: RUNNING
  ├─ SITE-002: RUNNING
  ├─ SITE-003: OFFLINE
  └─ SITE-004: BACKLOG
```

### Runtime 제어

Control UI → Control API → MQTT command topic → VM Agent 또는 Runtime 순서로 명령을 전달한다.

지원할 명령 후보:

- `start_runtime`
- `stop_runtime`
- `restart_runtime`
- `change_rate`
- `inject_network_fault`

Runtime 상태에는 명령 상태와 실제 상태를 따로 기록한다.

```text
desired_state: RUNNING
reported_state: OFFLINE
```

권장 상태:

```text
RUNNING
STOPPED
DEGRADED
OFFLINE
BACKLOG
ERROR
```

## 5. 샘플링·요약·전송 기준

내부 샘플링과 외부 메시지 전송은 별도 속성으로 관리한다.

```text
sampling_rate_hz = 100
publish_interval = 1 second
```

Runtime의 1초 처리:

```text
100개 샘플 생성
  → 평균·최대·분산 등 요약
  → 위험도 판정
  → 요약 이벤트 생성
  → SQLite Outbox 저장
  → MQTT 전송
```

P0에서 Generator Runtime 8개가 각각 100 samples/s를 생성하면 내부 생성량은 800 samples/s다.

SITE 하나에 벽 또는 가상 디바이스 스트림이 `W`개라면:

```text
내부 생성량 = 8 × W × 100 samples/s
Kafka 전송량 = 8 × W × 1 summary/s
```

벽별 상태를 UI에서 보여줘야 하므로 벽 스트림별 1초 요약 이벤트를 유지한다.

## 6. MQTT Broker

P0에서는 MQTT Broker 1대를 사용한다.

권장 토픽:

```text
gapguard.telemetry.v1
gapguard.alert.v1
gapguard.runtime-status.v1
gapguard.command.v1
gapguard.dlq.v1
```

기본 방향:

- MQTT QoS 1
- Runtime persistent session
- Runtime 상태 retained message 검토
- telemetry 전체 retained 사용 금지
- Broker는 private network에 배치
- P0는 Load Balancer 없이 단일 Broker
- MQTT 다중화와 L4 Load Balancer는 후순위 HA 실험

MQTT–Kafka Bridge는 Kafka 저장 성공을 확인한 뒤 MQTT 메시지를 완료 처리해야 한다.

```text
MQTT 수신
  → Kafka publish
  → Kafka 성공 확인
  → MQTT 처리 완료
```

사용하는 MQTT 클라이언트가 수동 ACK를 제어할 수 없으면 Bridge 내부에도 SQLite spool을 둔다.

## 7. Kafka

P0에서는 오픈소스 Kafka 단일 노드 KRaft를 사용한다.

- Kafka VM 1대
- 영속 디스크
- private IP
- Load Balancer 없음
- Bridge와 Consumer가 broker 주소를 직접 사용
- Producer `acks=all`
- Producer `enable.idempotence=true`
- Consumer `enable.auto.commit=false`
- 업무 처리 성공 후 offset commit
- Cloud SQL은 `eventId` 기준 멱등 저장

초기 토픽:

```text
gapguard.telemetry.v1       partitions=3
gapguard.alert.v1           partitions=3
gapguard.runtime-status.v1  partitions=1
gapguard.dlq.v1             partitions=1
```

같은 벽 또는 센서 스트림의 순서를 유지하기 위해 `streamId`를 partition key로 사용한다.

## 8. 메시지 식별자와 데이터 보존

모든 요약 이벤트에 다음 식별자를 포함한다.

```json
{
  "testRunId": "run-20260915-001",
  "generatorId": "generator-001",
  "fieldId": "field-001",
  "siteId": "site-001",
  "wallId": "wall-001",
  "streamId": "site-001:wall-001",
  "eventId": "site-001:wall-001:1842",
  "seqNo": 1842,
  "sampleCount": 100,
  "measuredAt": "2026-09-15T00:00:00Z",
  "dataHash": "sha256:..."
}
```

`eventId`는 재전송·중복 검증의 기준이고, `seqNo`는 스트림별 누락·순서 검증의 기준이다. 테스트 실행이 달라도 충돌하지 않도록 감사 식별자는 `(test_run_id, event_id)` 조합으로 유일하게 관리한다.

벽 상태는 현재 상태와 이력으로 분리한다.

```text
wall_state_current
wall_state_history
alerts
```

권장 제약조건:

```text
UNIQUE(event_id)
UNIQUE(stream_id, seq_no)
INDEX(stream_id, measured_at DESC)
```

## 9. 복구·유실 검증

복구 성공은 재연결 여부가 아니라 최종 데이터 완전성으로 판정한다.

검증 지점:

```text
Runtime 생성
  → MQTT publish
  → Kafka 저장 성공
  → Consumer 처리
  → Cloud SQL 유일 저장
  → API 조회 가능
```

주요 지표:

```text
generated_count
mqtt_publish_success
kafka_publish_ack
consumer_processed
db_unique_count
duplicate_count
missing_count
seq_gap_count
sample_gap_count
```

손실률:

```text
loss_rate =
(expected_unique_events - persisted_unique_events)
÷ expected_unique_events × 100
```

Runtime은 스트림별 워터마크를 주기적으로 기록한다.

```text
stream_id
expected_first_seq
expected_last_seq
expected_count
sample_count
generated_at
```

P0에서는 다음 감사 테이블을 둔다.

```text
event_audit
  test_run_id
  event_id
  PRIMARY KEY (test_run_id, event_id)
  stream_id
  generator_id
  seq_no
  kafka_topic
  kafka_partition
  kafka_offset
  first_seen_at
  processed_at
  stored_at
  delivery_count
  status
```

장애 실험마다 `faultId`를 발급하고 다음 시간을 기록한다.

```text
fault_started_at
fault_detected_at
reconnected_at
first_recovered_event_at
backlog_zero_at
recovery_completed_at
```

최종 복구 조건:

```text
backlog = 0
missing_count = 0
loss_rate = 0%
duplicate_count 기록
seq_gap_count 기록
```

## 10. UI에서 보여줄 정보

### Runtime 화면

- VM heartbeat
- Runtime heartbeat
- desired/reported state
- MQTT 연결 상태
- Outbox depth
- 마지막 전송 시각
- 최근 오류
- 장애 주입 상태

### 현장·벽 화면

- Field 2개
- SITE 8개
- Zone 및 Wall 계층
- 현재 상태
- 위험도
- 마지막 측정 시각
- 마지막 처리 시각
- 벽 상태 Timeline
- alert 이력

### 복구·신뢰성 화면

- generated count
- Kafka publish count
- consumer processed count
- DB unique count
- missing count
- duplicate count
- seqNo gap count
- Kafka lag
- backlog drain time
- 최종 복구율
- 최종 유실률

예시:

```text
Wall Status: WARNING
Data Status: RECOVERED
Completeness: 100%
Missing: 0
Duplicate: 3
Seq Gap: 0
Backlog Drain: 42s
```

## 11. P0 장애 시나리오

### Runtime 하나 중지

```text
SITE-003 Runtime 중지
  → SITE-003 OFFLINE
  → 해당 Outbox 또는 backlog 증가
  → Runtime 재시작
  → backlog 재전송
  → 유실·중복·순서 검증
```

### Simulator VM 중지

```text
Simulator VM 중지
  → 8개 SITE Runtime 중단
  → VM과 Runtime 상태를 UI에 표시
  → VM 복구
  → 8개 Runtime 재기동
  → 전체 backlog drain 검증
```

### MQTT Broker 중지

```text
MQTT 연결 실패
  → Runtime SQLite Outbox 증가
  → Broker 복구
  → MQTT 재연결
  → backlog 재전송
```

### Kafka 중지

```text
Kafka publish 실패
  → Bridge 재시도 또는 Bridge spool
  → Kafka 복구
  → Kafka 저장 수와 DB 저장 수 비교
```

### Storage Consumer 중지

```text
Consumer 중지
  → Kafka lag 증가
  → Consumer 재시작
  → DB 유일 이벤트 수와 expected 이벤트 수 비교
```

## 12. 후순위 범위

- Kafka 3노드 KRaft HA
- MQTT Broker 다중화 및 L4 Load Balancer
- API 다중화 및 API Load Balancer
- Managed Kafka
- BigQuery
- 복잡한 프론트엔드
- Raspberry Pi 실장치 연동

## 13. P0 완료 기준

다음 흐름을 실제 실행과 지표로 확인한다.

```text
8개 Generator Runtime
  → MQTT
  → Kafka
  → Storage Consumer
  → Cloud SQL
  → API/UI
```

그리고 다음을 검증한다.

1. Runtime 1개를 중지하면 해당 SITE만 OFFLINE이 된다.
2. 1초 요약·판정 결과가 벽 상태 현재값과 이력에 저장된다.
3. MQTT, Kafka, Consumer 장애 후 backlog가 복구된다.
4. 복구 후 missing, duplicate, seqNo gap을 계산할 수 있다.
5. 최종 유실률과 복구율이 test run 단위로 저장된다.
6. VM 장애와 Runtime 장애를 UI에서 구분할 수 있다.
