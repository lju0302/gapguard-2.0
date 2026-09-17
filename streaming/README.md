# GAPGUARD 2.0 Streaming

상태: 로컬 P0 검증 구현

이 디렉터리는 MQTT–Kafka Bridge와 Kafka Consumer를 API, Simulator, 계약 정의에서 분리하기 위한 경계다. 현재는 외부 브로커 없이 흐름을 검증하는 인메모리 Bridge와 SQLite telemetry Consumer를 제공한다. 실제 MQTT·Kafka 클라이언트, Docker, Terraform은 후속 단계다.

## MQTT–Kafka Bridge를 독립 서비스로 두는 이유

MQTT는 Edge 연결, 세션, QoS, 재연결을 다루고 Kafka는 Cloud 내부의 내구성 있는 이벤트 기록과 소비자별 처리를 다룬다. 두 시스템의 장애·재시도·확장 단위가 다르므로 Bridge를 독립 서비스로 두면 다음 경계를 유지할 수 있다.

- MQTT 수신과 Kafka 기록 성공을 별도로 관찰한다.
- Kafka 기록이 확인된 뒤에만 MQTT 메시지 처리를 완료한다.
- Bridge 장애와 Consumer 장애를 각각 복구하고 지표화한다.
- Bridge와 Consumer를 서로 다른 배포·확장 주기로 운영한다.

사용 중인 MQTT 클라이언트가 수동 ACK를 제어하지 못하면 Bridge 내부 spool이 필요할 수 있다. spool의 구현 방식은 아직 결정하지 않는다.

## 디렉터리 경계와 책임

### `bridge/`

MQTT Broker에서 이벤트를 수신하고, `contracts/`의 계약에 따라 검증한 뒤 Kafka에 발행하는 독립 서비스 경계다. Producer 재시도, Kafka publish 확인, MQTT 완료 처리, 수신·성공·실패·중복 지표를 담당한다. Cloud SQL 저장이나 알림 같은 업무 처리는 담당하지 않는다.

### `consumers/`

Kafka 이벤트를 목적별 독립 Consumer Group으로 처리하는 서비스 경계다. 저장, 알림, 분석을 각자의 offset·lag·재시도·DLQ 흐름으로 운영한다. 한 Group의 지연이나 중단이 다른 목적의 소비를 막지 않아야 한다.

### `common/`

Bridge와 Consumer가 함께 사용하는 계약 로딩, envelope 검증, 식별자·키·오류 메타데이터 같은 공통 경계다. 이벤트 payload의 별도 스키마를 복제하거나 `contracts/`의 정의를 재해석하지 않는다.

### 로컬 검증 경로

`InMemoryBridge`는 검증 성공 뒤 topic에 메시지를 append하고, `TelemetryStorageConsumer`는 저장 성공 뒤에만 다음 offset을 반환한다. SQLite 저장소는 `eventId`를 기본 키로 사용해 재전송을 멱등 처리한다.

전체 로컬 흐름은 다음 명령으로 실행한다.

```bash
PYTHONPATH=. python3 -m streaming.p0_flow
```

출력의 `published_count`, `stored_count`, `next_offset`으로 8개 Runtime의 처리 결과를 확인한다.

실제 로컬 Broker를 띄울 준비가 되면 다음 Compose 파일을 사용한다.

```bash
docker compose -f docker-compose.local.yml config
docker compose -f docker-compose.local.yml up -d
docker compose -f docker-compose.local.yml ps
docker compose -f docker-compose.local.yml down
```

실제 클라이언트 Bridge는 `streaming/bridge/live.py`에 있으며, 실행 전 다음 의존성을 설치한다.

```bash
python3 -m pip install -r streaming/requirements-transport.txt
PYTHONPATH=. python3 -m streaming.bridge.live
```

8개 Runtime의 full V3 telemetry를 한 번 발행하려면 다음 명령을 사용한다.

```bash
.venv/bin/python -m simulators.mqtt_publisher
```

Alert 이벤트는 `KafkaAlertConsumer`가 `eventId` 기준으로 멱등 저장한다. 소비 실패 재시도와 최종 DLQ 레코드 형식은 `streaming/common/retry.py`에 정의한다.

## P0 Topic과 Consumer Group

현재 두 기준 문서에 정의된 P0 논리 토픽은 다음과 같다.

| Topic | 용도 | 초기 파티션 기준 |
| --- | --- | ---: |
| `gapguard.telemetry.v1` | 1초 요약 telemetry | 3 |
| `gapguard.alert.v1` | 위험 판정·알림 이벤트 | 3 |
| `gapguard.device-status.v1` | 디바이스 연결·상태 변화 | 3 |
| `gapguard.runtime-status.v1` | Runtime heartbeat·상태 변화 | 1 |
| `gapguard.command.v1` | Control API에서 Runtime으로 가는 명령 | 미정 |
| `gapguard.dlq.v1` | 반복 처리 실패 이벤트 격리 | 1 |

초기 메시징 검증의 필수 업무 Consumer Group은 다음과 같다.

| Consumer Group | 책임 |
| --- | --- |
| `telemetry-storage-v1` | telemetry를 저장소에 멱등 반영 |
| `alert-dispatch-v1` | alert 전달·기록 |
| `analytics-v1` | 집계와 실험 지표 기록 |

`runtime-status`의 처리 주체와 별도 reconciliation Consumer Group의 필요 여부는 구현 단계에서 확정한다. 같은 이벤트를 여러 목적이 소비할 때는 목적마다 별도 Group을 사용한다.

같은 벽·센서 스트림의 순서가 필요하면 동일한 `streamId`를 partition key로 사용한다. 기존 초안의 `deviceId` 키와의 최종 선택은 열린 결정사항이다.

## 전달·멱등성·재처리 원칙

- 전체 흐름은 **at-least-once**로 설계한다. P0에서 exactly-once 처리를 주장하지 않는다.
- 모든 이벤트는 `contracts/`가 정의하는 `eventId`를 가져야 한다. 재전송과 중복은 정상적으로 발생할 수 있으며, 각 Consumer와 외부 저장소의 side effect는 `eventId` 기준으로 멱등 처리한다.
- Kafka Producer는 `acks=all`, `enable.idempotence=true`와 재시도를 검토한다. Producer 설정만으로 외부 저장소의 exactly-once가 보장되지는 않는다.
- Consumer는 `enable.auto.commit=false`를 사용하고, 업무 처리와 downstream 저장이 성공한 뒤에만 offset을 commit한다.
- 일시 오류는 제한된 retry를 수행한다. 반복 실패 이벤트는 원본 topic·partition·offset·`eventId`·`retryCount`·오류 정보를 포함해 `gapguard.dlq.v1`로 보낸다.
- DLQ 이벤트를 버리지 않는다. 원본 `eventId`로 원인 확인과 재처리가 가능해야 한다.
- 중복 수, `seqNo` gap, Consumer lag, 처리 latency, backlog drain time을 숨기지 않고 기록한다.

## 계약 원천

메시지 envelope과 이벤트별 payload의 단일 계약 원천은 저장소 최상위의 `contracts/`다.

- Bridge, Consumer, Simulator는 `contracts/`의 JSON Schema와 fixture를 기준으로 검증한다.
- `streaming/` 아래에 동일한 schema·enum·필수 필드를 복사해 관리하지 않는다.
- 계약 변경은 `eventId`, `seqNo`, `schemaVersion`, `measuredAt`, `producedAt`, `dataHash`와 호환성 영향을 함께 검토한다.
- 계약이 확정되기 전에는 payload 필드와 enum을 임의로 추가·변경하지 않는다.

## 아직 결정하지 않은 항목

다음은 이 구조에서 의도적으로 확정하지 않은 구현 기술과 운영 설정이다.

- MQTT Broker와 Bridge의 구현 언어·라이브러리, 수동 ACK 지원 여부, Bridge spool 방식
- Kafka 배포 버전, Docker 이미지, Producer·Consumer 클라이언트 라이브러리
- 실제 최대 telemetry rate와 평균 payload 크기, 이에 따른 파티션·Consumer 병렬도
- `device-status.v1`와 `runtime-status.v1`의 명칭·payload·Consumer 소유권 및 partition key
- topic별 보존 기간, retry 횟수·간격, DLQ 재처리 절차
- P0 단일 노드와 이후 3노드 KRaft의 VM 수·디스크·복제 설정
- TLS/SASL 적용 시점과 인증서·secret 보관 방식
- Storage Consumer의 Cloud SQL 스키마 연계와 Alert Consumer의 실제 전달 대상

이 항목들은 기술·운영 검증 결과와 사용자 승인 후 확정한다.
