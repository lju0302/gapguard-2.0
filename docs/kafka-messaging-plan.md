# GAPGUARD 2.0 Kafka 메시징 계획

상태: 초안
작업 브랜치: `spike/kafka-messaging-plan`
범위: Edge–Cloud 내부 메시징과 Kafka 검증 흐름

## 1. 결론

장치 통신은 MQTT에 두고, Kafka는 Cloud 내부 이벤트 스트리밍에만 사용한다.

```text
Edge Sensor
  → SQLite Outbox
  → MQTT Broker
  → MQTT–Kafka Bridge
  → Kafka Topic
  → 독립 Consumer Group
  → Storage / Alert / Analytics
```

초기 검증은 Docker 기반 단일 노드 KRaft로 시작한다. 메시지 계약과 소비 흐름이 검증되면 3노드 Kafka로 확장하여 broker 장애, consumer 장애, backlog 복구를 측정한다. Managed Kafka 도입은 이 실험 결과와 비용을 확인한 뒤 결정한다.

## 2. P0 완료 기준

다음 한 세로 흐름을 실제 호출과 지표로 확인한다.

1. Simulator 또는 Edge가 MQTT로 telemetry를 발행한다.
2. Bridge가 MQTT 메시지를 Kafka에 기록한다.
3. `telemetry-storage-v1` Consumer Group이 메시지를 처리한다.
4. `alert-dispatch-v1`와 `analytics-v1`은 서로 독립적으로 같은 이벤트를 소비한다.
5. `eventId` 기준 유일 메시지 수, 중복 수, Consumer Lag, end-to-end latency를 확인한다.

P0에서는 정확히 한 번 처리한다고 주장하지 않는다. Kafka 기록은 중복을 허용하는 at-least-once 흐름으로 설계하고, Consumer와 저장소에서 `eventId` 또는 `dataHash`로 멱등 처리를 검증한다.

## 3. 토픽 초안

| 토픽 | 목적 | 초기 파티션 | 보존 기본값 | 키 |
| --- | --- | ---: | ---: | --- |
| `gapguard.telemetry.v1` | 센서 측정값 | 3 | 24시간 | `deviceId` |
| `gapguard.alert.v1` | 위험 판정 이벤트 | 3 | 7일 | `deviceId` |
| `gapguard.device-status.v1` | 연결·상태 변화 | 3 | 7일 | `deviceId` |
| `gapguard.dlq.v1` | 처리 불가 이벤트 | 1 | 7일 | 원본 `eventId` |

파티션 수는 현장 수가 아니라 목표 처리량과 Consumer 병렬도에 맞춰 조정한다. 위 값은 구현 전 검증용 시작값이며, 1/10/100 msg/s 부하 실험 후 확정한다. Kafka는 파티션 내부 순서만 보장하므로 같은 디바이스의 순서가 필요하면 모든 이벤트에 동일한 키를 사용한다.

## 4. 공통 메시지 계약

모든 Kafka 이벤트는 동일한 envelope을 사용하고, 이벤트별 데이터만 `payload`에 둔다.

```json
{
  "eventId": "device-001:1842",
  "eventType": "telemetry",
  "schemaVersion": 1,
  "siteId": "site-001",
  "zoneId": "zone-001",
  "deviceId": "device-001",
  "seqNo": 1842,
  "measuredAt": "2026-09-15T00:00:00Z",
  "producedAt": "2026-09-15T00:00:01Z",
  "severity": "NORMAL",
  "dataHash": "sha256:...",
  "payload": {}
}
```

필수 검증 항목:

- `eventId`: 재전송·중복 확인용. 기본값은 `deviceId:seqNo` 조합을 검토한다.
- `seqNo`: 디바이스별 순서와 누락 탐지용.
- `measuredAt`: 센서가 실제 측정한 시각.
- `producedAt`: Bridge 또는 Producer가 Kafka 기록을 시도한 시각.
- `schemaVersion`: 호환성 변경을 명시한다.
- `dataHash`: 재전송된 payload의 동일성 확인용.

계약 확정 전에는 `payload` 내부 필드와 enum을 임의로 늘리지 않는다. 계약 확정 시 JSON Schema와 유효·무효 fixture를 함께 추가한다.

## 5. Producer와 Bridge 동작

Bridge는 MQTT와 Kafka 사이의 경계 컴포넌트다.

- MQTT QoS 1을 기본 검토하고, Kafka `send` 성공 응답을 받은 뒤 MQTT 측 처리를 완료한다.
- Producer는 `acks=all`, `enable.idempotence=true`, 재시도를 사용한다.
- Kafka publish 실패 시 MQTT 연결 복구와 Producer retry를 분리해 기록한다.
- 중복 publish는 오류로 숨기지 않고 `eventId` 기준으로 집계한다.
- Bridge가 장시간 중단되면 MQTT broker의 보존 정책과 Edge SQLite Outbox의 재전송 동작을 함께 확인한다.

Producer의 멱등성 설정은 `acks=all`과 함께 사용해야 하며, 복제 토픽은 `min.insync.replicas`와 같이 검증한다. 이 설정은 메시지 손실 가능성을 줄이지만 Consumer의 외부 저장소까지 exactly-once를 보장하지 않는다.

## 6. Consumer Group 설계

| Consumer Group | 책임 | 처리 성공 기준 |
| --- | --- | --- |
| `telemetry-storage-v1` | telemetry 저장 | 저장소 멱등 반영 후 offset commit |
| `alert-dispatch-v1` | alert 전달·기록 | 알림 처리 성공 후 offset commit |
| `analytics-v1` | 집계·실험 지표 | 집계 저장 성공 후 offset commit |

각 목적은 별도 Consumer Group으로 둔다. 같은 Group 안에서는 파티션을 나눠 처리하고, 다른 Group은 같은 이벤트를 독립적으로 받는다.

기본 소비 정책:

- `enable.auto.commit=false`
- 업무 처리 성공 뒤 offset commit
- 일시 오류는 제한된 retry 후 재처리
- 반복 실패 이벤트는 `gapguard.dlq.v1`에 원본 topic, partition, offset, error, retryCount와 함께 기록
- DLQ 이벤트를 버리지 않고 원본 `eventId`로 재처리 가능하게 유지

## 7. 신뢰성·장애 시나리오

| 시나리오 | 관찰할 결과 |
| --- | --- |
| MQTT와 Bridge 사이 네트워크 단절 | Edge Outbox 또는 MQTT 보존 후 재전송, 중복 수 |
| Kafka broker 중단 | Producer 오류·재시도·복구 후 기록 누락 여부 |
| Consumer 중단 | Consumer Lag 증가와 재기동 후 backlog drain |
| Consumer 처리 지연 | `records-lag-max`, 처리 latency, DLQ 변화 |
| 3노드 중 1노드 장애 | ISR 변화, leader 재선출, 기록 가능 여부 |
| 잘못된 schemaVersion | 검증 실패, DLQ 격리, 정상 이벤트 처리 지속 |

3노드 검증 환경에서는 기본적으로 replication factor 3, `min.insync.replicas=2`, Producer `acks=all` 조합을 검토한다. 단일 노드 개발 환경은 replication factor 1로 별도 설정하며, 개발 결과를 HA 증거로 해석하지 않는다.

## 8. 관측 지표

최소 수집 지표는 다음과 같다.

- 입력: MQTT 수신 수, Edge Outbox depth, Bridge 수신·실패 수
- Kafka: publish 성공·실패, topic별 처리량, ISR, under-replicated partitions
- Consumer: `records-lag-max`, 현재 offset, commit 실패, 재시도 수, DLQ 수
- 품질: `eventId` 유일 수, duplicate 수, `seqNo` gap 수, message loss 수
- 지연: `measuredAt → Kafka 기록`, `Kafka 기록 → 처리 완료`, p95/p99
- 복구: failover time, backlog drain time

모든 지표에는 `siteId`, `deviceId`, topic, consumer group을 추적할 수 있는 식별자를 남긴다. 원본 payload 전체를 로그에 남기지 않아 민감정보와 로그 비용을 통제한다.

## 9. 구현 순서

### 단계 0 — 계약 고정

- envelope과 이벤트별 payload를 JSON Schema로 정의
- `eventId`, `seqNo`, `measuredAt`, `dataHash` fixture 작성
- 정상·중복·순서 역전·schema 오류 사례 작성

### 단계 1 — 로컬 P0

- Docker 기반 단일 노드 KRaft 실행
- 3개 업무 토픽과 DLQ 생성
- MQTT–Kafka Bridge, telemetry Consumer, alert Consumer 최소 구현
- 1/10/100 msg/s 시뮬레이션과 end-to-end 지표 확인

### 단계 2 — 재처리·관측

- manual offset commit
- retry와 DLQ
- Consumer Lag, duplicate, seqNo gap, latency 측정
- Consumer 재기동과 backlog drain 검증

### 단계 3 — HA 실험

- 3노드 KRaft 구성
- replication factor 3, `min.insync.replicas=2` 검증
- broker 1대 중단, leader 재선출, Producer 재시도 측정

### 단계 4 — GCP·비용 판단

- Terraform으로 VM·방화벽·모니터링을 재현
- 평시와 부하 실험의 VM 수·디스크·네트워크 비용 기록
- Managed Kafka 전환 여부를 비용·운영 부담·실험 결과로 결정

## 10. 확정이 필요한 항목

- 실제 최대 telemetry rate와 이벤트 평균 payload 크기
- Kafka 배포 버전과 Docker 이미지 기준
- GCP에서 Kafka를 실행할 VM 수와 디스크 방식
- MQTT broker 후보와 Bridge 구현 방식
- 저장소와 alert 전달 대상
- TLS/SASL 적용 시점과 인증서 보관 방식
- topic별 보존 기간과 DLQ 재처리 운영 절차

## 참고

- [Apache Kafka Design](https://kafka.apache.org/41/design/design/)
- [Apache Kafka KRaft](https://kafka.apache.org/40/operations/kraft/)
- [Apache Kafka Producer Configs](https://kafka.apache.org/40/configuration/producer-configs/)
