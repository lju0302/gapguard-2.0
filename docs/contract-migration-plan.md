# GAPGUARD 2.0 메시지 계약 마이그레이션 계획

상태: 계약 제안 문서
기준 브랜치: `spike/kafka-messaging-plan`
참조 저장소: `/tmp/gapguard-reference`

이 문서는 참조 저장소의 Edge V2 계약을 P0 Simulator–MQTT–Kafka–Cloud SQL 흐름에
재사용하기 위한 제안이다. 이번 작업에서는 JSON Schema와 producer 코드를 수정하지
않는다. 현재 `docs/` 아래의 기존 미추적 변경사항도 보존한다.

## 1. 확인한 기준과 결론

참조 계약의 현재 기준은 다음 세 가지다.

- `contracts/common-message.schema.json`: Edge 공통 메타데이터 7개
- `contracts/edge-telemetry.schema.json`: 100Hz 샘플을 1초 단위로 요약한 telemetry V2
- `contracts/edge-event.schema.json`: 생명주기·임계치·연결·재전송·센서 장애 event V2

참조 구현은 `edge/edge_monitor/contracts/message_builder.py`에서 payload를 만들고,
`hashing.py`에서 `dataHash`를 계산한다. `cloud/runtime_delivery.py`는 재전송 시
`measuredAt`, `seqNo`, `dataHash`를 바꾸지 않는다. `cloud/iot_hub.py`의
`messageType`은 JSON body가 아닌 transport custom property다.

P0에는 참조 V2를 그대로 덮어쓰지 않고, 다음 원칙을 적용한다.

1. Edge business payload의 필드명·단위·계산식을 최대한 유지한다.
2. P0 운영에 필요한 `eventId`, `streamId`, `schemaVersion`, `producedAt`은 source에서
   한 번 만들고 재전송·Bridge·Consumer가 보존한다.
3. `dataHash`는 source payload의 무결성 hash로 유지한다. Bridge가 envelope을 다시
   만들면서 hash를 재계산하지 않는다.
4. 필수 필드와 timestamp 의미가 바뀌므로 기존 V2 schema identity를 in-place 수정하지
   않는다. P0는 병렬 V3 계약으로 시작한다.
5. Kafka value는 MQTT body와 같은 canonical P0 message shape를 사용한다. Kafka
   record timestamp, topic, partition, offset, Bridge 수신 시각은 transport metadata로
   남기고 business payload에 중복하지 않는다.

## 2. 참조 계약 요약

### 2.1 공통 Edge 필드

| 필드 | 참조 타입/필수 | 현재 의미 | P0 처리 |
|---|---|---|---|
| `siteId` | string / 필수 | GAPGUARD 현장 식별자 | 유지. Simulator의 SITE 식별자와 매핑 |
| `sectionId` | string / 필수 | 현장 내 retaining-wall section 식별자 | 유지. `wallId`를 별도 도입하면 매핑을 결정 |
| `deviceId` | string / 필수 | IoT device 식별자 | 유지. P0에서는 Generator Runtime의 논리 device ID로 사용 가능 |
| `sourceType` | `physical` 또는 `simulator` / 필수 | 데이터 생성 방식 | 유지. P0 Generator는 `simulator` |
| `measuredAt` | date-time / 필수 | telemetry 1초 window의 마지막 paired sample 시각, event 발생 시각 | 유지. UTC, replay 중 변경 금지 |
| `seqNo` | integer ≥ 0 / 필수 | Edge message sequence | 유지하되 P0의 순서 namespace를 `streamId` 기준으로 확정 |
| `dataHash` | 64자리 lowercase SHA-256 / 필수 | `dataHash`를 제외한 canonical JSON hash | 유지. 새 source 필드를 포함해 source에서 계산 |

참조 canonicalization은 UTF-8, sorted object keys, insignificant whitespace 제거,
non-finite number 거부, `dataHash` 자체 제외다. 배열 순서는 유지된다. 이 규칙은
Edge, replay, Bridge 검증, Consumer 검증에서 공통으로 사용해야 한다.

### 2.2 Edge telemetry V2

- 100개 샘플을 1초 window로 처리하고 기본적으로 window당 1건을 발행한다.
- `NORMAL`, `WARNING`, `DANGER`는 해당 window의 1차 상태 snapshot이다.
- `sensors.top`은 `0x69`, `sensors.bottom`은 `0x68`이다.
- `tiltX`, `tiltY`는 calibration 후 설치 baseline 대비 1초 평균 delta이며 단위는 도(deg)다.
- `vibrationRms`, `peakAcceleration`은 1초 dynamic acceleration 값이며 단위는
  `m/s^2`다. 두 값 모두 0 이상이다.
- `relativeTiltX = sensors.top.tiltX - sensors.bottom.tiltX`,
  `relativeTiltY = sensors.top.tiltY - sensors.bottom.tiltY`다.
- 센서 객체와 telemetry 최상위 객체는 `additionalProperties: false`인 닫힌 구조다.
  필드를 추가하면 구버전 consumer가 거부할 수 있다.

### 2.3 Edge event V2

event는 비주기 incident 기록이며 `eventType`, `severity`, `reason`, `details`가
필수다. eventType은 다음 13종이다.

`DEVICE_STARTED`, `DEVICE_STOPPED`, `CALIBRATION_COMPLETED`,
`CALIBRATION_FAILED`, `THRESHOLD_EXCEEDED`, `CONNECTION_LOST`,
`CONNECTION_RECOVERED`, `LOCAL_BUFFERING_STARTED`, `LOCAL_BUFFERING_STOPPED`,
`RETRANSMISSION_STARTED`, `RETRANSMISSION_COMPLETED`, `RETRANSMISSION_FAILED`,
`SENSOR_ERROR`

severity는 `INFO | WARNING | DANGER | ERROR`다. `details`는 event별 context를 담는
확장 객체이며 없을 때 `{}`를 사용한다. 임계치 event의 `relatedSeqNo`, rule 정보,
violation 목록 같은 값은 `details` 아래에 둔다. event payload에는 V1의
`eventId`, `eventAt`, `message`, `createdAt`이 없고 `measuredAt`과 `reason`을 쓴다.

### 2.4 self-test와 Twin desired

- `edge-self-test.request.schema.json`: 빈 객체 `{}`만 허용
- `edge-self-test.response.schema.json`: `PASS | DEGRADED | FAIL`, `checkedAt`,
  hardware/workers/storage/snapshotUpload 검사 결과
- `edge-self-test.error.schema.json`: `error` 문자열
- `edge-twin-desired.schema.json`: `telemetryIntervalSec` 1 또는 10, threshold와
  `rule_version`; 알 수 없는 desired property는 허용

이 네 계약은 MQTT telemetry/event 및 Kafka business event와 직접 같은 payload가
아니다. P0에서는 파일을 유지·복사하고, Kafka topic으로 보내지 않는다. self-test는
Control API의 동기 호출 경계에, Twin desired는 Simulator/Edge 제어 경계에 둔다.

## 3. 홉별 필드 정책

정책의 기준은 “source가 만든 business field는 양쪽 홉에서 동일하고, transport
관측값은 body와 분리”다.

| 필드/속성 | MQTT device → broker | MQTT → Kafka bridge → consumer | 비고 |
|---|---|---|---|
| `siteId`, `sectionId`, `deviceId` | 유지 | 유지 | 기존 Edge 소비자와 API의 핵심 identity |
| `sourceType` | 유지 | 유지 | `simulator`/`physical` 의미 변경 금지 |
| `sensors`, `relativeTiltX/Y`, `status` | 유지 | 유지 | telemetry payload와 단위·계산식 보존 |
| `eventType`, `severity`, `reason`, `details` | event에서 유지 | 유지 | 13종 enum과 details 구조 보존 |
| `measuredAt` | source 관측/발생 시각으로 포함 | 원문 그대로 전달 | UTC, 재전송·Bridge retry 때 갱신 금지 |
| `seqNo` | source가 발급 | 원문 그대로 전달 | 순서·gap 진단용. Bridge가 새 번호를 만들지 않음 |
| `dataHash` | source payload를 canonical hash한 값 | 먼저 검증 후 원문 보존 | hash mismatch는 DLQ, 저장하지 않음 |
| `eventId` | **추가. source에서 1회 발급** | 그대로 보존 | deduplication의 primary identity |
| `streamId` | **추가. source의 논리 순서 stream** | 그대로 보존, Kafka partition key로 사용 | retry/restart에도 같은 stream이면 유지 |
| `schemaVersion` | **추가. P0 V3 값** | 그대로 보존 | topic 이름과 함께 호환성 판정에 사용 |
| `producedAt` | **추가. source가 business payload를 만든 시각** | 그대로 보존 | Bridge 시도 시각으로 덮어쓰지 않음 |
| MQTT `messageType` | topic 또는 MQTT property로 전달 | Kafka topic/header로 전달 | body field로 중복하지 않음 |
| `testRunId` | Simulator P0에서 추가 권장 | 유지 | 실험별 completeness·audit 범위 |
| `generatorId`, `fieldId`, `wallId` | P0 trace context로 추가 검토 | 유지 또는 API projection | `sectionId`와 중복 identity가 되지 않게 매핑 확정 |
| `bridgeReceivedAt` | 없음 | Kafka header/bridge audit에 추가 | source `producedAt`과 구분 |
| `kafkaPublishedAt` | 없음 | Kafka record metadata 또는 audit에 추가 | end-to-end latency 측정용 |
| topic/partition/offset/retryCount | 없음 | Kafka metadata/DLQ envelope에 추가 | business `dataHash` 대상에서 제외 |

`messageType`은 참조 Azure IoT Hub에서는 custom property로 정의되어 있다. P0 MQTT
에서는 `gapguard.telemetry.v3`, `gapguard.event.v3` topic을 우선 사용하고, Bridge가
Kafka topic과 header에 같은 종류를 기록한다. 하나의 body에 `eventType`이 있다는
이유만으로 telemetry와 event를 혼합하지 않는다.

Kafka draft의 `payload: {}` envelope를 그대로 도입하면 기존 top-level Edge field와
새 envelope field가 이중화될 위험이 있다. P0에서는 body를 중첩하지 않고 동일한
V3 message를 Kafka value로 전달하는 것을 권장한다. 별도 envelope이 꼭 필요해지면
`dataHash`의 hash scope와 source payload를 명시한 `kafka-envelope-v1.schema.json`을
별도 계약으로 만들고, consumer가 source payload를 먼저 검증하도록 해야 한다.

## 4. P0 핵심 필드 의미와 중복 처리 기준

### 4.1 제안 의미

| 필드 | P0 제안 |
|---|---|
| `eventId` | 논리 메시지 하나의 불변 ID. source가 발급하고 replay/Bridge/consumer가 절대 재생성하지 않는다. 권장 생성 규칙은 `streamId:seqNo`이며, `streamId`가 실험 간 전역 유일하지 않으면 `testRunId`를 stream namespace에 포함한다. |
| `streamId` | 같은 벽·센서 논리 stream의 안정적인 순서 namespace. Kafka partition key이며 retry, process restart, broker 장애 후에도 유지한다. |
| `seqNo` | `streamId` 안에서 telemetry와 event가 공유하는 0 이상 정수 sequence. 정상 생성에서는 증가하고, runtime 재시작 후에도 저장된 값에서 이어간다. 누락 판정은 gap을 발견한 즉시 유실로 확정하지 않고 test run 종료 또는 watermark 이후 확정한다. |
| `measuredAt` | 센서 window의 마지막 paired sample 시각 또는 event가 실제로 관찰된 시각. source UTC timestamp이며 backlog가 길어져도 변경하지 않는다. |
| `producedAt` | source가 완성된 business message와 hash를 만든 시각. `producedAt - measuredAt`는 Edge 생성·처리 지연을 나타낸다. Bridge 수신/ Kafka 기록 시각은 별도 metadata다. |
| `dataHash` | source V3 payload에서 `dataHash`를 제외한 canonical JSON의 lowercase SHA-256. 새 P0 source field를 포함한 뒤 계산한다. Bridge는 검증만 하고 body를 바꾸지 않는다. |
| `schemaVersion` | payload contract의 major version. 기존 `edge-* V2` 의미를 보존하는 파일은 V2로 남기고 P0 필수 identity와 시간 의미를 포함하는 신규 계약은 V3으로 한다. 알 수 없는 major는 DLQ로 격리한다. |

### 4.2 중복·충돌·순서 처리

Consumer와 Data 저장소는 `eventId`를 업무 idempotency key로 사용한다.

1. 같은 `eventId`와 같은 `dataHash`: 같은 논리 메시지의 재전송이다. business row를
   다시 적용하지 않고 `duplicate_count`와 `delivery_count`만 증가시킨다.
2. 같은 `eventId`와 다른 `dataHash`: identity collision 또는 변조다. 기존 row를
   덮어쓰지 않고 conflict/DLQ로 보낸다.
3. 같은 `(streamId, seqNo)`와 다른 `eventId`: sequence collision이다. DLQ와 계약
   오류 지표에 남긴다.
4. 다른 `eventId`지만 같은 `dataHash`: hash만으로 dedup하지 않는다. 서로 다른
   논리 메시지일 수 있으므로 별도 처리하고 hash 재사용 지표만 기록한다.
5. `seqNo`가 마지막 값보다 작거나 gap이 있어도 메시지를 즉시 폐기하지 않는다.
   Kafka key 순서, replay, late arrival을 기록하고 test run의 expected watermark와
   비교해 missing 여부를 판정한다.
6. `schemaVersion` 검증 실패, JSON Schema 실패, hash mismatch는 retry 횟수 제한 후
   원본 body와 오류 원인, source topic, partition, offset, `eventId`를 DLQ에 남긴다.

권장 저장 제약은 P0 실험 범위에 따라 다음과 같다.

```text
UNIQUE(test_run_id, event_id)
UNIQUE(test_run_id, stream_id, seq_no)
INDEX(stream_id, measured_at DESC)
```

실제 장치의 sequence가 test run을 넘어 지속되면 `event_id`는 전역 unique로 승격할
수 있다. 현재 P0 문서의 `UNIQUE(event_id)`, `UNIQUE(stream_id, seq_no)`는
`testRunId`와 sequence reset 정책을 확정한 뒤 조정해야 한다.

## 5. Schema 파일 마이그레이션 제안

### 5.1 그대로 복사·재사용할 파일

다음은 MQTT/Kafka telemetry/event contract와 독립적이므로 참조 파일을 그대로
복사해 사용한다.

- `contracts/edge-self-test.request.schema.json`
- `contracts/edge-self-test.response.schema.json`
- `contracts/edge-self-test.error.schema.json`
- `contracts/edge-twin-desired.schema.json`
- 위 네 계약의 `contracts/examples/` fixture

단, `$id`가 새 저장소 위치와 맞는지 확인하고, self-test와 Twin desired의
`schemaVersion`이라는 내부 설정 필드와 메시지 계약의 `schemaVersion`을 혼동하지
않는다. Simulator의 `tilt-drift` state 파일에도 `schemaVersion`이 있지만 이는
메시지 계약과 무관한 별도 상태 파일이다.

### 5.2 V2를 보존하고 새로 작성할 파일

다음 파일은 기존 V2를 archive/reference로 보존하고, P0에서는 새 파일로 작성한다.

- `contracts/common-message-v3.schema.json`
  - 기존 7개 필드를 유지
  - `eventId`, `streamId`, `schemaVersion`, `producedAt` 추가
  - `testRunId`, `generatorId`, `fieldId`, `wallId`의 required/optional 여부 확정
- `contracts/edge-telemetry-v3.schema.json`
  - telemetry V2의 sensors, 단위, relative tilt 공식, status enum 유지
  - V3 common metadata를 `$ref`로 연결
- `contracts/edge-event-v3.schema.json`
  - 13종 eventType, 4종 severity, details extensibility 유지
  - V3 common metadata를 `$ref`로 연결

기존 `common-message.schema.json`, `edge-telemetry.schema.json`,
`edge-event.schema.json`은 V2 소비자와 buffered message를 위해 수정하지 않는다.
필수 필드 추가, timestamp 의미 변경, hash scope 변경은 JSON Schema validation만
통과해도 호환성이 유지되지 않으므로 병렬 V3와 명시적인 rollout이 필요하다.

### 5.3 Kafka envelope은 조건부 신규 파일

P0 기본안은 Edge V3 body를 Kafka value에 그대로 전달하므로 추가 envelope schema가
필요하지 않다. Kafka payload를 반드시 `{metadata, payload}`로 감싸야 하는 경우에만
다음 파일을 별도로 만든다.

- `contracts/kafka-message-envelope-v1.schema.json`

이 파일에는 source message의 `eventId`, `streamId`, `schemaVersion`, `producedAt`,
`dataHash`와 원문 payload, bridge 관측 metadata의 소유권을 명시해야 한다. source
payload hash와 envelope 전체 hash를 같은 `dataHash` 이름으로 표현하지 않는다.

## 6. 영향 범위와 작업 순서

| 영역 | 참조에서 확인한 관계 | P0 영향 | 필요한 조치 |
|---|---|---|---|
| Edge | `message_builder.py`가 telemetry/event를 만들고 `hashing.py`가 hash 계산. `runtime_delivery.py`가 replay에서 기존 식별·시간·hash를 보존 | 구현 producer | V3 metadata 생성, sequence 영속화, hash 계산 순서 고정, replay 시 네 필드 보존 |
| MQTT transport | `cloud/iot_hub.py`가 `messageType`을 custom property로 설정 | P0 broker/Bridge 경계 | telemetry/event topic·property 규칙, QoS 1, 재전송 시 body 불변 규칙 확정 |
| Simulator | `simulators/vm_simulator.py`가 V2 Edge pipeline과 100Hz scenario를 사용. 내부 scenario state의 `schemaVersion`은 별도 개념 | 구현 예정 producer | `testRunId`, `generatorId`, `streamId`, persistent seqNo를 발급하고 deterministic fixture 추가 |
| Streaming | 참조 저장소에 MQTT–Kafka Bridge와 Kafka consumer 구현은 확인되지 않음 | 계획/미구현 | schema/hash 검증, `streamId` partition key, `acks=all`, retry, manual commit, DLQ, audit metadata 구현 |
| Data | 참조 SQL은 eventId 부재를 보완해 `deviceId:event:seqNo`를 생성하고 `data_hash`를 저장. ADX는 telemetry leaf path를 매핑 | 구현 consumer/저장 + 일부 미검증 | `eventId`를 source of truth로 전환, test run 범위 unique key, UTC 보존, schema V3 컬럼·mapping 추가 |
| API | 참조 Functions가 `measuredAt`, `deviceId`, status와 SQL/ADX 결과를 조회 | 구현 consumer | Kafka를 직접 노출하지 않고 query model로 projection. latest 정렬·latency·duplicate·gap 지표 API 추가 |
| Web | 참조 Web은 API 결과의 `deviceId`, `measuredAt`, relative tilt, status를 표시 | 구현 예정 consumer | API 응답 타입만 확장하고 MQTT/Kafka 직접 연결 금지. timeline은 eventId/streamId를 key로 사용 |
| Tests/fixtures | `tests/edge/test_message_contracts.py`, hash/blackbox tests가 V2와 replay를 검증 | 검증 | V3 positive/negative, duplicate/conflict, gap, old buffered replay, unknown version, hash mismatch fixture 추가 |

참조 저장소의 deterministic consistency audit도 실행했다. common/telemetry/event
JSON parse와 required field 선언은 통과했지만 ADX mapping datatype 누락 및 schema
leaf path 감사 실패가 보고됐다(FAIL 21, WARN 2). 따라서 Data 영역은 “참조 구조가
있다”와 “P0 ingest가 검증됐다”를 같은 상태로 취급하지 않는다. 이 audit 경고와
기존 SQL의 `source_event_id` 생성 로직은 V3 rollout 전에 별도 수정·검증해야 한다.

권장 rollout 순서는 다음과 같다.

1. V3 schema와 fixture, event identity/sequence/hash 결정 고정
2. Consumer/Storage를 V2·V3 tolerant reader로 배포하고 DLQ·audit 저장 준비
3. API projection과 Web 타입·timeline key 업데이트
4. Simulator와 Edge producer를 V3로 전환하고 MQTT body를 발행
5. Bridge가 topic·schema·hash를 검증하고 Kafka에 동일 body를 기록
6. P0 장애 실험에서 duplicate, gap, missing, backlog drain, latency를 확인
7. buffered V2 수명과 rollback window가 끝난 뒤 V2 topic/reader 종료 여부 결정

## 7. P0에서 열어둘 결정사항

다음 항목은 구현자가 임의로 확정하면 안 된다.

1. **event identity namespace**: 권고는 `eventId = streamId:seqNo`이며 Simulator의
   `streamId`에 `testRunId`를 포함하는 방식이다. test run 간 동일 stream을 허용할지
   결정해야 한다.
2. **seqNo 범위**: 참조 README는 `deviceId + messageType + seqNo`를 duplicate
   기준으로 설명하지만, 참조 SQL 문서는 telemetry/event가 하나의 seqNo series를
   공유한다고 적고 있다. P0 권고는 `streamId`별 telemetry/event 단일 series이며
   restart 후에도 유지하는 것이다.
3. **producedAt 소유자**: Kafka 초안은 Bridge/Producer의 Kafka 기록 시도를
   `producedAt`으로 설명한다. P0 권고는 source 생성 시각을 `producedAt`으로 고정하고
   Bridge 수신·Kafka 기록 시각을 별도 transport metadata로 두는 것이다.
4. **V3 운용 방식**: 기존 V2 topic과 병렬 V3 topic을 운용할지, tolerant reader 후
   단일 topic을 유지할지 결정해야 한다. 필수 필드가 늘어나는 현재 상황에는 병렬
   V3 topic을 권고한다.
5. **identity mapping**: P0 문서의 `wallId`·`streamId`·`generatorId`와 참조
   계약의 `sectionId`·`deviceId` 중 어느 값이 원천 identity인지 정해야 한다. 권고는
   `sectionId`를 기존 wall section의 canonical ID로 유지하고 `streamId`를 그 아래
   ordered stream으로 추가하는 것이다.
6. **hash scope**: P0 기본안은 MQTT body와 Kafka value가 동일하므로 source body
   hash 하나만 사용한다. envelope 도입 시 source hash와 envelope hash를 분리할지
   결정해야 한다.

## 8. 이번 작업의 완료 상태

- 작성 파일: `docs/contract-migration-plan.md`
- 수정하지 않은 것: 모든 JSON Schema, Edge producer, Simulator, Streaming, Data,
  API, Web 코드와 기존 미추적 변경사항
- 열린 결정: 위 6개 항목
- 다음 담당: 계약 결정 승인 후 V3 schema·fixture 담당자가 schema 파일과 검증 테스트를
  같은 변경 단위로 작성
