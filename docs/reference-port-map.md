# GAPGUARD 1.0 참조 코드 이식 맵

상태: P0 이식 기준안
참조 기준: `team-between/gapguard` `main` (`e98e4a3`)
현재 기준: `spike/kafka-messaging-plan`

이 문서는 참조 저장소의 디렉터리와 GAPGUARD 2.0 P0의 대응 범위를 정한다. 참조
저장소의 Azure 전용 구현은 보존하고, P0 실행 경로에는 필요한 모듈만 이식한다.

## 1. 재사용 우선순위

### 높은 재사용 가치

- `edge/edge_monitor/processing.py`
  - 100Hz 수집 윈도우와 요약 처리
- `edge/edge_monitor/runtime.py`
  - Collector–Spooler–Sender 3-worker 실행 구조
- `edge/edge_monitor/pipeline.py`
  - 위험도 판정 전후의 메시지·상태 처리 흐름
- `edge/edge_monitor/rules.py`
  - `NORMAL`, `WARNING`, `DANGER` 및 incident transition
- `edge/edge_monitor/storage/blackbox.py`
  - SQLite Outbox와 sequence snapshot
- `edge/edge_monitor/cloud/reliable_sender.py`
  - FIFO drain, retry, exponential backoff의 동작 원리
- `edge/edge_monitor/contracts/hashing.py`
  - canonical JSON과 SHA-256 규칙
- `edge/edge_monitor/snapshots/`
  - raw artifact를 telemetry outbox와 분리하는 구조
- `simulators/vm_simulator.py`
  - 시나리오 생성과 Edge pipeline 연결 흐름
- `web/src/components/dashboard/`
  - P0에서 필요한 현장·센서·상태 화면 컴포넌트
- `web/src/services/apiClient.ts`, `web/src/types/`
  - API 접근과 화면 타입 구조

## 2. 교체가 필요한 경계

| 참조 경로 | 결합된 기술 | 2.0 대응 |
|---|---|---|
| `edge/edge_monitor/cloud/iot_hub.py` | Azure IoT Hub | MQTT client adapter로 교체 |
| `edge/edge_monitor/cloud/runtime_delivery.py` | IoT Hub Direct Method/Twin | MQTT command와 runtime status 경계로 재작성 |
| `edge/edge_monitor/handlers/command_handler.py` | Direct Method, Device Twin | Control API → MQTT command 처리로 재작성 |
| `functions/function_app.py` | Azure Functions runtime | P0 API 실행 방식에 맞춰 유지 또는 FastAPI adapter 작성 |
| `functions/telemetry/` | Event Hub, Service Bus, ADX, ADT | Kafka consumers와 Cloud SQL writer로 분리 |
| `functions/services/adx_service.py` | Azure Data Explorer | P0에서는 보류. 필요한 조회는 PostgreSQL로 대체 |
| `functions/services/adt_service.py` | Azure Digital Twins | P0에서는 보류 |
| `functions/weather/` | 기상청 + Azure Blob/ADT/SQL 연계 | P0 세로 흐름에서 보류 |
| `functions/reports/` | Azure SQL/Blob 기반 보고서 | P0에서 보류 |
| `data/sql/` | Azure SQL 문법·객체 | PostgreSQL schema/migration으로 변환하며 구조는 유지 |
| `data/adx/` | ADX table/mapping/KQL | P0에서는 보류 |
| `digital-twins/` | DTDL와 Azure Digital Twins | P0에서는 보류 |
| `infrastructure/terraform/*.tf` | `azurerm_*` 리소스 | GCP Terraform을 별도 구성 |
| `azure-pipelines-*.yml` | Azure Pipelines | GCP 배포 경로 확정 후 CI/CD 재작성 |

## 3. 2.0 디렉터리 대응

```text
contracts/                 참조 계약을 V2 보존 + P0 V3 병렬 추가
edge/                      3-worker, outbox, rules, hashing 중심 이식
simulators/                vm_simulator.py를 8 Runtime·fault 시나리오로 확장
streaming/                 신규: MQTT–Kafka Bridge와 Kafka consumers
data/sql/                  PostgreSQL schema, migration, seed, verify
functions/                 Control/Query API와 storage projection
web/                       기존 Next.js 화면과 API client 재사용
infrastructure/terraform/  GCP VM, broker, Kafka, Cloud SQL, monitoring
tests/edge/                Edge 동작 테스트
tests/functions/           API·저장 테스트
tests/integration/         신규: MQTT → Kafka → SQL 세로 흐름
tests/load/                신규: 처리량·lag·latency·backlog 검증
```

`contracts/`, `edge/`, `simulators/`, `functions/`, `data/`, `web/`, `tests/`의 상위
경계는 참조 저장소와 동일하게 유지한다. 신규 `streaming/`만 Cloud 내부 메시징의
독립 실행 경계로 추가한다.

## 4. 담당자별 파일 경계

- 계약 담당: `contracts/`, 계약 관련 fixture와 계약 테스트만 수정
- Edge 담당: `edge/`, `tests/edge/` 수정. `streaming/` 구현은 수정하지 않음
- Simulator 담당: `simulators/`, Simulator 전용 테스트 수정
- Streaming 담당: `streaming/`, `tests/integration/`의 메시징 부분 수정
- Data/API 담당: `data/sql/`, `functions/`, 해당 테스트 수정
- Web 담당: `web/`, API response type에 필요한 계약 요청
- Infra 담당: `infrastructure/`, CI/CD 설정. 실제 GCP 생성·배포는 별도 승인

`tests/`는 공용이지만 각 담당자가 자기 기능의 하위 디렉터리를 소유한다. 공통
fixture나 계약을 바꿀 때는 계약 담당자의 검토를 먼저 받는다.

## 5. P0에서 보류할 참조 기능

- Azure Data Explorer 적재·KQL 분석
- Azure Digital Twins 모델·관계·동기화
- 기상청 수집과 ADT weather patch
- 보고서 렌더링과 Blob archive
- Azure IoT Hub Direct Method/Device Twin 구현
- 기존 Azure 리소스의 Terraform 직접 수정

보류는 파일 삭제를 의미하지 않는다. 참조 코드와 2.0 이식 코드의 실행 경로를
분리해 rollback과 비교 검증이 가능하도록 한다.

## 6. 이식 전 사용자 결정사항

계약 담당 문서에서 다음 항목을 먼저 확정해야 한다.

1. `eventId` namespace와 `testRunId` 포함 여부
2. `seqNo`의 stream 범위와 재시작 후 지속 방식
3. `producedAt`의 소유자와 Bridge 관측시각 필드
4. V2/V3 topic 병렬 운영 여부
5. `sectionId`, `wallId`, `streamId`, `deviceId`의 canonical identity
6. Kafka envelope 도입 여부와 `dataHash` 범위

이 결정 전에는 Edge producer와 Streaming consumer를 동시에 구현하지 않는다.
