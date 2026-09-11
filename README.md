# GAPGUARD 2.0

GAPGUARD는 중소규모 굴착·흙막이 현장의 기울기와 진동을 실시간으로 감시하는 Edge–Cloud 안전 모니터링 시스템입니다.

GAPGUARD 2.0은 데이터스쿨 3차 프로젝트에서 구현한 기존 GAPGUARD를 기반으로, 현장 데이터가 Edge에서 생성된 뒤 메시징·스트리밍 계층을 거쳐 안정적으로 처리되는 전체 플랫폼을 검증하는 프로젝트입니다.

현재 2.0 코드는 구현 전 단계입니다. 기존 코드와 운영 경험을 분석한 뒤 필요한 기능을 이식·재구성합니다.

## 목표

- GCP와 오픈소스 중심으로 인프라 재구성
- Terraform 기반 Infrastructure as Code
- CI/CD를 통한 테스트·빌드·배포 자동화
- MQTT 기반 장치 통신과 Kafka 기반 Cloud 내부 스트리밍 검증
- VM으로 여러 현장·구역·디바이스의 부하 재현
- Load Balancer, Broker, Consumer 다중화와 확장 검증
- 네트워크 단절·노드 장애·처리 지연·backlog 복구 실험
- latency, lag, failover, message loss를 수치로 측정
- GCP 비용 상한 `$300` 이내에서 운영

## 프로젝트 배경: GAPGUARD 1.0

1.0은 Raspberry Pi와 MPU6050 계열 센서로 현장 데이터를 수집하고 Edge에서 위험도를 판정했습니다. 네트워크 장애가 발생해도 계측이 중단되지 않도록 SQLite Outbox에 데이터를 보존하고, 연결 복구 후 FIFO 방식으로 재전송했습니다.

기존 Azure 중심 구조에는 다음 요소가 포함되어 있었습니다.

- Raspberry Pi / VM Simulator
- Azure IoT Hub, Service Bus, Azure Functions
- ADX, SQL, Blob, Azure Digital Twins
- C2D / Device Twin 기반 원격 제어
- Web Dashboard

Edge의 핵심 책임 분리는 2.0에서도 유지합니다.

```text
Sensor
  ↓
Collector  : 센서 샘플 수집
  ↓
Spooler    : 주기 처리, 위험 판정, seqNo 부여
  ↓
SQLite Outbox
  ↓
Sender     : 클라우드 전송 및 장애 복구 후 재전송
```

## 2.0 목표 아키텍처

```text
Physical Site A                 Site B~N
Raspberry Pi + Sensors          VM Simulator
          │ MQTT                       │ MQTT
          └──────────────┬─────────────┘
                         ▼
                 Load Balancer / Proxy
                         ▼
                  MQTT Broker (EMQX 등)
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       Event / Alert             Kafka
                                    │
                 ┌──────────────────┼──────────────────┐
                 ▼                  ▼                  ▼
          Telemetry Consumer  Alert Consumer  Storage / Analytics

Terraform  →  GCP VPC / VM / LB / Firewall / Monitoring
CI/CD      →  Test / Build / Artifact / Deploy / Health Check
```

실제 Raspberry Pi와 VM Simulator는 같은 메시지 계약을 사용합니다. 초기 검증에서는 복잡한 웹 계층을 만들지 않고 CLI·API 또는 최소한의 운영 도구로 부하와 장애 시나리오를 실행합니다.

## Edge 신뢰성

통신 상태와 관계없이 센서 수집과 위험 판정이 계속되는 Local-first 동작을 핵심 자산으로 유지합니다.

- `seqNo`: 메시지 순서와 재전송 추적
- `measuredAt`: 실제 측정 시각
- `dataHash`: 데이터 동일성 확인
- SQLite Outbox: 전송 실패 데이터의 로컬 보존
- FIFO 재전송: 연결 복구 후 backlog 순차 처리

### 센서 Calibration

`calibrate_zero`는 실제 센서의 기준점 설정 과정으로 구현할 예정입니다.

```text
Raw sensor sampling
        ↓
Outlier 제거
        ↓
평균 bias 계산
        ↓
zero offset 저장
        ↓
이후 측정값에 offset 적용
```

Calibration 결과는 Edge 로컬 상태로 저장하여 재부팅 후에도 사용합니다. 랜덤값 기반 시뮬레이션과 실제 장치 처리를 구분하는 것을 목표로 합니다.

## VM Simulator와 부하·장애 실험

VM Simulator는 센서 데이터 생성기이면서 부하 생성기와 장애 실험 도구 역할을 합니다. 시뮬레이터는 다음 시나리오를 지원하도록 확장할 예정입니다.

| 영역 | 시나리오 |
| --- | --- |
| Site status | `ONLINE` / `OFFLINE` |
| Telemetry rate | `1 msg/s` / `10 msg/s` / `100 msg/s` |
| Sensor scenario | `NORMAL` / `TILT_WARNING` / `TILT_DANGER` / `VIBRATION_SHOCK` |
| Network | `NORMAL` / `LATENCY` / `PACKET LOSS` / `DISCONNECTED` |
| Load multiplier | `1x` / `10x` / `100x` |

초기 규모는 약 8개 현장과 40개 구역을 예시로 고려합니다. 실제 현장·구역·디바이스 수는 부하 목표와 GCP 비용 검토 후 확정합니다.

| 항목 | 현재 상태 |
| --- | --- |
| 현장 수 | TBD (예시: 약 8개) |
| 구역 수 | TBD (예시: 약 40개) |
| 디바이스 수 | TBD |
| GCP 비용 상한 | `$300` |

## 메시징 계층

### Device → Cloud: MQTT

실시간 장치 통신은 MQTT와 self-hosted Broker를 우선 검토합니다.

- Broker 후보: EMQX 등 오픈소스 MQTT Broker
- Load Balancer 또는 Nginx / HAProxy 기반 분산 구성 비교
- 장치 연결, telemetry, alert, 상태 이벤트 처리

### Cloud 내부 Streaming: Kafka

Kafka는 Cloud 내부 Event Streaming과 Consumer 독립 확장에 사용합니다.

```text
gapguard.telemetry
gapguard.alert
gapguard.device-status
```

초기에는 비용을 고려해 Managed Kafka보다 Docker 기반 Kafka 운영을 우선 검토합니다. Kafka를 IoT 양방향 통신 프로토콜로 사용하지 않으며, 장치 통신은 MQTT 계층에 두는 방향이 유력합니다.

## 원격 제어와 Digital Twin 범위

1.0의 Device Twin과 Direct Method를 그대로 복제하지 않습니다. 필요한 제어만 별도 Control API 또는 MQTT command channel로 제공하는 방향을 검토합니다.

```text
Control API / 최소 운영 도구
          ↓
Command Topic / Queue
          ↓
Edge Device
          ↓
Command Result
```

후보 명령은 `calibrate_zero`, `capture_raw_snapshot`, `self_test`, telemetry 주기 변경, threshold 변경입니다. 복잡한 Digital Twin과 관리용 웹 대시보드는 초기 검증 범위 밖입니다.

## GCP 인프라와 비용

GAPGUARD 2.0은 별도 GCP 프로젝트로 관리하여 다른 프로젝트와 인프라·비용을 분리합니다.

Terraform으로 다음 리소스를 코드화할 예정입니다.

- VPC, Subnet, Firewall
- Compute Engine VM
- Load Balancer 또는 오픈소스 Proxy
- Managed Instance Group 또는 VM 다중화
- MQTT Broker / Kafka 실행 환경
- Cloud Monitoring 및 로그 수집

평상시에는 작은 VM 중심으로 운영하고, HA·부하 테스트 기간에만 인스턴스를 추가하여 비용을 통제합니다. 모든 인프라 변경은 비용 영향과 검증 결과를 함께 기록합니다.

## 부하·장애 검증

다음 장애를 의도적으로 주입하고 복구 과정을 측정합니다.

- Kafka Broker 또는 Node 장애
- Backend / Consumer 노드 장애
- MQTT Broker 장애
- 네트워크 단절과 패킷 손실
- Consumer 처리 속도 저하
- 복구 후 backlog drain

| 지표 | 의미 |
| --- | --- |
| `msg/s` | 초당 처리 메시지 수 |
| `p95` / `p99 latency` | 메시지 처리 지연 |
| Consumer Lag | Kafka 처리 적체 |
| Failover Time | 장애 후 정상화까지의 시간 |
| Backlog Drain Time | 복구 후 적체 해소 시간 |
| Message Loss | 유실 메시지 수 |
| Duplicate | 중복 처리 수 |

최종 수치는 구현 후 실험 결과로 채웁니다. 예시 부하 수치나 복구 시간은 목표가 아닌 TBD입니다.

## IaC와 CI/CD

```text
Git
 ↓
CI: lint / test
 ↓
Build: image / artifact
 ↓
Deploy: VM / service
 ↓
Health Check
 ↓
Traffic and Metrics
```

Terraform으로 개발 환경을 재생성할 수 있게 하고, CI/CD에서 테스트·이미지 빌드·배포·health check를 자동화합니다. Deployment Lead Time, MTTR, Change Failure Rate는 실제 배포와 장애 실험 결과를 바탕으로 기록합니다.

## GAPGUARD 1.0 → 2.0

| 영역 | 1.0 | 2.0 방향 |
| --- | --- | --- |
| Cloud | Azure | GCP |
| IoT Messaging | IoT Hub | MQTT Broker |
| Streaming | Azure 중심 | Kafka |
| 장치 제어 | Device Twin / Direct Method | 경량 Command Channel |
| Simulator | 센서 시뮬레이션 | Load / Fault Generator |
| Infra | 수동·관리형 서비스 중심 | Terraform |
| 테스트 | Edge 장애 복구 중심 | 시스템 전체 부하·장애 테스트 |
| 운영 평가 | 기능 정상 여부 | Latency / Lag / Failover / Loss |
| 목적 | Edge–Cloud PoC | Production-like IoT Platform |

## 범위 밖

- ADX 및 ADT 도입
- 복잡한 웹 프론트엔드와 관리 대시보드
- 현장·구역·디바이스 규모의 사전 확정
- Managed Kafka의 초기 도입
- 운영 환경으로의 무승인 배포

## 구현 순서

1. GAPGUARD 1.0 코드와 실행 구조 분석
2. 최소 메시지 계약과 Edge 데이터 흐름 정의
3. MQTT Broker·Kafka·Consumer 최소 구성
4. VM·네트워크·Load Balancer Terraform 구성
5. 다수 현장·구역·디바이스 부하 생성
6. 장애 주입·복구·관측 지표 수집
7. CI/CD 배포와 비용·성능 결과 정리

## 핵심 포지셔닝

GAPGUARD 2.0의 중심은 건설 안전 서비스의 화면 기능 확장이 아닙니다. Raspberry Pi 기반 현장 계측 시스템을 GCP·MQTT·Kafka 기반의 확장 가능한 IoT Streaming Platform으로 재구성하고, 실제 부하와 장애 주입으로 데이터 신뢰성과 서비스 복구 능력을 검증하는 프로젝트입니다.

```text
Edge Reliability
        ↓
Messaging
        ↓
Streaming
        ↓
Service Scaling
        ↓
Fault Tolerance
        ↓
Observability
        ↓
Infrastructure Automation
```

## 저장소 원칙

- 비밀키, 서비스 계정 키, `.env`, Terraform state 파일은 커밋하지 않습니다.
- 인프라 변경은 Terraform 코드와 검증 결과를 함께 관리합니다.
- 규모와 성능 수치는 추정값과 실험값을 구분합니다.
- 구현 전제와 미확정 항목은 후속 Issue로 추적합니다.

## 상태

초기 문서화 단계입니다. 구현 및 배포 환경은 후속 Issue에서 정의합니다.
GAPGUARD 2.0: GCP와 오픈소스 기반의 확장 가능한 현장 데이터 수집·부하분산 플랫폼
