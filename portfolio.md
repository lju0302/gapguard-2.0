# GAPGUARD 2.0: 장애 복원형 Edge–Cloud IoT 안전 모니터링 플랫폼

- 기간 : 2025.09 ~ 2026.02
- 주요 기술 : Raspberry Pi, Python, MQTT/EMQX, Kafka, SQLite, GCP Compute Engine, Terraform, Docker, CI/CD
- 핵심 성과
  - 8개 현장·40개 구역·120개 디바이스 규모를 VM Simulator로 재현
  - 최대 12,000 msg/s 부하와 네트워크 단절·Broker 장애 검증
  - SQLite Outbox와 FIFO 재전송으로 메시지 유실률 0% 달성
  - p95 처리 지연 420ms, 장애 전환 18초, backlog 복구 2분 40초 기록
  - Terraform·CI/CD로 신규 실행 환경을 10분 이내 재현

---

## 프로젝트 배경

> 굴착·흙막이 현장의 기울기와 진동을 실시간 감시하고, 네트워크 장애 상황에서도 센서 데이터와 위험 판정을 보존하는 Edge–Cloud 안전 플랫폼이다. 기존 Azure 중심 구조를 GCP·MQTT·Kafka 기반으로 재구성해 확장성, 장애 복구력, 운영 자동화를 검증했다.

## 프로젝트 과정

> 센서 데이터는 Edge Collector에서 수집한 뒤 Spooler가 위험도를 판정하고 `seqNo`, `measuredAt`, `dataHash`를 부여했다. SQLite Outbox에 먼저 저장한 후 MQTT Broker로 전송하고, Cloud에서는 Kafka의 `telemetry`, `alert`, `device-status` 토픽으로 분리해 Consumer를 독립 확장했다.

```text
Sensor → Collector → Spooler → SQLite Outbox → MQTT/EMQX
                                           ↓
                              Kafka → Telemetry / Alert / Storage Consumer
```

8개 현장·40개 구역·120개 디바이스를 VM Simulator로 생성하고, 1~100 msg/s 전송률·패킷 손실·10분 네트워크 단절·Broker 노드 장애를 반복 시험했다. GCP VPC, VM, 방화벽, 로드밸런서, 모니터링은 Terraform으로 관리하고 CI/CD에서 테스트·빌드·배포·Health Check를 자동화했다.

## 결과

### 주요 성과

- 네트워크가 10분간 끊겨도 Edge 수집과 위험 판정을 지속하고, 복구 후 backlog를 FIFO 순서로 100% 재전송했다.
- 최대 12,000 msg/s 처리 부하에서 p95 지연 420ms, 메시지 유실 0건을 확인했다.
- MQTT Broker·Kafka Consumer 장애 시 18초 이내 서비스 전환, 2분 40초 이내 backlog 정상화에 성공했다.
- 월 GCP 운영비를 약 `$186`으로 관리해 `$300` 예산 상한 이내로 운영했다.

### 기대 효과(있으면)

- 센서·메시징·스트리밍 계층을 분리해 현장과 디바이스가 늘어도 Consumer를 독립적으로 확장할 수 있다.
- 장애·지연·lag·failover를 수치로 관리해 안전 관제 시스템의 운영 신뢰도를 높인다.

## 배운 점

### 데이터 신뢰성 관점

- IoT 시스템의 신뢰성은 연결 상태보다 로컬 저장, 순서 번호, 재전송 정책을 함께 설계할 때 확보된다.
- `seqNo`, `dataHash`, FIFO 재전송을 적용하면 유실·중복·순서 오류를 추적할 수 있다.

### 성능 검증 관점

- 평균 처리량만으로는 운영 가능성을 판단할 수 없으며, p95 지연·Consumer Lag·Failover Time·Backlog Drain Time을 함께 측정해야 한다.
- 정상 상황과 장애 주입 상황을 분리해 측정해야 병목과 복구 성능을 정확히 비교할 수 있다.

### 운영 자동화 관점

- Terraform으로 인프라를 코드화하면 동일한 환경을 반복 재현하고 변경 이력을 관리할 수 있다.
- CI/CD에 테스트·배포·Health Check를 연결하면 배포 품질과 장애 대응 속도를 함께 높일 수 있다.
