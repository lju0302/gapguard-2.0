# GAPGUARD GCP Cloud SQL

Google provider 기반의 PostgreSQL 16 Cloud SQL 모듈입니다.

구성:

- GAPGUARD 전용 custom-mode VPC와 subnet
- Cloud SQL private services access용 내부 IP range와 VPC peering
- ZONAL 인스턴스
- `PD_SSD` 디스크
- 자동 백업 및 point-in-time recovery 활성화
- 공개 IPv4 비활성화, VPC private IP 사용
- PostgreSQL 데이터베이스와 사용자 생성

## 사전 조건

- 대상 프로젝트에서 Cloud SQL Admin API가 활성화되어 있어야 합니다.
- Terraform 인증과 필요한 IAM 권한은 실행 환경에서 별도로 준비합니다.

## 사용

```sh
cp terraform.tfvars.example terraform.tfvars
terraform init
terraform fmt
terraform validate
terraform plan -var-file=terraform.tfvars
```

`db_password`는 저장소에 커밋하지 않는 비밀값으로 입력하세요. 이 준비 작업에서는 인증, API 활성화, `terraform apply`를 수행하지 않습니다.

출력값은 `connection_name`, `private_ip`, `database_name`, `network_name`, `subnet_name`입니다.
