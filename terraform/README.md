# Terraform — AWS Infrastructure (write-only)

Declarative AWS stack for the ledger: **VPC · EKS · RDS Postgres · ElastiCache
Redis · MSK Kafka · S3**. This is written but **not applied** — the project runs
locally on docker-compose. Apply it only when you want a real cloud deployment
(it costs money).

## Files

| File              | What it provisions                                    |
|-------------------|-------------------------------------------------------|
| `versions.tf`     | Terraform + AWS provider constraints, default tags    |
| `backend.tf`      | S3 remote state + DynamoDB lock (bootstrap once)      |
| `variables.tf`    | Inputs (region, sizes, db_password)                   |
| `network.tf`      | VPC, public/private subnets (3 AZs), NAT              |
| `eks.tf`          | EKS cluster + SPOT managed node group                 |
| `data-stores.tf`  | RDS Postgres, ElastiCache Redis, MSK Kafka + SG       |
| `s3.tf`           | Reports bucket (versioned, 90-day Glacier lifecycle)  |
| `outputs.tf`      | Endpoints for wiring the app / kubectl                |
| `dev.tfvars`      | Dev sizing (free-tier where possible)                 |

## Cost controls (per the plan, keep dev < \$50/mo)

- SPOT EKS nodes (60-70% cheaper)
- `db.t3.micro` RDS (free-tier eligible), `cache.t3.micro` Redis
- single NAT gateway in dev
- `terraform destroy` when idle

## Usage (when you actually deploy)

```bash
export TF_VAR_db_password='...'        # never commit
terraform init -backend-config=dev.s3.tfbackend
terraform plan  -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
aws eks update-kubeconfig --name ledger-dev   # then: kubectl apply -k ../k8s
terraform destroy -var-file=dev.tfvars         # tear down to stop billing
```

> Not installed / not applied in this environment. `terraform validate` requires
> `terraform init` to download the AWS provider + community modules first.
