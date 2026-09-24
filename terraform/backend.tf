# Remote state in S3 with DynamoDB locking. The bucket/table must exist before
# `terraform init` (bootstrap them once, out of band). Values are supplied via
# `-backend-config` so they aren't hardcoded per environment.
terraform {
  backend "s3" {
    key     = "distributed-ledger-engine/terraform.tfstate"
    encrypt = true
    # bucket         = "..."   # via -backend-config=dev.s3.tfbackend
    # region         = "..."
    # dynamodb_table = "..."
  }
}
