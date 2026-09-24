# S3 buckets for reconciliation reports and archived transaction data.
resource "aws_s3_bucket" "reports" {
  bucket = "ledger-${var.environment}-reports"
}

resource "aws_s3_bucket_versioning" "reports" {
  bucket = aws_s3_bucket.reports.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "reports" {
  bucket = aws_s3_bucket.reports.id
  rule {
    id     = "archive-old-reports"
    status = "Enabled"
    filter {}
    transition {
      days          = 90
      storage_class = "GLACIER" # cheap cold storage after 90 days
    }
  }
}

resource "aws_s3_bucket_public_access_block" "reports" {
  bucket                  = aws_s3_bucket.reports.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
