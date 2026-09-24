output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API endpoint (configure kubectl: aws eks update-kubeconfig)"
  value       = module.eks.cluster_endpoint
}

output "db_endpoint" {
  value = aws_db_instance.ledger.endpoint
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.ledger.cache_nodes[0].address
}

output "kafka_bootstrap_brokers" {
  value = aws_msk_cluster.ledger.bootstrap_brokers
}

output "reports_bucket" {
  value = aws_s3_bucket.reports.bucket
}
