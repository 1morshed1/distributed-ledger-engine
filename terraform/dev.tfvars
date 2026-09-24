region            = "us-east-1"
environment       = "dev"
cluster_version   = "1.30"
db_instance_class = "db.t3.micro"   # free-tier eligible
redis_node_type   = "cache.t3.micro"
# db_password is provided via TF_VAR_db_password, never committed.
