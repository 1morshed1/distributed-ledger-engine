# RDS Postgres (ledger DB), ElastiCache Redis (cache + Asynq), MSK (Kafka).
# All placed in private subnets, reachable only from within the VPC.

# --- security group shared by the data stores (ingress from the VPC) ---
resource "aws_security_group" "data_stores" {
  name_prefix = "ledger-data-${var.environment}-"
  vpc_id      = module.vpc.vpc_id

  ingress {
    description = "Postgres/Redis/Kafka from within the VPC"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = [var.vpc_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- RDS Postgres ---
resource "aws_db_subnet_group" "ledger" {
  name       = "ledger-${var.environment}"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_db_instance" "ledger" {
  identifier             = "ledger-${var.environment}"
  engine                 = "postgres"
  engine_version         = "16"
  instance_class         = var.db_instance_class
  allocated_storage      = 20
  storage_encrypted      = true
  db_name                = "ledger"
  username               = "ledger"
  password               = var.db_password
  db_subnet_group_name   = aws_db_subnet_group.ledger.name
  vpc_security_group_ids = [aws_security_group.data_stores.id]
  skip_final_snapshot    = var.environment == "dev"
  multi_az               = var.environment == "prod"
}

# --- ElastiCache Redis ---
resource "aws_elasticache_subnet_group" "ledger" {
  name       = "ledger-${var.environment}"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_cluster" "ledger" {
  cluster_id           = "ledger-${var.environment}"
  engine               = "redis"
  node_type            = var.redis_node_type
  num_cache_nodes      = 1
  subnet_group_name    = aws_elasticache_subnet_group.ledger.name
  security_group_ids   = [aws_security_group.data_stores.id]
}

# --- MSK (managed Kafka) ---
resource "aws_msk_cluster" "ledger" {
  cluster_name           = "ledger-${var.environment}"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.data_stores.id]
    storage_info {
      ebs_storage_info { volume_size = 20 }
    }
  }
}
