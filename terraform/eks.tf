# EKS cluster with a managed node group on SPOT instances (60-70% cheaper).
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = "ledger-${var.environment}"
  cluster_version = var.cluster_version

  cluster_endpoint_public_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    default = {
      min_size     = 2
      max_size     = 6
      desired_size = 2

      instance_types = ["t3.large"]
      capacity_type  = "SPOT" # cheap; workloads must tolerate interruption
    }
  }

  # Let the cluster creator administer it.
  enable_cluster_creator_admin_permissions = true
}
