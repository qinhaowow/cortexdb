terraform {
  required_version = ">= 1.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
       }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }

  backend "s3" {
    bucket         = "coretexdb-terraform-state"
    key            = "coretexdb/main.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "coretexdb-terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "coretexdb"
      Environment = var.environment
      ManagedBy  = "terraform"
    }
  }
}

provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  token                 = data.aws_eks_cluster_auth.cluster.token
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    token                 = data.aws_eks_cluster_auth.cluster.token
  }
}

data "aws_eks_cluster_auth" "cluster" {
  name = module.eks.cluster_name
}

data "aws_availability_zones" "available" {}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project}-vpc-${var.environment}"
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, var.number_of_azs)
  private_subnets = var.private_subnets
  public_subnets  = var.public_subnets

  enable_nat_gateway     = true
  single_nat_gateway     = var.environment != "prod"
  enable_dns_hostnames  = true
  enable_dns_support    = true

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = "${var.project}-${var.environment}"
  cluster_version = var.k8s_version

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_subnets
  control_plane_subnet_ids = module.vpc.private_subnets

  eks_managed_node_group_defaults = {
    ami_type       = "AL2_x86_64"
    instance_types = ["m5.large"]

    ebs_volume_size = 100
    ebs_volume_type = "gp3"

    tags = {
      Project     = var.project
      Environment = var.environment
    }
  }

  node_groups = var.node_groups

  enable_irsa = true

  cluster_addons = {
    vpc-cni = {
      most_recent = true
    }
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.irsa_ebs_csi.iam_role_arn
    }
  }

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

module "irsa_ebs_csi" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name             = "${var.project}-ebs-csi-${var.environment}"
  aws_partition         = "aws"
  eks_cluster_arns       = [module.eks.cluster_arn]
  iam_policy_statements = {
    ebs = {
      actions = [
        "ec2:AttachVolume",
        "ec2:CreateSnapshot",
        "ec2:CreateTags",
        "ec2:CreateVolume",
        "ec2:DeleteSnapshot",
        "ec2:DeleteTags",
        "ec2:DeleteVolume",
        "ec2:DescribeInstances",
        "ec2:DescribeSnapshots",
        "ec2:DescribeVolumes",
        "ec2:DescribeTags",
        "ec2:DetachVolume",
        "ec2:GetDiskUsage",
        "ec2:ListTagsForResources",
        "ec2:ModifyVolume",
      ]
      resources = ["*"]
      conditions = [{
        test     = "ArnEquals"
        variable = "aws:ResourceTag/kubernetes.io/cluster/${module.eks.cluster_name}"
        values   = ["owned"]
      }]
    }
  }
}

module "efs" {
  source  = "terraform-aws-modules/efs/aws"
  version = "~> 1.0"

  creation_token = "${var.project}-efs-${var.environment}"

  performance_mode = "generalPurpose"
  throughput_mode  = "bursting"
  provisioned_throughput_in_mibps = 100

  lifecycle_policy_transition_to_ia = {
    transition_to_ia = "AFTER_30_DAYS"
  }

  subnet_ids = module.vpc.private_subnets

  security_group_rules = {
    vpc = {
      cidr_blocks = [module.vpc.vpc_cidr_block]
    }
  }

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

module "efs_access_point" {
  source  = "terraform-aws-modules/efs/aws//modules/access-points"
  version = "~> 1.0"

  file_system_id = module.efs.id

  access_points = {
    coretexdb = {
      name = "coretexdb"
      posix_user = {
        gid            = 1000
        uid            = 1000
      }
      root_directory = {
        path = "/coretexdb"
        creation_info = {
          owner_gid   = 1000
          owner_uid   = 1000
          permissions = "755"
        }
      }
    }
  }
}

module "security_groups" {
  source  = "terraform-aws-modules/security-group/aws"
  version = "~> 5.0"

  name        = "${var.project}-sg-${var.environment}"
  description = "Security group for CoretexDB"
  vpc_id      = module.vpc.vpc_id

  ingress_rules = [
    { rule = "http-80-tcp" },
    { rule = "https-443-tcp" },
    { rule = "grpc-tcp" },
    { rule = "prometheus-tcp" },
  ]

  ingress_cidr_blocks = var.allowed_cidr_blocks

  egress_rules = [
    { rule = "all-all" },
  ]

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

module "elasticache" {
  source  = "terraform-aws-modules/elasticache/aws"
  version = "~> 1.0"

  cluster_id           = "${var.project}-cache-${var.environment}"
  engine               = "redis"
  node_type            = var.redis_node_type
  num_cache_nodes      = var.redis_num_nodes
  parameter_group_name = "default.redis7.0"
  engine_version       = "7.0"
  family               = "redis7.0"

  subnet_ids         = module.vpc.private_subnets
  security_group_ids = [module.security_groups.security_group_id]

  auto_minor_version_upgrade = true
  maintenance_window          = "sun:02:00-sun:04:00"

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  auth_token                 = random_password.redis_auth_token.result

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "random_password" "redis_auth_token" {
  length  = 32
  special = false
}

resource "random_password" "admin_password" {
  length  = 32
  special = false
}

resource "random_password" "encryption_key" {
  length = 32
}

resource "aws_secretsmanager_secret" "coretexdb" {
  name = "coretexdb/${var.environment}"
  description = "CoretexDB secrets for ${var.environment} environment"

  tags = {
    Project     = var.project
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "coretexdb" {
  secret_id = aws_secretsmanager_secret.coretexdb.id
  secret_string = jsonencode({
    admin_password  = random_password.admin_password.result
    encryption_key  = random_password.encryption_key.result
    redis_auth_token = random_password.redis_auth_token.result
  })
}

resource "aws_s3_bucket" "backup" {
  bucket = "${var.project}-backup-${var.environment}-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "backup" {
  bucket = aws_s3_bucket.backup.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "backup" {
  bucket = aws_s3_bucket.backup.id
  rule {
    id     = "archive-old-backups"
    status = "Enabled"
    filter {
      prefix = "daily/"
    }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}

data "aws_caller_identity" "current" {}

output "cluster_endpoint" {
  description = "EKS cluster endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "efs_id" {
  description = "EFS file system ID"
  value       = module.efs.id
}

output "efs_access_point_id" {
  description = "EFS access point ID"
  value       = module.efs_access_point.coretexdb.id
}

output "redis_endpoint" {
  description = "ElastiCache Redis endpoint"
  value       = module.elasticache.primary_endpoint_address
}

output "security_group_id" {
  description = "Security group ID"
  value       = module.security_groups.security_group_id
}

output "backup_bucket_name" {
  description = "Backup S3 bucket name"
  value       = aws_s3_bucket.backup.id
}
