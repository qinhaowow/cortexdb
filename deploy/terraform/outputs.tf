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

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet IDs"
  value       = module.vpc.private_subnets
}

output "public_subnet_ids" {
  description = "Public subnet IDs"
  value       = module.vpc.public_subnets
}

output "k8s_context" {
  description = "Kubernetes context"
  value       = "${var.project}-${var.environment}"
}
